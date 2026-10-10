# InfiniDisk2 — optimisations Astra

Cette page décrit la campagne historique du binaire `b39b705f43b6`. Pour les défauts désormais générés, le lecteur adaptatif et la correction de l’erreur d’index sur écriture partielle, consulter [la nouvelle qualification](adaptive-reads.md). Les preuves du rapport Astra restent inchangées.

Autorisation de Joseph : réaliser et mesurer les six pistes proposées le 10 octobre 2026. Branche `astra-optimizations`, référence `29b45441c0050fa3d3a40bb21c57e0ab25788cb1`. Aucun service de production ne participe aux essais.

## Réalisation

1. Cache asynchrone borné, fusion des écritures et vérification des versions.
2. Segments préparés, checkpoints hors verrou global et références capturées par génération.
3. Lectures locales par requête, cache partitionné, buffers et transport ublk.
4. Réutilisation/alignement du WAL et synchronisation des segments modifiés uniquement.
5. Index paginé et publication distante des versions utiles.
6. Mode expérimental par générations : retour au préfixe S3 publié, durabilité transactionnelle volontairement différée et limitation du retard. La cohérence applicative doit être vérifiée dans les scénarios de reprise.

Les gains annoncés dans les propositions sont des objectifs, pas des résultats. Chaque variante conserve ses preuves de mesure et ses contrôles de reprise. Les nouveaux formats refusent les configurations incompatibles. Les modes durables et différés sont mesurés dans des campagnes distinctes.

### 1. Cache logique asynchrone

`async_cache=true` déplace le remplissage du cache SSD dans un thread dédié. L'acquittement reste après l'écriture du WAL et l'installation de sa référence ; le cache n'est jamais l'autorité. La file limite ensemble les données en attente et en cours à `cache_queue_mib` (16 Mio par défaut, 1–128 autorisés), avec au plus 256 messages. Un lot regroupe jusqu'à 64 messages et fusionne les versions d'une même page. Une file pleine abandonne un remplissage, jamais une écriture de volume. Les compteurs exposent saturation et erreurs.

Une réponse au client peut précéder l'installation dans le cache ; la lecture suit alors le WAL. Toute entrée est comparée au triplet segment, offset, CRC attendu. Une écriture concurrente dépassant un ancien remplissage peut provoquer un miss, pas la lecture d'une ancienne version. L'arrêt joint le worker avant de relâcher le verrou du volume.

### 2. Checkpoint et segments préparés

Le checkpoint capture les références de shards et les descripteurs des segments sous `State`, puis synchronise, vérifie et publie hors de ce verrou. Un seul checkpoint travaille à la fois. Les shards résidents utilisent un instantané partagé immuable, les shards évincés un lien vers leur fichier immuable. Au plus quatre objets/shards sont traités simultanément ; les payloads de tous les shards ne sont pas accumulés en RAM.

`checkpoint_pipeline=true` prépare les segments fixes dans un thread dédié et recycle les anciens inodes publiés. Il exige `wal_fixed_size=true`. Le pool compte trois unités maximum, y compris un fichier en cours de préparation. Le budget des WAL en attente doit contenir ces trois unités et deux segments actifs. La rotation garde un chemin synchrone vérifié si aucun fichier n'est prêt.

Un segment ne peut être recyclé qu'après publication et après disparition de ses lecteurs. Si des lecteurs le détiennent encore, son nom est supprimé et leurs descripteurs restent valides jusqu'à la fin des lectures ; cet inode n'est pas recyclé. Le recyclage commence par sortir le fichier du répertoire des WAL récupérables, synchroniser les répertoires, effacer son ancien contenu, puis lui donner une nouvelle identité. Aucun ancien enregistrement ne doit pouvoir réapparaître après une panne.

Le minimum de budget utilise la capacité physique initialisée, qui comprend la marge de requête de 8 Mio et les en-têtes : deux unités sans pool, cinq avec pool. Un budget correspondant seulement à deux fois `segment_mib` est donc insuffisant pour certains profils fixes et est refusé au chargement.

### 3. Lectures et ublk

`fast_local_reads=true` capture une requête entière puis fait ses lectures locales dans un seul travail bloquant. Les pages complètes arrivent dans le buffer du demandeur ; seules les bordures partielles utilisent une page temporaire. Le cache est réparti sur 16 verrous. Un changement du partitionnement invalide uniquement ce cache jetable.

`ublk_fast_path=true` ajoute un worker Tokio persistant par tag, transfère la propriété du buffer de transport et regroupe les notifications de complétion par file. Les réponses ne sont pas retardées volontairement. La limite globale de requêtes reste active. Il s'agit de copies évitées dans notre code ; aucun zero-copy noyau n'est revendiqué. Détails et limites dans [astra-ublk.md](astra-ublk.md).

La commande hors ligne `warm --concurrency 32` regroupe les pages par segment et étendue physique, puis traite jusqu'à 32 groupes simultanément. La limite accepte 1 à 128 groupes ; elle porte sur des téléchargements distincts et leurs remplissages, au lieu de compter des pages voisines partageant un seul GET. Un groupe conserve son buffer jusqu'à la vérification de toutes ses pages manquantes et leur écriture dans le cache SSD, même avec un cache RAM nul. Les consultations et remplissages SSD sont groupés pour réduire les tâches bloquantes.

La commande attend les remplissages, propage leurs erreurs et draine les groupes engagés avant de libérer le verrou du volume. Elle refuse aussi un jeu de pages trop concentré pour tenir dans une seule partition du cache. Les logs exposent progression, GET, octets et pic d'appels de plage simultanés. Ce chemin sert au préchauffage explicite et ne change pas l'ordre des I/O applicatives. Les durées de préparation restent séparées du débit en régime chaud ; un gain de préchauffage ne devient pas un gain de TPS. Détails et limites mémoire dans [astra-warm-parallel.md](astra-warm-parallel.md).

### 4. WAL aligné et synchronisations

`selective_sync=true` ne resynchronise pas les segments immuables déjà couverts par la frontière durable. Le segment actif reste synchronisé. Les marqueurs de commit et leur validation de préfixe restent les preuves de durabilité locale.

`aligned_wal=true` crée le format `IDWAL002` : en-tête de fichier et en-têtes d'enregistrement de 4 Kio, payloads alignés. Le padding est validé et les données restent checksummées. Ce n'est ni `O_DIRECT`, ni une promesse de gain : les petites écritures et commits consomment davantage d'octets. Les anciens binaires refusent ces WAL. La comparaison avec l'ancien binaire utilise donc des fixtures sans ce format ; les essais alignés ont leur volume séparé.

### 5. Index paginé et pages finales sur S3

`paged_index=true` conserve un répertoire de shards et une quantité bornée de cartes de pages résidentes. Les shards jetables portent identité de session, nombre d'entrées et checksum. Ils sont reconstruits depuis HEAD et les WAL à chaque ouverture. Une erreur de lecture de l'index ne signifie jamais « page vide » : l'opération échoue. Les chemins principaux de lecture et de mutation marquent aussi le moteur en échec fermé.

La revue relève une exception à harmoniser : la lecture de la page de bordure avant une écriture partielle propage actuellement une erreur d'index sans marquer cet état global (`Engine::write`, appel `s.reference(p)?`). Elle échoue avant l'ajout au WAL ; aucun succès incorrect ni dommage supplémentaire n'est démontré dans ce chemin. Cette différence de traitement reste identifiée dans le binaire gelé, sans présenter les tests passés comme une preuve que tous les chemins d'erreur sont identiques.

La limite `max_index_mib` concerne les cartes résidentes, pas tout le RSS. S'y ajoutent le répertoire des shards, une génération d'instantanés, les buffers de requêtes et d'upload. Les misses SSD sont encore synchrones sous le verrou d'état ; un accès dispersé avec une limite très faible peut donc régresser. Les anciens répertoires scratch laissés par un SIGKILL ne sont pas réutilisés ; leur collecte automatique après crash reste à compléter. Détails dans [astra-index.md](astra-index.md).

`compact_checkpoints=true` vérifie d'abord chaque WAL scellé, extrait uniquement la dernière version des pages du snapshot, puis produit des segments distants regroupés par shard. Les objets sont envoyés avant l'index et HEAD est remplacé conditionnellement en dernier. Les références courantes ne sont remplacées que si elles pointent encore vers la version capturée. Une écriture plus récente est conservée. En cas d'échec de publication, les WAL et l'ensemble dirty restent disponibles pour réessayer.

La compaction réduit surtout les octets envoyés lorsque les mêmes pages sont réécrites. Elle change leur identité distante : le cache logique est réindexé sans réécrire le payload, mais une page absente de ce cache peut devoir être relue sur S3 malgré la présence de son ancien WAL local. Les mesures doivent donc examiner lectures, uploads et latence ensemble. Le nombre de PUT peut augmenter lorsque beaucoup de shards peu remplis changent.

Un shard devenu vide est retiré du HEAD. Son objet ancien reste éligible à une collecte ultérieure. L'encodeur refuse de publier une racine dépassant le plafond de 64 Mio du décodeur, afin de ne jamais remplacer un HEAD lisible par un HEAD trop grand pour être rouvert. Ce plafond demeure une limite de capacité ; l'index local paginé ne la supprime pas.

Un TRIM de grande taille est supprimé shard par shard, y compris pendant le rejeu du WAL. La mémoire temporaire ne contient plus une référence par page supprimée ; seuls les identifiants des shards modifiés sont conservés. Les instantanés déjà capturés gardent leur ancienne version. Les shards froids sont vérifiés avant suppression, afin qu'une corruption d'index reste une erreur explicite.

### 6. Reprise par générations

`generation_mode=true` est réservé à un nouveau volume HEAD de format 2. Le mode est enregistré dans le format et ne peut pas être changé par une option au redémarrage. Une ouverture normale de ce volume sans l'option, ou une ouverture en génération d'un volume de format 1, échoue.

Dans ce mode, FLUSH/FUA ordonnent les écritures sans fournir la durabilité locale par transaction. Le checkpoint persiste et publie un préfixe complet. Après arrêt, tous les WAL locaux non publiés sont abandonnés et la reprise part du dernier HEAD complet, même si des données plus récentes semblent encore lisibles. L'application doit être arrêtée, le périphérique détaché et le système de fichiers remonté avant la reprise. Le mode vise un retour cohérent à un état antérieur ; il n'autorise pas le remplacement silencieux du disque sous une base encore vivante.

Le retard de publication est exposé. À `generation_max_lag_seconds` (30 s par défaut), les nouvelles écritures attendent la publication, puis échouent si l'attente de 50 s expire. L'intervalle nominal de checkpoint est 5 s ; ce n'est pas une garantie de réplication en 5 s pendant une panne S3. Le mode reste expérimental et distinct des tableaux de durabilité locale. Détails et campagne applicative dans [generation-mode.md](generation-mode.md).

Un TRIM sur une région déjà vide avance aussi la séquence. Le checkpoint publie cette opération même sans shard modifié, afin de ne pas laisser artificiellement croître le retard et bloquer les futures écritures. Le watermark local du format 2 est recréé après validation du HEAD, même s'il manque ou a été endommagé.

L'arrêt propre arrête d'abord le service, puis publie un checkpoint final, qui inclut sa propre synchronisation durable. Il ne passe pas auparavant par la barrière d'admission FLUSH du mode génération : si le retard est dépassé et le publieur périodique déjà arrêté, cette barrière attendrait un checkpoint que le même arrêt n'a pas encore lancé. Un test du véritable CLI reproduit une publication indisponible, dépasse le retard, restaure le stockage puis envoie SIGTERM ; HEAD et les données doivent être récupérables au redémarrage.

## Invariants communs et limites

- Une page lue doit correspondre à sa référence et à son checksum ; un cache corrompu est contourné.
- Une corruption détectée dans la seule copie WAL autoritaire provoque un arrêt des écritures. Un WAL déjà copié sur S3 peut être relu depuis cette copie vérifiée.
- Les objets distants sont immuables ; l'index complet de la génération précède son HEAD conditionnel. Un conflit de propriétaire provoque un refus de continuer.
- La VM et le fournisseur de disque doivent réellement honorer la synchronisation. Les SIGKILL et fautes injectées ne prouvent pas le comportement sous coupure électrique ou panne interne du fournisseur.
- Le cache SSD bénéficie aussi du page cache Linux, hors RSS moteur. Les résultats « cache actif » ne décrivent pas un volume dont tout le jeu de travail est froid sur S3.

Toutes les nouvelles options restent désactivées par défaut. La recommandation d'un profil dépend des mesures de cette campagne ; l'alignement, la pagination et la compaction ne sont pas activés simplement parce qu'ils sont implémentés.

## Références de protocole

La documentation [Linux FLUSH/FUA](https://docs.kernel.org/block/writeback_cache_control.html) précise le rôle des barrières et des acquittements persistants. Le mode génération change volontairement ce contrat et doit donc être annoncé séparément. La documentation [ublk](https://kernel.org/doc/html/latest/block/ublk.html) décrit les transports et les mécanismes zero-copy disponibles ; notre chemin ne les active pas. La [documentation S3 des écritures conditionnelles](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) décrit les préconditions de publication ; le backend compatible S3 utilisé ici doit être vérifié séparément, il n'hérite pas automatiquement des garanties d'AWS.

La documentation [cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html#cpu-interface-files) définit le quota `cpu.max` et les compteurs `cpu.stat`. L'utilisation CPU couvre les processus descendants ; les compteurs de limitation indiquent celle imposée par ce cgroup, sans inclure toutes les limites ancestrales. La proportion de périodes limitées n'est pas un percentile de latence des I/O. Le protocole garde donc les comparaisons à un et deux CPU séparées.

## Validation et qualification

Le binaire gelé `b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151` passe 92 tests Rust avec la feature `ublk`, ainsi que le test io_uring explicitement activé : 49 152 complétions réparties sur trois threads système, puis aucun poll restant lors de l’arrêt du runtime. `fmt`, Clippy avec refus des avertissements et le build release passent. Le [manifeste de compilation](../validation/astra/builds/final-v3/manifest.json) conserve les commandes, les journaux et les empreintes des sources.

Ces tests couvrent les versions concurrentes, caches saturés/corrompus, la reprise du WAL, les frontières FLUSH/FUA, la publication conditionnelle et la restauration distante. Le [préchauffage parallèle mesuré](astra-warm-measurement.md) complète les tests avec un vrai backend S3.

Les deux [qualifications durables core/aligned](astra-recovery-qualification.md) passent leurs six contrôles de reprise sur ce binaire, notamment le SIGKILL du moteur pendant PostgreSQL et la restauration depuis S3 seul. Le [comptage des opérations](astra-s3-operations.md) distingue frais de requêtes, octets, reprise chaude/froide et confirmation de performance sans proxy ; la réduction d'octets de la compaction ne garantit pas moins de PUT.

Les campagnes MySQL, PostgreSQL et fio utilisent des volumes de test ; références natives et ZeroFS avec contrats explicités. Le [protocole PostgreSQL](astra-postgres-comparison.md) rejoue également le binaire avant Astra, avec ses optimisations antérieures et les mêmes budgets, pour mesurer le gain sans reprendre une ancienne mesure non appariée. Le [rapport courant](../validation/astra/rapport.html) indique les étapes terminées et les incidents conservés.

La qualification utilise trois passages par charge, des références avant/après pour le screening et des compteurs pris autour des fenêtres mesurées. Les campagnes sont séquentielles sur une VM partagée ; elles ne constituent pas un essai randomisé. Les pannes de processus ne remplacent pas une qualification des pannes électriques.

## Comparaison fio finale

La campagne `fio-0560e0126c74` compare le binaire gelé, ZeroFS 2.3.5 et un fichier natif O_DIRECT sur le disque de la VM. Chaque charge comporte trois passages de 15 secondes. Les moteurs utilisent un bloc brut, 64 Mio de cache RAM, 128 Mio de SSD pour les écritures et lectures froides, puis 512 Mio après préchauffage. Chaque passage froid reçoit un cache moteur neuf ; aucun cache global Linux ni fournisseur n'est purgé.

| Charge | InfiniDisk2 Astra | ZeroFS durable | Natif |
|---|---:|---:|---:|
| Écriture 4 Kio sans fsync individuel, IOPS | 21 802,64 | 471,85 | 15 097,00 |
| Écriture 4 Kio avec fsync, IOPS | 9 462,04 | 1,49 | 13 092,53 |
| Lecture 4 Kio depuis cache moteur neuf, IOPS | 2 867,93 | 2 734,71 | Sans objet S3 |
| Lecture 4 Kio préchauffée, IOPS | 54 429,14 | 16 330,02 | 15 097,87 |
| Lecture séquentielle préchauffée, Mio/s | 3 201,40 | 924,17 | 16 846,41 |

Les valeurs sont des médianes, avec minima et maxima dans les graphiques. Les passages froids ZeroFS sont très dispersés : 122,02 / 2 734,71 / 4 824,33 IOPS. Les écritures sans fsync sont aussi variables. InfiniDisk2 attend le WAL local, ZeroFS durable attend S3 ; ZeroFS conserve LZ4 et chiffrement. La variante ZeroFS avec fsync ignoré est conservée séparément dans le rapport et ne constitue pas un profil recommandé pour une base.

Le natif contourne le page cache Linux par O_DIRECT, tandis que les caches des moteurs peuvent en bénéficier. Ses débits ne décrivent donc pas un plafond matériel à ressources égales. Les petites lectures mettent surtout en évidence la valeur du cache ; le natif garde une avance en écritures synchrones et en lecture séquentielle. Les CRC32C du jeu natif et des restaurations S3 d'InfiniDisk2 et ZeroFS passent après tous les chronométrages. Les [preuves complètes](../validation/astra/fio/report.json) et leurs 164 fichiers archivés conservent les paramètres et empreintes des binaires.
