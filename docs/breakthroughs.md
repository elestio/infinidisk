# InfiniDisk2 — campagne de changements d’architecture

Campagne expérimentale terminée le 10 octobre 2026 sur 159.195.104.60, dans des volumes de test isolés. Les services et volumes de production ne participent pas aux essais. [Rapport complet](../validation/breakthroughs/rapport.html), [synthèse machine](../validation/breakthroughs/summary.json) et preuves brutes dans `validation/breakthroughs/raw/`.

## Résultats et décision

Médianes de trois passages ; les répétitions et p95 restent dans le rapport. Les multiplicateurs sont des observations de cette campagne, avec les limites de comparaison ci-dessous.

| Essai | Résultat | Décision |
|---|---|---|
| Cache par étendues 128 Mio → cache logique 128 Mio | Lecture MySQL : 31,22 → 49,02 TPS, ×1,57 | Piste utile sans augmenter la capacité SSD configurée. |
| Compaction, cache par étendues 128 Mio | 31,22 → 62,91 TPS, ×2,02 ; préparation hors ligne ≈15 min | Utile pour les lectures froides, coût et indisponibilité à réduire. Le cache logique après compaction donne 53,33 TPS : les gains ne s’additionnent pas. |
| Volume préchauffé, budget SSD 4 Gio | Lecture NBD 1 002,51 TPS ; référence ultérieure 874,61 TPS | ×28 à ×32 face aux 31,22 TPS du petit cache. La capacité est différente et le page cache Linux participe au gain. Préchauffage 386,49 s. |
| ublk, même cache complet | 1 142,68 TPS face aux 874,61 TPS NBD proches, +31 % | Garder en option : p95 médian 45,79 ms contre 33,72 ms sur NBD. |
| Marqueur de commit seul, première série | Écriture petite base 945,30 → 1 022,85 TPS, +8 % | Gain modéré ; garder les contrôles du préfixe durable. |
| Attente de regroupement 100 / 500 µs | 480,88 / 397,83 TPS | Rejetée : garder zéro. |
| WAL entièrement initialisé, seul | Petite base 920,49 → 1 264,67 TPS, +37 % | Gain réel, sans extrapoler le microbenchmark ×3,34. |
| WAL fixe + marqueurs | Petite base 1 466,45 TPS ; références avant/après 920,49 / 886,24 | +59 % à +65 %, p95 médian 9,06 ms. |
| Grande base, cache complet + marqueurs : ajout du WAL fixe | Mixte 193,04 → 227,32 TPS ; écriture 313,53 → 427,76 TPS | +18 % / +36 % observés. L’essai suit d’autres écritures : pas de gain causal garanti identique sur toute DB. |
| Disque natif, grande base et accès uniformes | Lecture 1 080,66 ; mixte 464,44 ; écriture 1 573,64 TPS | Le natif reste nettement devant en mixte/écriture et en latence de lecture. Aucun ×3–×10 universel n’est établi en écriture. |

Le natif conserve O_DIRECT ; le cache du moteur est buffered. Le débit ublk légèrement supérieur au natif en lecture ne démontre donc pas une supériorité à ressources mémoire égales. Tous les caches et les données récentes passent par la VM partagée, sans randomisation de l’ordre des essais.

Deux erreurs SQL ignorées par sysbench apparaissent dans des références : `wal-reference-after` et `fixed-reference`. Leurs codes SQL ne figurent pas dans les agrégats ; elles restent visibles et ne sont pas attribuées au stockage. Les variantes optimisées mesurées n’ont aucune erreur ignorée dans leurs échantillons. Les contrôles DB après crash ont réussi.

## Variantes

Les options suivantes restent désactivées par défaut. Les volumes de production ne sont pas migrés.

| Variante | Implémentation | Garantie et limite |
|---|---|---|
| `logical_cache=true` | Deux fichiers conteneurs, pages de 4 Kio et métadonnées de 40 octets. LRU par adresse logique ; identité du volume, version du bloc et CRC contrôlés. Les écritures remplissent le cache. | Cache jetable. Pas de `fsync` pour ce cache ; un contenu incomplet ou ancien devient un miss. Le budget SSD inclut les métadonnées sur disque, mais l’index LRU ajoute de la RAM au processus. |
| `compact` | Commande hors ligne sous verrou exclusif du volume. Lecture des blocs vivants en ordre logique, nouveaux segments immuables et nouveaux shards. Publication conditionnelle de HEAD en dernier. | Les CRC sont vérifiés avant écriture et upload. Les objets précédents restent sur S3. Ce prototype fait une compaction complète hors ligne ; il ne livre pas encore une compaction incrémentale automatique. |
| `wal_commit_records=true` | Enregistrements `CMT1` dans le WAL. Synchronisation des fichiers couvrant le préfixe avant acquittement, sans seconde synchronisation du fichier watermark. Reprise vérifiant les séquences et CRC. | Une barrière pour le segment actif, plusieurs fichiers à synchroniser si un groupe traverse des segments. Les anciennes versions du logiciel ne comprennent pas les nouveaux marqueurs. Aucune activation sur un volume existant de production. |
| `flush_batch_us` | Attente bornée avant de capturer le groupe à synchroniser. | FLUSH et FUA restent durables localement. Le timer Tokio peut dépasser le délai demandé ; il faut mesurer le débit et la latence réelle. |
| `warm` | Chargement hors ligne de toutes les pages allouées dans le cache logique. Refus si elles dépassent le budget. | Préchauffage exclu du débit mais chronométré. S3 demeure nécessaire pour reconstruire le cache après perte du disque local. |
| `ublk` | Adaptateur Rust optionnel, libublk épinglé à un commit, quatre files, 32 requêtes par file. Même moteur et checkpoints périodiques que NBD. | Prototype avec copies ; aucun zero-copy revendiqué. Cache d’écriture volatile annoncé au noyau, FLUSH exécuté et FUA traité. Les opérations non annoncées/non prises en charge renvoient une erreur. |
| `wal_fixed_size=true` | Initialisation complète des segments, capacité dans l’en-tête protégé par CRC, taille logique séparée. Upload de la partie logique uniquement. | Variante ajoutée après un microbenchmark positif ; qualification distincte. La création et la rotation paient l’initialisation et la capacité physique dépasse la taille logique ; les limites du WAL en attente et du WAL récent comptent la capacité physique, avec une réserve pour la rotation. Ancien lecteur incompatible avec le padding de ces fichiers locaux. |

L’adaptateur ublk initial attendait les résultats d’un autre runtime Tokio : son réveil ne pouvait pas interrompre le `io_uring_enter` de la file, ce qui ajoutait jusqu’au timeout de sécurité d’une seconde. La correction envoie d’abord le résultat dans un canal, puis signale un eventfd que la file attend via `IORING_OP_POLL_ADD`. Le résultat est donc prêt avant le réveil. Cette passerelle ajoute un eventfd par tag et des opérations de notification ; ce n’est pas un moteur de blocs entièrement exécuté sur les rings. La suppression est asynchrone après contrôle de propriété et prise exclusive du périphérique, pour libérer ce descripteur avant que le noyau termine la suppression.

Le premier essai ublk n’a pas produit de mesure MySQL valide : le montage a dépassé son timeout et le nettoyage a nécessité l’arrêt des processus de test après suppression du périphérique. Ses logs restent conservés. La reprise de campagne utilise un nouveau binaire et refait une référence NBD avec ce même binaire avant le nouvel essai ublk. Les manifests et les empreintes de chaque cas distinguent ces versions.

## Mesures

Même MySQL 8.0.46, quatre tables, huit threads, une CPU, 1 Gio de RAM, buffer pool 256 Mio, doublewrite ON, O_DIRECT, binlog actif, sync_binlog=1 et innodb_flush_log_at_trx_commit=1. Aucun fsync n’est ignoré.

Le petit dataset mesure les commits, avec références avant et après et trois passages de 15 secondes. Le grand dataset comporte quatre millions de lignes ; les lectures uniformes ont 60 secondes d’échauffement et trois passages de 15 secondes. Les comparaisons du cache 128 Mio et de la compaction gardent cette capacité. Le mode local actif dispose explicitement de 4 Gio : son gain ne doit pas être présenté comme un gain à ressources identiques. Les index et buffers du processus consomment de la RAM en plus du budget de cache RAM de 64 Mio. Les fichiers de cache sont lus en mode buffered : le page cache Linux peut conserver leur contenu en RAM, surtout dans le mode local actif. La référence MySQL native conserve O_DIRECT ; les gains du mode local actif mêlent donc capacité SSD et chauffe du page cache Linux. Aucun drop_caches global n’est exécuté. « Pages allouées » désigne les adresses non nulles connues du moteur ; des blocs libérés par ext4 restent présents tant qu’un TRIM ne les a pas effacés.

Les répétitions en lecture seule réutilisent une base précédemment contrôlée et ne refont pas systématiquement CHECK TABLE. Les essais en écriture font SIGKILL MySQL et vérifient les lignes et les tables. Une campagne séparée tue le moteur pendant des transactions et une autre restaure PostgreSQL/ext4 depuis S3. SIGKILL ne simule pas une panne électrique réelle.

## Limites à conserver dans la review

VM partagée, essais courts, ordre fixe et historiques de chauffe différents. Les TPS ne suffisent pas : conserver p95, erreurs ignorées par sysbench, durées de démarrage/préchauffage, GET et octets distants. Les erreurs ignorées doivent rester visibles et ne peuvent être qualifiées sans leur code SQL.

Le microbenchmark `probe_fsync_layout.py` compare un pwrite de 4 128 octets suivi de fdatasync dans un fichier qui grandit ou dans un fichier de 512 Mio initialisé. Il ne teste ni une transaction MySQL ni la récupération du moteur. Son multiplicateur n’est pas celui du produit.

La durabilité S3 reste asynchrone. Les modifications du cache n’autorisent pas la suppression du WAL en attente. La compaction ne collecte pas d’objets ; une politique de rétention et des injections de crash à chaque étape de publication devront précéder sa généralisation en ligne.

## Review et recommandations

La séparation la plus utile est entre les blocs actifs disponibles localement et le stockage S3 qui reçoit des générations cohérentes. Le mode local actif supprime les lectures réseau du chemin chaud quand le jeu tient réellement dans le cache. Dimensionner le SSD d’après ce jeu et les écritures en attente, puis précharger les blocs nécessaires : un volume virtuel très grand n’exige pas de rapatrier tous les blocs, mais une DB qui lit uniformément tout son dataset exige une capacité correspondante pour conserver ce débit.

Le cache logique améliore l’utilisation d’un petit SSD en conservant les versions actuelles des pages, au prix d’un index supplémentaire en RAM et de copies. La compaction améliore la proximité des blocs sur S3, mais son prototype complet impose un arrêt. Avant une généralisation, passer à des réécritures incrémentales avec budget d’I/O, publier une racine qui tient compte des écritures concurrentes et conserver les anciennes générations pendant la qualification. La publication conditionnelle de HEAD demeure la frontière de cohérence.

Le journal fixe et les marqueurs de commit méritent une qualification distincte : ils changent le WAL local. Ne jamais retirer une barrière demandée par la DB pour afficher un meilleur débit. Garder `flush_batch_us=0` : les attentes de 100 et 500 microsecondes essayées ont dégradé les TPS. Le moteur regroupe déjà les demandes concurrentes sans cette temporisation ajoutée ; un regroupement supplémentaire doit améliorer à la fois le débit et les latences sur la charge réelle.

ublk peut améliorer le débit, mais son premier adaptateur avait des erreurs de réveil et de nettoyage. Sa correction garde des copies et une passerelle eventfd ; un gain mesuré n’en fait pas un transport prêt pour tous les noyaux. Conserver NBD comme chemin de référence et poursuivre les tests de teardown, FUA, saturation et crashes avant de choisir un transport par défaut. Le p95 observé avec ublk est moins favorable que celui de la référence NBD proche dans le temps.

Les prochains obstacles de capacité et de démarrage sont l’index global en RAM et les longues reprises/préchauffages depuis S3. Prioriser un index paginé, le préchargement par étendues distantes distinctes avec concurrence bornée et des métriques de retard S3. En dehors des budgets configurés, inclure le page cache Linux dans les mesures de mémoire. Une panne réseau ne doit jamais conduire à supprimer le WAL en attente pour faire de la place.

Les crashes de processus et contrôles de structure constituent une qualification de prototype. Il reste à injecter des PUT interrompus et conditionnels concurrents, des erreurs d’espace disque et d’E/S, des corruptions de métadonnées, puis à faire des essais longs et des coupures électriques sur une machine dédiée. Aucune promesse d’absence absolue de corruption ou de débit identique pour toutes les DB ne découle de cette campagne.


## Qualification et provenance

La version finale passe 20 tests unitaires et le test du protocole NBD, avec la feature ublk, ainsi que rustfmt, Clippy sans avertissement et le build release verrouillé. Deux campagnes ext4/PostgreSQL couvrent SIGKILL du moteur, SIGKILL de PostgreSQL, contrôles pg_amcheck, scrub et restauration depuis S3 seul. Trois campagnes MySQL vérifient la reprise après SIGKILL du moteur : NBD avec marqueurs, NBD avec WAL fixe et marqueurs, puis ublk avec les mêmes options. Les séries MySQL en écriture vérifient aussi la reprise du processus DB, le nombre de lignes et CHECK TABLE EXTENDED.

Le premier essai de crash ublk a échoué au nettoyage : après mort du serveur, le noyau avait retiré le disque bloc mais conservé sa cible de contrôle. La suppression accepte désormais cette situation uniquement pour une cible identifiée comme expérimentale InfiniDisk2, signalée DEAD par le noyau, sans disque bloc ni montage canonique restant. Une cible vivante exige la prise exclusive du disque. Le banc attend aussi la disparition des interfaces avant réutilisation de l’identifiant. Les erreurs et leurs logs sont conservés, puis le test de crash ublk complet a été refait avec succès.

Les deux rapports de campagne conservent les essais ratés puis leurs reprises réussies. Chaque mesure porte l’empreinte du binaire ; les snapshots et manifests conservent les sources correspondant à chaque binaire. Le dernier binaire change uniquement le nettoyage ublk par rapport au binaire du factoriel WAL : le code du moteur NBD/WAL est identique. Il a subi les checks complets, le crash ublk et la mesure combinée sur la grande base. Les preuves exportées excluent les credentials, les bases, les caches et les journaux locaux.

Les services infinidisk@bench/data ont conservé leurs PID et leur date de démarrage. Les conteneurs applicatifs sont toujours actifs ; les périphériques nbd31/ublk31 et les montages de test sont libérés. Le code et le rapport sont également synchronisés dans /root/infinidisk2 sur la VM.
