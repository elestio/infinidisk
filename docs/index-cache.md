# Redémarrage chaud : cache vérifié des index distants

Le moteur conserve maintenant une copie SSD des objets d'index immuables. Il relit toujours HEAD sur S3 avant de les utiliser et rejoue ensuite le WAL local. Sur la fixture PostgreSQL de 14 shards, la comparaison contrôlée passe de **15 à 1 GET par ouverture**, avec zéro GET de données et zéro PUT pendant ces ouvertures. [Rapport et graphiques](../validation/index-cache/rapport.html).

## Configuration et contrat

`remote_index_cache_mib = 128` est ajouté au profil généré par `config` et au [modèle recommandé](../configs/recommended.toml). Ce budget SSD est **séparé** du cache des données (`disk_cache_mib`) et du budget RAM des cartes d'index (`max_index_mib`). Une ancienne configuration qui omet ce champ garde la valeur historique 0 : désactivé. À 0, les copies existantes restent sur disque mais sont ignorées ; aucun remplissage n'est effectué.

Le répertoire est `local_dir/remote-index-cache/`. Il est jetable, contrairement à l'identité, au WAL et au marqueur durable. Le cache ne modifie ni le format du volume, ni le contrat `FLUSH/FUA`, ni les règles de fencing. Les copies sont limitées par leur taille totale et par **16 384 entrées**, avec un maximum de 1 Mio par objet. Le budget porte sur les payloads des fichiers ; métadonnées du système de fichiers et RAM ne sont pas incluses. Une entrée par shard courant doit tenir dans ces limites pour espérer un seul GET de HEAD.

## Lecture, publication et reprise

1. Lire et vérifier HEAD depuis S3 ; contrôler format, identité du volume, taille et identité de l'écrivain avant toute réutilisation du cache.
2. Pour chaque shard référencé, calculer une clé locale depuis un domaine de format, le UUID du volume, le numéro du shard, la clé S3 complète et le SHA-256 attendu dans HEAD.
3. Lire au plus 1 Mio + 1 octet de la copie locale, refuser les liens symboliques de fichier, puis vérifier le SHA-256 contre HEAD. Une copie tronquée, incohérente ou illisible devient un miss. Le moteur récupère alors l'objet distant et le valide normalement ; une erreur distante est propagée, jamais remplacée par un index vide.
4. Vérifier aussi les références décodées : pages du bon shard, bornes du volume, offset et longueur du segment. Reconstruire l'index de travail depuis ces objets vérifiés, puis rejouer les WAL postérieurs. Le scratch mutable de l'index paginé n'est jamais réutilisé comme autorité de reprise.
5. Lors d'un checkpoint ou d'une compaction hors ligne, remplir le cache après succès du PUT de l'objet d'index. HEAD reste publié conditionnellement en dernier. Si la publication échoue, une éventuelle copie orpheline reste jetable et ne sera choisie que si un futur HEAD désigne exactement sa clé et son hash.

Les copies utilisent un fichier temporaire puis un rename, sans `fsync` supplémentaire. La perte de ces fichiers après crash coûte des GET supplémentaires. Les E/S et hashes du cache se font dans des travaux bloquants dédiés, hors du verrou global de l'état moteur. Les remplissages sont sérialisés : au plus un temporaire en écriture. La place nécessaire est libérée avant le remplissage ; l'échec d'une éviction ou d'un remplissage désactive ce cache pour l'instance et laisse la récupération depuis S3 disponible.

Au démarrage, les fichiers temporaires reconnus sont supprimés et les limites sont réappliquées. L'ordre LRU en mémoire n'est pas persisté ; l'ordre de rétention initial dépend donc du parcours du répertoire, pas d'une chronologie garantie. Les copies d'anciennes générations peuvent rester jusqu'à leur éviction. Un arrêt du moteur ne rend pas les anciennes configurations compatibles avec un ancien binaire qui ne reconnaît pas ce nouveau champ.

`adopt`, `scrub` et la collecte des objets ignorent ce cache pour contrôler réellement les index distants. Conséquence actuelle : une adoption à froid lit l'index pour le valider, puis l'ouverture du serveur le lit à nouveau pour constituer son cache. Cette double lecture reste visible dans la mesure et constitue une prochaine optimisation possible.

## Mesures du 10 octobre 2026

Binaire : `69a5dd75b73344d99ec6f467c73467da6f455305673a1482c37257ae44621308`. Appels réels sur Elestio S3, comptés par le proxy déjà qualifié, qui conserve toutes les tentatives et distingue les phases. Tous les appels de cette campagne sont classés sans incertitude de facturation. Les tarifs sont une projection Tigris Standard : A 5 $/million, B 0,50 $/million, hors franchise et stockage. [Grille officielle vérifiée le 10 octobre](https://www.tigrisdata.com/pricing/).

| Ouverture sans I/O applicative | Cache désactivé | Cache activé |
|---|---:|---:|
| GET de HEAD | 1 | 1 |
| GET d'index | 14 | 0 |
| GET de données / PUT | 0 / 0 | 0 / 0 |
| Total B | 15 | 1 |
| Coût projeté par 10 000 ouvertures identiques | 0,075 $ | 0,005 $ |

Même binaire, même HEAD (égalité contrôlée avant/après), mêmes fichiers locaux, ordre désactivé–activé–activé–désactivé. Deux passages par réglage ; seule la limite du cache d'index change entre 0 et 128 Mio. Le résultat 15→1 est identique dans les deux passages. Les ouvertures instrumentées durent toutes environ 0,303 s : la cadence de détection du serveur/attachement masque les différences fines. **Aucun gain de temps de démarrage n'est démontré par cette comparaison** ; le gain validé est la baisse des appels et des téléchargements d'index.

Sur le redémarrage complet PostgreSQL avec cache conservé, ouverture du moteur et disponibilité de la DB consomment ensemble **0 A, 1 B**, en 0,576 s instrumentées. Avec un répertoire local neuf et adoption, elles consomment **1 A, 166 B**, en 5,652 s : 28 GET d'index, 136 GET de données, deux GET de HEAD et un PUT de HEAD. Le contrôle d'intégrité et l'arrêt de PostgreSQL sont mesurés dans des phases séparées. Ce ne sont donc pas les opérations de tout le cycle ouverture–vérification–arrêt. PostgreSQL peut écrire pendant son démarrage et son arrêt ; son arrêt chaud ajoute ici 10 A. Aucun état physique identique entre le redémarrage complet chaud et froid n'est revendiqué, contrairement aux quatre ouvertures de contrôle.

### Coût du lot SQL du profil actuel

Le lot exécute exactement 256 transactions TPC-B-like sans erreur : 1280 requêtes métier (3 UPDATE, 1 SELECT, 1 INSERT par transaction), ou 1792 commandes en incluant BEGIN/END. Les phases **charge + attente de fin des uploads lors de l'arrêt propre** totalisent 10 A et 0 B : un segment, huit index et un HEAD. Le coût n'est donc pas déclaré nul sous prétexte que les uploads arrivent après pgbench.

| Unité normalisée | Coût des opérations projeté |
|---|---:|
| Lot mesuré de 1280 requêtes métier | 0,000050 $ |
| 1000 requêtes métier équivalentes | 0,0000390625 $ |
| 10 000 requêtes métier équivalentes | 0,000390625 $ |

Un seul lot court, mêmes caches réduits que les précédentes campagnes (RAM 64 Mio, données SSD 128 Mio, index résident 64 Mio) et PostgreSQL limité à 1 CPU/512 Mio. Les créations initiales, redémarrages et contrôles sont exclus de cette unité SQL et conservés séparément. La normalisation n'est pas une prévision de trafic continu. Aucun nouveau gain de TPS n'est revendiqué ; la mesure précédente de 3000 TPS concerne le binaire antérieur et son propre protocole.

## Validation et limites

**26 tests Rust ciblés** : quatre nouveaux cas couvrant redémarrage, WAL plus récent, fencing, HEAD changé, corruption locale/distante, budget, identité et cache indisponible ; un test du profil et 21 régressions moteur. Fmt, Clippy et build avec ublk passent. Une seule campagne `--checks-only` exerce les sept contrôles ext4/PostgreSQL/S3, incluant SIGKILL moteur pendant les transactions et vérification CRC32C des 256 Mio restaurés. Les preuves de build, comptage HTTP et reprise sont dans `validation/index-cache/`.

Le maintien du cache ajoute des écritures SSD et des hashes pendant les checkpoints. Le chemin d'acquittement des écritures n'appelle pas directement ce cache, mais aucun benchmark complet de p99/TPS sous saturation n'a été refait pour quantifier ce coût indirect. Pas de qualification de coupure électrique matérielle ou de panne fournisseur. Un volume dense de très grande taille peut dépasser le cache ; sa reconstruction conserve alors des GET et le coût du décodage de tous les shards. Les limites de HEAD, du scratch paginé et du fencing externe restent inchangées.

## Suite recommandée

La priorité suivante est la concurrence des lectures adaptatives : borner globalement les octets en vol et mesurer un scénario alternant aléatoire/séquentiel avec plusieurs lecteurs pour traiter le p99 séquentiel qui avait augmenté de 19 %. Critère : débit utile et p99, pas seulement le maximum de MiB/s. Ensuite, réutiliser les index fraîchement vérifiés par `adopt` pour éviter leur second GET à froid, tout en gardant le contrôle distant lors de l'adoption. Les packs de compaction entre shards restent un changement distinct du chemin de publication, à qualifier séparément.
