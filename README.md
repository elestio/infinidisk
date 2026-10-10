# InfiniDisk2

Moteur bloc Linux autonome en Rust : journal SSD local, cache borné et persistance asynchrone sur S3. Il expose un disque NBD et fournit son propre client Linux multiconnexion. Aucun processus ZeroFS, wrapper Bash ou `nbd-client` ne participe au chemin des données.

Version **0.1.0 expérimentale**, fonctionnelle de bout en bout. Les essais ext4/PostgreSQL et les mesures réelles figurent dans [le rapport](validation/rapport.html). Cette version ne constitue pas une certification de sûreté pour toutes les bases de données ou tous les fournisseurs S3.

## Contrat de stockage

* `WRITE` normal : append au journal local avant réponse ; les écritures non synchronisées peuvent être perdues lors d'une panne.
* `FLUSH` / `WRITE FUA` : synchronisation du journal et d'un marqueur de durabilité local avant réponse. Les barrières ne sont jamais ignorées.
* Publication distante : segments immuables, index vérifiés, puis remplacement conditionnel atomique de `HEAD`. La restauration utilise un préfixe cohérent du journal.
* Perte complète de la VM et de son disque : retour à la dernière génération S3 publiée. L'intervalle de 5 secondes est une cible de lancement, **pas une borne garantie de perte** : le transfert et une panne réseau peuvent accroître le retard.
* Corruption détectée : une copie de cache est reconstruite depuis une copie distante vérifiée ; une donnée faisant autorité qui échoue au contrôle d'intégrité produit une erreur. Elle n'est jamais remplacée silencieusement par des zéros.
* Un seul hôte écrivain. Une reprise sur un autre hôte nécessite l'arrêt ou le fencing externe de l'ancien hôte. Aucun basculement automatique avec bail distribué n'est promis.

Les garanties supposent un disque local qui respecte `fsync`, un stockage objet qui respecte les PUT atomiques/conditionnels et une configuration correcte du système de fichiers et de la base. S3 contient un **format de volume privé** ; les objets d'un bucket existant ne deviennent pas automatiquement des fichiers Linux.

## Compilation sur la VM de développement

```sh
cd /root/infinidisk2
export CARGO_HOME=/root/infinidisk2/.cargo
export RUSTUP_HOME=/root/infinidisk2/.rustup
export PATH=/root/infinidisk2/.cargo/bin:$PATH
cargo build --release --locked -j 2
cargo test --locked -j 2
cargo clippy --locked --all-targets -j 2 -- -D warnings
cargo fmt --all --check
```

Rust 1.99.0, Linux x86_64 et `Cargo.lock` ont été utilisés pour cette livraison. Les dépendances proviennent de crates.io ; l’adaptateur ublk optionnel utilise libublk épinglé à un commit Git. La publication Cargo est désactivée ; aucune licence de distribution du nouveau code n’est imposée par cette livraison interne. Le backend `file://` permet de tester le protocole sans compte S3.

## Créer et utiliser un volume

```sh
./target/release/infinidisk2 -c volume.toml config
```

Adapter le fichier avant `init` :

```toml
local_dir = "/root/infinidisk2/runtime/volume"
store = "s3://MON_BUCKET/infinidisk2/volume-01"
endpoint = "https://storage.elestio.com"
region = "auto"
listen = "127.0.0.1:11900"
checkpoint_seconds = 5
memory_cache_mib = 128
disk_cache_mib = 2048
hot_wal_mib = 1024
max_index_mib = 1024
read_extent_kib = 64
max_pending_mib = 8192
segment_mib = 16
max_inflight = 128
```

Fournir `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` et, si nécessaire, `AWS_SESSION_TOKEN` dans l'environnement du serveur. Aucun secret n'est écrit dans les configs, logs ou rapports du dépôt. Une région AWS standard doit remplacer `auto` pour AWS S3.

```sh
./target/release/infinidisk2 -c volume.toml init --size 10GiB
./target/release/infinidisk2 -c volume.toml serve
```

Dans un second terminal root, choisir **un NBD libre**, charger le module si nécessaire, puis garder le client en premier plan :

```sh
modprobe nbd nbds_max=32 max_part=8
./target/release/infinidisk2 -c volume.toml attach --device /dev/nbd31 --connections 8
```

Dans un troisième terminal, uniquement sur ce volume neuf que tu viens de créer :

```sh
mkfs.ext4 /dev/nbd31
mkdir -p /mnt/infinidisk2
mount -o noatime /dev/nbd31 /mnt/infinidisk2
```

`init` refuse les données distantes existantes ; `attach`, `serve` et `adopt` ne formatent jamais. Aucun `mkfs` automatique. Pour un volume existant, monter son système de fichiers existant après l'attachement. Ne pas supprimer un journal local en attente pour libérer de la place.

Les modèles systemd dans `scripts/` servent à superviser les deux processus. Ils ne sont pas installés ni activés automatiquement sur la VM.

## Arrêt et reprise

Arrêt normal : arrêter la base, démonter le système de fichiers, exécuter `detach`, puis envoyer SIGTERM au serveur. Le serveur synchronise localement et tente une publication finale ; son code de sortie signale un échec de publication. Conserver le journal si S3 est indisponible.

```sh
umount /mnt/infinidisk2
./target/release/infinidisk2 -c volume.toml detach --device /dev/nbd31
```

Après crash du serveur, conserver `local_dir`, redémarrer le serveur et rattacher le disque. Effectuer la récupération normale ext4 et de la base. `e2fsck` doit s'exécuter sur un système de fichiers **démonté**. Ne pas utiliser `norecovery` pour masquer une récupération nécessaire.

Après perte du disque local, arrêter/fencer l'ancien hôte, configurer **un nouveau `local_dir` vide** avec le même `store`, puis :

```sh
./target/release/infinidisk2 -c recovery.toml adopt --takeover
./target/release/infinidisk2 -c recovery.toml serve
```

`--takeover` atteste que l'ancien écrivain a été arrêté/fencé ; il ne l'arrête pas à distance. Ne jamais faire fonctionner deux copies de la même identité locale en parallèle.

## Maintenance

```sh
./target/release/infinidisk2 -c volume.toml status
./target/release/infinidisk2 -c volume.toml scrub
# Serveur arrêté : aperçu des objets non référencés, vieux d'au moins 24 h.
./target/release/infinidisk2 -c volume.toml gc
# Appliquer la collecte après examen de l'aperçu.
./target/release/infinidisk2 -c volume.toml gc --apply
```

`status` décrit la génération **distante** ; les logs du serveur indiquent les séquences locale et distante et les octets en attente. `scrub` contrôle l'index et toutes les pages référencées, avec GET groupés par étendue.

La collecte est hors ligne, limitée au préfixe du volume et protège les objets référencés par `HEAD`. Pendant les suppressions, `HEAD` porte un écrivain réservé nul : démarrage et adoption sont refusés. Si la collecte est interrompue, reprendre `gc --apply` depuis le même `local_dir` ; son `gc-token.json` permet de retrouver le propriétaire et la génération. Ne jamais supprimer ce token ou lever le fencing manuellement. `--min-age-seconds 0` est réservé aux essais isolés.

## Performances et limites

Le chemin chaud utilise le disque local. Le cache SSD doit couvrir le working set de la base ; une lecture froide S3 conserve la latence du réseau. Les tests mesurent séparément données récentes locales, cache distant insuffisant et cache distant suffisant. La synchronisation actuelle ajoute une amplification de barrières et deux synchronisations locales ; c'est le principal coût restant pour les écritures durables.

Le défaut `sync_data_only=true` utilise maintenant `fdatasync` pour les segments WAL et le watermark : les données et métadonnées nécessaires à leur lecture restent persistées, sans imposer la persistance des horodatages internes. Les créations, répertoires et écritures atomiques gardent leurs barrières de métadonnées. `sync_data_only=false` conserve le chemin `fsync` initial pour comparaison. Les compteurs `flush_calls`, `flush_groups`, `flush_wait_ns`, `wal_sync_ns` et `watermark_sync_ns` apparaissent dans les statuts périodiques ; ils sont cumulés depuis le démarrage.

Le journal en attente, les segments récents et le cache SSD ont des limites séparées. Un journal plein applique une attente jusqu'à 50 secondes, puis renvoie une erreur si aucune publication ne libère de place. Prévoir l'espace local correspondant aux trois budgets, plus la marge du système hôte.

L'index de pages est actuellement en RAM, avec limite conservatrice `max_index_mib` (128 octets budgétés par page allouée). Le défaut de 1 Gio autorise environ 32 Gio de pages non nulles. Les volumes virtuels peuvent être plus grands s'ils sont creux, mais les très gros volumes remplis nécessitent davantage de RAM ou une prochaine implémentation d'index paginé. Atteindre la limite refuse une nouvelle allocation ; la taille virtuelle ne réserve pas toute la RAM au démarrage.

La collecte supprime des objets entièrement inutilisés. Les segments contenant quelques pages encore vivantes restent conservés : pas de compacteur automatique de segments partiellement vivants dans cette version. Pas de snapshots utilisateur, resize en ligne, chiffrement applicatif, réplication multi-écrivain ou compatibilité démontrée avec toutes les DB. Le nombre de connexions, la concurrence et les caches sont bornés ; leur dimensionnement reste à adapter au matériel.

## Validation reproductible

```sh
python3 scripts/validate_vm.py --postgres
# S3 : préfixe unique, uniquement des données d'essai, aucun objet du volume existant.
python3 scripts/validate_vm.py --s3 --credentials /chemin/credentials.env --postgres
```

Le script vérifie que son export nouvellement créé est vierge avant formatage. Il réalise les tests sur un NBD libre, utilise des conteneurs PostgreSQL isolés sans réseau et laisse les résultats sous `test-output/run-<id>`. Il ne vide jamais le cache global de la VM. Les rapports ne certifient pas une panne électrique réelle, une destruction du disque matériel ou une charge longue de plusieurs téraoctets.

Voir [les spécifications et décisions](docs/architecture.md), [la review et les mesures](validation/rapport.html) et [les résultats bruts](validation/s3-report.json).

## Variantes expérimentales de performance

La [spécification des variantes](docs/breakthroughs.md) décrit le cache par page logique, la compaction S3 hors ligne, le journal à marqueurs de commit, les segments entièrement préinitialisés, le préchauffage complet et le transport ublk. Les options `logical_cache`, `wal_commit_records` et `wal_fixed_size` sont désactivées par défaut ; `flush_batch_us` vaut zéro. Les formats de journal expérimentaux ne doivent pas être ouverts ensuite par un ancien binaire. Conserver le binaire et le journal associés jusqu’à une migration qualifiée.

La [campagne complète du 10 octobre](validation/breakthroughs/rapport.html) observe ×1,57 en lecture à cache SSD identique et ×28 à ×32 avec le volume préchauffé dans un budget de 4 Gio, ainsi que +59 à +65 % en écriture sur la petite base avec WAL fixe et marqueurs. Le grand dataset reste derrière le disque natif en mixte/écriture. Les coûts de préparation, le page cache Linux, les erreurs et les reprises après crash figurent dans le rapport ; ces chiffres ne sont pas des garanties universelles.

Les commandes hors ligne prennent le verrou exclusif du volume : arrêter le serveur avant de les exécuter. `warm` exige `logical_cache=true` et un cache SSD assez grand pour toutes les pages allouées. `compact` réécrit les pages vers de nouveaux objets ; les anciens objets restent présents jusqu’à une collecte ultérieure.

```sh
./target/release/infinidisk2 -c volume.toml warm
./target/release/infinidisk2 -c volume.toml compact
# Linux avec ublk_drv et io_uring, compilateur C/Clang pour libublk :
cargo build --release --features ublk --locked -j 2
modprobe ublk_drv
# Choisir un identifiant libre ; le serveur reste au premier plan.
./target/release/infinidisk2 -c volume.toml ublk --id 31 --queues 4
```

ublk expose `/dev/ublkb31`. Après arrêt de la base et démontage du système de fichiers, `ublk-delete --id 31` supprime uniquement un périphérique identifié comme cible expérimentale InfiniDisk2 et refuse un périphérique encore utilisé. Les commandes de formatage restent des opérations explicites de l’administrateur, limitées à un volume neuf.

## Comparaison avec ZeroFS

La [comparaison mesurée](validation/comparaison-zerofs.html) et son [protocole détaillé](docs/comparaison-zerofs.md) utilisent des volumes S3 neufs sur la même VM, huit connexions NBD et trois répétitions par charge. Les résultats distinguent le `fsync` local d'InfiniDisk2 du `fsync` vers S3 de ZeroFS : leurs garanties après perte du SSD diffèrent.

Sur la VM de validation, avec les credentials privés déjà configurés :

```sh
python3 scripts/compare_zerofs.py
python3 scripts/compare_zerofs.py --postgres-only
```

Les scripts réservent un NBD libre et leurs propres préfixes S3 UUID, sans modifier les volumes existants. Résultats sous `test-output/comparison-<id>` ; les objets de test restent conservés pour audit.

Voir aussi [l’optimisation des commits et MySQL](validation/optimisation-mysql.html), son [protocole et la review](docs/optimisation-mysql.md).

```sh
python3 scripts/compare_zerofs.py --mysql-only
```

Ce test lance des conteneurs MySQL isolés sans réseau, vérifie les réglages InnoDB/binlog et mesure les modes ZeroFS S3, ZeroFS qui ignore fsync, InfiniDisk2 et disque natif. Les mots de passe des comptes d’essai sont conservés uniquement dans des fichiers `*.secret` privés sous le répertoire d’essai, exclus des exports de preuves.

Le chemin WAL peut envoyer en-tête et données en un `writev` (`wal_writev=true`), avec reprise correcte des écritures partielles. `wal_preallocate=false` reste le défaut : l’expérience de réservation de blocs ne montre pas de gain sur cette VM. Le format WAL et les deux barrières durables restent inchangés. Voir [la campagne WAL et grande base MySQL](validation/optimisation-wal.html).

Avant publication, le buffer de chaque segment scellé est vérifié intégralement (identité, longueur, enregistrements, CRC, séquence). Un WAL local endommagé bloque le volume et conserve le précédent checkpoint distant ; le test de régression vérifie sa restauration.
