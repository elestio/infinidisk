# InfiniDisk2 : expérience WAL et limites du cache MySQL

## Changement testé

Le WAL conserve IDWAL001 et sa taille logique : aucune migration des volumes. L’expérience teste writev (en-tête et charge utile en un appel, gestion des écritures partielles/EINTR/WriteZero) et fallocate KEEP_SIZE (réservation de blocs sans prolonger EOF). Les blocs réservés après EOF sont libérés par PUNCH_HOLE seulement au-delà du dernier bloc contenant des données, à la rotation et à la récupération. Les barrières WAL puis watermark, les contrôles CRC, FLUSH/FUA et publication S3 ne sont pas supprimés. Les erreurs d’écriture rendent toujours le volume indisponible.

## Décision

writev=true est conservé pour réduire les appels système, sans gain net de débit établi : références 866,34 et 831,69 TPS, writev seul 874,64 TPS. La préallocation reste désactivée : 849,75 TPS seule, 849,39 combinée. Le binaire final confirme 849,13 TPS. Les extents de 64 Kio restent la valeur par défaut : le passage 16 Kio ne montre aucun bénéfice dans cette campagne à chauffe partielle.

## Méthode

MySQL 8.0.46, même base de quatre tables de 25 000 lignes, huit threads, une CPU, 1 Gio de RAM, buffer pool 256 Mio. Moteur RAM 1 Gio/SSD 4 Gio, hot_wal_mib=0. InnoDB flush_log_at_trx_commit=1, doublewrite=ON, O_DIRECT, binlog actif et sync_binlog=1. Trois passages write_only de 15 secondes par mode, sans strace, dix secondes d’échauffement. Ordre : référence A, préallocation seule, writev seul, combinaison, référence B. Même binaire pour les cinq modes, mêmes données ; évolution des journaux et charge d’une VM partagée restent des facteurs de variance. Après chaque mode : SIGKILL MySQL, reprise, comptes de lignes et CHECK TABLE EXTENDED. Deux passages signalent chacun une erreur ignorée par sysbench (writev seul, passage 1 ; combinaison, passage 2). Les logs agrégés ne précisent pas le code SQL : ces passages restent présentés, sans être qualifiés de sans erreur. Les moyennes WAL/watermark viennent du dernier statut cumulatif du processus, préparation et reprise incluses.

## Interprétation du profil

Les strace agrégés sont des diagnostics distincts, exclus des résultats TPS. Le premier -c rapporte des temps CPU système ; le second -c -w rapporte des temps écoulés. Les attentes futex de plusieurs threads se cumulent et ne représentent ni un pourcentage de CPU consommé ni la durée réelle du benchmark. Le tracing réduit sensiblement le débit ; les compteurs internes et les passages sans tracing servent à juger les optimisations.

| Mode | Médiane TPS | Trois passages | WAL ms | Watermark ms |
|---|---:|---|---:|---:|
| wal-baseline-a | 866.34 | 866.34, 891.9, 862.65 | 0.739 | 0.111 |
| wal-prealloc | 849.75 | 903.39, 849.75, 839.89 | 0.736 | 0.113 |
| wal-writev | 874.64 | 887.19, 872.22, 874.64 | 0.730 | 0.108 |
| wal-combined | 849.39 | 892.36, 849.39, 805.71 | 0.778 | 0.108 |
| wal-baseline-b | 831.69 | 819.26, 831.69, 856.37 | 0.775 | 0.111 |

## Base au-delà des caches

Base neuve de quatre tables de 1,000,000 lignes, fichiers de tables 960.0 Mio. Buffer pool MySQL 256 Mio ; cache moteur RAM 64 Mio et SSD 128 Mio ; hot WAL désactivé. Volume 4 Gio. Même limite CPU/MySQL et durabilité que le petit test ; distribution sysbench spéciale par défaut, seed 42, trois passages de 15 secondes. La taille dépasse les caches configurés, mais il ne s’agit pas d’un benchmark entièrement froid : le skew, les copies de pages entre caches et l’historique des lectures influencent le working set. La première vérification CHECK TABLE EXTENDED a dépassé 300 secondes sur le petit SSD ; la vérification a été reprise avec un SSD de cache de 2 Gio et une limite de 1 800 secondes. Les chiffres de débit proviennent exclusivement de la configuration initiale 128 Mio. La référence native utilise le même nombre de lignes, un buffer pool de 256 Mio et le disque ext4 de la VM. Préparation et reprise sont exclues du débit.

| Charge | InfiniDisk2 TPS | p95 ms | Natif TPS | p95 ms |
|---|---:|---:|---:|---:|
| read_write | 61.58 (24.66 → 61.58 → 119.86) | 383.33 | 785.61 (762.13 → 877.09 → 785.61) | 59.99 |
| read_only | 303.49 (150.6 → 303.49 → 478.1) | 114.72 | 1716.50 (1628.71 → 1716.5 → 1732.68) | 3.25 |
| write_only | 141.94 (32.2 → 141.94 → 304.16) | 215.44 | 2406.99 (1991.02 → 2406.99 → 2411.64) | 4.82 |

## Accès uniformes et taille du SSD

Accès uniformes en lecture seule sur la même grande base vérifiée, huit threads, trois passages de 15 secondes et 60 secondes d’échauffement par configuration. InfiniDisk2 conserve 64 Mio de cache RAM ; le SSD passe de 128 Mio à 2 Gio. Un passage supplémentaire conserve 2 Gio de SSD et réduit les GET/cache extents de 64 à 16 Kio. Même fichier de volume, caches conservés, ordre petit SSD puis grand SSD puis natif : ce test montre un effet opérationnel du cache et de la chauffe, sans isoler tous les effets d’ordre. Le passage 16 Kio est effectué ensuite, avec le correctif de validation avant publication ; les fichiers du cache d’extents sont remis à zéro lors du changement de taille. Ces répétitions en lecture seule n’ajoutent pas de SIGKILL/CHECK TABLE ; le dataset avait déjà passé ces contrôles dans la campagne initiale. Les CRC sont toujours vérifiés sur les pages lues. Le natif utilise les mêmes paramètres MySQL et la même distribution uniforme.

| Configuration | Médiane TPS | Trois passages | p95 ms |
|---|---:|---|---:|
| cache-uniform-small | 49.88 | 49.88, 26.82, 55.8 | 601.29 |
| cache-uniform-large | 153.21 | 113.35, 153.21, 206.85 | 253.35 |
| cache-uniform-page16 | 58.56 | 37.31, 58.56, 64.91 | 719.92 |
| cache-uniform-native | 1056.58 | 1061.43, 1053.55, 1056.58 | 9.56 |

## Temps de reprise de la grande base

Reprise après SIGKILL sur grande base/petit SSD : environ 341 secondes entre le retour de docker start et la dernière sonde ready, dérivées des horodatages des logs exportés. Le binlog lu pendant cette reprise faisait environ 780 Mo. Un profil strace agrégé de cinq secondes a été attaché pendant cette reprise, sans intervenir dans les mesures TPS. InnoDB seul avait terminé son initialisation en environ 45 secondes ; la durée complète est plus longue. Ce résultat montre que la taille du cache et la disponibilité locale des journaux sont des enjeux de performance de reprise.

## Priorités recommandées

La priorité pour les DB est de garantir la présence locale du working set et des journaux de reprise : suivre les défauts de cache, dimensionner le SSD et évaluer un préchargement contrôlé des données récentes après publication. Le hot_wal est volontairement à zéro dans ces essais pour isoler les caches distants ; la configuration normale conserve par défaut 1 Gio de WAL récent. Pour les commits, l’hypothèse suivante à mesurer est un WAL de taille fixe dont les blocs ont déjà été initialisés : KEEP_SIZE ne supprime pas la croissance de la taille logique lors des append. fdatasync doit persister cette taille lorsqu’elle change. Une initialisation complète pourrait éviter une partie de cette synchronisation de métadonnées, mais exige un nouveau format local, une comptabilité physique correcte et des tests de pannes. Ce n’est pas un gain démontré par les essais actuels. Ne pas supprimer FLUSH/FUA ni la barrière du watermark pour gagner artificiellement des TPS.

## Correction d’intégrité avant publication

Un test a injecté une corruption dans un WAL local déjà synchronisé, avant le checkpoint. La version précédente publiait ce segment et avançait la racine distante ; le test échouait avant correction. La version finale vérifie sur le buffer exact envoyé l’identité du segment/volume, la longueur attendue, les formats et bornes des enregistrements, leurs CRC et la dernière séquence. Une erreur bloque les nouvelles écritures et empêche la publication de HEAD. Le test confirme que le checkpoint distant précédent reste récupérable. Quatorze tests unitaires et un test NBD passent, ainsi que rustfmt et clippy sans avertissement.

## Intégrité et limites

Les configurations expérimentales passent les tests de récupération après SIGKILL MySQL. La version finale passe aussi SIGKILL du moteur pendant transactions MySQL, récupération ext4/InnoDB, comptes et CHECK TABLE ; la campagne S3/PostgreSQL vérifie SIGKILL local, SIGKILL du moteur, CRC du fichier durable, scrub distant et récupération uniquement depuis S3 avec pg_amcheck. Ces résultats détectent des classes de corruption, sans constituer une preuve universelle. Panne électrique, fsync défaillant et campagnes longues restent à qualifier. La durabilité distante reste asynchrone : la cadence de checkpoint n’est pas une borne RPO.

Références : [fallocate](https://man7.org/linux/man-pages/man2/fallocate.2.html), [writev](https://man7.org/linux/man-pages/man2/writev.2.html), [fdatasync](https://man7.org/linux/man-pages/man2/fsync.2.html). Preuves : ../validation/wal-artifacts/, wal-recovery.json, wal-mysql-crash.json, mysql-large-report.json et wal-build-manifest.json.
## Chronologie et confirmation du binaire final

Les cinq essais factoriels et les premiers passages de cache utilisent le binaire de191e1d (13 tests), avant cette correction. Le passage 16 Kio et les reprises finales utilisent le binaire d683de69 (14 tests). Sur le petit dataset, le binaire final confirme write_only à 849.13 TPS, avec les trois passages [849.13, 843.11, 872.47], sans gain net établi par rapport aux références. Les manifestes historiques et finaux sont distincts ; ce résultat ne qualifie pas toutes les DB ni tous les profils de volume.

Reproduction : `python3 scripts/compare_zerofs.py --mysql-only --engine infinidisk2 --mysql-rows 1000000 --mysql-volume-gib 4 --mysql-memory-cache-mib 64 --mysql-disk-cache-mib 128`. Les accès uniformes utilisent `--mysql-repeat-report <report.json> --phase-label <label> --engine infinidisk2 --mysql-workloads read_only --mysql-rand-type uniform --mysql-warm-seconds 60 --mysql-repeat-disk-cache-mib 2048 --mysql-skip-crash-check`. Les sources exactes des runners récents sont archivées dans les preuves.
