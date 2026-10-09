# Comparaison mesurée : InfiniDisk2 / ZeroFS

Même VM partagée, même endpoint S3 https://storage.elestio.com et bucket testperf-6czebk, préfixes UUID distincts. ZeroFS 2.3.5, InfiniDisk2 0.1.0, Rust natif pour attacher les deux exports via 8 connexions TCP NBD, blocs Linux de 4 Kio. Volume 2 Gio, données fio 256 Mio, quatre jobs de profondeur 32 en aléatoire ; quatre jobs de profondeur 1 et fsync=1 en écriture synchrone. Trois échantillons de 15 secondes par cas, médiane et plage complète. Zone de lecture à 16 Mio ; écritures aléatoires à 512 Mio : les lectures portent sur un jeu stable et vérifié par CRC32c avant les autres charges. Mémoire configurée 64 Mio ; disque 128 Mio au démarrage froid, puis 512 Mio pour précharger le jeu de 256 Mio. InfiniDisk2 hot_wal_mib=0 ; ZeroFS compression lz4 et chiffrement applicatif, InfiniDisk2 sans compression ni chiffrement applicatif. Checkpoints/flush périodiques configurés à 5 secondes. Aucun drop_caches global. Aucun service préexistant modifié. Passage complémentaire de lecture avec 1 Gio RAM et 4 Gio SSD sur les deux moteurs, jeu préchargé et trois répétitions. Après les restaurations, relecture intégrale des 256 Mio avec vérification CRC32c réussie sur les deux.

| Charge | ZeroFS | InfiniDisk2 | Ratio |
|---|---:|---:|---:|
| Lecture 4 Kio, cache initialement vide, SSD 128 Mio | 461 IOPS | 3 028 IOPS | ×6.57 |
| Lecture 4 Kio, SSD 512 Mio préchargé | 13 580 IOPS | 50 615 IOPS | ×3.73 |
| Écriture 4 Kio sans barrière par opération | 756 IOPS | 29 169 IOPS | ×38.59 |
| Écriture 4 Kio + fsync local/S3 (garanties différentes) | 2 IOPS | 1 401 IOPS | ×862.43 |
| Lecture séquentielle 1 Mio préchargée | 834 Mio/s | 3 076 Mio/s | ×3.69 |
| Lecture 4 Kio, RAM 1 Gio préchargée | 25 584 IOPS | 77 893 IOPS | ×3.04 |
| Lecture séquentielle 1 Mio, RAM 1 Gio | 1 633 Mio/s | 3 524 Mio/s | ×2.16 |

## Durabilité et interprétation

InfiniDisk2 honore FLUSH/FUA sur le disque local, puis publie S3 de façon asynchrone. ZeroFS ignore_fsync=false publie vers S3 avant de terminer une barrière. Une perte du SSD juste après fsync peut donc perdre des transactions avec InfiniDisk2 ; cette fenêtre peut dépasser 5 secondes si S3 ralentit. Le ratio des écritures synchrones et de PostgreSQL compare ces deux contrats explicitement différents. ZeroFS ignore_fsync=true, utilisé par le wrapper précédent, ignore la barrière : son débit ne valide pas la durabilité d'une DB. Aucun test de ce rapport ne prouve l'absence absolue de corruption.

Les chiffres rendent visibles les plafonds du chemin NBD/cache et le coût des barrières S3. Le débit synchrone ne permet pas de conclure qu'InfiniDisk2 est supérieur à contrat S3 identique : un comparateur qui force un checkpoint S3 à chaque FLUSH reste à mesurer. Le passage complémentaire ZeroFS sans fsync atteint 34 IOPS (plage 15–345), face à 1 401 IOPS avec barrière locale honorée par InfiniDisk2. Ce passage ZeroFS intervient après les lectures et chauffe une autre zone ; sa forte variation empêche d'attribuer son résultat au seul changement de ignore_fsync. Il ne reproduit pas nécessairement les chiffres du wrapper sur un autre jeu de données. Le gain en lecture doit être interprété à partir des p99 et des répétitions, pas du meilleur passage seul. Avec les grands caches, PostgreSQL atteint 888 TPS sur InfiniDisk2 contre 1401 TPS sur ZeroFS qui ignore fsync. Le prototype ne domine donc pas tous les cas : le débit des commits avec vraie synchronisation locale reste une priorité de développement.

## PostgreSQL

PostgreSQL 16 sous Docker, une CPU et 512 Mio, pgbench scale 2, quatre clients/quatre threads, trois fenêtres de 15 secondes. fsync, synchronous_commit, full_page_writes et data_checksums vérifiés actifs sur les deux. Médiane : ZeroFS 0.70 TPS ; InfiniDisk2 870.07 TPS (×1244.16). Aucune transaction échouée dans les échantillons. Après SIGKILL de PostgreSQL, redémarrage, égalité des sommes accounts/branches/tellers et pg_amcheck réussis sur les deux. Il s'agit d'un crash du processus DB ; les tests antérieurs distincts d'InfiniDisk2 couvrent aussi SIGKILL du moteur et restauration S3 seule. Ce petit jeu de données tient en cache ; ce n'est pas un benchmark de grande DB distante. Passage complémentaire sur les mêmes DB avec 1 Gio RAM et 4 Gio SSD pour chaque moteur : ZeroFS ignore_fsync=true 1401.29 TPS ; InfiniDisk2 avec fsync local honoré 887.80 TPS (ratio 0.63). PostgreSQL affiche toujours fsync=on, mais ZeroFS ignore sa demande dans ce passage : ce chiffre décrit un mode à durabilité relâchée. Le crash testé est celui de PostgreSQL pendant que le moteur de stockage reste actif ; son succès ne valide pas la reprise après perte du moteur ou de la VM avec fsync ignoré. Le rapport précédent du wrapper utilisait scale 100 et huit clients : ses 1 568 TPS ne sont pas directement comparables à notre scale 2/quatre clients/une CPU.

## Limites

Les lectures « froides » commencent avec un nouveau répertoire de cache, puis chauffent pendant l'échantillon ; ce n'est pas une mesure de 100 % de misses S3. Le SSD de 128 Mio reste inférieur au jeu de 256 Mio. Les couches de cache et leurs métadonnées diffèrent entre moteurs : budgets configurés égaux ne signifient pas RSS ou nombre de pages utiles égaux. La VM héberge d'autres services ; les moteurs sont testés successivement, ZeroFS puis InfiniDisk2, sans randomisation de l'ordre. Les variations S3 et la dégradation des écritures au fil des répétitions figurent dans les résultats bruts. Les données ne remplissent pas un grand volume, et chaque fenêtre est courte : ces résultats ne prouvent pas un débit soutenu de plusieurs heures, ni une capacité de production. Les appels directs évitent le cache Linux côté client ; ils ne suppriment pas tous les caches du serveur.

## Recommandations

Priorité 1 : conserver WAL et barrières locales, instrumenter le temps de synchronisation des segments et du watermark, puis réduire le nombre de barrières avec une preuve de récupération ; ne pas gagner des IOPS en ignorant fsync. Priorité 2 : tester PostgreSQL au-delà du cache, pendant plusieurs heures et sous coupures réseau, disque plein et panne VM ; vérifier pg_amcheck et sommes métier après chaque reprise. Priorité 3 : un cache SSD dimensionné pour le working set, avec préchargement ciblé et métriques de misses/p99. Priorité 4 : compaction des segments partiellement vivants, index paginé et visibilité du retard S3 ; sans cela les mesures sur 256 Mio ne représentent pas un gros disque. Garder un seul écrivain avec fencing externe pour takeover.

Résultats complets : `../validation/comparison-raw.json`, `../validation/comparison-postgres.json`. Reproduction sur la VM : `python3 scripts/compare_zerofs.py`, puis `python3 scripts/compare_zerofs.py --postgres-only`. Le script utilise uniquement des volumes neufs dans des préfixes S3 UUID et les credentials privés existants ; il laisse les objets de test pour audit.


Les phases complémentaires de cette exécution se reproduisent, moteurs arrêtés, avec :

```sh
python3 scripts/compare_zerofs.py --verify-report /root/infinidisk2/test-output/comparison-8e8464fb817a/report.json
python3 scripts/compare_zerofs.py --warm-memory-report /root/infinidisk2/test-output/comparison-8e8464fb817a/report.json
python3 scripts/compare_zerofs.py --postgres-roomy-report /root/infinidisk2/test-output/comparison-fd6f782061b2/report.json
```

Les fichiers fio complets, logs et configurations sont conservés dans `../validation/comparison-artifacts/raw/` et `../validation/comparison-artifacts/postgres/`. Les empreintes binaires sont dans les JSON de synthèse d'exécution. Les exports excluent les credentials, WAL et caches ; les traces ont été contrôlées contre les secrets avant copie.
