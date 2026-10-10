# PostgreSQL : comparaison contemporaine avant/après Astra

## Résultat qualifié du 10 octobre 2026

La campagne `cde40aeb205e` est terminée sur le binaire Astra `b39b705f43b6…`. Les douze passages ne signalent aucune transaction échouée. Les contrôles SQL et les reprises prévues par le protocole passent ; les preuves sont dans [postgres/report.json](../validation/astra/postgres/report.json).

| Outil | TPS médian | Médiane des latences moyennes, ms |
|---|---:|---:|
| InfiniDisk2 avant Astra | 1 778,98 | 2,248 |
| InfiniDisk2 Astra | 2 937,57 | 1,362 |
| ZeroFS durable S3 | 0,823 | 4 860,403 |
| Natif | 4 833,82 | 0,828 |

Astra gagne **65,1 % de débit** face à la référence locale au même contrat de durabilité et avec les mêmes budgets configurés. La latence moyenne diminue de 39,4 %. La série ZeroFS attend le stockage distant à chaque barrière ; son écart ne démontre pas un gain à contrat identique.

L'essai antérieur `0fbd76048095` reste archivé, incomplet. Son précontrôle traitait un port en TIME_WAIT comme un listener actif ; après correction et vérification du refus d'un vrai listener, les quatre variantes ont été rejouées sur des fixtures neuves. Aucun score de cet essai interrompu n'est réutilisé dans le tableau.

`scripts/run_astra_postgres_compare.py` prépare quatre bases neuves pour mesurer le gain d'InfiniDisk2 Astra face au binaire précédent, avec deux références supplémentaires : natif et ZeroFS durable. Il ne reprend aucun chiffre d'une ancienne campagne. Le résultat n'est qualifié qu'après exécution complète, vérification des données et export des preuves.

## Protocole commun

| Paramètre | Valeur |
|---|---|
| Ordre séquentiel | Natif, InfiniDisk2 avant Astra, Astra, ZeroFS durable |
| PostgreSQL | Image `postgres:16`, même image ID effectivement observé pour les quatre conteneurs |
| Taille pgbench | Scale 2, quatre clients et quatre workers |
| Mesures | Trois passages de quinze secondes par série |
| Ressources PostgreSQL | Un CPU, 512 Mio de RAM |
| Durabilité PostgreSQL | `fsync`, `full_page_writes`, `synchronous_commit` activés |
| Intégrité PostgreSQL | Checksums de pages activés à l'initialisation |
| Stockage moteur | Volume neuf de 2 Gio par série, ext4 sur NBD ; préfixe S3 UUID distinct |
| Référence native | Répertoire neuf dans `test-output`, même image et réglages SQL |
| Cache des deux InfiniDisk2 | RAM 64 Mio, SSD 128 Mio, hot WAL 64 Mio, index 64 Mio |
| Cache ZeroFS | RAM 64 Mio, SSD 128 Mio ; `ignore_fsync=false` |
| Préparation | Initialisation pgbench ; aucun préchauffage séparé ni purge globale des caches Linux |

Le protocole demande **180 secondes nominales de charge** : quatre séries × trois passages configurés à quinze secondes. La durée réelle comprend aussi les lancements et fins de commandes, quatre initialisations de base, les contrôles SQL, trois redémarrages PostgreSQL après SIGKILL, les créations de volumes et les arrêts avec publication S3. Ces coûts ne sont pas connus avant la campagne ; 180 secondes n'est donc pas une estimation de sa durée totale. Le timeout de 5 400 secondes par étape moteur est une borne de protection, pas une durée attendue.

Les TPS et latences moyennes sont ceux de pgbench. Aucun p99 n'est inventé. Chaque série doit fournir exactement trois échantillons finis et strictement positifs, sans transaction échouée. Le ratio avant/après divise le débit médian Astra par le débit médian de la baseline. Le ratio de latence divise la latence moyenne médiane de la baseline par celle d'Astra : supérieur à un signifie ici une diminution de latence.

## Référence avant Astra et delta exact

La baseline provient de `options('small', 'baseline')` dans `scripts/run_astra_mysql.py`, chargée par AST sans lancer cette campagne. Seuls ses quatre budgets RAM, SSD, index et hot WAL sont adaptés aux valeurs PostgreSQL ci-dessus. Le harness refuse une différence supplémentaire entre les options communes des deux binaires.

Les optimisations antérieures restent activées dans les deux versions : `logical_cache=true`, `wal_fixed_size=true`, `wal_commit_records=true`, `wal_writev=true` et `sync_data_only=true`. Les autres paramètres communs restent identiques : `wal_preallocate=false`, `flush_batch_us=0`, checkpoint 5 s, segments 8 Mio, backlog 1 024 Mio, `max_inflight=128` et lectures par extents de 64 Kio.

Le profil Astra est `scripts/profiles/astra-recovery-core.json`. Six options supplémentaires y sont activées :

- `async_cache` : remplissage du cache asynchrone, avec file de 16 Mio.
- `fast_local_reads` : lectures locales groupées et cache partitionné.
- `checkpoint_pipeline` : préparation des segments.
- `selective_sync` : synchronisation des fichiers nécessaires.
- `compact_checkpoints` : publication des versions de pages finales.
- `paged_index` : index paginé.

`aligned_wal`, `generation_mode` et `ublk_fast_path` restent désactivés. Le binaire ancien n'a pas ces clés ; `--legacy-config` les exclut du TOML et le rapport distingue cette absence de la valeur explicite `false` du binaire récent. La limite de retard de génération de 30 s est présente dans Astra mais inactive dans ce mode. `protocol.options_delta` expose toutes ces différences, y compris les paramètres nouveaux inactifs.

La comparaison avant/après mesure l'ensemble du changement de version et de ces optimisations, sans attribuer tout le gain à une option unique. Le préchargement S3 hors ligne n'est pas lancé ici ; son accélération relève de la campagne warm séparée.

## Contrats et vérification

Les deux versions d'InfiniDisk2 acquittent la durabilité dans leur WAL local et publient ensuite sur S3. ZeroFS durable attend S3 ; sa série n'a donc pas le même contrat de commit distant. La référence native dépend du disque de la VM. Les budgets configurés ne bornent pas à eux seuls toute la mémoire physique, notamment le cache Linux. Le quota d'un CPU s'applique au conteneur PostgreSQL ; le service de stockage est séparé. Il ne s'agit donc pas d'un plafond CPU global identique pour l'ensemble base et moteur.

Le helper `compare_zerofs.py --postgres-only` crée une fixture séparée pour chacun des trois moteurs, puis, après les mesures, tue le processus PostgreSQL par SIGKILL, le redémarre, contrôle les sommes comptes/agences/guichetiers et exécute `pg_amcheck`. La série native contrôle aussi les sommes et `pg_amcheck`, sans ajouter de crash dans ce comparatif. Les rapports distinguent ces scénarios ; ils ne prétendent pas qualifier ici une coupure électrique ou une perte de tout le stockage local.

La VM reste partagée et les tests sont séquentiels, dans un ordre fixe. Les services de production, nbd0/1 et leurs conteneurs restent actifs. Le précontrôle ne bloque que les emplacements réservés nbd31/ublk31, les ports 11990/11991/12991, une autre campagne ou un compilateur/benchmark connu. Aucun service de production n'est arrêté.

## Commande préparée pour les binaires gelés

À exécuter seulement après le handoff VM du coordinateur :

```bash
cd /root/infinidisk2
python3 scripts/run_astra_postgres_compare.py \
  --binary /root/infinidisk2/target/release/infinidisk2-astra-b39b705f43b6 \
  --expected-binary-sha256 b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151 \
  --baseline /root/infinidisk2/target/release/infinidisk2-pre-astra \
  --expected-baseline-sha256 6096fff8d5b69e9e3f21c8a500cb7a1c98d737a876a018d2faf8d8a1b64046d1
```

Les deux SHA sont obligatoires et revérifiés avant/après chaque étape. Les démarrages réels du helper doivent aussi porter le SHA, les options et le mode legacy attendus. Le ZeroFS lancé via PATH doit être le fichier `/usr/local/bin/zerofs` dont l'empreinte est enregistrée. L'image PostgreSQL et les sources Python/profile sont figées pendant la campagne.

## Schéma et preuves

Le rapport canonique est `/root/infinidisk2/validation/astra/postgres/report.json`. Le schéma reste `infinidisk2.astra.postgres-comparison.v1`, avec :

- `binary_sha256.baseline`, `.astra`, `.zerofs`, `.helper` et leurs chemins dans `binary_paths`.
- `protocol.baseline_options`, `.astra_options`, `.options_delta`, `.measurement_window_seconds`.
- `series.baseline`, `.astra`, `.zerofs-durable`, `.native` : trois objets `samples` contenant `tps`, `latency_ms` et `failed_transactions`, médianes, réglages SQL et contrat.
- Pour chaque moteur : `binary_sha256`, `engine_options`, `legacy_config`, `engine_starts`, `database_SIGKILL_recovery` et `source_report` dans sa série.
- `ratios.astra_over_baseline_tps`, `.baseline_over_astra_mean_latency`, `.astra_over_native_tps`, `.astra_over_zerofs_durable_tps`.
- `source_report` et `source_manifest` : chemins vers les preuves du run UUID conservé.

Le dossier UUID archive séparément `baseline/report.json`, les logs pgbench et de reprise de la baseline, `baseline-options.json`, les preuves des autres moteurs et les SHA des fichiers. L'export accepte uniquement les rapports/logs textuels nommés explicitement et les options publiques ; il masque les credentials et signatures, refuse les symlinks et exclut les données, WAL, caches, configs brutes, secrets et binaires. Un échec reste `complete=false` et remplace aussi le rapport canonique, sans supprimer les anciens runs.

Les vérifications locales disponibles sont `--self-test`, `--plan-only` et `python3 -m py_compile scripts/run_astra_postgres_compare.py`. Elles couvrent notamment la conservation des anciennes optimisations, les budgets identiques, le refus des options communes divergentes, les SHA/modes legacy incohérents et l'archivage distinct de la baseline. Elles ne constituent pas une mesure PostgreSQL exécutée sur la VM.
