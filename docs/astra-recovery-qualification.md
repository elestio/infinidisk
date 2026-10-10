# Qualification durable Astra : core et aligned

Le lanceur `scripts/run_astra_recovery.py` exécute séquentiellement le validateur existant `validate_vm.py --s3 --postgres`. Il ne compile pas le moteur. Son exécution nécessite le SHA-256 attendu du binaire final et une VM réservée à cette campagne.

Les deux profils sont `scripts/profiles/astra-recovery-core.json` et `scripts/profiles/astra-recovery-aligned.json`. Ils spécifient les 27 options autorisées par le validateur, extraites de son AST sans exécuter le script. Ils diffèrent uniquement par `aligned_wal` : faux pour core, vrai pour aligned. Le nom exact du budget WAL est **`max_pending_mib`**, fixé à 1024 ; l'index paginé dispose de 64 Mio. Le contrat reste `generation_mode=false`, avec les acquittements fsync durables localement.

Ces profils activent le cache logique et asynchrone, les lectures locales groupées, la synchronisation sélective, les segments fixes préparés, les marqueurs de commit, la compaction des checkpoints et l'index paginé. Ils utilisent 64 Mio de cache RAM, 64 Mio de WAL publié conservé localement et 16 Mio de file de remplissage. Les segments font 8 Mio. La réserve du pool est comprise dans le budget WAL. Le transport est NBD ; `ublk_fast_path=false` car ce validateur ne qualifie pas ublk. Le nom « recovery-core » désigne ici le profil durable complet, et non le sous-ensemble « core » du screening MySQL.

Le cache SSD commence à 128 Mio. Le validateur le conserve à 128 Mio lors de la restauration distante puis l'augmente à 512 Mio pour les lectures préchauffées. Le lanceur vérifie ces changements dans les manifestes de démarrage ; toute autre variation d'option invalide la qualification.

## Préparation locale

Ces commandes ne démarrent aucun validateur et ne lisent aucun identifiant S3 :

```sh
python3 scripts/run_astra_recovery.py --plan-only
python3 scripts/run_astra_recovery.py --self-test
```

Le self-test couvre l'identité des profils, l'expurgation des valeurs de secrets et des signatures S3, l'exclusion des fichiers privés, le refus des liens symboliques, les différences de profil/binaire et la détection d'une preuve modifiée. Ses fichiers temporaires sont locaux et supprimés à la fin.

## Exécution après réservation de la VM

Depuis `/root/infinidisk2`, remplacer le SHA ci-dessous par celui fourni lors du gel du binaire :

```sh
python3 scripts/run_astra_recovery.py \
  --binary /root/infinidisk2/target/release/infinidisk2-astra \
  --expected-binary-sha256 SHA256_DU_BINAIRE_FINAL \
  --credentials /opt/elestio/infinidisk/bench.env
```

Le lanceur contrôle l'empreinte avant et après chaque profil, et refuse un port NBD 11990 occupé. Il prend son propre verrou et celui de la campagne MySQL lorsqu'il est présent. Ces verrous ne remplacent pas la coordination avec les autres charges de la VM. Il ne détache aucun volume préexistant : le validateur choisit un périphérique libre, un répertoire `test-output/run-<uuid>` neuf et un préfixe S3 unique.

L'ordre est core puis aligned. Un échec, un SHA différent, une option inattendue ou une preuve requise absente arrête la campagne avant le profil suivant. Le délai par profil est de 5400 secondes par défaut. Une interruption envoie SIGINT au validateur pour laisser son `finally` nettoyer son propre montage ; après 360 secondes sans sortie, le lanceur tue seulement le validateur et s'arrête. Dans ce dernier cas, inspecter les processus et le montage de la fixture privée avant toute relance.

`--resume` vérifie les archives existantes puis saute les profils déjà réussis avec les mêmes options, le même validateur et le même binaire. Un profil échoué est relancé entièrement dans une nouvelle fixture ; l'ancienne tentative reste archivée. Le lanceur n'utilise pas `--resume-report`, qui ne rejoue pas tous les tests de panne locale.

## Preuves et schéma

Le validateur doit terminer les six contrôles suivants : reprise ext4 après SIGKILL et fsync, reprise PostgreSQL après son SIGKILL, reprise PostgreSQL après SIGKILL du moteur de stockage, scrub distant, restauration ext4 depuis S3 seul et restauration PostgreSQL depuis S3 seul. Les neuf phases fio et pgbench doivent aussi figurer au rapport. Ces essais ne simulent pas une coupure électrique réelle ni toutes les pannes du fournisseur S3.

Les résultats publics se trouvent dans `validation/astra/recovery-core` et `validation/astra/recovery-aligned` :

- `report.json` conserve les champs du validateur, dont `passed`, `tests`, `benchmarks` et `binary_sha256`. Il ajoute `qualification` : profil, tentative, horaires, code retour, durée, SHA du profil, erreurs et chemin du manifeste. Ce schéma est directement lu par `scripts/render_astra.py`, qui parcourt `recovery-*/report.json`.
- `manifest.json` pointe vers le manifeste de la tentative courante et en donne le SHA-256, ainsi que celui de son rapport archivé.
- `attempts/<horodatage-uuid>/manifest.json` porte le schéma `infinidisk2.astra.durable-evidence.v1`, les empreintes du binaire, du validateur, du lanceur, du profil et des sources observées avant exécution. La liste `files` donne nom, taille et SHA-256 de chaque preuve expurgée. Les empreintes de sources décrivent le checkout observé ; elles ne prétendent pas attester le contenu compilé du binaire.
- Chaque tentative contient le rapport, `engine-options.json`, `runner.log`, les JSON fio et les journaux de commandes autorisés. Les manifestes du validateur conservent les options effectives et les SHA des configurations privées à chaque démarrage.

Aucune copie récursive de la fixture n'est faite. Les identifiants, fichiers `.secret`, configurations TOML privées, WAL, bases PostgreSQL, caches et binaires sont exclus. Les textes exportés sont expurgés des valeurs de secrets connues, des en-têtes d'autorisation et des paramètres de signature S3. Les preuves non régulières, les liens symboliques et les fichiers de plus de 64 Mio sont refusés. Les sorties brutes restent sous `test-output/`, hors des archives publiques.

Le rapport courant n'est publié qu'après création du manifeste et archivage de toute la tentative. Aucun rapport réussi n'est fabriqué par `--plan-only` ou `--self-test`.

## Résultats du binaire final

Les deux profils passent le 10 octobre 2026 avec le binaire `b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151` :

| Profil | Tentative | Durée de qualification | Résultat |
|---|---|---:|---|
| core | `20261010T141640Z-1c7c7678` | 138,03 s | Six contrôles de reprise/scrub réussis |
| aligned | `20261010T141858Z-b4dbd8d6` | 131,03 s | Six contrôles de reprise/scrub réussis |

Les contrôles couvrent fsync/ext4 après SIGKILL, SIGKILL de PostgreSQL, SIGKILL du moteur pendant PostgreSQL, scrub distant et reprise ext4/PostgreSQL avec S3 seul. `fsync`, `full_page_writes` et `synchronous_commit` sont activés dans les deux bases. Le code retour et la liste d'erreurs de qualification sont respectivement zéro et vide.

Les durées ci-dessus mesurent la campagne complète, pas un débit de stockage. Les mesures fio/pgbench associées à cette qualification n'ont qu'un passage ; le comparatif dédié à trois passages reste séparé. Une validation de processus et de contenu distant ne remplace pas un essai de coupure électrique.
