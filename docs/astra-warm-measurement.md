# Mesure du préchauffage hors ligne

Outil : `scripts/measure_astra_warm.py`. Le mode `--plan-only` ne lance aucune
charge. Toute exécution attend la libération explicite de la VM ; les mesures
réalisées figurent à la fin de ce document.

La fixture autorisée est exclusivement
`/root/infinidisk2/test-output/comparison-be4ba51a4a38`. Son fichier
`infinidisk2.toml` donne le répertoire local. La banque utilisée par la campagne
MySQL est `local_dir/astra-cache-bank/partitions-{1,16}` ; cet outil ne remplace
aucun de ses contenus.

## Protocole

1. Vérifier le SHA256 imposé du binaire final, l'arrêt de nos moteurs, bases et
   charges de test, l'absence de montages de la fixture et des seuls dispositifs
   réservés `nbd31`/`ublk31`. Les processus sont identifiés par leurs arguments
   et chemins de fixture, les bases conteneurisées par leurs noms jetables.
   Les services de production, notamment `nbd0`/`nbd1` et leurs bases, restent
   actifs : le préflight ne les refuse ni ne les arrête.
   Prendre les verrous des campagnes MySQL et recovery, puis le `LOCK` exclusif
   du volume original. Tous restent détenus jusqu'à la restauration.
2. Lire le HEAD distant avec `status`. Contrôler son identité et le format 1,
   le marqueur durable local, ainsi que les séquences et CRC des WAL locaux.
   Refuser des écritures locales non publiées : cette mesure porte exactement
   sur le HEAD, sans rejouer un état local plus récent.
3. Préserver par rename les répertoires présents `cache`, `logical-cache`,
   `astra-cache-bank` et `index-scratch`. Le journal de restauration et les
   données restent dans un répertoire privé `0700` de la fixture. Les WAL,
   l'identité et le marqueur durable originaux restent intacts.
4. Copier seulement l'identité et le marqueur durable vers un répertoire local
   privé neuf, avec un WAL vide. `warm` ouvre cette copie, ce qui permet au
   contrôleur de conserver le verrou original pendant tout le processus.
   Aucun `adopt`, `compact`, `serve`, montage ou changement du HEAD n'est exécuté.
5. Mesurer des `warm` froids du binaire final dans l'ordre **32, 8, 64, 16**
   groupes concurrents : durée, pages, GET et octets de données S3, pic réel
   d'appels range simultanés, débit utile (pages × 4096 / durée), débit réseau
   (octets de données / durée), occupation physique et métadonnées du cache.
   Chaque niveau reçoit un répertoire/cache privé neuf. Le cache RAM
   commence vide avec le nouveau processus ; les caches applicatifs SSD sont
   neufs. Aucun `drop_caches` global n'est utilisé.
6. Relancer le **binaire final** sur ce cache pour vérifier sa rétention : même
   nombre de pages, validation des versions et CRC par le moteur, **zéro GET et
   zéro octet de données S3**. Les lectures de HEAD et d'index restent permises.
   La durée de cette seconde passe est séparée de la mesure froide.
7. Supprimer le cache privé après sa rétention avant d'admettre le niveau
   suivant : un seul cache mesuré occupe le disque à la fois. Le préflight
   exige son budget de 4 Gio et 1 Gio de marge libres. Comparer le HEAD complet
   avant et après chaque passe. Restaurer les caches
   originaux par rename dans `finally`. Supprimer ensuite les copies de travail,
   les journaux privés et le cache mesuré. Aucun cache testé ne remplace celui
   de la fixture.

Les paramètres communs sont ceux du profil de qualification durable Astra, avec
cache logique de 4 Gio, cache RAM de 64 Mio, aucun WAL chaud, fills synchrones
et préparation de WAL désactivée. La préparation de WAL n'intervient pas dans
les lectures de `warm`. Les options exactes sont enregistrées dans le rapport.

## Commandes prévues, à ne lancer qu'après libération explicite de la VM

```bash
cd /root/infinidisk2
python3 scripts/measure_astra_warm.py --plan-only
python3 scripts/measure_astra_warm.py \
  --binary /root/infinidisk2/target/release/infinidisk2-astra \
  --expected-binary-sha256 SHA256_FINAL_VERIFIE
```

Le budget partagé par défaut des passages froids et de rétention est de 480 s
(`--campaign-timeout-seconds`) ; les contrôles HEAD et la restauration restent
obligatoires même après son expiration. `--concurrencies 32 8 64 16` explicite
l'ordre par défaut. Un niveau requis incomplet fait échouer la campagne, dont
les résultats déjà obtenus restent archivés.

`--include-single` ajoute une passe optionnelle à concurrence 1, limitée à 180 s,
sans ratio si elle reste incomplète. L'ancien binaire n'est jamais lancé par
défaut. Pour une comparaison
supplémentaire sur **le même HEAD**, après la mesure finale :

```bash
python3 scripts/measure_astra_warm.py \
  --binary /root/infinidisk2/target/release/infinidisk2-astra \
  --expected-binary-sha256 SHA256_FINAL_VERIFIE \
  --old-binary /root/infinidisk2/target/release/infinidisk2-astra-839abdf0cec29812 \
  --old-timeout-seconds 180
```

Le SHA de l'ancien binaire est fixé à
`839abdf0cec29812ba280cf24c26ed8f122970fb76195d5241f38c98f736d5e9`.
Son temps maximal est borné à 180 secondes. Un dépassement produit un essai
incomplet, sans ratio. Si l'ancien binaire ne publie pas les compteurs de warm,
ses GET/octets restent `null` ; ils ne sont pas estimés. Son cache est également
revérifié avec le binaire final. Un ratio de durées n'est produit que si les deux
passes sont complètes, retiennent toutes les pages et observent le même HEAD.
Le résultat historique de 1 322 secondes, obtenu sur un autre HEAD, est exclu.

## Preuves et limites

Chaque tentative publie uniquement des journaux expurgés, `report.json` et
`manifest.json` dans `validation/astra/warm/<date-id>/`. Le manifeste contient les
SHA256 des preuves, du script, du helper d'expurgation, du profil et des binaires.
Les credentials, configurations privées, WAL, index et données du cache ne sont
jamais exportés. Le lecteur `render_astra.py` intègre la tentative la plus récente
et vérifie son SHA avant de produire un rapport final. `report.json` contient
`schema=1`, `passed`, `HEAD`, `engine_options`, `concurrency_order` et
`runs[]`, avec les objets `cold`, `retention` et `cache_after_cold`. Chaque essai
porte `concurrency`, `optional`, `samples=1`, un label `final-c32`/`final-c8`/...,
et son SHA. Les mesures contiennent `max_range_inflight`, `useful_bytes`,
`useful_mib_per_second` et `network_mib_per_second` en plus des compteurs initiaux.
Le ratio historique optionnel compare l'ancien binaire à `final-c32` seulement
si les deux passages froids et de rétention sont complets.

Les compteurs du moteur couvrent les GET de données des segments, pas les
métadonnées HEAD/index ni d'éventuels retries HTTP internes. La durée froide
inclut le démarrage du processus et le chargement de l'index. Les caches du
système et du fournisseur S3 restent incontrôlés ; cette expérience mesure le
préchauffage des caches applicatifs, pas un stockage physiquement froid. Le
contrôle des WAL et les scans de métadonnées ne font pas partie de la durée
mesurée. Chaque niveau a un seul échantillon exploratoire, sans intervalle
statistique. La charge de production de cette VM partagée n'est pas neutralisée.

`SIGINT`, `SIGTERM`, une erreur ou un timeout déclenchent l'arrêt du sous-processus
puis la restauration. `SIGKILL` et une panne de la VM ne peuvent pas exécuter un
`finally` : la tentative conserve alors `restore.json` et la prochaine exécution
refuse de continuer. Après arrêt des services, restaurer explicitement :

```bash
python3 scripts/measure_astra_warm.py \
  --restore-from /root/infinidisk2/test-output/comparison-be4ba51a4a38/.warm-measure-DATE-ID
```

Cette commande vérifie les verrous et la fixture, refuse tout écrasement, restaure
les caches et conserve les journaux privés pour inspection. Les tests locaux
`--self-test`, `--plan-only` et `--help` n'accèdent ni à la VM ni aux credentials.

## Consolidation des campagnes interrompues

`scripts/consolidate_astra_warm.py` prend la première paire froid/rétention
complète de chaque niveau, dans l'ordre chronologique des tentatives. Il refuse
une divergence de binaire, HEAD, options, population de pages, GET ou octets
transférés. Chaque tentative doit avoir restauré les caches originaux, même si
son budget a expiré. Tous les fichiers doivent correspondre à leur manifeste
SHA256 ; seuls ces fichiers de preuves expurgés sont recopiés.

```bash
python3 scripts/consolidate_astra_warm.py \
  --reports validation/astra/warm/PREMIERE/report.json \
            validation/astra/warm/COMPLEMENT/report.json \
  --expected-binary-sha256 SHA256_FINAL_VERIFIE
```

La sortie conserve `schema=1` et `runs[]`, avec `source_report` sur chaque
mesure. `interrupted_runs[]` conserve les passages incomplets ;
`consolidation.source_reports[]` donne les sources, leurs SHA et leur résultat
initial. Les rapports originaux, manifestes et journaux se trouvent sous
`sources/`. Un nouveau manifeste protège cette archive complète. Le script
travaille uniquement sur ces preuves, sans accès à la VM, aux credentials ou
aux caches vivants ; `--self-test` vérifie aussi le rejet d'une source modifiée
et d'un niveau manquant.

## Résultats du 10 octobre 2026

Binaire `b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151` ;
HEAD canonique
`b19ecf1d672307a663521749e7c32b7da5d134d04eff0c26f12d4762503be86d`
(génération 228, séquence 616976). Les cinq passes complètes portent chacune
sur 484 370 pages, 62 422 GET et 4 341 374 304 octets réseau, pour
1 983 979 520 octets utiles. Les pics de requêtes réellement simultanées
atteignent chaque concurrence demandée.

| Concurrence | Préchauffage froid | Débit utile | Débit réseau | Rétention |
|---:|---:|---:|---:|---:|
| 8 | 213,12 s | 8,88 Mio/s | 19,43 Mio/s | 1,32 s |
| 16 | 116,76 s | 16,20 Mio/s | 35,46 Mio/s | 2,07 s |
| 32 | 105,30 s | 17,97 Mio/s | 39,32 Mio/s | 2,52 s |
| 64 | 56,32 s | 33,59 Mio/s | 73,51 Mio/s | 1,17 s |
| 128 | 42,24 s | 44,79 Mio/s | 98,02 Mio/s | 1,22 s |

Chaque rétention a retrouvé les 484 370 pages avec **zéro GET et zéro octet
de données distant**. Le cache original a été restauré après chaque campagne
et le HEAD est resté identique. Les dispositifs de production `nbd0`/`nbd1`
sont restés actifs et les seuls slots de test sont restés libres.

L'ordre d'exécution était 32, 8, 64, 16 interrompu, puis 16 et 128. Le premier
16 a rencontré le budget global de 480 secondes après 97,18 secondes ; son
journal reste présent, sans ratio calculé à partir de cette durée partielle.
Le complément a reçu un budget de 1 200 secondes. Les cinq résultats retenus
ont chacun **un seul échantillon**, sur une VM dont la production reste active.

Pour le préchauffage hors ligne de cette fixture, `warm --concurrency 128`
est le meilleur réglage mesuré : **2,49× plus rapide que 32**, **1,33× que 64**
et **5,05× que 8**. Le gain marginal diminue entre 64 et 128 ; cette campagne
ne détermine pas le plafond au-delà de 128 ni l'effet sur d'autres fournisseurs.
Le défaut CLI reste 32. Aucun ratio contrôlé n'est établi avec les mesures
antérieures de 1 185 ou 1 322 secondes, ni avec l'ancien binaire, non exécuté
dans cette campagne.

Rapport consolidé :
`validation/astra/warm/20261010T110940Z-consolidated/report.json`.
Tentatives sources : `20261010T105702Z-53d85a51` et
`20261010T110540Z-cae1bf24`, conservées intégralement dans l'archive expurgée.
