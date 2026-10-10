# Opérations S3 et projection de coût Tigris

Joseph a ajouté ce critère le 10 octobre 2026 : compter toutes les opérations du volume sur le bucket, en plus du débit et des latences. Les compteurs historiques `remote_gets`, `remote_bytes` et `uploaded_segment_bytes` ne suffisent pas : ils excluent certaines métadonnées et ne constituent pas un relevé complet des tentatives HTTP. Aucun total de requêtes n'est reconstruit artificiellement à partir de ces compteurs.

## Méthode de comptage

La campagne dédiée utilise les binaires déjà qualifiés, sans changement de Rust. Un proxy réservé à la boucle locale transmet les requêtes des seules fixtures de test vers S3 Elestio, sur TLS vérifié. Le signataire garde les vrais identifiants en mémoire. Il limite les appels au bucket et aux préfixes neufs autorisés ; aucun nom de bucket arbitraire, proxy public ou changement DNS global n'est nécessaire.

Chaque tentative envoyée au transport HTTP amont reçoit un identifiant. Le proxy ne réessaie pas lui-même les requêtes et ne suit pas les redirections. Les réessais du SDK sont donc des tentatives supplémentaires. La trace conserve les phases et l'état d'envoi, afin de retrouver aussi une tentative interrompue avant sa réponse. Elle n'enregistre ni autorisation, ni secret, ni clé d'objet, ni valeurs de query string. Le fichier consolidé contient une seule ligne par tentative ; les événements internes de début/envoi/fin ne doivent pas être additionnés comme trois appels.

Le décompte couvre données, index, racines, listes, tests d'existence, publications conditionnelles, multipart et suppressions observés. Une pagination LIST ou un GET de plage constitue un appel distinct. La classe dépend de l'API : télécharger l'objet nommé `HEAD` reste un `GetObject`, différent de la méthode HTTP `HEAD`.

« Envoyée au transport » ne prouve pas que le fournisseur a reçu ou facturé une requête. Une erreur réseau sans réponse, ou pendant le transfert du corps après réception des en-têtes, reste isolée comme tentative incertaine. Ces appels peuvent être facturés : ils sont exclus du sous-total connu, pas déclarés gratuits. Les API sans règle connue restent visibles et non tarifées ; elles ne deviennent jamais silencieusement des lectures gratuites.

## Campagne de mesure

Le protocole prépare des fixtures distinctes pour InfiniDisk2 avant Astra, Astra et ZeroFS durable. La référence native n'utilise pas S3. Les contrats de durabilité restent différents : les deux InfiniDisk2 attendent leur WAL local ; ZeroFS attend S3.

PostgreSQL 16 garde scale 2, quatre clients, un CPU et 512 Mio. La charge dédiée exécute un nombre fixe de transactions : 64 par client, soit 256. Les phases distinguent préparation, transactions, publication/arrêt, période inactive et redémarrage à froid. Le coût initial de création de la base reste séparé. Les contrôles SQL et de reprise restent requis. Une fixture fio distincte vérifie aussi données écrites et lecture après reprise.

Le proxy peut modifier la cadence des checkpoints et le temps total. Les TPS de cette campagne ne remplacent donc pas le comparatif de performance sans proxy. La normalisation par million de transactions décrit la charge et son drainage effectivement observés ; elle ne prédit pas un tarif marginal universel. Les opérations d'une période inactive et d'une reprise sont aussi présentées en nombres absolus.

Les mesures deviennent acquises uniquement après production des rapports, vérification des traces et nettoyage des fixtures. Un protocole écrit ou un self-test local ne constitue pas une mesure S3 réalisée.

La reprise à chaud conserve le répertoire local du WAL et du cache SSD, mais redémarre réellement les processus. La reprise à froid adopte le même volume dans un répertoire local neuf. Le rapport distingue l'ouverture du moteur, la disponibilité de PostgreSQL, puis son contrôle d'intégrité ; il compte aussi une lecture CRC32C identique avec fio dans les deux cas. Le démarrage et l'arrêt de PostgreSQL peuvent modifier le checkpoint physique avant la reprise à froid, tout en conservant le jeu de données logique. La RAM du processus n'est pas conservée ; aucun cache Linux global n'est vidé.

Le natif n'a aucun cache S3 à vider : ses deux valeurs sont des redémarrages PostgreSQL témoins sur les mêmes fichiers locaux, sans purge du cache Linux. Elles ne sont pas présentées comme un disque physique froid.

Le cache de données ne suffit pas à garantir zéro requête : InfiniDisk2 relit actuellement HEAD et les fragments d'index à chaque ouverture. Les catégories données, index et autres métadonnées sont donc exposées séparément. Les transactions PostgreSQL et leur drainage restent le seul périmètre normalisé par million de transactions ; les redémarrages ont leur propre coût par événement.

## Coût par 1 000 et 10 000 requêtes SQL

Le scénario [pgbench TPC-B-like de PostgreSQL 16](https://www.postgresql.org/docs/16/pgbench.html) exécute trois UPDATE, un SELECT et un INSERT par transaction, plus BEGIN/END. Le dénominateur « requêtes métier » compte les cinq premières instructions : 256 transactions terminées représentent 1 280 requêtes métier ou 1 792 commandes en incluant les frontières transactionnelles. Le coût de BEGIN/END reste inclus. Les deux dénominateurs SQL et transactionnel sont explicitement séparés.

Le runner fixe le script, la graine et un seul essai par transaction, exclut le vacuum automatique de la fenêtre et vérifie le détail des sept commandes ainsi que l'absence d'échec. Les tableaux montrent les USD bruts estimés par 1K/10K requêtes et par 1K/10K transactions, ainsi que les nombres A/B. Il s'agit d'une normalisation du lot observé avec son drainage, pas d'une prévision linéaire d'un service continu : la fréquence des checkpoints et le regroupement des écritures peuvent changer avec la durée et le débit.

## Recherche du compromis entre taille et coût

Le script `scripts/run_astra_s3_block_sweep.py` conserve le binaire qualifié et les pages logiques de 4 Kio. Il varie séparément les trois plages de lecture déjà supportées (16, 64, 256 Kio), puis la taille des segments (8, 16, 32, 64 Mio). Il ne confond pas ces deux paramètres avec la taille de bloc d'ext4 ou des pages de la DB.

- Lecture : même contenu distant vérifié de 256 Mio ; cache local neuf avant 4 096 lectures aléatoires de 4 Kio, puis à nouveau neuf avant une lecture séquentielle complète. Trois passages, ordre tournant équilibré pour les plages. Les CRC sont vérifiés pendant les lectures ; RAM moteur 64 Mio et SSD 128 Mio.
- Écriture : volume neuf par passage ; 256 Mio d'écritures séquentielles vérifiées, puis 4 096 écritures aléatoires de 4 Kio avec fsync dans une zone distincte. Le comptage inclut le drainage complet. Le jeu de 256 Mio est ensuite vérifié depuis S3 dans un répertoire local neuf. Trois passages par taille, avec et sans compaction, ordre tournant ; les deux modes sont conservés séparément.

Les traces conservent A/B, octets, temps d'ouverture et de drainage ; fio conserve débit, latence de complétion et latence des fsync séparément. Une plage plus grande peut éviter des GET futurs au prix de surlecture et de pollution du cache ; un segment plus grand peut réduire les PUT sans réduire les mises à jour d'index ou de HEAD. Ce sont des hypothèses à mesurer, pas des gains annoncés.

Le « sweet spot » désigne un compromis parmi les valeurs réellement testées, pour chaque charge. Ce diagnostic sur bloc brut ne remplace pas le comparatif PostgreSQL : il n'attribue pas artificiellement chaque I/O fio à une requête SQL. Le proxy influence aussi les timings. La lecture est explorée séparément de la matrice taille des segments × compaction ; l'interaction complète lecture/écriture n'est pas qualifiée par ce balayage. Avec compaction, les objets sont regroupés par shard de 4 096 pages (16 Mio utiles au plus) : la taille du WAL ne règle pas directement celle de ces objets S3.

Une seconde campagne, `scripts/run_astra_s3_block_direct.py`, confirme les temps sans proxy, directement entre le moteur Rust et S3. Elle reprend les trois plages de lecture et le même contenu distant, avec trois passages et des caches locaux neufs. Pour les écritures, elle garde toujours les profils compact 8 Mio et sans compaction 32 Mio, puis ajoute le réglage de coût minimal de chaque mode du balayage instrumenté ; les égalités sont départagées par p99 fsync puis taille. Le segment intermédiaire permet d'examiner le gain marginal du plus gros segment. La sélection précède tout résultat direct. Ce sous-ensemble ne prétend pas trouver l'optimum absolu de performance des quatre tailles. Aucun compteur HTTP n'est mesuré dans cette seconde campagne : les coûts de la première et les temps de la seconde restent des observations distinctes.

## Résultats : requêtes SQL et reprise

La campagne `3a275589e100` est complète sur le binaire Astra `b39b705f43b6`. Tous les contrôles de reprise passent ; les quatre références conservent exactement 256 lignes d'historique PostgreSQL après les reprises chaude et froide. Les CRC fio passent également. Un seul lot est mesuré par outil, avec proxy.

| Outil | A / lot | B / lot | USD / 1K requêtes métier | USD / 10K requêtes métier |
|---|---:|---:|---:|---:|
| InfiniDisk2 avant Astra | 11 | 0 | 0,00004297 | 0,00042969 |
| InfiniDisk2 Astra | 17 | 0 | 0,00006641 | 0,00066406 |
| ZeroFS durable | 1 625 | 3 751 | 0,00700586 | 0,07005859 |
| Natif | 0 | 0 | 0 | 0 |

Le lot comprend 256 transactions, soit 1 280 requêtes métier, et leur drainage. Les nombres A/B précèdent les exonérations de statuts ; le tarif les applique. Sur ce petit lot chaud, Astra envoie moins d'octets que la baseline (9 007 419 contre 11 226 126), mais publie huit segments compacts au lieu de deux WAL. Les huit PUT d'index et le PUT de HEAD sont identiques : moins d'octets ne signifie donc pas moins d'opérations A.

Une suite possible est de regrouper les petits objets compacts de plusieurs shards dans un même objet distant. Sa taille cible serait séparée de `segment_mib`, qui règle le WAL local. Cette piste conserve le bénéfice de supprimer les versions dépassées tout en visant moins de PUT ; elle n'est pas implémentée ni mesurée dans cette campagne. Elle doit conserver les CRC, des buffers bornés et la publication des objets avant les index puis HEAD.

Pour Astra, jusqu'à PostgreSQL prêt, la reprise chaude prend **0,760 s et 15 appels B** contre **3,116 s et 102 appels (1 A + 101 B)** avec cache vide. Les quinze appels chauds concernent HEAD et quatorze index ; aucun segment de données n'est téléchargé. Coût projeté par événement : **0,0000075 USD** à chaud contre **0,0000555 USD** à froid. La lecture CRC des 64 Mio de la fixture fio requiert ensuite **zéro GET à chaud**, contre **1 024 à froid**. Ouverture, lecture et contrôles restent séparés dans les traces.

La première campagne `87d055ce13bc` a été interrompue à cause d'un accès au système de fichiers qui bloquait l'event loop du proxy. Le harnais a été corrigé et un test de réentrance ajouté avant de reprendre toute la campagne avec de nouvelles fixtures. Ses durées incomplètes sont exclues ; l'incident et l'arrêt ciblé du moteur de test sont archivés, sans les présenter comme un test de résistance aux pannes du moteur.

## Résultats du balayage instrumenté

La campagne `5b6f051e1dd0` termine ses **34 scénarios** : un jeu de lecture commun, neuf reprises de lecture et vingt-quatre scénarios d'écriture/relecture distante. Les trois passages de chaque réglage passent leurs CRC et leur nettoyage. Les 553 fichiers de preuve sont vérifiés par SHA-256 ; les sommes sont recalculées depuis les traces HTTP.

| Plage S3 | GET aléatoires médians / 4 096 I/O | Mio reçus / 16 Mio utiles | GET séquentiels médians / 256 Mio | Sous-total USD séquentiel |
|---|---:|---:|---:|---:|
| 16 Kio | 3 647 | 71,21 | 17 546 | 0,008192 * |
| 64 Kio | 3 250 | 215,77 | 4 096 | 0,002048 |
| 256 Kio | 3 007 | 763,34 | 1 024 | 0,000512 |

\* Le séquentiel 16 Kio comporte 1 145 à 1 309 transferts interrompus après réception du statut 206, puis réessayés. La médiane du sous-total devient **0,008773 USD** si toutes ces tentatives incertaines sont facturées en B. Elles ne sont pas gratuites par hypothèse. Les autres lignes de lecture ne contiennent pas de tentative incertaine. Ces perturbations et le proxy interdisent de prendre ce tableau pour un chronométrage direct ; une seconde campagne vérifie les temps sans proxy.

Passer de 64 à 256 Kio divise donc par quatre les GET séquentiels sur ce contenu. En aléatoire, l'économie est seulement de **7,48 %** et les octets téléchargés sont multipliés par **3,54**. Les grandes plages augmentent la surlecture lorsque la page demandée est isolée ; l'egress gratuit n'élimine pas le temps réseau ni le travail de cache.

| Taille du WAL | Appels A avec compaction | USD / lot compact | Appels A sans compaction | USD / lot sans compaction |
|---|---:|---:|---:|---:|
| 8 Mio | 65 | 0,000325 | 68 | 0,000340 |
| 16 Mio | 65 | 0,000325 | 51 | 0,000255 |
| 32 Mio | 65 | 0,000325 | 42 | 0,000210 |
| 64 Mio | 65 | 0,000325 | 38 | 0,000190 |

Les comptes sont identiques dans les trois passages, sans B ni erreur de transport sur la fenêtre écriture + drainage. Elle contient 256 Mio séquentiels puis 4 096 écritures aléatoires avec fsync. Tous les profils publient **32 index et un HEAD**. Le mode compact publie aussi 32 segments, indépendamment de la taille du WAL ; sans compaction, les segments passent de 35 à 18, 9 puis 5. Le plancher de métadonnées explique pourquoi doubler de 32 à 64 Mio ne retire que quatre PUT, soit **9,52 %** des appels du lot.

Sur cette charge qui réécrit peu les mêmes pages, les octets envoyés sont proches : 275,57 Mio avec compaction contre environ 275,77 Mio sans compaction. Ce résultat ne prédit pas l'espace stocké sur une DB qui écrase continuellement les mêmes pages. Les anciens objets restent présents jusqu'au GC ; le coût du stockage et la rétention ne sont pas inclus dans ces frais de requêtes.

## Confirmation sans proxy et choix pratique

La campagne `cfcf836f379c` termine **18 scénarios**, tous avec CRC et nettoyage réussis ; ses 253 fichiers de preuve et ses sources correspondent à leurs empreintes. Les coûts du tableau précédent et les temps ci-dessous viennent de passages distincts. Aucun total HTTP n'est inventé pour cette seconde campagne.

| Plage S3 | IOPS aléatoires médians | p99 aléatoire ms | Séquentiel médian Mio/s | p99 séquentiel ms |
|---|---:|---:|---:|---:|
| 16 Kio | 1 723,18 | 84,41 | 2,05 | 17 112,76 |
| 64 Kio | 601,12 | 935,33 | 37,00 | 4 328,52 |
| 256 Kio | 359,05 | 3 070,23 | 97,97 | 354,42 |

La variabilité est importante : les IOPS aléatoires des trois passages à 64 Kio sont 601,12 / 1 471,26 / 415,12, et ceux à 256 Kio sont 359,05 / 598,83 / 133,78. Les graphiques affichent les minima et maxima. Le ralentissement séquentiel à 16 Kio persiste sans proxy, avec données correctes ; cette campagne ne localise pas sa cause entre moteur, SDK, réseau et fournisseur. Il ne faut donc pas attribuer entièrement les mauvaises durées instrumentées au compteur HTTP.

| Écriture directe | IOPS avec fsync médians | p99 fsync médian ms | Lot complet médian s |
|---|---:|---:|---:|
| Compact, WAL 8 Mio | 3 494,88 | 0,350 | 5,329 |
| Sans compaction, WAL 32 Mio | 3 506,85 | 0,350 | 4,911 |
| Sans compaction, WAL 64 Mio | 3 330,08 | 0,354 | 4,845 |

Recommandation parmi les valeurs testées :

- **Lecture froide aléatoire stricte : 16 Kio** est le meilleur débit observé, malgré davantage de GET. Ce choix convient mal au séquentiel mesuré.
- **Lecture séquentielle : 256 Kio** réduit nettement les GET et fournit le meilleur débit direct de ce balayage.
- **Écritures sans compaction : 32 Mio est un compromis raisonnable**. Il retire 38,24 % des appels A face à 8 Mio ; 64 Mio économise encore quatre PUT par lot, sans gain d'IOPS observé dans la confirmation directe, et augmente la réserve physique du WAL. Le minimum d'appels mesuré reste 64 Mio.
- **Avec compaction, grossir le WAL n'économise aucun appel dans ce lot**. Le profil 8 Mio conserve donc son intérêt ; la taille des objets compacts doit être traitée séparément.

Il n'existe pas ici de taille unique gagnante pour une DB mêlant petits accès, scans et checkpoints. Une future politique adaptative 16 Kio pour les misses isolés et 256 Kio pour les séquences est une piste à tester ; le moteur livré conserve un réglage global explicite. Les profils des benchmarks PostgreSQL/MySQL restent inchangés. Ces essais fio ne qualifient pas les nouvelles tailles sous crash ni leurs performances SQL, et ne justifient pas de désactiver la compaction pour toutes les bases.

## Ordre de priorité performance / coût

1. **Conserver les pages actives sur SSD local.** Dimensionner le cache sur le jeu réellement accédé, puis mesurer le taux de misses, le p99 et la pression disque. Le bénéfice recherché combine latence et GET évités ; le prix du SSD et son usure doivent entrer dans le coût total, sans prétendre qu'un gros cache est rentable pour toute charge.
2. **Garder le fsync local et les checkpoints S3 asynchrones pour le profil DB principal.** Les preuves de reprise couvrent ce contrat. La cadence nominale de cinq secondes n'est pas une borne garantie de perte en cas de disparition du disque local ; le retard effectif doit être surveillé. Le mode génération reste une option de produit distincte.
3. **Qualifier le WAL 32 Mio sans compaction sur les charges SQL avant de changer leur profil.** Il offre ici la majorité du gain d'appels A de 64 Mio, avec moins de réserve locale. Le coût total doit inclure les versions S3 retenues jusqu'au GC ; le mode sans compaction n'est pas automatiquement le moins cher en stockage.
4. **Développer ensuite les lectures adaptatives et le regroupement des objets compacts.** Les premières visent la surlecture et le p99 ; le second vise les PUT, dix fois plus chers que les B dans ce barème. Garder des index locaux vérifiés au redémarrage vise surtout le temps d'ouverture et le nombre de métadonnées relues. Ces pistes restent à implémenter.

Pour une DB active sur Tigris, la classe Standard est le point de départ proposé : les classes froides introduisent des frais de récupération ou des contraintes de rétention. L'egress gratuit ne supprime ni ces frais éventuels ni les limites de débit. La projection actuelle ne remplace pas un relevé de facturation sur un bucket Tigris réel.

## Tarification

La [grille Tigris vérifiée le 10 octobre 2026](https://www.tigrisdata.com/pricing/) indique **5 USD par million d'appels A** et **0,50 USD par million de B** ; DELETE/CANCEL et l'egress sont gratuits. La franchise mensuelle de 10 000 A et 100 000 B s'applique au compte entier, jamais séparément à chaque essai. Les coûts affichés sont bruts, avant franchise, et limités aux requêtes. Ils excluent stockage, éventuels frais de récupération, notifications et taxes.

Le [barème archivé](../validation/astra/s3-operations/pricing-tigris-2026-10-10.json) conserve aussi les statuts HTTP explicitement exonérés. Les opérations multipart non nommées individuellement sont estimées en A, avec cette hypothèse signalée. Les erreurs sans réponse restent hors du sous-total tarifé. Ce calcul transpose les appels observés chez Elestio : ce n'est pas une facture Tigris.

## Reproduction du calcul

`scripts/s3_request_pricing.py` classe les API et agrège les traces expurgées :

```sh
python3 scripts/s3_request_pricing.py --self-test
python3 scripts/s3_request_pricing.py --events /chemin/requests.jsonl
```

`--transactions N` normalise un sous-ensemble de traces correspondant exactement aux N transactions achevées et à leur drainage. Il ne faut pas l'appliquer à une trace regroupant initialisation et plusieurs charges sans préciser ce périmètre. Le total des classes, les statuts, les phases, les hypothèses et les requêtes non tarifées restent consultables dans le JSON.

Après validation et arrêt complet de chaque fixture S3, ses seuls répertoires locaux jetables sont supprimés pour borner l'occupation disque de la VM. Le contenu distant de test, les configurations et les preuves textuelles sont conservés. Aucun volume de production ni fixture d'une autre campagne n'est concerné.
