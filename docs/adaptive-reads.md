# Profil sélectionné et lectures adaptatives — 10 octobre 2026

Le profil perf/coût est maintenant produit par `infinidisk2 config`. Le modèle complet se trouve dans [configs/recommended.toml](../configs/recommended.toml), les mesures de cette itération dans [le rapport ciblé](../validation/adaptive/rapport.html). Le binaire qualifié est `2eb31a6845c083d9955bc171fe6c4ccaf7af6e8f927ffd4ebb9ace1f25f01aa5`.

## Choix appliqués

| Réglage | Valeur | Raison et portée |
|---|---:|---|
| `checkpoint_seconds` | 5 s | Publication distante asynchrone ; intervalle de déclenchement, pas RPO maximal garanti. |
| `segment_mib` | 32 Mio | Compromis entre nombre de PUT, coût de rotation et espace local. Le sweep historique sans compaction passait de 68 à 42 opérations A entre 8 et 32 Mio ; 64 Mio ne descendait qu'à 38. Ce lot n'est pas une facture mensuelle. |
| `compact_checkpoints` | false | Évite les petits objets par shard et les relectures dues au changement de références. Réduit le travail au checkpoint, mais conserve davantage de versions/bytes sur S3 jusqu'à compaction/GC. |
| `logical_cache`, `async_cache`, `fast_local_reads` | true | Cache SSD par version de page, remplissage asynchrone et travail local groupé par requête. |
| `wal_commit_records`, `wal_fixed_size`, `checkpoint_pipeline`, `selective_sync` | true | Marqueurs durables dans le WAL, segments préparés et synchronisation des segments modifiés. `FLUSH/FUA` garde sa durabilité locale. |
| `sync_data_only`, `wal_writev` | true | `fdatasync` pour les données et écriture vectorisée des enregistrements ; barrières de métadonnées conservées. |
| `adaptive_reads` | true | Choix de plages de 16 ou 256 Kio selon la densité physique des pages manquantes d'une requête. |
| `read_extent_kib` | 64 Kio | Chemin de repli non adaptatif, notamment lecture des bordures avant écriture partielle et certaines opérations hors ligne. |
| `paged_index` / `max_index_mib` | true / 128 Mio | Index paginé avec budget des cartes résidentes ; ni plafond du RSS total ni suppression des GET d'index au démarrage. |
| `memory_cache_mib` / `disk_cache_mib` | 128 / 4096 Mio | Point de départ explicite ; augmenter le SSD pour couvrir l'ensemble actif. Les comparaisons utilisent 64/128 Mio identiques, pas ce budget de déploiement. |
| `hot_wal_mib` / `max_pending_mib` | 64 / 1024 Mio | Limiter les journaux récents et en attente. Dimensionner l'espace physique pour les segments préparés, l'index, les caches et la marge hôte. |
| `max_inflight` / `cache_queue_mib` | 128 / 16 Mio | Concurrence des requêtes et charge de remplissage bornées. |
| `flush_batch_us` / `wal_preallocate` | 0 / false | Aucun délai ajouté aux commits ; réservation supplémentaire non retenue. |
| `ublk_fast_path` | true | Active les workers persistants lorsque le transport ublk est choisi. NBD reste utilisable ; le transport ne change pas automatiquement. |
| `aligned_wal` / `generation_mode` | false / false | Pas de nouveau format aligné ni de relâchement de durabilité pour ce profil. |
| `warm --concurrency` | 128 par défaut | Le sweep précédent mesurait 42,24 s à 128 contre 105,30 s à 32. Même nombre de GET, une mesure par valeur ; option explicite pour réduire la concurrence sur une autre machine. |

Le chargement d'une ancienne configuration conserve les **anciens défauts des champs omis**. L'optimisation n'effectue donc pas de migration implicite d'un volume existant. `config --legacy` écrit ces défauts historiques ; les deux commandes refusent d'écraser un fichier. Pour changer une installation existante, arrêter proprement la base et le montage, préserver son identité/store/local_dir, appliquer les options choisies, puis reprendre avec le binaire associé. Aucun service de production de la VM n'a été reconfiguré par cette campagne.

## Algorithme adaptatif effectivement implémenté

1. Capturer les références de pages de la requête et tenter le cache SSD vérifié puis le WAL local dans un travail groupé. Les trous logiques restent des zéros seulement si l'index déclare réellement une page absente.
2. Regrouper les misses par segment immutable et région physique de 256 Kio. À partir de 16 pages demandées dans la région, lire une grande plage ; sinon, subdiviser en plages de 16 Kio. Une grande lecture logique fragmentée conserve donc de petites plages.
3. Traiter au plus huit groupes simultanément par requête. Les tailles réseau peuvent inclure 4 Kio supplémentaires pour couvrir une page chevauchant une frontière, tronqués à la longueur du segment.
4. Un verrou commun à la région permet aux petites lectures de réutiliser une grande plage déjà en RAM. Vérifier le CRC de **toutes les pages demandées du groupe** avant d'en installer une en cache ou de renvoyer ce groupe.
5. Conserver la propriété du buffer jusqu'à sa consommation. Un cache RAM nul ne provoque plus un téléchargement par page d'un même groupe. Le cache LRU compte les octets de payload réellement conservés, quelle que soit la taille des plages.
6. Envoyer les pages utiles en un remplissage SSD compact, avec leurs références de version. Le worker fusionne les versions en gardant le bon offset dans le payload ; un vieux remplissage ne devient pas une lecture valide d'une page plus récente.

Cette adaptation mesure la densité **dans la requête courante**. Elle ne suit pas encore un flux séquentiel composé de requêtes indépendantes de 4 Kio et ne dispose pas d'un contrôleur de coût/p99. Les GET des bordures d'écriture et le préchauffage explicite gardent leur propre chemin. Les compteurs `adaptive_small_gets`, `adaptive_large_gets`, `remote_gets`, `remote_bytes` et `range_cache_bytes` sont exposés périodiquement et au dernier checkpoint d'un arrêt normal.

Le budget LRU concerne les payloads retenus. Les buffers de requêtes, références, allocations HTTP/TLS, remplissages et groupes en cours s'y ajoutent. Huit groupes par requête n'est pas une limite globale de huit GET ; les limites du transport encadrent les requêtes. Aucun plafond strict du RSS n'est revendiqué.

## Résultat lectures : bénéfice et compromis

Comparaison directe HTTPS sans proxy, même binaire candidat et mêmes budgets : fixe 64 Kio versus adaptatif. Deux répétitions dans l'ordre A–B–B–A, cache local neuf avant chaque charge, même dataset immutable vérifié CRC32C. Aléatoire : 4096 lectures de 4 Kio, QD32, 16 Mio utiles. Séquentiel : 256 Mio, requêtes de 1 Mio, QD16. Les caches du fournisseur et de l'hôte partagé ne sont pas contrôlés globalement.

| Médiane de deux passages | Fixe 64 Kio | Adaptatif | Lecture |
|---|---:|---:|---|
| Aléatoire, IOPS | 1210,08 | 1517,17 | +25,4 % observé, forte variabilité : fixe 798–1622, adaptatif 1444–1590 ; pas de gain causal stable démontré. |
| Aléatoire, GET de données réussis | 3250 | 3646 | +12,2 % : le gain de bytes n'est pas un gain de coût des opérations B. |
| Aléatoire, téléchargement | 215,77 Mio | 71,19 Mio | −67,0 %. |
| Séquentiel, débit | 80,19 Mio/s | 105,81 Mio/s | +32,0 % observé. |
| Séquentiel, GET de données réussis | 4096 | 1024 | −75 %. |
| Séquentiel, téléchargement | 271,94 Mio | 259,94 Mio | −4,4 %. |
| Séquentiel, p99 de complétion | 352,32 ms | 419,43 ms | +19,0 %, régression de latence de queue à traiter séparément du débit. |

Ces compteurs comptent les réponses de données réussies dans le moteur : ils excluent HEAD, index, tentatives ratées et retries internes. Ce ne sont **pas les totaux facturables A/B**. Avec le tarif Standard Tigris vérifié le 10 octobre (B : 0,50 $/million), le seul sous-total GET du lot séquentiel passe de 0,002048 $ à 0,000512 $ ; celui du lot aléatoire de 0,001625 $ à 0,001823 $. L'egress gratuit n'annule pas le temps CPU/réseau des téléchargements. Franchise et stockage exclus. [Tarifs officiels](https://www.tigrisdata.com/pricing/).

## Validation ciblée

Les preuves du [build](../validation/adaptive/build/manifest.json), des [lectures](../validation/adaptive/reads/report.json) et de [PostgreSQL/reprise](../validation/adaptive/selected/report.json) identifient les sources, options et binaires. Les tests ciblent la nouvelle logique de regroupement, la corruption, les remplissages SSD, les budgets mémoire et la compatibilité des configs. Les régressions moteur/transport, Clippy et le build ublk sont exécutés une fois. Une première exécution parallèle des tests NBD a rencontré un verrou local encore détenu à la réouverture ; le log d'échec est conservé, les deux tests passent en ordre isolé sans changement du moteur. La cause précise n'est pas établie.

Le mode `validate_vm.py --checks-only` supprime les timings fio redondants, garde les vérifications CRC, les crashs SIGKILL et la reprise depuis S3, puis vérifie intégralement les 256 Mio restaurés. PostgreSQL compare l'ancien profil compact 8 Mio et le nouveau profil, trois passages de 15 s chacun, à ressources égales. L'ensemble du profil est comparé : on ne peut attribuer un éventuel changement de TPS au seul lecteur adaptatif. Pas de nouveau bench MySQL/native/ZeroFS dans cette itération. Les anciennes campagnes restent consultables et datées.

Une erreur d'index lors de la lecture d'une bordure d'écriture partielle marque maintenant le moteur en échec fermé, comme les autres mutations. Cette harmonisation corrige l'exception documentée dans la revue Astra précédente. Les tests SIGKILL ne qualifient ni coupure électrique réelle ni panne complète du fournisseur.

## Suite des recherches, par priorité

**1. Réutiliser l'index distant vérifié au redémarrage chaud.** `Engine::open` reconstruit aujourd'hui les shards à chaque ouverture ; `Index::paged` crée volontairement une session scratch neuve. La fixture PostgreSQL précédente faisait 15 B au restart chaud : un HEAD et quatorze index, zéro GET de données. Prochaine implémentation proposée : un cache des **objets d'index distants immuables** identifié par volume + clé/hash du shard, relire HEAD une fois, valider les copies locales puis rejouer le WAL comme aujourd'hui. Ne pas réutiliser directement le scratch mutable comme autorité. Avec tous les objets déjà présents et inchangés, objectif testable : 15→1 GET dans cette fixture, pas zéro vérification distante. Tests uniques : restart inchangé, HEAD changé, index local corrompu, WAL postérieur.

**2. Ajouter une limite globale en octets aux téléchargements et une adaptation de concurrence.** Le séquentiel progresse en débit mais son p99 augmente. Un sémaphore global en octets empêcherait plusieurs grosses requêtes de multiplier les buffers ; un contrôleur pourrait réduire le nombre de groupes actifs quand p99/erreurs augmentent. Conserver une plage de taille/concurrence bornée et une hystérésis, ne pas retarder les hits SSD. Pour les suites de petites requêtes, mesurer la localité par flux sans mélanger les clients. Valider sur un seul scénario alternant phases aléatoires/séquentielles et une saturation concurrente ; accepter selon débit **et p99**, pas uniquement un record de MiB/s.

**3. Regrouper les pages compactées de plusieurs shards dans un même objet.** Le sweep précédent comptait 65 A quel que soit le WAL 8/16/32/64 Mio avec compaction : 32 objets de données, 32 index, un HEAD. Augmenter le WAL ne corrige pas cette granularité. Un pack immutable de 8–32 Mio traversant plusieurs shards réduirait les PUT de données ; les 32 PUT d'index resteraient. Pour ce lot, passer de 32 objets de données à un seul donnerait au mieux 34 A, soit −47,7 % ; c'est une borne arithmétique, pas un résultat ni une promesse générale. Conserver les CRC de page et la publication objet→index→HEAD conditionnel, avec mémoire bornée et récupération après interruption. [Principe des écritures conditionnelles S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).

**4. Décider la compaction selon le coût complet.** Mesurer le ratio pages vivantes/bytes WAL, le nombre de petits shards et le coût des relectures. Déclencher une compaction regroupée seulement si les économies de stockage sur l'horizon retenu dépassent PUT/GET et travail CPU. Le coût par 1K/10K SQL doit inclure la fin des uploads différés ; diviser uniquement les appels survenus pendant pgbench sous-estimerait le total. Les compteurs moteur actuels ne suffisent pas pour cette facture : réutiliser le proxy d'opérations une fois sur un lot fixe lors de la qualification du prochain changement.

Ces quatre suites sont des propositions issues du code et des preuves actuelles, pas des fonctions déjà livrées. La priorité recommandée est l'index distant conservé sur SSD : elle vise directement le redémarrage chaud demandé, sans modifier le format des données ni la sémantique des commits.
