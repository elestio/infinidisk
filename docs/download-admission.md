# Admission des téléchargements — 10 octobre 2026

Le limiteur est implémenté et testé, mais **reste optionnel**. Le profil généré et [le modèle recommandé](../configs/recommended.toml) gardent `download_budget_mib = 0`. Le [modèle expérimental](../configs/downloads-experimental.toml) active 8 Mio / 64 groupes. Les [graphiques et preuves](../validation/downloads/rapport.html) montrent les deux mesures individuelles de chaque mode.

## Résultat et sélection

Comparaison A–B–B–A sur le même binaire, données et budgets de cache identiques, accès direct HTTPS au S3 Elestio. Chaque charge repart avec un processus et un cache local neufs. Les caches Linux et fournisseur ne sont pas purgés sur la VM partagée.

| Mesure, médiane de 2 passages | Sans limiteur | Budget 8 Mio | Variation |
|---|---:|---:|---:|
| Séquentiel seul, Mio/s | 85,99 | 103,45 | +20,3 % |
| Séquentiel seul, p99 ms | 408,94 | 413,14 | +1,0 % |
| Séquentiel en charge mixte, Mio/s | 97,36 | 100,39 | +3,1 % |
| Séquentiel en charge mixte, p99 ms | 392,17 | 362,81 | −7,5 % |
| Aléatoire en charge mixte, p99 ms | 152,83 | 130,29 | −14,8 % |

Le critère fixé avant les mesures exigeait un p99 séquentiel amélioré d’au moins 5 %, avec au plus 5 % de perte de débit séquentiel et au plus 5 % de hausse du p99 aléatoire mixte. Le premier critère échoue. Le limiteur n’est donc pas activé par défaut. Ce choix évite de transformer un gain partiel en recommandation générale.

Le premier débit de référence, 71,91 Mio/s, est sensiblement inférieur au second, 100,08 Mio/s ; les deux mesures bornées sont 99,57 et 107,34 Mio/s. L’environnement partagé et la durée courte empêchent d’attribuer un gain stable de 20 % au seul limiteur. Les points individuels sont conservés, sans retrait de la première mesure ni multiplication des essais jusqu’à obtenir un résultat favorable.

Le séquentiel lit 256 Mio par requêtes de 1 Mio à QD16. Le mixte démarre simultanément deux jobs fio sur des régions disjointes : 128 Mio séquentiels à QD16, et 4 Mio de lectures aléatoires de 4 Kio à QD8 dans une région de 128 Mio. Le travail est fixe, mais le chevauchement n’est pas permanent : les jobs ne terminent pas ensemble. Les p99 sont les latences de complétion fio, pas les temps individuels des objets S3.

## Options et portée

| Option | Défaut historique et recommandé | Essai | Fonction |
|---|---:|---:|---|
| `download_budget_mib` | 0 | 8 | Zéro désactive l’admission supplémentaire. Valeurs 1–1024 : budget des payloads de ranges admis, comptés par unités de 4 Kio. |
| `download_max_requests` | 64 | 64 | Plafond des groupes admis, de 1 à 1024, effectif seulement si le budget est activé. |

Le budget est commun aux connexions NBD et aux workers ublk **d’un même moteur/volume**. Plusieurs volumes gardent des budgets distincts. Il couvre les GET en ligne de données, y compris les bordures lues avant une écriture partielle. Les lectures depuis le WAL ou les caches vérifiés ne prennent pas de crédit réseau.

Le préchauffage `warm`, les GET d’index/HEAD, la vérification distante hors ligne, la compaction et les uploads conservent leurs chemins et limites propres. Il ne s’agit pas d’un plafond de tous les appels S3 du processus.

L’adaptation porte sur **la taille des ranges et la charge simultanée**, pas sur une mesure de latence. Aucun AIMD, objectif de p99 ou nombre de threads piloté automatiquement n’est implémenté. Les appels S3 utilisent les futures asynchrones existantes ; une place de téléchargement n’est pas un thread.

## Algorithme et choix

1. Le moteur capture les références des pages et vérifie les caches comme auparavant. Les régions physiques denses restent en 256 Kio et les régions éparses en 16 Kio. Les 4 Kio de recouvrement éventuels sont inclus dans le budget, avant troncature à la fin du segment.
2. Au moment d’un véritable manque, le groupe prend des crédits correspondant à la longueur demandée, arrondie à 4 Kio. Le domaine autorisé est 1–260 Kio ; une longueur anormale échoue au lieu d’attendre indéfiniment un permis impossible.
3. Un huitième du budget, plafonné à 1 Mio, est réservé aux plages de 20 Kio ou moins. Le reste est commun. Un petit transfert tente d’abord le budget commun ; s’il doit attendre, il peut obtenir sa réserve dédiée ou une place dans la file commune.
4. La file commune est FIFO pour ne pas affamer les gros transferts. La réserve évite qu’une petite lecture soit systématiquement bloquée derrière un gros transfert dans cette file. Ce choix découle de la sémantique documentée des acquisitions pondérées du [sémaphore Tokio](https://docs.rs/tokio/1.53.2/tokio/sync/struct.Semaphore.html).
5. Les crédits d’octets sont pris **avant** la place de requête. Des gros transferts en attente d’octets ne peuvent ainsi occuper toutes les places de requêtes et neutraliser la réserve des petits transferts.
6. Les permis sont possédés par des gardes RAII. Annulation, erreur réseau, erreur CRC ou retour normal libèrent les crédits. Dans le chemin groupé, le permis accompagne les pages jusqu’à leur copie dans le buffer appelant. Dans le chemin de repli par page, une copie des 4 Kio utiles évite de conserver tout le range téléchargé chez l’appelant après libération.
7. Le CRC de toutes les pages demandées est vérifié avant installation du groupe. Les références de version du cache SSD, les verrous de récupération des ranges, le WAL et la publication objet → index → HEAD conditionnel sont conservés.

Avec 8 Mio, 7 Mio sont communs et 1 Mio réservés. Au plus **27 ranges de 260 Kio** occupent simultanément le budget commun ; les petites lectures peuvent utiliser plus de places, jusqu’au plafond de 64. Pendant le mixte mesuré, le pic est de 35 groupes et de 7,012 Mio réservés. La réserve a servi 250 puis 251 admissions.

Le verrou existant d’une région physique reste pris avant l’admission afin de mutualiser les lectures du même range. La réserve ne contourne pas ce verrou, ni une collision entre deux régions dans les 256 verrous existants. Ce n’est donc pas une garantie absolue de latence pour toute petite requête.

## Mémoire et visibilité

`status.downloads` expose `enabled`, `budget_bytes`, `small_reserve_bytes`, `max_requests`, `waiting`, `reserved_bytes`, `peak_reserved_bytes`, `active`, `peak_active`, `admissions`, `small_reserve_admissions` et `wait_ns`. `wait_ns` additionne les temps d’attente des admissions terminées ; il ne mesure pas le temps du fournisseur S3. `active` inclut les groupes admis jusqu’à leur consommation, pas uniquement les sockets en train de recevoir.

Lorsque le limiteur est désactivé, ses compteurs d’activité restent à zéro : **la concurrence historique n’est pas mesurée par ces champs**. Le rapport indique « non mesuré », jamais zéro téléchargement.

Le budget compte les payloads réservés pour l’admission. Ce n’est pas un plafond de RSS : allocations TLS/SDK, buffers du transport, cache RAM, copies et tâches de remplissage du cache SSD, métadonnées et pages Linux ont leur comptabilité propre ou s’y ajoutent. En particulier, un travail de cache déjà lancé via `spawn_blocking` peut continuer après annulation de son appelant. Les compteurs de réservations ne comptent pas toutes ces copies.

## Coût S3

Le séquentiel garde exactement 1024 GET de données et 272 566 272 octets reçus dans chacun des quatre passages. Le mixte utilise 1487 GET dans les deux contrôles et 1490/1489 avec le limiteur, soit environ +0,17 % sur les médianes ; l’ordre des remplissages et évictions peut changer.

Le limiteur ne lance ni duplication spéculative ni prélecture supplémentaire. Ces compteurs de données réussies excluent néanmoins HEAD, index, erreurs et retries. Ils ne prouvent pas l’égalité exacte des opérations facturables A/B. Aucun nouveau sweep de coût ni nouvelle projection par 1K/10K SQL n’a été lancé pour cette seule admission de lectures ; les [mesures complètes précédentes](../validation/index-cache/rapport.html) gardent leur périmètre propre.

## Qualification et limites

- Tests de saturation, réserve des petits transferts, annulation d’une attente d’octets ou de requête et interruption de tâche ; aucun permis restant.
- Tests existants de lectures denses/éparses, cache RAM nul, cache corrompu et rejet d’un groupe distant corrompu, désormais avec admission activée.
- Profil généré, anciennes configurations, régressions moteur, formatage, Clippy et build ublk : 30 tests ciblés au build mesuré, puis contrôle ciblé du seul changement de valeur par défaut.
- Une campagne S3 de récupération sur le binaire final avec **8 Mio activés** : 7 contrôles, dont SIGKILL moteur pendant PostgreSQL, vérifications PostgreSQL/ext4 et récupération depuis S3 avec CRC sur 256 Mio.

Les mesures utilisent le binaire identifié dans `validation/downloads/build/manifest.json`. Le binaire final, identifié dans `build-final/manifest.json`, retire seulement la valeur recommandée de 8 Mio pour revenir à 0 ; les chemins Rust d’exécution sont inchangés. Les deux fichiers modifiés entre builds, réglage et test du profil, sont archivés et vérifiés par le renderer.

Cette campagne n’est pas une qualification de panne électrique réelle, un benchmark d’une base froide volumineuse ni une garantie universelle contre la corruption. Les services de production nbd0/nbd1 n’ont pas été reconfigurés. Les anciennes configurations omettant ces champs conservent l’admission historique.

## Recommandation et suite

Conserver `download_budget_mib = 0` pour le profil général. Essayer explicitement 8 Mio / 64 groupes quand la maîtrise des transferts simultanés ou la coexistence de petites et grosses lectures importe ; valider le p99 de la charge concernée avant de généraliser. Utiliser le modèle complet uniquement pour une nouvelle configuration ou reporter les deux options dans celle du volume arrêté, sans changer son identité ni son stockage.

Pour réduire réellement le p99 séquentiel, la prochaine mesure utile est la distribution séparée du temps d’attente d’admission et du temps de réponse S3, par taille de range. Elle permettra de distinguer contention interne et objets réellement lents. Un contrôleur de latence ajouté avant ce diagnostic risquerait de réduire le débit sans supprimer les requêtes lentes. Pas de nouveau benchmark MySQL/PostgreSQL/natif/ZeroFS redondant dans cette itération.
