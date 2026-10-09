# InfiniDisk2 — spécifications, décisions et review technique

Livraison du 9 octobre 2026. Sources principales : `src/engine.rs`, `src/wal.rs`, `src/nbd.rs`, `src/linux.rs`, `src/store.rs`, `src/cache.rs`, `src/config.rs`. Les choix ci-dessous décrivent le code livré ; les recommandations en fin de document ne sont pas présentées comme des fonctionnalités déjà implémentées.

## Objectif et domaine de panne

Un volume bloc Linux utilisable par ext4 et une base de données, avec chemin chaud local et restauration distante cohérente. La priorité est l'intégrité, puis la performance ; un retour à une génération antérieure lors de la perte totale du stockage local est acceptable. La durabilité locale et la durabilité S3 sont deux frontières distinctes.

Les événements couverts par le modèle sont : arrêt du processus, écriture locale interrompue, lecture corrompue, indisponibilité S3, réponse ambiguë à une publication conditionnelle et reprise administrative sur un autre hôte. Un disque qui ment sur `fsync`, des suppressions manuelles d'objets, deux hôtes sans fencing ou une base configurée sans protection sortent du contrat démontré.

## Chemin d'une écriture

1. Valider les bornes et la taille ; attendre si le budget de journal en attente est plein.
2. Sous un mutex commun, préparer les pages de 4 Kio. Une écriture alignée ne nécessite aucune lecture des anciennes pages. Seules les extrémités d'une écriture partielle nécessitent un read-modify-write.
3. Attribuer la prochaine séquence, écrire un en-tête de record puis son payload dans le WAL. L'index mémoire change uniquement après le succès des écritures du record.
4. Répondre pour une écriture ordinaire. Pour FUA, demander la synchronisation globale avant réponse.
5. Lors d'un FLUSH, capturer une séquence et des descripteurs stables, synchroniser les fichiers du WAL, puis synchroniser un marqueur de durabilité à deux slots. Des demandes simultanées sont regroupées autour d'une frontière commune.

Une erreur d'append empoisonne le moteur : on ne poursuit jamais un journal après un record partiellement écrit. Une erreur de synchronisation ne reçoit pas de succès NBD. Le regroupement n'introduit pas de délai artificiel de 1 ms ; il laisse les autres tâches prêtes avancer avant la capture du groupe.

## Format local

`identity.json` contient l'UUID du volume, l'UUID d'écrivain et la taille virtuelle. `LOCK` porte un flock exclusif pendant toute la vie du moteur. `durable` contient deux slots de 16 octets : magic `IDWM`, séquence LE u64, CRC32. Les slots alternent indépendamment de la parité de la séquence.

Les fichiers `wal/<séquence_initiale sur 20 chiffres>-<uuid>.wal` commencent par 64 octets : magic `IDWAL001`, UUID segment, UUID volume, réserve et CRC32 d'en-tête. Les records ont 32 octets : magic, longueur de payload, séquence, première page logique, nombre de pages et CRC32 de l'en-tête utile plus du payload. Tous les entiers de stockage sont little-endian ; les messages NBD sont big-endian.

`WRT1` porte des pages complètes. `ZER1` décrit une plage entièrement nulle, sans payload : un TRIM de 512 Mio coûte un record, et non 512 Mio de zéros. Les bordures de secteur non alignées sur 4 Kio passent par le chemin d'écriture partielle. L'absence d'une référence dans l'index signifie une page logique nulle.

Chaque référence de page contient UUID de segment, offset physique, CRC32 de page et longueur finale du segment distant. Le CRC de record protège également la séquence, l'adresse et la longueur ; les contrôles de bornes précèdent l'allocation du payload.

## Récupération locale

Lire et vérifier `HEAD` et les shards, puis rejouer les records locaux de séquence supérieure à la frontière distante, dans l'ordre. Une séquence manquante est une erreur. Un en-tête/payload final incomplet peut être tronqué ; un record complet invalide ne devient pas une page zéro. La séquence retrouvée doit couvrir le marqueur de durabilité valide.

Les checkpoints scellent des segments entiers. Un segment dont la séquence initiale est déjà couverte par `HEAD` est une copie de cache, même s'il est encore dans le dossier WAL. Sa corruption peut donc être éliminée sans sacrifier des écritures non publiées. Un segment plus récent reste une donnée faisant autorité et échoue strictement en cas de corruption. Au redémarrage, les segments déjà publiés servent de cache chaud borné ; les autres restent en attente de publication.

## Transaction de publication S3

Les clés sont contenues dans un préfixe dédié au volume : `segments/<uuid>`, `indexes/<shard>/<uuid>` et `HEAD`. Aucun parcours des objets arbitraires d'un bucket pour les exposer comme fichiers.

1. Synchroniser localement, sceller le segment actif et capturer une séquence, les fichiers scellés et les shards modifiés.
2. Continuer les nouvelles écritures dans un autre segment.
3. Envoyer les segments, avec quatre transferts concurrents au maximum.
4. Envoyer uniquement les index de shards modifiés. Un shard couvre 4 096 pages, soit 16 Mio logiques. Les index sont sérialisés et protégés par SHA-256.
5. Remplacer `HEAD` conditionnellement avec le jeton ETag/version de la génération précédente. `HEAD` contient format, UUID, taille, propriétaire, génération, séquence et répertoire de shards. Son payload est également protégé par SHA-256.
6. Déclasser les segments publiés en cache chaud local. Seul le dépassement du budget de cache provoque leur éviction.

Le pointeur racine ne référence jamais un nouvel objet avant le succès de son upload. En cas d'échec, conserver les fichiers et réunir les shards capturés avec les nouvelles modifications. Les UUID permettent de réessayer un upload avec les mêmes octets ; aucune clé de segment n'est réutilisée pour un contenu différent par le moteur.

Si le remplacement conditionnel renvoie une erreur ambiguë, relire `HEAD`. Si ses octets sont exactement ceux du candidat, considérer la publication comme réussie et adopter son jeton. Si la génération a changé pour un autre candidat, échouer et empoisonner le moteur ; si son état reste inconnu, conserver le WAL.

Le backend `file://` implémente ses remplacements conditionnels avec flock, comparaison SHA-256, fichier temporaire, fsync et rename atomique ; la librairie `LocalFileSystem` ne fournit pas directement l'update CAS requis. Les objets locaux de test sont synchronisés avant `HEAD`, pour conserver le même ordre de durabilité.

## Lectures et cache

Sous un verrou bref, capturer les références et les descripteurs de fichiers locaux. Effectuer ensuite les lectures hors du verrou, avec au maximum 32 pages simultanées par requête.

Ordre : WAL/segments chauds locaux, cache RAM LRU, cache SSD LRU, GET Range S3. Les GET de lecture couvrent par défaut des étendues de 64 Kio, configurables à 16/64/256 Kio, avec une marge de page, car les en-têtes de records peuvent faire traverser une frontière à une page. Des verrous répartis évitent plusieurs téléchargements simultanés de la même étendue. Chaque page effectivement retournée est vérifiée ; une page correcte ne valide pas implicitement toutes ses voisines.

Un cache SSD corrompu est supprimé et relu ; une page de segment scellé endommagée peut être remplacée par sa copie distante vérifiée. Un WAL non publié ne possède pas cette preuve de secours. La RAM et le cache SSD sont des accélérateurs, jamais un critère de durabilité. Le cache SSD ne nécessite pas de fsync.

## NBD et Linux

Fixed-newstyle, replies simples, options EXPORT_NAME, INFO, GO, LIST et ABORT. Les exports acceptés sont vide et `infinidisk2`. Annonce FLUSH, FUA, TRIM, WRITE_ZEROES et MULTI_CONN ; aucun structured reply ni TLS NBD. Le bind doit être loopback.

Un lecteur de socket valide la requête et reçoit son payload ; des tâches concurrentes l'exécutent ; un écrivain sérialise les réponses. Les handles permettent les réponses hors ordre. Les commandes acceptées continuent même si la connexion tombe. Une limite globale de tâches et une réservation de mémoire maintenue jusqu'à l'envoi de réponse empêchent une accumulation de grosses réponses sur des clients lents.

Les clients doivent attendre les réponses aux écritures avant de demander FLUSH ; le serveur couvre globalement les écritures terminées et les caches sont partagés entre connexions, conformément au protocole. Les requêtes ordinaires sont limitées à 8 Mio ; les grandes plages TRIM/WRITE_ZEROES sont traitées sans allocation proportionnelle à la plage. Les flags non supportés reçoivent une erreur explicite.

Le client natif négocie les sockets TCP puis utilise `NBD_SET_SOCK`, `NBD_SET_BLKSIZE`, `NBD_SET_SIZE_BLOCKS`, `NBD_SET_TIMEOUT`, `NBD_SET_FLAGS`, `NBD_DO_IT`, `NBD_DISCONNECT` et `NBD_CLEAR_SOCK`. Il valide `/dev/nbd<number>`, réserve un verrou local et refuse un device déjà attaché. Il ne nettoie un device qu'après avoir réussi sa propre acquisition. L'attachement reste en premier plan afin qu'un superviseur puisse le suivre. Le détachement prend une revendication exclusive noyau (O_EXCL) : un filesystem monté dans un autre namespace ou une partition encore utilisée bloque aussi la déconnexion. Ce cas a été testé avec un montage privé créé par unshare.

## Décisions et conséquences

| Choix livré | Motif | Conséquence / compromis |
|---|---|---|
| Rust, Tokio, moteur autonome | Maîtriser le chemin de données | Complexité du protocole et de la récupération à tester nous-mêmes |
| NBD, client ioctl natif | Utiliser ext4/DB sans réimplémenter POSIX | Deux processus supervisés ; accès root pour le client |
| Pages 4 Kio, zéro lecture pour écriture alignée | Éviter amplification de RMW pour DB | Index par page et coût RAM important |
| WAL SSD append-only | Réponse chaude locale et ordre global | Espace local indispensable et coût fsync |
| FLUSH/FUA réels | Préserver barrières DB et filesystem | Moins rapide qu'un benchmark qui ignore fsync |
| Marqueur local à deux slots | Détecter manque d'écritures annoncées durables | Une synchronisation locale supplémentaire |
| CRC32 page/record ; SHA-256 index/racine | Détecter dommages et erreurs d'adresse | Ce n'est pas une authentification cryptographique des données |
| Publication HEAD par CAS | Commits distants complets et fencing | S3 compatible doit supporter les écritures conditionnelles |
| Shards modifiés seulement | Éviter réécrire tout l'index à chaque tick | Chargement de l'index complet encore nécessaire au boot |
| UUID d'objets immuables | Retries simples et pas d'écrasement d'anciennes données | Des objets orphelins doivent être collectés |
| Cache chaud WAL + cache SSD + cache RAM | Rester local pour le working set | Trois budgets indépendants à dimensionner |
| GET Range par 64 Kio, configurable | Amortir les lectures voisines et le débit séquentiel | Surlecture importante pour random froid ; réglage à optimiser |
| Concurrence globale bornée | Éviter saturation RAM/CPU | Une requête lourde peut consommer plusieurs slots |
| Attente WAL plein, deadline 50 s | Absorber retard S3 sans faux succès | Longue panne : erreurs I/O possibles ; surveiller retard/espace |
| Propriétaire distant permanent | Refuser second écrivain sans reprise explicite | Pas de failover automatique ; fencing externe nécessaire |
| GC hors ligne, fence nil, token durable | Ne pas supprimer pendant une publication | Maintenance avec arrêt ; reprise du GC indispensable après crash |
| Aucun formatage automatique | Empêcher effacement à la reprise | Le premier mkfs est une action séparée de l'opérateur |
| Pas de snapshots ni resize automatique | Limiter les états à valider initialement | Fonctionnalités à concevoir et tester avant ajout |
| Limite d'index RAM conservatrice | Empêcher allocation non bornée | La version n'est pas un moteur multi-To de faible RAM |
| Échec fermé sur données autoritaires invalides | Préférer erreur à données fausses | Disponibilité sacrifiée jusqu'à reprise/réparation vérifiée |

## Review : risques restants et recommandations

**P0 avant usage critique** : campagne de fault injection indépendante au niveau système (arrêt électrique/VM, fsync qui échoue, disque plein, perte de blocs, coupure réseau durant CAS) ; matrice ext4 et DB ; tests longs au-delà de la RAM et du cache. Les SIGKILL et restaurations actuels sont encourageants, pas une preuve générale d'absence de corruption.

**P1 performance** : profiler l'amplification des barrières ext4 → NBD → WAL → marqueur, garder les barrières correctes et diminuer les syncs redondants uniquement avec preuve. Mesurer au moins p50/p99 avec charge PostgreSQL durable identique au disque natif. Le test fio synchrone est environ dix fois moins rapide que le fichier natif sur la VM ; ne pas cacher cet écart derrière les 80k IOPS en lecture chaude.

**P1 capacité** : remplacer le chargement global de l'index par un index paginé sur SSD et un cache de shards borné. La limite actuelle protège la VM, mais elle limite les volumes remplis avec peu de RAM. Ajouter un compacteur transactionnel des segments partiellement vivants ; le GC actuel ne récupère que les objets totalement non référencés.

**P1 froid** : évaluer une adaptation automatique de la taille des GET et un préchargement ciblé des fichiers actifs de la DB. Le cache doit couvrir le working set ; promettre des latences NVMe à froid sur S3 serait incorrect. Distinguer cache froid, cache trop petit et cache entièrement chaud dans tous les tableaux commerciaux.

**P1 exploitation** : exporter des métriques de retard S3, bytes en attente, latences FLUSH et occupation des caches, puis définir une limite RPO/backpressure adaptée au service. Une périodicité de 5 secondes ne suffit pas à promettre 5 secondes de perte maximale. Prévoir une procédure de récupération indépendante des secrets et de la VM perdue.

**P2 fonctionnalités** : snapshots par racines immuables et GC multi-racine, restauration DB testée sur snapshots, resize explicite non destructif, chiffrement applicatif si requis. Ne pas ajouter de montage multi-écrivain sans redesign du verrouillage et une vraie campagne de partitionnement réseau.

## Références primaires

* [Protocole NBD](https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md) — FLUSH, FUA, MULTI_CONN et négociation.
* [Pilote Linux NBD](https://github.com/torvalds/linux/blob/master/drivers/block/nbd.c) — comportement des ioctls et sockets multiples.
* [Journal ext4](https://docs.kernel.org/filesystems/ext4/journal.html) — rôle du journal du système de fichiers.
* [Modes de PUT object_store](https://docs.rs/object_store/0.14.2/object_store/enum.PutMode.html) — création conditionnelle et update CAS.

Le code de ZeroFS a servi de référence pour identifier les compromis du premier essai. Ce moteur a été écrit séparément ; il ne compile ni ne lance ZeroFS et ne réutilise pas son format de stockage.
