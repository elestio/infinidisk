# Index paginé et snapshots de génération

Implémentation : `src/index.rs`. Le moteur choisit un backend mémoire ou un backend paginé sur le SSD local. Les deux partagent la même organisation : un shard contient au plus 4 096 références de pages de 4 Kio, soit 16 Mio logiques. La représentation est un `BTreeMap<u64, Ref>` partagé par `Arc`, avec copie lors de la première modification d'une version capturée par un checkpoint.

## Contrat d'intégration

```rust
let mut index = PageIndex::memory(max_bytes);
// Ou :
let mut index = PageIndex::paged(&scratch_root, max_bytes)?;

index.replace_shard(id, verified_remote_map)?;
index.insert(page, wal_reference)?;
let reference = index.get(&page)?; // référence possédée, pas d'emprunt interne
index.remove(&page)?;
let dirty_shards = index.remove_range(first_page..end_page)?; // grand TRIM borné par shard
index.check_additional_pages(number_of_new_pages)?;

// Sous le verrou de l'état moteur, après choix du frontier/rotation :
let snapshots = index.snapshot(dirty_ids.iter().copied())?;
// Ensuite, hors verrou, avec concurrence bornée :
for (id, snapshot) in snapshots {
    let map = snapshot.load()?;
    // Résoudre segment_len via les longueurs figées de la génération,
    // sérialiser, calculer SHA-256 et publier le shard.
}
```

`get`, `insert`, `remove`, `remove_range`, `contains_key`, `range`, `load_shard` et `entries` peuvent échouer. Toutes les erreurs doivent remonter ; une erreur ne doit jamais être traduite par une page nulle. Une erreur de mise à jour après ajout au WAL doit placer le moteur en échec fermé : il ne peut pas continuer avec un index partiellement mis à jour. Le contrôle des pages supplémentaires s'effectue avant la mutation du WAL lorsque le backend mémoire est utilisé.

`replace_shard` vérifie le nombre de pages et leur appartenance au shard. Le moteur reste responsable de la validation du hash S3, de la taille du volume, des identités et des limites des références de segments. Les références locales peuvent conserver `segment_len = 0` ; le module n'effectue aucun parcours global pour mettre à jour leur longueur.

`range` retourne des références possédées pour une plage et ne convient pas aux grandes plages en ligne. `remove_range` supprime une plage en ne chargeant qu'un shard à la fois, avec copie du shard uniquement si un snapshot retient sa version précédente. Il retourne les numéros des shards effectivement modifiés, sans liste de références ou de clés par page. Le TRIM en ligne et son rejeu WAL utilisent cette méthode. `entries` matérialise tout l'index : cette méthode est réservée aux traitements hors ligne dont le budget mémoire le permet. `load_shard` retourne un `Arc` à libérer après usage ; retenir arbitrairement ces Arcs contournerait le budget du cache.

## Budget mémoire et capacité

Le backend mémoire applique le plafond historique de 128 octets conservateurs par référence. Il est partitionné en shards pour borner chaque copie lors d'un checkpoint.

Le backend paginé réserve une unité de budget de 512 Kio par shard résident, même si ce shard contient peu de pages. Son LRU contient au plus `floor(max_bytes / 512 KiB)` shards. Le minimum est un shard. Il n'applique plus de plafond par page allouée à l'ensemble du volume.

Les postes sont distincts et exposés par `stats()` :

| Poste | Borne ou croissance |
| --- | --- |
| Cartes de pages résidentes | Au plus le budget configuré, arrondi vers le bas à 512 Kio |
| Versions résidentes figées par le checkpoint | Au plus un second budget résident ; souvent partagé avec le premier avant réécriture |
| Chargement d'un shard froid | Une carte transitoire et un buffer de décodage de 512 Kio maximum chacun |
| Shards de snapshot chargés pour publication | À borner dans le moteur par la concurrence choisie ; par exemple quatre shards à la fois |
| Annuaire des shards non vides | Une entrée par 16 Mio logiques ; estimation conservatrice de 128 octets par shard |
| Descripteurs de snapshots | Une petite structure par shard capturé ; aucun descripteur de fichier ouvert conservé |

Pour 1 Tio entièrement alloué, l'annuaire contient 65 536 entrées, soit 8 Mio dans cette comptabilité conservatrice. Les références individuelles ne restent en RAM que dans les shards résidents. Le plafond décrit les cartes de l'index, **pas le RSS complet du processus** : allocateur, cache de pages Linux, métadonnées HEAD, files du moteur et buffers ont leurs propres coûts. Les mesures de performances doivent compter ces postes.

Un second appel à `snapshot` échoue tant qu'un handle de la génération précédente subsiste. Cette contre-pression évite une accumulation non bornée de versions de checkpoints. Le moteur doit libérer les snapshots après réussite ou abandon de la publication. Un `Arc<PageMap>` explicitement conservé par le code appelant après libération de son snapshot reste à la charge de cet appelant.

### Dimensionnement d'un volume dense

Calculs du format actuel, **pas mesures de durée ni de RSS**. Le volume est
entièrement alloué, avec 4 096 références par shard, des numéros de shards
contigus à partir de zéro, les clés `indexes/<id décimal>/<UUID à tirets>`,
un hash SHA-256 hexadécimal de 64 caractères et un propriétaire présent.
Une sonde locale liée aux bibliothèques compilées vérifie : `Ref` occupe
40 octets en mémoire, `(u64, Ref)` 48 octets, et `Ref` encodée 44 octets.
Bincode encode aussi la clé de page sur 8 octets et la longueur de carte sur
8 octets : un shard plein fait donc **213 000 octets**. Le UUID sérialisé
occupe 24 octets, dont un préfixe de longueur de 8 octets.

| Volume dense | Shards / GET d'index à chaque ouverture | Payloads d'index cumulés | Annuaire paginé, estimation | HEAD sérialisé avec enveloppe |
| ---: | ---: | ---: | ---: | ---: |
| 1 Tio | 65 536 | 13,0005 Gio | 8 Mio | 9 032 983 octets, soit 8,61 Mio |
| 4 Tio | 262 144 | 52,0020 Gio | 32 Mio | 36 327 031 octets, soit 34,64 Mio |

`Engine::load_index` lit encore tous les shards, avec 16 GET concurrents, puis
alimente l'index local (`src/engine.rs:301`). L'index paginé borne la résidence
des cartes ; il ne supprime donc ni ces transferts au démarrage ni l'espace
scratch correspondant. Un shard froid est encore chargé de façon synchrone
sous le verrou d'état (`src/index.rs:610`). Les cartes en mémoire et les
buffers de décodage sont plus coûteux que les octets sérialisés du tableau.

Le HEAD unique est réencodé à chaque publication (`src/engine.rs:1310`) et
refusé au-delà de 64 Mio (`src/engine.rs:173`). Avec les noms et la densité
ci-dessus, le plafond correspond à environ **7,38 Tio** : c'est un exemple
calculé, **pas une limite universelle de taille logique**. Il dépend du nombre
de shards non vides et de la longueur des clés. Un volume très creux peut
dépasser cette taille ; quelques pages réparties sur beaucoup de shards
consomment néanmoins un annuaire et un HEAD importants.

Le cache logique SSD a un annuaire distinct de `max_index_mib` et
`memory_cache_mib` (`src/page_cache.rs:47`). Un cache de 4 Gio contient
1 038 435 emplacements : sa seule liste d'emplacements libres représente au
moins 7,92 Mio à l'ouverture à vide ; plein, les clés et valeurs d'entrées
représentent au moins 47,54 Mio, avant liens LRU, table de hachage, capacités
de vecteurs conservées et allocateur. Avec 64 Gio de cache, ces deux postes
sont respectivement 126,76 Mio et 760,57 Mio. Le fichier de métadonnées de
40 octets par emplacement est déjà inclus dans le budget SSD ; son annuaire
RAM ne l'est pas.

Enfin, `entries()` construit un vecteur global (`src/index.rs:401`) utilisé
hors ligne par `warm`, `compact`, `gc` et `scrub`. Pour 1 Tio dense, les paires
seules demandent **12 Gio de RAM**, avant leurs autres structures. Ces outils
ne deviennent donc pas des parcours multi-Tio à mémoire bornée par la seule
activation de `paged_index`.

## Capture sans sérialiser sous le verrou global

Un shard résident est capturé par clone d'`Arc`. La première écriture ultérieure copie uniquement ce shard, puis les écritures suivantes modifient sa nouvelle version.

Un shard évincé est capturé par un lien physique vers son fichier de cache. Le module remplace toujours les fichiers de cache par écriture d'un nouveau fichier puis `rename` ; il ne les modifie jamais sur place. Le lien conserve donc exactement l'ancienne version, même après mise à jour ou suppression de la version active.

La capture ne recharge aucun shard et n'effectue aucune sérialisation des références. Elle effectue toutefois un appel `link` par shard évincé et alloue les petits descripteurs de capture ; ce coût doit être mesuré pour les générations très nombreuses. L'appel `ShardSnapshot::load` effectue la lecture, la validation et le décodage hors verrou. Il faut le faire sur un worker bloquant si le contexte appelant est un exécuteur asynchrone.

Les liens temporaires sont supprimés à la libération des handles. Une référence partagée conserve le répertoire de session jusqu'à la libération du dernier snapshot disque, même si l'index actif a déjà été détruit.

## Intégrité et récupération

Le cache paginé n'est jamais une source de récupération. Chaque construction crée un nouveau sous-dossier `session-<uuid>` et repart d'un index vide. Les anciens fichiers, même valides, ne sont pas chargés. Le moteur reconstruit les shards depuis son HEAD S3 vérifié, puis rejoue le WAL suivant les règles de durabilité existantes.

Un fichier local contient :

| Champ | Taille |
| --- | --- |
| Magic `IDIDX001` | 8 octets |
| UUID de session | 16 octets |
| Numéro de shard | 8 octets |
| Nombre de pages | 8 octets |
| Taille du payload | 8 octets |
| SHA-256 du payload | 32 octets |
| `BTreeMap<u64, Ref>` encodé en bincode | Au plus 512 Kio |

La lecture vérifie la taille avant allocation, l'identité de session, le numéro de shard, le nombre de pages attendu, le checksum, le décodage borné sans octets supplémentaires et l'appartenance des clés au shard. Une absence, troncature, mauvaise identité ou corruption en fonctionnement retourne une erreur. Aucun de ces cas ne devient un shard vide.

Les écritures de cache ne font ni `fsync` ni `fdatasync`. Une erreur d'éviction laisse la dernière copie à jour dans le LRU et n'incrémente pas le nombre de pages. Cette politique ne modifie pas les garanties du WAL : le moteur reste responsable de ses barrières et de ses séquences durables.

La fin normale supprime le répertoire créé par l'instance. Un arrêt brutal peut laisser des répertoires de session inutilisés sur SSD. Leur nettoyage ne doit intervenir qu'après établissement de l'exclusivité sur le volume, en excluant toute session encore active ; le module ne supprime pas les répertoires d'autres instances.

## Validation réalisée

Les onze tests `index::tests` passent avec `cargo test --lib index::tests` :

- Éviction/rechargement de 19 shards avec budget de deux shards, comparés à une carte de référence ; plages et suppressions incluses.
- Snapshot simultané de sept shards sur disque et d'un shard résident, sans lecture supplémentaire lors de la capture ; réécritures et suppressions ne changent pas la génération figée.
- Refus d'une seconde génération tant que la première vit, puis libération du budget de snapshot.
- Nouvelle session toujours vide malgré des fichiers valides de l'ancienne session.
- Corruption ou fichier manquant détectés sans transformer la référence en zéro ni détruire les shards encore valides.
- Session, numéro de shard et nombre de pages incorrects rejetés.
- Échec forcé du `rename` d'éviction : dernière copie actuelle conservée, nouvelle insertion non appliquée.
- Limite mémoire, import de shard, suppression et copie lors d'une écriture validés.
- Durée de vie des snapshots après destruction de l'index et nettoyage final ; aller-retour d'un shard complet de 4 096 pages.
- TRIM d'un index dense couvrant 256 Mio logiques avec seulement 512 Kio résidents : limites de plage, suppression des shards vides et conservation des snapshots disque/RAM vérifiées.
- Suppression de plage sur backend mémoire : trous, plage vide, limites et ancienne version de snapshot préservés ; un TRIM rencontrant un shard corrompu retourne une erreur.

Le fichier `tests/astra_engine.rs` ajoute vingt et un scénarios d'intégration, tous passés ensemble. Ils utilisent le vrai backend d'objets local, le WAL, l'index et les caches. Les cas directement liés à l'index couvrent :

- Écriture de 2 Mio dont le remplissage de cache est rejeté par une file de 1 Mio, puis récupération locale et adoption depuis les objets publiés.
- Trois écritures concurrentes, trois lecteurs et seize checkpoints sur six shards avec seulement deux shards résidents ; chaque lecture doit correspondre à une version entière de sa requête.
- Même scénario avec publication compacte, préparation de segments, WAL de taille fixe et format aligné activés ensemble ; réouverture locale et adoption sont vérifiées.
- Volume logique creux de 2 Tio, 48 shards éloignés, reconstruction et adoption.
- Allocation de 8 193 pages avec budget index de 1 Mio : elle dépasse réellement le plafond de 8 192 pages du backend mémoire ; reprise locale et S3 vérifiées.
- Grand TRIM sur seize shards d'un volume de 256 Mio, avec 8 208 pages allouées et index résident de 1 Mio : reprise du ZRO depuis le WAL, suppression exacte de quatorze shards publiés et adoption sans ancien état local.
- Corruption d'un shard évincé : le moteur refuse les opérations suivantes, puis reconstruit un index sain depuis le WAL au redémarrage.
- Corruption du payload de cache logique moteur fermé : relecture de la version durable correcte après réouverture.
- Mode générations : écritures et TRIM postérieurs au HEAD publié disparaissent intégralement à la réouverture, y compris avec un WAL ultérieur détruit ; adoption également vérifiée.
- Rejet d'un changement de mode de durabilité à l'ouverture ou à l'adoption, sans modifier le propriétaire distant.
- Limite de retard d'une génération : une écriture attend réellement, puis reprend après publication réussie.

Les compléments couvrent la faute CAS et sa reprise, le fencing d'un ancien écrivain, la réduction des octets publiés sur deux générations de pages réécrites, la corruption du HEAD, la corruption du WAL actif ou scellé sur les deux chemins de lecture, le repli depuis un WAL publié corrompu vers sa copie distante et la récupération du format 2 avec watermark local corrompu ou absent. Ce dernier cas demeure refusé pour le format 1 utilisant un watermark durable.

Ces essais de redémarrage utilisent fermeture/réouverture ; ils ne remplacent pas une coupure du processus ou de la VM. Les campagnes NBD/ublk, bases de données et crash restent nécessaires pour qualifier l'ensemble et mesurer le compromis entre RAM réduite, accès SSD supplémentaires et durée de checkpoint.
