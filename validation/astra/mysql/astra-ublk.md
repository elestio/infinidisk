# Transport ublk : buffers réutilisables et réveils regroupés

La variante `ublk_fast_path = true` conserve le protocole ublk classique et les
garanties du moteur. Elle supprime du travail entre les files du noyau et ce
moteur ; elle ne prétend pas supprimer les copies que le noyau effectue entre
ublk et le processus.

## Changements

| Élément | Chemin précédent | Variante rapide |
| --- | --- | --- |
| Écriture | Copie du buffer aligné dans un nouveau `Vec` | Transfert de propriété du buffer au worker |
| Lecture | Retour d'un `Vec`, puis copie dans le buffer aligné | `Engine::read_buffer` remplit le buffer transféré |
| Exécution | Une nouvelle tâche Tokio par requête | Un worker Tokio persistant par tag |
| Réponse interne | Un nouveau canal oneshot par requête | Emplacement de réponse réutilisé par tag |
| Réveil io_uring | Un eventfd et une attente par tag | Un eventfd et une attente par file |
| Notifications | Une écriture eventfd par requête | Une écriture à la transition lot vide vers lot non vide |

Le regroupement utilise les complétions déjà présentes et n'ajoute aucun timer.
Une charge à faible profondeur peut encore produire une notification par I/O.
Les tâches du transport présentent ensuite leurs commandes COMMIT_AND_FETCH à
libublk, dont le runtime regroupe déjà les soumissions io_uring lorsqu'il se gare.
Le nouveau mode ne dépend pas du protocole kernel `UBLK_F_BATCH_IO` et n'active
ni enregistrement de buffer pour zéro copie, ni `MLOCK_IO_BUFFER`.

L'eventfd reste indispensable : le runtime libublk attend dans `io_uring_enter`,
que le seul réveil d'une tâche depuis l'autre runtime Tokio ne suffit pas à
interrompre. Le supprimer recréerait le blocage observé auparavant.
La complétion réveille également le waker du tag depuis le runtime moteur,
après publication du résultat. Les écritures eventfd sont regroupées ; cela ne
supprime pas le réveil logique de chaque tag terminé.

## Limites et durée de vie

- Au plus huit files, 32 tags par file et un buffer de 1 Mio par tag. La profondeur
  rapide est réduite selon `max_inflight / queues`, avec au moins un tag par file.
- Un sémaphore unique respecte `max_inflight` pour les requêtes en exécution de
  toutes les files. La mémoire fixe du transport reste au maximum à 256 Mio ;
  elle doit être incluse dans le budget total avec les caches et le moteur.
- Chaque tag n'a qu'une requête en cours. Son canal de travail est borné à un
  élément et son emplacement de réponse à un buffer. Un masque de 32 bits borne
  aussi les notifications, même si une complétion précède l'attente.
- Entre la réception de FETCH et la soumission de COMMIT, le buffer appartient au
  worker. Aucune commande noyau ne doit alors utiliser ce buffer. L'adresse reste
  identique sur le chemin réussi.
- Si une lecture consommant le buffer échoue, un buffer neuf de même capacité le
  remplace. Cela est permis par le transport classique utilisé ici, mais serait
  incompatible avec l'hypothèse d'adresse stable de buffers épinglés. Toute
  future activation de ces modes doit revoir explicitement ce chemin d'erreur.
- Les buffers sont initialisés avant la première exposition sous forme de slice.
- À l'arrêt d'une file, les workers terminent les opérations acceptées avant que
  `serve` rende la main au flush/checkpoint final. Une écriture ne doit pas se
  poursuivre en tâche détachée après cette barrière finale.

## Durabilité et erreurs

WRITE suivi de FUA attend toujours `Engine::flush`. FLUSH attend cette même
opération et ne réussit pas avant elle. Les erreurs et les paniques du moteur
produisent une erreur I/O, pas un succès partiel. Ce transport ne modifie pas le
contrat choisi dans le moteur, y compris si un mode de génération expérimentale
est activé ailleurs.

Le décodage distingue d'abord l'opération : les champs secteur et longueur d'un
FLUSH n'adressent aucune donnée et sont ignorés. Seuls READ et WRITE convertissent
les secteurs en octets et vérifient la taille du buffer. Une requête invalide ou
non supportée reçoit une réponse d'erreur ; elle n'abandonne pas son tag.

Une erreur interne du transport déclenche une seule notification vers un thread
de contrôle dédié. Le superviseur fait d'abord échouer les attentes en mémoire
et signale leurs eventfd : STOP_DEV seul ne réveille pas un tag qui attend un
worker disparu. Le thread vérifie l'ID et le PID propriétaire avant STOP_DEV,
qui annule les commandes noyau restantes. Les workers déjà acceptés terminent
avant le retour des files, et `serve` renvoie la première erreur. La fermeture
normale `QueueIsDown` ne déclenche pas cet arrêt d'urgence. Si STOP_DEV échoue,
le processus sort avec erreur : c'est alors un crash, sans promesse de checkpoint
final. Ce dernier recours évite de garder un périphérique dont les I/O attendent
indéfiniment. Les tests unitaires vérifient notification unique, propagation et
mort d'un worker avant sa réponse, avec STOP simulé ; ils n'injectent pas encore
une défaillance de STOP_DEV sur le noyau.

Le chemin historique est conservé lorsque `ublk_fast_path = false` afin de
permettre une comparaison directe avec le même binaire.

## Validation

Les tests du module `ublk::tests` vérifient les complétions avant et après
l'attente, le regroupement des 32 tags, le réarmement pendant une arrivée
concurrente, la réutilisation immédiate d'un tag, 1 000 allers-retours entre
threads, et la conservation de FLUSH/FUA après réouverture du moteur. Le test
du moteur vérifie aussi la lecture partielle directement dans le buffer et la
réutilisation du tag après une erreur de lecture.

Commande : `cargo test --features ublk ublk::tests`.

Le test ignoré `queue_runtime_delivers_cross_runtime_completions` exerce aussi
le véritable LocalSet/libublk et un io_uring avec 32 tags, 16 384 échanges et des
complétions précoces ou retardées. Il ne crée pas de périphérique bloc :
`timeout 30 cargo test --features ublk queue_runtime_delivers_cross_runtime_completions -- --ignored --test-threads=1`.

Le premier essai MySQL sur le binaire `839abdf0cec29812…` a bloqué au démarrage :
une I/O de journal attendait, tandis que quatre lectures directes supplémentaires
réussissaient. Cet essai ne fournit aucun score ublk valide. Les traces par
file/tag ajoutées ensuite distinguent envoi, début/fin moteur, réception et
COMMIT ; les erreurs de sortie d'un tag sont journalisées par le transport.

La reproduction instrumentée `439c6d0236c5…` a ensuite localisé une sortie du tag
`qid=3, tag=27` dans `decode_request` : le calcul vérifié `start_sector * 512`
débordait avant le dispatch. Les 365 autres requêtes tracées avaient toutes leur
complétion et leur COMMIT. L'opération FLUSH avec un secteur sentinelle est à ce
stade une inférence forte ; la valeur brute n'a pas pu être lue dans le mapping
spécial du noyau. Le correctif trace ces champs pour confirmation au prochain
essai et teste explicitement FLUSH avec `u64::MAX` et une longueur arbitraire.
Le durcissement du réveil et l'augmentation de SQ/CQ ne constituent donc pas une
preuve que des notifications étaient perdues.

Les essais de ces structures en espace utilisateur ne remplacent pas les essais
sur le noyau. Qualification nécessaire sur le périphérique de test : fio en
lecture/écriture/mixte à plusieurs profondeurs, FLUSH/FUA, démontage et suppression
du périphérique, arrêt brutal du serveur et récupération du volume, puis MySQL
avec les mêmes caches et garanties de persistance. Comparer également CPU, p95 et
p99, ainsi que débit. Aucun gain chiffré n'est déduit de la seule réduction du
nombre d'allocations ou de copies.

Référence SDK : libublk-rs révision
`0d172817b3737079477337b39538ccdc91766e6e`, notamment `runtime.rs`, `executor.rs`,
`helpers.rs`, `ops.rs` et les contrats de durée de vie dans `io.rs`.
