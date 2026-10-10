# Mode générations : contrat, récupération et campagne MySQL

Le mode `generation_mode = true` vise un contrat distinct : revenir à un préfixe cohérent des écritures d'un volume, en acceptant la perte de transactions récentes. Une requête FLUSH/FUA ordonne les écritures déjà reçues, mais ne promet pas leur persistance individuelle. Une transaction SQL acquittée peut disparaître lors du retour à la dernière génération S3 complète.

La qualification porte sur les scénarios réellement exécutés et leur binaire identifié. Elle ne constitue pas une garantie générale d'absence de corruption pour toute application. Les essais ci-dessous séparent le retour à une référence arrêtée proprement de la récupération d'un checkpoint publié pendant une activité MySQL/ext4. Un succès dans le premier scénario ne suffit pas à qualifier le second. La cohérence entre plusieurs volumes indépendants n'est pas couverte par ce contrat.

Ce mode crée un volume de format HEAD 2. Il ne s'active pas sur un volume de format 1 existant : ouverture et adoption refusent une configuration dont le mode ne correspond pas au format. Le mode local durable reste le comportement par défaut.

## Règle de récupération

Le moteur vérifie l'identité, le propriétaire, le checksum du HEAD et ses shards avant de sélectionner son point de reprise. En mode générations, le HEAD validé est l'unique génération de données à rejouer : les journaux locaux ultérieurs sont abandonnés, même s'ils contiennent des transactions acquittées ou des marqueurs de synchronisation valides. Un HEAD corrompu ou inaccessible provoque un échec d'ouverture ; il n'autorise pas un repli silencieux sur quelques écritures locales plus récentes.

Après validation du HEAD, le watermark local du format 2 est reconstruit. Son absence ou sa corruption ne transforme pas un HEAD sain en volume irrécupérable. Cette règle ne s'applique pas au format 1 utilisant ce marqueur pour sa durabilité locale.

Le checkpoint capture un préfixe d'écritures, rend disponibles ses objets immuables, puis remplace le HEAD par une mise à jour conditionnelle. La publication compacte peut enlever les versions de pages devenues inutiles dans ce préfixe. Les snapshots d'index conservent la même génération pendant que les écritures suivantes continuent. Un conflit de propriétaire empêche l'ancien moteur de publier une racine concurrente.

La limite `generation_max_lag_seconds` applique une contre-pression : lorsque la génération non publiée est trop ancienne, les écritures attendent une publication réussie. Un intervalle de checkpoint de cinq secondes ne prouve pas à lui seul une perte maximale de cinq secondes : durée des uploads, erreurs S3, charge et reprise doivent être mesurées. La contre-pression ne rétroagit pas sur les transactions déjà acquittées.

## Restauration d'une application

Le retour à une génération antérieure exige un arrêt complet des applications utilisant le volume, la disparition de leur ancien montage et le détachement de l'ancien périphérique. Le système de fichiers et les bases doivent ensuite redémarrer sur le volume récupéré.

Rattacher une génération ancienne sous une base encore active laisserait ses caches mémoire, descripteurs et journaux applicatifs décrire un état ultérieur. Ce scénario n'est pas pris en charge. Les tests de génération arrêtent le moteur, la DB et le montage avant toute reprise.

## Campagne dédiée sur la VM

Le script `scripts/validate_generation.py` prépare uniquement une nouvelle fixture :

- Volume de 2 Gio, préfixe S3 `infinidisk2-generation-<uuid>`.
- `/dev/nbd31`, huit connexions NBD et port loopback 11990, tous contrôlés libres avant démarrage.
- ext4, MySQL 8.0 dans un conteneur isolé sans réseau, un CPU et 1 Gio de RAM.
- Pool InnoDB de 256 Mio, `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, doublewrite actif, `O_DIRECT` et binlog actif. Ces réglages SQL restent explicites malgré le contrat relâché du stockage.
- Cache RAM moteur de 1 Gio, cache logique SSD de 4 Gio, index paginé de 64 Mio, WAL aligné à taille fixe, préparation de segments, cache asynchrone et checkpoints compacts.
- Les secrets AWS sont lus depuis le fichier de credentials, jamais copiés dans le rapport. Le mot de passe MySQL de test est transmis au conteneur par environnement et à sysbench par un fichier privé `sysbench.secret` de mode 0600. Il faut exclure ce fichier de tout export.

Commande, à lancer uniquement après coordination avec les autres campagnes sur la VM :

```bash
cd /root/infinidisk2
python3 scripts/validate_generation.py \
  --binary /root/infinidisk2/target/release/infinidisk2-astra \
  --expected-binary-sha256 SHA256_DU_BINAIRE_FINAL \
  --credentials /opt/elestio/infinidisk/bench.env
```

Le SHA-256 attendu est obligatoire et contrôlé avant la création de la fixture, à chaque redémarrage du moteur et autour des mesures. Le script crée `/root/infinidisk2/test-output/generation-<uuid>/report.json`, les configurations et les sorties brutes. Il ne réutilise aucune fixture existante, ne reformate aucun volume préexistant, ne modifie ni firewall ni service partagé et ne touche pas aux périphériques NBD 0/1 de production. En cas d'échec, les preuves sont conservées et `complete` reste faux. Le nettoyage final vérifie le montage et le détachement NBD.

Le précontrôle réserve uniquement les emplacements des campagnes : nbd31/ublk31, ports 11990/11991/12991 et verrous de test. Il détecte les compilateurs et charges de benchmark connus. Les services de production `infinidisk@bench`/`infinidisk@data`, nbd0/1 et leurs conteneurs peuvent rester actifs : leur présence ne bloque pas le test et le script ne les arrête pas. Leur activité fait partie des limites d'une mesure sur une VM partagée.

Après nettoyage, il exporte une liste explicite de preuves textuelles vers `/root/infinidisk2/validation/astra/generation/<uuid>/` : rapport, configurations et logs. Les valeurs des credentials sont masquées, les fichiers `.secret`, caches, journaux binaires et répertoires de données sont exclus. `manifest.json` contient les SHA-256 des fichiers exportés. Un essai incomplet est exporté avec `complete = false`, sans être converti en résultat validé.

L'export publie ensuite atomiquement `/root/infinidisk2/validation/astra/generation/report.json`. Cette copie du dernier rapport expurgé expose directement les métriques au renderer et contient `source_report` et `source_manifest`, deux chemins relatifs vers le dossier UUID d'origine. Les essais précédents sont conservés. Un nouvel essai incomplet remplace aussi ce rapport canonique avec `complete = false`, afin de ne pas laisser croire qu'un ancien succès qualifie le dernier binaire.

## Phase de mesures

Les mesures utilisent quatre tables sysbench de 25 000 lignes, huit threads, la distribution `special` et un préchauffage en lecture de dix secondes. Chacune des charges `read_write`, `read_only` et `write_only` est mesurée trois fois pendant trente secondes. Le percentile demandé et parsé est le p99. Les helpers de parsing et de mesure CPU/RSS proviennent de `compare_zerofs.py`, chargés par AST sans exécuter sa campagne.

La chaîne de mesure comprend les helpers CPU cgroup v2. Leurs compteurs incluent les descendants du cgroup ; le temps de throttling ne représente pas directement une latence d'I/O. Un cgroup invisible ou remplacé rend cette seule mesure indisponible, sans invalider la fenêtre CPU du processus.

Cette phase conserve de vrais checkpoints périodiques de cinq secondes et une limite de retard de trente secondes. TPS, latence moyenne, p99, erreurs ignorées et fenêtres de ressources des processus moteur/MySQL/client NBD sont enregistrés. Les résultats doivent apparaître dans une série portant explicitement la mention « générations — transactions acquittées pouvant être perdues ». Ils ne doivent pas être présentés comme un gain à durabilité égale face au natif ou au mode WAL durable.

La comparaison utilise aussi le cache de pages Linux hors RSS du moteur. Les budgets configurés ne prouvent pas une consommation mémoire totale égale entre outils.

## Phase de crash contrôlée

Après les mesures, le script crée un point de référence complet en arrêtant proprement MySQL, en démontant ext4 et en attendant la publication finale du moteur. Son HEAD de format 2, sa séquence et ses shards sont enregistrés. Deux tables de contrôle font partie de cette référence : une paire de soldes dont la somme vaut 2 000 000 et un journal de transactions contenant uniquement le marqueur zéro.

Pour isoler le mécanisme de retour cohérent, la phase de crash utilise ensuite un intervalle de publication et une limite de retard de 3 600 secondes. **Cette pause est une injection de faute contrôlée, distincte de la configuration mesurée. Elle ne démontre pas le RPO de production.** Les écritures applicatives de chaque essai ne durent que quelques secondes.

Pour chaque seed 17, 42 et 91 :

1. Le moteur ouvre le HEAD de référence. Le script attend son premier tick de checkpoint avant d'attacher le périphérique ; aucune publication initiale inattendue ne peut donc capturer les futures écritures du test.
2. ext4 et MySQL sont remontés ; un sysbench `read_write` actif modifie les tables.
3. Une transaction distincte transfère une unité entre les deux soldes et insère un marqueur unique. Le retour SQL prouve que le COMMIT a été acquitté.
4. Le script vérifie que le HEAD est toujours exactement celui de référence, puis tue réellement le moteur par SIGKILL et arrête complètement la DB. Il enregistre le PID, le code de sortie et le délai entre acquittement et SIGKILL.
5. Il supprime l'ancien montage, termine le client et vérifie que le périphérique NBD est détaché avant de rouvrir le volume.
6. Après `e2fsck`, remontage et redémarrage MySQL, il exige quatre comptages à 25 000 lignes, six `CHECK TABLE ... EXTENDED` valides, les deux soldes initiaux et l'absence du marqueur pourtant acquitté.
7. La reprise de contrôle est arrêtée sans publication pour pouvoir répéter exactement le même scénario à partir du même HEAD.

Ces trois essais qualifient uniquement le retour à une génération créée après un arrêt propre. Ils ne prouvent pas la cohérence d'un checkpoint pris pendant des transactions.

## Checkpoint publié pendant une charge active

Un cas distinct rétablit les publications toutes les cinq secondes et la limite de retard de trente secondes. MySQL et ext4 restent actifs, avec deux clients : sysbench `read_write` et une suite de transferts contrôlés. Chaque transfert exécute, dans une seule transaction, une décrémentation du premier solde, une incrémentation du second et l'ajout d'un identifiant unique dans le journal. Le client écrit une ligne d'acquittement après le COMMIT. La somme des soldes doit toujours être 2 000 000 ; le nombre de transferts récupérés doit correspondre à chaque solde et à un préfixe contigu du journal.

Après au moins vingt acquittements, le script exige **deux nouvelles publications HEAD** pendant que les clients progressent. Il conserve, pour chacune, la racine complète, le statut moteur confirmant la publication, les horodatages, les acquittements et l'identité du processus MySQL. Le PID et son instant de création doivent rester identiques, le montage et le périphérique doivent rester présents. Attendre deux publications évite de sélectionner uniquement un checkpoint qui aurait déjà été en cours avant le début des transferts.

Le script suspend alors seulement le moteur avec SIGSTOP afin de figer le HEAD observé, puis le tue par SIGKILL et tue complètement MySQL. **La DB n'a pas été arrêtée proprement avant cette publication.** Cette suspension facilite l'identification du point testé ; elle ne simule pas une coupure électrique de l'hôte. Un changement tardif du HEAD invalide le contrôle. Le montage et l'ancien client NBD sont retirés avant toute reprise.

La publication est ensuite désactivée pour la seule vérification du point choisi. Après récupération ext4 et redémarrage MySQL, le script exige les comptages et `CHECK TABLE ... EXTENDED`, des checksums InnoDB actifs, aucun mode de récupération forcée, au moins un transfert récupéré et l'accord exact soldes/journal. Le nombre de transferts récupérés n'est pas contraint à égaler les acquittements observés : des transactions acquittées peuvent disparaître, et un COMMIT peut précéder la réception de sa réponse par le client. Le résultat et les preuves figurent dans `tests.live_checkpoint_recovery` ; un échec conserve l'étape atteinte avec `passed = false`.

Le script effectue enfin un scrub S3 de ce **HEAD publié en activité**, puis une adoption dans un répertoire local neuf, après avoir déplacé l'ancien répertoire hors du chemin de récupération. La séquence et les shards doivent rester identiques ; les contrôles ext4/SQL doivent retrouver exactement le même préfixe de transferts sans lecture de l'ancien WAL ni du cache. Le scrub contrôle les données référencées par ce HEAD ; il ne constitue pas un test exhaustif de toutes les pannes possibles.

L'indisponibilité S3 prolongée pendant une charge active n'est pas simulée par ce script : le rapport l'indique explicitement. Le test déterministe `generation_lag_backpressure_waits_for_successful_publication` vérifie déjà le blocage puis le réveil des écritures au niveau moteur ; une campagne réseau isolée reste distincte.

## Vérifications locales disponibles

La qualification VM `cdd41edc0c11`, sur le binaire `b39b705f43b6…`, est terminée : les trois retours au HEAD propre (graines 17, 42 et 91), la récupération du checkpoint publié sous charge, le scrub de cette racine et l'adoption sans état local passent. Le cas sous charge récupère un préfixe contigu de **278 transferts**, avec les soldes **999 722 / 1 000 278** et la somme attendue de 2 000 000. L'adoption retrouve le même préfixe. Les preuves sont dans [generation/report.json](../validation/astra/generation/report.json).

La partie performance de cette campagne comporte trois erreurs SQL ignorées en écriture ; l'ensemble de ses séries reste brut et exclu des ratios de gain. Les reprises validées ne transforment pas ce résultat de débit en comparaison qualifiée. Elles ne prouvent pas non plus un RPO maximal de cinq secondes ni une résistance à toute panne matérielle.

```bash
python3 scripts/validate_generation.py --self-test
python3 -m py_compile scripts/validate_generation.py
cargo test --test astra_engine
```

Les vingt et un tests moteur ont passé dans la validation locale : concurrence pendant checkpoints ordinaires et compacts, index paginé et grand TRIM avec rejeu WAL, corruption autoritaire et cache reconstructible, faute CAS réessayable, fencing, réduction des octets publiés pour des pages réécrites, refus d'un changement de mode, HEAD corrompu, watermark de génération corrompu/absent, retour exact à une génération et contre-pression.

Le self-test local exerce le parsing, les dépendances de mesure processus/cgroup, le refus de comparer deux cgroups différents, la validation des acquittements et des invariants transactionnels, et la publication canonique sans effacement des preuves précédentes. Il refuse notamment un journal troué, un solde divergent et une simple référence propre sans transfert récupéré. Les mesures MySQL, les SIGKILL et la génération prise pendant une charge active doivent être qualifiés par le `report.json` réellement produit ; cette spécification n'est pas un résultat d'exécution.
