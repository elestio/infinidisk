#!/usr/bin/env python3
"""Render the review from measured JSON artifacts; no external assets required."""
import html,json,pathlib,re
ROOT=pathlib.Path(__file__).resolve().parents[1]
V=ROOT/'validation'
r=json.loads((V/'s3-report.json').read_text())
escape=html.escape
def num(v):return f'{v:,.0f}'.replace(',',' ')
def metric(name,side='read'):
    return r['benchmarks'][name][side]
def p99(d):return d.get('clat_ns',{}).get('percentile',{}).get('99.000000',0)/1e6
names=[('randread-hot','Lecture 4 Kio, segments récents locaux','read'),
       ('randread-cold-remote','Lecture 4 Kio, fichier froid S3, SSD 128 Mio','read'),
       ('randread-undersized-cache-pass2','Lecture 4 Kio, seconde passe, SSD toujours trop petit','read'),
       ('randread-fully-warm-cache','Lecture 4 Kio, SSD 512 Mio préchargé','read'),
       ('randwrite-fsync','Écriture 4 Kio + fsync, 4 clients','write'),
       ('baseline-randwrite-fsync','Fichier natif sur le disque VM + fsync, 4 clients','write'),
       ('prime-512m-cache','Lecture séquentielle distante de préchargement, 1 Mio','read')]
rows=[]
for name,label,side in names:
    d=metric(name,side)
    rows.append(f'<tr><td>{escape(label)}</td><td>{num(d["iops"])}</td><td>{d["bw_bytes"]/1e6:.1f}</td><td>{p99(d):.2f}</td></tr>')
sync=r['benchmarks']['randwrite-fsync']['sync']['lat_ns']
base=r['benchmarks']['baseline-randwrite-fsync']['sync']['lat_ns']
pg=r['benchmarks']['pgbench-durable']
tps=float(re.search(r'tps = ([0-9.]+)',pg).group(1))
testrows=[]
descriptions={
 'local_SIGKILL_ext4_fsync':'SIGKILL du moteur ; reprise ext4 ; fichier fsync identique',
 'postgres_SIGKILL_amcheck':'Crash PostgreSQL ; égalité des soldes ; pg_amcheck',
 'storage_engine_SIGKILL_postgres_amcheck':'Crash moteur pendant pgbench ; reprise ext4/PostgreSQL ; pg_amcheck',
 'remote_scrub':'Contrôle de toutes les pages référencées dans S3',
 'remote_only_ext4_recovery':'Nouveau répertoire local ; restauration uniquement depuis S3 ; hash du fichier',
 'remote_only_postgres_amcheck':'PostgreSQL restauré depuis S3 seul ; soldes cohérents ; pg_amcheck',
}
for key,label in descriptions.items():
    testrows.append(f'<tr><td>{escape(label)}</td><td class="ok">{escape(str(r["tests"].get(key,"absent")))}</td></tr>')
gc=json.loads((V/'gc-applied.json').read_text()) if (V/'gc-applied.json').exists() else None
gc_text=f'{gc["deleted_objects"]} objets orphelins, {gc["candidate_bytes"]/1024**2:.1f} Mio collectés ; scrub après collecte réussi.' if gc else 'Voir les résultats de collecte séparés.'
old=None
if (V/'s3-report-256k-extents.json').exists():old=json.loads((V/'s3-report-256k-extents.json').read_text())
comparison=''
if old:
    a=old['benchmarks']['randread-cold-remote']['read'];b=metric('randread-cold-remote')
    comparison=f'<p>Comparaison exploratoire des GET : 256 Kio → {num(a["iops"])} IOPS, p99 {p99(a):.0f} ms ; 64 Kio → {num(b["iops"])} IOPS, p99 {p99(b):.0f} ms. Le réseau et les caches évoluent entre essais ; cette comparaison ne prouve pas à elle seule un gain causal. La taille est maintenant configurable.</p>'
css='''body{margin:0;background:#f1f5f9;color:#162637;font:16px/1.65 system-ui,sans-serif}main{max-width:1080px;margin:auto;padding:48px 24px}header{padding:36px;background:#12283e;color:#fff;border-radius:18px}h1{font-size:38px;line-height:1.15;margin:8px 0}h2{margin-top:44px;color:#12324d}h3{color:#12324d}small,.muted{color:#657487}header small{color:#bdd8e8}.tag{display:inline-block;background:#22516e;padding:4px 12px;border-radius:20px;font-size:13px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:24px 0}.card,section{background:white;border:1px solid #d9e2eb;border-radius:12px;padding:20px}.card strong{display:block;font-size:29px;color:#12645f}.callout{padding:18px;border-left:5px solid #d8a02b;background:#fff8e8;margin:22px 0}.good{border-color:#1b9084;background:#edf9f6}table{width:100%;border-collapse:collapse;font-size:14px}th,td{padding:12px;text-align:left;border-bottom:1px solid #dde5ed}th{background:#eaf1f7}td:not(:first-child){white-space:nowrap}.ok{color:#10736a;font-weight:600}code,pre{background:#e9eff4;border-radius:5px;padding:3px 6px}pre{padding:18px;overflow:auto}a{color:#166ca1}li{margin:9px 0}.tablewrap{overflow:auto}@media(max-width:740px){.cards{grid-template-columns:repeat(2,1fr)}h1{font-size:29px}main{padding:18px}}@media print{body{background:white}main{padding:0}.card,section{break-inside:avoid}h2{break-after:avoid}}'''
page=f'''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>InfiniDisk2 — livraison, mesures et review</title><style>{css}</style><main>
<header><span class="tag">RUST · LINUX · WAL SSD · S3</span><h1>InfiniDisk2 : version autonome livrée</h1><p>Un vrai moteur bloc, des barrières de durabilité locales et des générations S3 atomiques.</p><small>9 octobre 2026 · version 0.1.0 expérimentale · VM 159.195.104.60</small></header>
<div class="cards"><div class="card"><strong>{num(metric('randread-hot')['iops'])}</strong>IOPS lecture locale récente</div><div class="card"><strong>{num(metric('randread-fully-warm-cache')['iops'])}</strong>IOPS cache SSD suffisant</div><div class="card"><strong>{num(tps)}</strong>transactions/s PostgreSQL durable</div><div class="card"><strong>11</strong>tests automatisés du moteur/protocole</div></div>
<div class="callout good"><b>Résultat concret.</b> Serveur NBD Rust et attachement Linux natif à huit connexions, montés réellement en ext4. Les crash-tests PostgreSQL et la restauration depuis S3 seul passent. Aucun volume préexistant n'a été arrêté ou formaté.</div>
<div class="callout"><b>Verdict de review :</b> une base fonctionnelle pour continuer InfiniDisk, pas encore une solution certifiée pour toute base de données. Le cache trop petit reste lent, les synchronisations sont coûteuses et l'index global en RAM limite les gros volumes.</div>
<h2>Ce qui est développé</h2><section><p>Le binaire gère <code>init</code>, <code>adopt</code>, <code>serve</code>, <code>attach</code>, <code>detach</code>, <code>status</code>, <code>scrub</code> et <code>gc</code>. Aucun processus ZeroFS ni wrapper Bash n'intervient dans le chemin des données.</p>
<p>Les écritures vont au WAL local avant réponse. FLUSH/FUA synchronisent le journal et le marqueur local. S3 reçoit d'abord les segments et les shards, puis une nouvelle racine <code>HEAD</code> par CAS. Une panne avant le CAS laisse l'ancienne génération valide.</p>
<p>Contrôles CRC32 sur pages et records, SHA-256 sur index et racine, reprise de queue tronquée, refus des séquences manquantes, caches bornés, attente du WAL plein et collecte hors ligne avec fencing de maintenance.</p><p>{escape(gc_text)}</p></section>
<h2>Mesures réelles</h2><section><p>VM partagée : 4 vCPU, environ 8 Gio RAM, disque système ext4 ; bucket <code>testperf-6czebk</code>, préfixe d'essai dédié, endpoint <code>https://storage.elestio.com</code>. Volume virtuel 2 Gio, fichier fio 256 Mio, PostgreSQL 16, pgbench scale 2, conteneur limité à 1 CPU et 512 Mio. Les processus et conteneurs existants restent actifs.</p>
<p>Fio utilise O_DIRECT et libaio. Lectures random : 4 jobs × iodepth 32, 10 secondes ; écritures durables : 4 jobs × iodepth 1, fsync après chaque écriture, 10 secondes. Les caches du moteur sont séparés ; le cache global de la VM n'est pas vidé.</p><div class="tablewrap"><table><thead><tr><th>Scénario</th><th>IOPS</th><th>MB/s décimaux</th><th>p99 I/O (ms)</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<p><b>Fsync :</b> latence moyenne InfiniDisk2 {sync['mean']/1e6:.2f} ms, p99 {sync['percentile']['99.000000']/1e6:.2f} ms ; fichier natif {base['mean']/1e6:.2f} ms, p99 {base['percentile']['99.000000']/1e6:.2f} ms. Le p99 d'écriture seul du tableau ne comprend pas l'appel fsync suivant.</p>
<p><b>PostgreSQL :</b> {tps:.1f} transactions/s, 4 clients/threads, 15 secondes, aucune transaction échouée ; <code>fsync=on</code>, <code>full_page_writes=on</code>, <code>synchronous_commit=on</code>. La taille et la durée de l'essai restent modestes.</p>
<p>La passe cache insuffisant couvre 128 Mio SSD pour un fichier de 256 Mio : une seconde passe n'est donc pas un cache entièrement chaud. La vraie passe chaude utilise 512 Mio SSD après préchargement complet. Les métadonnées ext4 et les données PostgreSQL peuvent déjà avoir réchauffé certaines étendues lors de la reprise, sans précharger le fichier fio.</p>{comparison}
<p class="muted">Les mesures chaudes/écritures proviennent du premier moteur 0.1 testé ; les passes de restauration ont été répétées avec le binaire final et les étendues configurables. Les JSON préservent la provenance. Les conditions diffèrent du rapport ZeroFS initial ; aucun ratio strict ZeroFS/InfiniDisk2 n'est affirmé.</p></section>
<h2>Tests d'intégrité et de reprise</h2><section><table><thead><tr><th>Essai sur le vrai backend S3</th><th>Résultat</th></tr></thead><tbody>{''.join(testrows)}</tbody></table>
<p>Les 11 tests Rust couvrent records tronqués/corrompus, slots de durabilité, écritures partielles, modèle de données, concurrence FLUSH, FUA multiconnexion, grand TRIM, échec/retry de publication, données distantes corrompues, reconstruction de caches corrompus, attente du WAL plein et GC interrompu/repris.</p>
<p>Le refus de détacher un filesystem monté dans un autre namespace Linux a également été testé sur la VM ; le détachement réussit après démontage. Les vérifications qualité sont <code>cargo test</code>, <code>cargo fmt --check</code> et <code>cargo clippy --all-targets -- -D warnings</code>. Un crash de processus et un nouveau répertoire local simulant la perte du disque ont été testés ; pas une coupure électrique réelle ni tous les modèles de disque/DB.</p></section>
<h2>Contrat de perte de données</h2><section><p>Si le disque local survit, une réponse FLUSH/FUA garantit que les appels fsync locaux ont réussi. Si la VM et le disque sont perdus, restaurer la dernière racine publiée permet un retour à un préfixe du volume ; les transactions du suffixe peuvent être perdues.</p>
<p>Les publications sont lancées toutes les 5 secondes. <b>Ce n'est pas une borne de RPO de 5 secondes.</b> La latence S3 et les indisponibilités peuvent augmenter le retard. Le WAL plein ralentit les écritures, puis produit une erreur après 50 secondes si aucune place n'est libérée.</p>
<p>Un seul hôte peut écrire. <code>adopt --takeover</code> nécessite de fencer l'ancien hôte, il ne remplace pas une procédure HA. Un filesystem Linux et une DB correctement configurés restent responsables de leur propre récupération.</p></section>
<h2>Review et recommandations</h2><section><ol>
<li><b>Avant données critiques :</b> tester panne VM/électrique, erreurs fsync, disque plein, corruption physique et coupures réseau à chaque étape du CAS ; campagne longue avec données au-delà de la RAM/cache. Les tests actuels valident des cas concrets, pas une garantie absolue.</li>
<li><b>Priorité performance durable :</b> profiler et réduire les barrières redondantes sans ignorer FLUSH/FUA. Le débit fio synchrone reste environ dix fois inférieur au fichier natif.</li>
<li><b>Priorité capacité :</b> index paginé sur SSD et cache de shards ; compacteur de segments partiellement vivants. Le budget d'index RAM de 1 Gio représente environ 32 Gio de pages allouées au défaut actuel.</li>
<li><b>Priorité lectures froides :</b> dimensionner le SSD pour le working set de la DB, mesurer les tailles de GET et ajouter du préchargement ciblé. Une latence réseau S3 ne devient pas une latence NVMe grâce au seul langage Rust.</li>
<li><b>Priorité exploitation :</b> métriques de retard S3, p99 FLUSH, occupation WAL/cache et politique RPO explicite. Tester la récupération avec des secrets et procédures conservés hors de la VM.</li>
</ol><p>Pas encore de snapshots utilisateur, resize en ligne, chiffrement applicatif, compactage automatique de pages vivantes ou multi-écrivain. Les modèles systemd sont fournis, sans activer un service de production.</p></section>
<h2>Sources et livraison</h2><section><p>Sur la VM : <code>/root/infinidisk2</code>, binaire <code>/root/infinidisk2/target/release/infinidisk2</code>. Copie locale : <code>/opt/app/data/infinidisk2</code>.</p>
<p><a href="../README.md">Guide de démarrage et reprise</a> · <a href="../docs/architecture.md">Spécifications et décisions techniques détaillées</a> · <a href="s3-report.json">Résultats JSON</a> · <a href="gc-applied.json">Collecte S3</a> · <a href="scrub-after-gc.txt">Scrub après collecte</a></p>
<p><a href="https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md">Protocole NBD</a> · <a href="https://docs.kernel.org/filesystems/ext4/journal.html">Journal ext4</a> · <a href="https://docs.rs/object_store/0.14.2/object_store/enum.PutMode.html">PUT conditionnels object_store</a></p></section>
</main></html>'''
(V/'rapport.html').write_text(page,encoding='utf-8')
print(V/'rapport.html')
