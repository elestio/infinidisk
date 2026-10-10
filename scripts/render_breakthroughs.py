#!/usr/bin/env python3
"""Render measured cases; missing cases and ignored SQL errors stay visible."""
import argparse,html,json,pathlib,re,statistics
root=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--conclusion',required=True);p.add_argument('--allow-incomplete',action='store_true');a=p.parse_args()
v=root/'validation/breakthroughs';raw=v/'raw';summary={'conclusion':a.conclusion,'cases':{}}
cases=['wal-reference','wal-one-barrier','wal-batch-100','wal-batch-500','wal-reference-after','extent-128','logical-128','logical-2048','compact-extent-128','compact-logical-128','local-active-nbd','local-active-nbd-recheck','local-active-ublk','local-active-mixed','local-active-fixed-mixed','fixed-reference','fixed-only','fixed-growing-one-barrier','fixed-one-barrier','fixed-reference-after','native-final']
rows=[]
for label in cases:
    candidates=list(raw.rglob('optimization-'+label+'.json'))
    if not candidates:
        if not a.allow_incomplete:raise RuntimeError('missing measurement: '+label)
        rows.append([label,'absent','—','—','—','—']);continue
    d=json.loads(candidates[0].read_text());engine='native' if label=='native-final' else 'infinidisk2';entry=d['mysql'][engine+'-'+label]
    assert d.get('complete'),label
    for workload,samples in entry['samples'].items():
        tps=[s['tps'] for s in samples];assert len(tps)==3
        ignored=[s['ignored_errors'] for s in samples]
        value={'median_tps':statistics.median(tps),'samples':tps,'p95_ms':[s['p95_ms'] for s in samples],'ignored_errors':ignored,'binary_sha256':d['binary_sha256']['infinidisk2'],'database_recovery':entry.get('database_SIGKILL_recovery'),'startup_seconds':entry.get('startup_seconds'),'engine_rss_kib_before_samples':entry.get('engine_rss_kib_before_samples'),'ssd_cache_bytes_before_samples':entry.get('ssd_cache_bytes_before_samples'),'preparation':d.get('preparation',{}).get(engine+'-'+label,{})}
        logs=list(raw.rglob(engine+'-'+label+'-server.log'))
        if logs:
            statuses=[json.loads(x) for line in logs[0].read_text().splitlines() for x in re.findall(r'\{"volume".*\}',line)]
            if statuses:value['cumulative_engine_status']=statuses[-1]
        summary['cases'][label+'/'+workload]=value
        rows.append([label+'/'+workload,f"{value['median_tps']:.2f}",', '.join(map(str,tps)),f"{statistics.median(value['p95_ms']):.2f}",str(sum(x for x in ignored if x is not None)),entry.get('database_SIGKILL_recovery','absent')])
probe=list(raw.rglob('fsync-layout/report.json'))
if probe:summary['fsync_layout_probe']=json.loads(probe[0].read_text())
checks=[]
for f in raw.glob('run-*/report.json'):
    d=json.loads(f.read_text());checks.append({'path':str(f.relative_to(v)),'passed':d.get('passed'),'tests':d.get('tests'),'binary_sha256':d.get('binary_sha256')})
for f in [*raw.rglob('optimization-breakthrough-recovery.json'),*raw.rglob('optimization-breakthrough-fixed-recovery.json'),*raw.rglob('optimization-breakthrough-ublk-recovery.json')]:
    d=json.loads(f.read_text());checks.append({'path':str(f.relative_to(v)),'complete':d.get('complete'),'mysql':{k:{s:t for s,t in value.items() if 'recovery' in s} for k,value in d.get('mysql',{}).items() if 'storage-recovery' in k},'binary_sha256':d.get('binary_sha256')})
summary['recovery_evidence']=checks
if not a.allow_incomplete:
    for label in ('campaign','fixed-campaign'):
        campaign=json.loads((raw/label/'report.json').read_text())
        assert campaign['complete'],label
        latest={s['label']:s['returncode'] for s in campaign['stages']}
        assert all(code==0 for code in latest.values()),label
    assert len(checks)==5 and all(c.get('passed',c.get('complete')) for c in checks),'recovery incomplete'
def table(headers,rows):return '<div class="scroll"><table><thead><tr>'+''.join('<th>'+html.escape(h)+'</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join('<td>'+html.escape(str(c))+'</td>' for c in row)+'</tr>' for row in rows)+'</tbody></table></div>'
method='MySQL 8.0.46, 8 threads, 1 CPU/1 Gio, buffer pool 256 Mio, fsync InnoDB et binlog réels, doublewrite ON, O_DIRECT. Trois passages de 15 s. Petite base : 4 × 25 000 lignes, chauffe 10 s. Grande base : 4 × 1 000 000 lignes, lectures uniformes, chauffe 60 s, cache RAM moteur 64 Mio. Compaction et préchauffage hors mesure TPS mais chronométrés. Les variantes locales utilisent 4 Gio de SSD ; elles ne sont pas des comparaisons à capacité égale avec 128 Mio.'
limits='VM partagée, ordre fixe, essais courts et chauffe parfois progressive : les multiplicateurs sont ceux de cette campagne, sans garantie universelle. Les erreurs ignorées par sysbench sont affichées ; leur code SQL n’est pas présent dans les logs agrégés. Les compteurs moteur sont cumulatifs et incluent démarrage, chauffe et reprise : ils ne sont pas les seuls GET des fenêtres chronométrées. Le RSS du moteur dépasse son seul budget de cache RAM, notamment à cause des index. Les fichiers du cache utilisent aussi le page cache Linux, hors RSS ; le mode local actif bénéficie de sa chauffe, tandis que MySQL natif conserve O_DIRECT. Les répétitions readonly ne refont pas toutes SIGKILL/CHECK TABLE. Une panne électrique réelle et les pannes du fournisseur S3 restent hors qualification.'
design='Toutes les variantes restent expérimentales et désactivées par défaut. Le cache packed vérifie version + CRC et reste jetable. La compaction complète est hors ligne, sous verrou exclusif, avec objets vérifiés et HEAD conditionnel publié en dernier. Le WAL avec marqueurs de commit maintient la durabilité locale avant FLUSH/FUA, mais change les enregistrements que comprennent les anciens lecteurs. Le WAL fixe sépare EOF physique et fin logique, et paie son initialisation/rotation. ublk utilise le même moteur avec des copies : aucun zero-copy revendiqué. La réplication S3 reste asynchrone.'
css='body{margin:0;background:#edf2f7;color:#172839;font:16px/1.6 system-ui}main{max-width:1250px;margin:auto;padding:24px}section{background:white;border-radius:12px;padding:22px;margin:18px 0}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:10px;border-bottom:1px solid #ddd;text-align:left}a{color:#056}'
page='<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>InfiniDisk2 — breakthroughs</title><style>'+css+'</style><main><h1>InfiniDisk2 : changements d’architecture mesurés</h1>'
for title,body in [('Verdict',a.conclusion),('Méthode',method),('Choix techniques',design),('Limites',limits)]:page+='<section><h2>'+title+'</h2><p>'+html.escape(body)+'</p></section>'
page+='<section><h2>Résultats</h2>'+table(['Cas/charge','Médiane TPS','Trois passages','Médiane p95 ms','Erreurs ignorées','Reprise MySQL'],rows)+'</section>'
if 'fsync_layout_probe' in summary:
    page+='<section><h2>Microbenchmark du journal</h2><p>pwrite de 4 128 octets + fdatasync : fichier croissant ou entièrement initialisé. Le multiplicateur de ce test ne représente pas une transaction MySQL.</p><pre>'+html.escape(json.dumps(summary['fsync_layout_probe'],ensure_ascii=False,indent=2))+'</pre></section>'
page+='<section><h2>Preuves et spécifications</h2><a href="summary.json">Synthèse JSON</a> · <a href="../../docs/breakthroughs.md">Spécifications des variantes</a>'
for f in sorted(raw.rglob('build-manifest*.json')):page+=' · <a href="'+html.escape(str(f.relative_to(v)))+'">Manifeste '+html.escape(f.parent.name)+'</a>'
for c in checks:page+=' · <a href="'+html.escape(c['path'])+'">Récupération '+html.escape(pathlib.Path(c['path']).parent.name)+'</a>'
page+='</section></main></html>'
(v/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');(v/'rapport.html').write_text(page)
print(v/'rapport.html')
