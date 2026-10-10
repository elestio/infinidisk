#!/usr/bin/env python3
"""Small report for the completed immutable-index cache qualification."""
import base64
import hashlib
import html
import json
import os
from pathlib import Path
import statistics
import tomllib

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/index-cache'
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'test-output/matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table(headers, rows):
    cell = lambda tag, x: f'<{tag}>' + html.escape(str(x)) + f'</{tag}>'
    return '<div class="table"><table><thead><tr>' + ''.join(cell('th', x) for x in headers) + '</tr></thead><tbody>' + ''.join(
        '<tr>' + ''.join(cell('td', x) for x in row) + '</tr>' for row in rows) + '</tbody></table></div>'


def main():
    build = read(OUT / 'build/manifest.json')
    report = read(OUT / 'restart/report.json')
    recovery = read(OUT / 'recovery/report.json')
    count = 0
    for document in (build, report, recovery):
        assert document['complete'], 'incomplete qualification'
        for name, expected in document['source_sha256'].items():
            assert sha(ROOT / name) == expected, name
    assert build['binary_sha256'] == report['binary_sha256']['astra'] == recovery['binary_sha256']
    for name, check in build['checks'].items():
        assert check['returncode'] == 0 and sha(OUT / 'build' / name) == check['sha256'], name
        count += 1
    manifest = read(OUT / 'restart' / report['source_manifest'])
    for name, expected in manifest['files_sha256'].items():
        assert sha(OUT / 'restart' / report['id'] / name) == expected, name
        count += 1
    for name, expected in recovery['evidence_sha256'].items():
        assert sha(OUT / 'recovery/raw' / name) == expected, name
        count += 1
    v = report['variants']['astra']
    assert v['complete'] and not v['cleanup']['failures']
    assert all(v['cleanup'][k] for k in ('nbd31_detached', 'mount_absent', 'container_removed'))
    assert set(v['integrity'].values()) == {'passed'}
    assert not v['summary']['cost_has_unpriced_or_uncertain_requests']
    passes = [k for k, value in recovery['result']['tests'].items() if value == 'passed']
    assert len(passes) == 7 and recovery['returncode'] == 0 and recovery['cleanup']
    config = tomllib.loads((ROOT / 'configs/recommended.toml').read_text())
    for key, value in report['protocol']['options'].items():
        assert {'memory_cache_mib':64, 'disk_cache_mib':128, 'max_index_mib':64}.get(key, config[key]) == value, key
    phases = v['phases']
    cases = []
    for record in v['metadata_repeats']:
        label = record['label']
        opened = phases['postgres.' + label + '_open']
        drained = phases['postgres.' + label + '_drain']
        s = opened['summary']
        assert drained['summary']['total_upstream_attempts'] == 0
        assert s['request_classes']['A'] == 0
        assert s['by_object_type'] == ({'infinidisk_head':1, 'infinidisk_index':14} if label.startswith('off') else {'infinidisk_head':1})
        cases.append({'label':label, 'B':s['request_classes']['B'], 'bytes':s['response_payload_bytes'],
                      'seconds':opened['seconds'], 'cost_per_10K_opens':s['estimated_gross_request_cost_usd']*10000})
    restart_rows = []
    for mode in ('warm', 'cold'):
        parts = [phases['postgres.' + mode + suffix] for suffix in ('_open', '_database_open')]
        restart_rows.append([mode, sum(p['summary']['request_classes']['A'] for p in parts),
            sum(p['summary']['request_classes']['B'] for p in parts),
            f"{sum(p['seconds'] for p in parts):.3f}",
            f"{sum(p['summary']['estimated_gross_request_cost_usd'] for p in parts):.8f}"])
    summary = {'complete':True, 'binary_sha256':build['binary_sha256'], 'proof_files_verified':count,
               'metadata_ABBA':cases, 'startup_until_database_ready':restart_rows,
               'sql_query_cost':v['query_normalized'], 'SQL_load_and_drain':v['workload_normalized'],
               'recovery_passed':passes, 'counter_scope':'All instrumented HTTP attempts, each named phase separately; zero uncertain/unclassified requests.',
               'limits':'One fixed SQL lot, ABBA two repeats for metadata-only restarts. Instrumented durations do not establish a speedup. Fees projected on Tigris, not billed there.'}
    (OUT / 'summary.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False)+'\n')
    charts = OUT / 'charts'
    charts.mkdir(exist_ok=True)
    colors = ['#557696','#008e80']
    plt.rcParams.update({'font.family':'DejaVu Sans','axes.spines.top':False,'axes.spines.right':False})
    fig, axes = plt.subplots(1,2,figsize=(11,4.2),layout='constrained')
    for axis, key, title, unit, factor in [(axes[0],'B','Appels B par ouverture ↓','GET réussis',1),
                    (axes[1],'bytes','Index + HEAD téléchargés ↓','Kio',1/1024)]:
        values = [[c[key]*factor for c in cases if c['label'].startswith(p)] for p in ('off','on')]
        medians = [statistics.median(x) for x in values]
        axis.bar([0,1],medians,color=colors,width=.55)
        for i, points in enumerate(values):
            axis.scatter([i-.05,i+.05],points,c='#193047',s=20,zorder=4)
            axis.text(i,max(points)+max(medians)*.05,f'{medians[i]:,.2f}',ha='center')
        axis.set_xticks([0,1],['Sans cache d’index','Avec cache vérifié'])
        axis.set_title(title,fontweight='bold');axis.set_ylabel(unit);axis.set_ylim(0,max(medians)*1.25)
        axis.grid(axis='y',alpha=.15);axis.set_axisbelow(True)
    fig.savefig(charts/'metadata.png',dpi=145,facecolor='white');fig.savefig(charts/'metadata.svg',facecolor='white');plt.close(fig)
    picture=base64.b64encode((charts/'metadata.png').read_bytes()).decode()
    q=v['query_normalized'];sql=v['workload_normalized']
    body=f'''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>InfiniDisk2 — cache d’index et coût de reprise</title>
<style>*{{box-sizing:border-box}}body{{margin:0;background:#f0f4f8;color:#172d42;font:16px/1.55 system-ui,sans-serif}}main{{max-width:1080px;margin:auto;padding:32px 20px}}h1{{font-size:clamp(26px,4vw,42px);line-height:1.15}}h2{{margin-top:36px}}a{{color:#075db9}}.tag{{color:#087e70;font-weight:700}}.note{{background:#fff1ce;padding:16px;border-left:4px solid #b8810c}}figure{{background:white;border-radius:12px;margin:24px 0;padding:12px}}img{{width:100%;height:auto}}figcaption{{font-size:13px;color:#52657a}}.table{{overflow:auto}}table{{background:white;border-collapse:collapse;width:100%;margin:12px 0}}th,td{{text-align:left;padding:10px;border-bottom:1px solid #d9e4ef}}th{{background:#dfeaf4}}code{{overflow-wrap:anywhere}}li{{margin:8px 0}}@media(max-width:600px){{main{{padding:20px 12px}}th,td{{font-size:13px;padding:8px}}figure{{padding:5px}}}}</style>
<main><p class="tag">10 OCTOBRE 2026 · CODE ET REPRISE QUALIFIÉS</p><h1>Redémarrer à chaud avec un seul appel S3</h1>
<p>Le cache SSD conserve les objets d’index immuables, vérifiés contre le HEAD courant. Sur notre fixture PostgreSQL : <strong>15 → 1 GET par ouverture, soit −93,3 %</strong>. HEAD reste relu ; le WAL plus récent est rejoué et l’identité de l’écrivain est contrôlée.</p>
<figure><img src="data:image/png;base64,{picture}" alt="Appels B et octets reçus, cache désactivé contre cache vérifié"><figcaption>Deux passages par réglage, ordre sans/avec/avec/sans, même HEAD et même binaire. Points : mesures individuelles. <a href="charts/metadata.svg">SVG</a> · <a href="charts/metadata.png">PNG</a>.</figcaption></figure>
<p class="note">Il s’agit d’un gain d’appels et de bytes téléchargés. Les ouvertures instrumentées restent toutes proches de 0,303 s ; la cadence de détection du serveur masque les petites différences. Aucun gain de temps de démarrage n’est démontré ici.</p>
<h2>Comparaison contrôlée du cache d’index</h2>{table(['Passage','Appels B','Octets reçus','Ouverture (s)','$/10K ouvertures équivalentes'],[[c['label'],c['B'],c['bytes'],f"{c['seconds']:.4f}",f"{c['cost_per_10K_opens']:.3f}"] for c in cases])}
<p>Sans cache : un GET de HEAD et quatorze GET d’index. Avec cache : un GET de HEAD. Zéro GET de données et zéro PUT pendant ces quatre ouvertures et leurs arrêts. Égalité de HEAD contrôlée avant/après. Seul <code>remote_index_cache_mib</code> change entre 0 et 128 ; fichiers locaux, binaire et autres options restent identiques.</p>
<h2>Du démarrage du moteur à PostgreSQL disponible</h2>{table(['Cache local','A','B','Secondes instrumentées','Coût projeté du démarrage ($)'],restart_rows)}
<p>Chaud : même répertoire local. Froid : répertoire neuf et adoption. Le froid inclut 28 GET d’index, 136 GET de données, deux GET de HEAD et un PUT de HEAD. L’adoption vérifie les objets distants et le serveur les charge à nouveau : cette double lecture des index reste à optimiser. Les vérifications et l’arrêt de la DB sont exclus de cette fenêtre et comptés séparément ; l’arrêt chaud ajoute ici 10 A. PostgreSQL retrouve ses 256 transactions et passe pg_amcheck après chaque reprise.</p>
<h2>Coût SQL du profil actuel</h2><p>Un lot complet : 256 transactions sans échec, 1280 requêtes métier (1792 commandes avec BEGIN/END). Les phases charge <strong>et attente des uploads lors de l’arrêt propre</strong> totalisent {sql['request_classes']['A']} A et {sql['request_classes']['B']} B : un segment, huit index et un HEAD.</p>
{table(['Normalisation','Coût des opérations projeté ($)'],[['Lot mesuré de 1280 requêtes',f"{sql['estimated_gross_request_cost_usd']:.8f}"],['1000 requêtes métier',f"{q['estimated_usd_per_1000_queries']:.10f}"],['10 000 requêtes métier',f"{q['estimated_usd_per_10000_queries']:.9f}"]])}
<p>Comptage réel via proxy sur Elestio ; tarifs <a href="https://www.tigrisdata.com/pricing/">Tigris Standard vérifiés le 10 octobre</a> : A 5 $/million, B 0,50 $/million. Toutes les tentatives sont conservées, aucune requête non classée ou incertaine dans ce lot. Hors franchise, stockage, retrieval, notifications et autres ressources. Un lot court normalisé ne prédit pas une charge mensuelle. Les frais de création et de reprise restent séparés.</p>
<h2>Choix implémentés</h2><ul><li><code>remote_index_cache_mib = 128</code> dans le profil généré ; champs omis des anciennes configs : 0.</li><li>Budget SSD distinct des données et de la RAM de l’index ; maximum 16 384 fichiers et 1 Mio par objet.</li><li>Clé volume/shard/objet/hash, SHA-256 vérifié à chaque lecture, copie corrompue retéléchargée.</li><li>Écriture temporaire puis rename, sans barrière fsync supplémentaire ; erreur de cache : repli S3.</li><li>HEAD et WAL restent les autorités. Le scratch mutable n’est pas repris. Adopt, scrub et GC vérifient directement S3.</li></ul>
<h2>Validation ciblée</h2><p>26 tests Rust passent : quatre nouveaux cas, profil et 21 régressions moteur. Fmt, Clippy et build ublk passent. Une seule campagne de reprise, sans répéter les benchmarks de débit :</p>
{table(['Contrôle','Résultat'],[[name,'PASS'] for name in passes])}
<p>Ces vérifications couvrent SIGKILL, ext4, PostgreSQL et une restauration S3 avec CRC32C intégral de 256 Mio. Elles ne certifient pas une panne électrique matérielle. Les hashes et écritures SSD ajoutés aux checkpoints n’ont pas fait l’objet d’un nouveau benchmark de saturation TPS/p99.</p>
<p>Binaire <code>{build['binary_sha256']}</code>. {count} fichiers de preuve SHA-256 contrôlés avant le rendu. <a href="build/manifest.json">Build</a> · <a href="restart/report.json">Comptage et phases</a> · <a href="recovery/report.json">Reprise</a> · <a href="summary.json">Synthèse JSON</a>.</p>
<h2>Suite</h2><p>Priorité suivante : borner globalement les octets téléchargés en vol et adapter la concurrence, avec un seul scénario alternant aléatoire/séquentiel. Le p99 séquentiel avait augmenté de 19 % dans la campagne précédente ; il reste à traiter. Ensuite : supprimer le second téléchargement des index après adoption et regrouper les petits objets compactés.</p>
<p><a href="../../docs/index-cache.md">Spécifications et limites</a> · <a href="../../configs/recommended.toml">Configuration actuelle</a> · <a href="../adaptive/rapport.html">Lectures adaptatives et PostgreSQL précédents</a> · <a href="../astra/rapport.html">Comparaison complète historique avec ZeroFS et natif</a>. Aucun nouveau gain de TPS ou de débit applicatif n’est revendiqué dans cette itération. Les volumes de production restent inchangés.</p></main></html>'''
    (OUT/'rapport.html').write_text(body)
    print(json.dumps({'report':str(OUT/'rapport.html'),'verified_proofs':count,'B_off':[c['B'] for c in cases if c['label'].startswith('off')],'B_on':[c['B'] for c in cases if c['label'].startswith('on')]}))


if __name__ == '__main__':
    main()
