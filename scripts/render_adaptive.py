#!/usr/bin/env python3
"""Render only completed, hashed evidence from the focused adaptive campaign."""
import base64
import hashlib
import html
import json
import os
from pathlib import Path
import statistics
import tomllib

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/adaptive'
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'test-output/matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sources(manifest):
    for name, expected in manifest['source_sha256'].items():
        current = ROOT / name
        if current.is_file() and sha(current) == expected:
            continue
        archived = OUT / 'source-snapshots' / (expected + '-' + Path(name).name)
        if not archived.is_file() or sha(archived) != expected:
            raise ValueError('qualification source mismatch: ' + name)


def table(headers, rows):
    return '<div class="table"><table><thead><tr>' + ''.join(
        '<th>' + html.escape(str(x)) + '</th>' for x in headers) + '</tr></thead><tbody>' + ''.join(
        '<tr>' + ''.join('<td>' + html.escape(str(x)) + '</td>' for x in row) + '</tr>'
        for row in rows) + '</tbody></table></div>'


def main():
    build = read(OUT / 'build/manifest.json')
    reads = read(OUT / 'reads/report.json')
    selected = read(OUT / 'selected/report.json')
    for value in (build, reads, selected):
        if not value['complete']:
            raise ValueError('incomplete qualification')
        sources(value)
    binary = build['binary_sha256']
    if reads['binary_sha256']['astra'] != binary or selected['binary_sha256']['selected'] != binary:
        raise ValueError('candidate binaries differ')
    verified = 0
    for name, expected in build['checks'].items():
        if sha(OUT / 'build' / name) != expected:
            raise ValueError('build log changed: ' + name)
        verified += 1
    manifest = read(OUT / 'reads' / reads['source_manifest'])
    for name, expected in manifest['files_sha256'].items():
        if sha(OUT / 'reads' / reads['id'] / name) != expected:
            raise ValueError('read proof changed: ' + name)
        verified += 1
    for name, case in selected['cases'].items():
        if not case['complete'] or case['returncode'] or not case.get('cleanup'):
            raise ValueError('qualification case incomplete: ' + name)
        for proof, expected in case['evidence_sha256'].items():
            if sha(OUT / 'selected' / name / proof) != expected:
                raise ValueError('selected-profile proof changed: ' + proof)
            verified += 1
    config = tomllib.loads((ROOT / 'configs/recommended.toml').read_text())
    reduced = {'memory_cache_mib': 64, 'disk_cache_mib': 128, 'max_index_mib': 64}
    for key, value in reads['protocol']['base_options'].items():
        if reduced.get(key, config[key]) != value:
            raise ValueError('documented profile differs from qualified profile: ' + key)

    series, sample_rows = {}, []
    for policy in ('fixed64', 'adaptive'):
        items = [v for n, v in reads['variants'].items() if n.startswith(policy + '-')]
        if len(items) != 2:
            raise ValueError('expected two ABBA repeats')
        for item in items:
            if (not item['complete'] or set(item['integrity'].values()) != {'passed'}
                    or item['cleanup']['failures'] or not all(item['cleanup'][k] for k in
                        ('nbd31_detached', 'mount_absent', 'container_removed'))):
                raise ValueError('read integrity or cleanup failed')
        series[policy] = {}
        for job in ('random-read', 'sequential-read'):
            values = []
            for item in items:
                s = item['samples'][job]['read']
                c = item['engine_data_counters'][job]
                row = {'iops': s['iops'], 'mib_s': s['bw_bytes'] / 2**20,
                       'p99_ms': s['clat_ns']['percentile']['99.000000'] / 1e6,
                       'gets': c['remote_gets'], 'mib': c['remote_bytes'] / 2**20}
                values.append(row)
                sample_rows.append([policy, job, len(values), f"{row['iops']:.2f}",
                                    f"{row['mib_s']:.2f}", f"{row['p99_ms']:.2f}",
                                    row['gets'], f"{row['mib']:.2f}"])
            series[policy][job] = {'samples': values, 'median': {
                key: statistics.median(x[key] for x in values) for key in values[0]}}
    pg = {}
    for profile in ('previous', 'selected'):
        value = selected['cases']['postgres-' + profile]['result']['postgres']['infinidisk2']
        if (value['settings'] != ['on'] * 4 or value['database_SIGKILL_recovery'] != 'passed'
                or any(s['failed_transactions'] for s in value['samples'])):
            raise ValueError('PostgreSQL qualification failed')
        pg[profile] = {**value, 'median_tps': statistics.median(s['tps'] for s in value['samples'])}
    recovery = selected['cases']['recovery-selected']['result']['tests']
    passes = [k for k, value in recovery.items() if value == 'passed']
    if len(passes) != 7:
        raise ValueError('seven recovery checks required')
    summary = {'complete': True, 'binary_sha256': binary, 'verified_proof_files': verified,
               'reads': series, 'postgres': pg, 'recovery_passed': passes,
               'counter_scope': reads['protocol']['counter_scope'],
               'limits': selected['protocol']['limits'], 'recommended_config': config,
               'pricing': {'url': 'https://www.tigrisdata.com/pricing/', 'checked_utc_date': '2026-10-10',
                           'B_USD_per_million': .5, 'scope': 'Successful data GET subtotal only; no complete billable accounting.'}}
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n')
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False})
    charts = OUT / 'charts'
    charts.mkdir(exist_ok=True)
    colors = ['#496a95', '#009788']

    def panel(ax, title, unit, values, labels):
        medians = [statistics.median(x) for x in values]
        ax.bar([0, 1], medians, color=colors, width=.55)
        for i, points in enumerate(values):
            ax.scatter([i + (j - (len(points)-1)/2) * .1 for j in range(len(points))],
                       points, c='#172235', marker='o', s=22, zorder=4)
            ax.text(i, max(points) + max(medians)*.035, f'{medians[i]:,.2f}', ha='center', fontsize=10)
        ax.set_xticks([0, 1], labels)
        ax.set_title(title, fontweight='bold', pad=12)
        ax.set_ylabel(unit)
        ax.set_ylim(0, max(max(x) for x in values) * 1.20)
        ax.grid(axis='y', alpha=.15)
        ax.set_axisbelow(True)

    def figure(name, definitions, labels):
        fig, axes = plt.subplots(1, len(definitions), figsize=(6*len(definitions), 4.3), layout='constrained')
        if len(definitions) == 1:
            axes = [axes]
        for axis, (title, unit, values) in zip(axes, definitions):
            panel(axis, title, unit, values, labels)
        fig.savefig(charts / (name + '.png'), dpi=140, facecolor='white')
        fig.savefig(charts / (name + '.svg'), facecolor='white')
        plt.close(fig)
        encoded = base64.b64encode((charts / (name + '.png')).read_bytes()).decode()
        return '<figure><img alt="' + html.escape(name) + '" src="data:image/png;base64,' + encoded + '"><figcaption>Barres : médianes ; points : passages individuels. <a href="charts/' + name + '.svg">SVG</a> · <a href="charts/' + name + '.png">PNG</a></figcaption></figure>'

    def values(job, key):
        return [[x[key] for x in series[policy][job]['samples']] for policy in ('fixed64', 'adaptive')]

    charts_html = figure('debit-lectures', [
        ('Aléatoire à froid ↑', 'IOPS, 4 Kio QD32', values('random-read', 'iops')),
        ('Séquentiel à froid ↑', 'Mio/s, 1 Mio QD16', values('sequential-read', 'mib_s'))], ['Fixe 64 Kio', 'Adaptatif'])
    charts_html += figure('operations-donnees', [
        ('GET aléatoires ↓', 'GET de données réussis / lot', values('random-read', 'gets')),
        ('GET séquentiels ↓', 'GET de données réussis / lot', values('sequential-read', 'gets'))], ['Fixe 64 Kio', 'Adaptatif'])
    charts_html += figure('latence-lectures', [
        ('p99 aléatoire ↓', 'ms, complétion', values('random-read', 'p99_ms')),
        ('p99 séquentiel ↓', 'ms, complétion', values('sequential-read', 'p99_ms'))], ['Fixe 64 Kio', 'Adaptatif'])
    charts_html += figure('postgres', [('PostgreSQL, profil complet ↑', 'Transactions/s',
        [[x['tps'] for x in pg[name]['samples']] for name in ('previous', 'selected')])], ['Astra précédent', 'Profil sélectionné'])
    pg_gain = (pg['selected']['median_tps'] / pg['previous']['median_tps'] - 1) * 100
    config_rows = [[k, str(v).lower() if isinstance(v, bool) else v] for k, v in config.items()]
    costs = []
    for policy in ('fixed64', 'adaptive'):
        for job in ('random-read', 'sequential-read'):
            v = series[policy][job]['median']
            costs.append([policy, job, f"{v['mib']:.2f} Mio", f"{v['gets']:.0f}", f"{v['gets']*.5/1e6:.6f} $"])
    body = f'''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>InfiniDisk2 — profil sélectionné et lectures adaptatives</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#eff3f8;color:#182a3e;font:16px/1.55 system-ui,sans-serif}}main{{max-width:1120px;margin:auto;padding:32px 20px}}h1{{font-size:clamp(26px,4vw,42px);line-height:1.16}}h2{{margin-top:38px}}a{{color:#075cc7}}.tag{{color:#087e70;font-weight:700}}.note{{background:#fff3d4;padding:16px;border-left:4px solid #b98005}}figure{{margin:22px 0;background:white;border-radius:12px;padding:12px}}img{{width:100%;height:auto}}figcaption{{font-size:13px;color:#4c5c70}}.table{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;background:white;margin:12px 0}}th,td{{text-align:left;padding:10px;border-bottom:1px solid #dee5ee}}th{{background:#dfe9f3}}code{{overflow-wrap:anywhere}}li{{margin:8px 0}}details{{margin:18px 0}}@media(max-width:600px){{main{{padding:20px 12px}}td,th{{padding:8px;font-size:13px}}figure{{padding:5px}}}}
</style><main><p class="tag">10 OCTOBRE 2026 · QUALIFICATION CIBLÉE TERMINÉE</p>
<h1>Les réglages retenus, avec une lecture adaptative mesurée</h1>
<p>WAL 32 Mio, compaction des checkpoints désactivée, cache et pipeline Astra activés, lectures S3 de 16 ou 256 Kio selon la densité physique. Le contrat reste <strong>fsync local et publication distante asynchrone</strong>. Les anciens fichiers de configuration gardent leurs défauts implicites.</p>
<p><strong>Résultats :</strong> séquentiel à froid +32 % de débit et −75 % de GET de données ; aléatoire −67 % d'octets téléchargés mais +12,2 % de GET ; PostgreSQL {pg_gain:+.1f} % sur ce test court. Sept contrôles de reprise passent.</p>
<p class="note">Le p99 séquentiel augmente d'environ 19 %. Le débit aléatoire varie fortement entre passages. Ces résultats ne démontrent pas un gain universel, ni une baisse de tous les coûts S3. Les points dans les graphiques montrent chaque mesure.</p>
{charts_html}
<h2>Protocole et périmètre</h2><p>Lectures : un même binaire, fixe 64 Kio contre adaptatif, ordre A–B–B–A, deux passages par politique. Cache local neuf par charge, HTTPS direct vers Elestio, dataset CRC32C identique. Aléatoire : 4096 × 4 Kio, QD32, 16 Mio utiles ; séquentiel : 256 Mio, blocs 1 Mio, QD16. Pas de purge globale du cache de la VM partagée.</p>
<p>PostgreSQL : ancien binaire Astra compact 8 Mio contre nouveau profil sans compaction, trois passages de 15 s par profil, bases neuves scale 2, quatre clients, CPU PostgreSQL limité à un cœur et mémoire à 512 Mio. Fsync, synchronous_commit, full_page_writes et checksums activés. Caches moteur identiques : RAM 64 Mio, SSD 128 Mio, index résident 64 Mio. Zéro transaction échouée ; reprise PostgreSQL vérifiée dans chaque cas. La comparaison porte sur l'ensemble du profil.</p>
<p>Pas de nouveau benchmark MySQL, natif ou ZeroFS cette fois. La <a href="../astra/rapport.html">campagne complète précédente, avec ses graphiques par outil</a> reste séparée et conserve sa date, son binaire et ses contrats de durabilité.</p>
<h2>Octets et sous-total GET</h2>{table(['Politique','Charge','Téléchargement','GET réussis','Projection B du lot'], costs)}
<p>Le compteur moteur exclut HEAD, index, requêtes échouées et retries : <strong>ce n'est pas le total facturable A/B</strong>. Projection du seul sous-total de données à 0,50 $/million B selon <a href="https://www.tigrisdata.com/pricing/">Tigris Standard, vérifié le 10 octobre 2026</a>, hors franchise et stockage. Aucun nouveau coût par SQL n'est inventé à partir de ces compteurs ; la <a href="../../docs/astra-s3-operations.md">mesure complète précédente par 1K/10K SQL</a> conserve son protocole.</p>
<details><summary>Mesures individuelles des lectures</summary>{table(['Politique','Charge','Passage','IOPS','Mio/s','p99 ms','GET','Mio reçus'], sample_rows)}</details>
<details><summary>Valeurs de configuration de déploiement</summary>{table(['Paramètre','Valeur'], config_rows)}<p>Les budgets recommandés ci-dessus sont plus grands que ceux de la comparaison contrôlée. <a href="../../configs/recommended.toml">Configuration complète</a>.</p></details>
<h2>Intégrité et reproductibilité</h2><p>30 tests Rust ciblés passent, ainsi que fmt, Clippy et le build avec ublk. La première exécution parallèle NBD a rencontré un verrou local à la réouverture ; les deux tests passent en ordre isolé. L'échec initial est archivé et sa cause précise n'est pas établie.</p>
{table(['Contrôle de reprise','Résultat'], [[name,'PASS'] for name in passes])}
<p>Les tests couvrent SIGKILL du serveur après fsync, crash PostgreSQL, crash moteur pendant les transactions, scrub distant, restauration sans état local et CRC32C intégral des 256 Mio restaurés. Ils ne qualifient pas une coupure électrique matérielle. La commande <code>--checks-only</code> conserve ces contrôles et supprime les mesures de débit redondantes.</p>
<p>Binaire candidat : <code>{binary}</code>. {verified} fichiers de preuve vérifiés par SHA-256 avant ce rendu. <a href="build/manifest.json">Build et commandes</a> · <a href="reads/report.json">Lectures</a> · <a href="selected/report.json">PostgreSQL et reprise</a> · <a href="summary.json">Synthèse JSON</a>.</p>
<h2>Recherches suivantes</h2><ol><li><strong>Index distant conservé et vérifié sur SSD :</strong> objectif 15→1 GET au redémarrage chaud de la fixture PostgreSQL historique, en relisant HEAD et en rejouant le WAL. Proposition, pas résultat livré.</li><li><strong>Concurrence adaptative bornée en octets :</strong> viser le p99 séquentiel et les suites de petites lectures, avec un scénario alternant les charges.</li><li><strong>Objets compactés regroupant plusieurs shards :</strong> réduire les petits PUT de données avant d'adapter la fréquence de compaction au coût complet.</li></ol>
<p><a href="../../docs/adaptive-reads.md">Spécification complète, décisions, limites et critères des prochains essais</a>. Aucun volume de production n'a été reconfiguré.</p></main></html>'''
    (OUT / 'rapport.html').write_text(body)
    print(json.dumps({'report': str(OUT / 'rapport.html'), 'verified_proofs': verified,
                      'postgres_change_percent': pg_gain, 'charts': 4}))


if __name__ == '__main__':
    main()
