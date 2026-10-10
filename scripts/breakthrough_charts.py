"""Charts derived from archived measurements, without extrapolating missing series."""
import html
import json
import statistics

COLORS = {'infinidisk2': '#087fbc', 'zerofs': '#c86a18', 'native': '#30865a'}
CSS = '''
.charts-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,460px),1fr));gap:18px}
.chart{border:1px solid #d9e3eb;border-radius:10px;padding:18px;min-width:0}
.chart h3{margin:0 0 4px}.chart small,.chart-note{color:#526579;font-size:13px}
.chart-row{margin:12px 0}.bar-label{display:flex;justify-content:space-between;gap:10px;font-size:14px}
.bar-label strong{white-space:nowrap}.bar-track{height:16px;background:#edf2f7;border-radius:3px;margin-top:4px}
.bar-fill{height:100%;border-radius:3px}.legend{display:flex;gap:18px;flex-wrap:wrap}
.legend i{display:inline-block;width:12px;height:12px;margin-right:6px;border-radius:3px}
.chart details{font-size:12px;margin-top:12px}.chart details ul{padding-left:18px}
.chart-group{margin:28px 0 10px}.scale-control{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:16px 0}
.scale-control select{font:inherit;padding:5px;border:1px solid #bacbd8;border-radius:5px}
@media print{.chart{break-inside:avoid}.scale-control{display:none}}
'''


def render(root, cases):
    charts = []
    def load(path):
        return json.loads((root / 'validation' / path).read_text())
    def series(label, engine, samples, source, note=''):
        return {'label': label, 'engine': engine, 'samples': samples,
                'value': statistics.median(samples) if samples else None,
                'source': source, 'note': note}
    def add(group, title, unit, note, values):
        charts.append(dict(group=group, title=title, unit=unit, note=note, series=values))

    mysql_source = 'mysql-report.json'
    mysql = load(mysql_source)['mysql']
    engines = [('infinidisk2', 'InfiniDisk2 initial', 'infinidisk2'),
               ('zerofs-durable', 'ZeroFS · fsync S3', 'zerofs'),
               ('zerofs-async', 'ZeroFS · fsync ignoré', 'zerofs'),
               ('native', 'Natif · disque VM', 'native')]
    for workload, title in [('read_only', 'Lecture'), ('read_write', 'Lecture + écriture'), ('write_only', 'Écriture')]:
        add('MySQL / sysbench · comparaison initiale des trois outils', title, 'TPS',
            '4 × 25 000 lignes, 8 threads, 1 CPU, MySQL 1 Gio / buffer pool 256 Mio ; moteurs 1 Gio RAM / 4 Gio SSD. Trois passages de 15 s.',
            [series(label, color, [s['tps'] for s in mysql[key]['samples'][workload]], mysql_source)
             for key, label, color in engines])

    for workload, title in [('read_only', 'Lecture'), ('read_write', 'Lecture + écriture'), ('write_only', 'Écriture')]:
        values = []
        for key, label, color in [('local-active-fixed-mixed', 'InfiniDisk2 optimisé · NBD', 'infinidisk2'), ('native-final', 'Natif · disque VM', 'native')]:
            case = cases[key + '/' + workload]
            values.append(series(label, color, case['samples'], 'breakthroughs/summary.json',
                                 'p95 médian : %.2f ms' % statistics.median(case['p95_ms'])))
        values.insert(1, series('ZeroFS', 'zerofs', [], '', 'Non mesuré sur cette campagne'))
        add('MySQL / sysbench · dernière version optimisée, grande base', title, 'TPS',
            '4 × 1 000 000 lignes, accès uniformes, trois passages de 15 s. InfiniDisk2 : RAM moteur 64 Mio, SSD 4 Gio, cache actif préchauffé + page cache Linux ; natif MySQL O_DIRECT. Budgets de cache effectifs différents.', values)

    pg_source = 'comparison-postgres.json'
    pg = load(pg_source)['postgres']
    for keys, title, note in [
        (['infinidisk2', 'zerofs'], 'Transactions · fsync honoré', 'RAM moteur 64 Mio / SSD 128 Mio. InfiniDisk2 valide le journal local ; ZeroFS publie sur S3.'),
        (['infinidisk2-roomy', 'zerofs-async-roomy'], 'Transactions · ZeroFS asynchrone', 'RAM moteur 1 Gio / SSD 4 Gio. ZeroFS ignore fsync ; InfiniDisk2 respecte fsync local.')]:
        labels = ['InfiniDisk2 initial', 'ZeroFS · fsync ignoré' if 'async' in keys[1] else 'ZeroFS · fsync S3']
        values = [series(label, engine, [s['tps'] for s in pg[key]['samples']], pg_source)
                  for key, label, engine in zip(keys, labels, ['infinidisk2', 'zerofs'])]
        values.append(series('Natif', 'native', [], '', 'Non mesuré avec ce protocole'))
        add('PostgreSQL / pgbench', title, 'TPS', 'Scale 2, 4 clients, 1 CPU / 512 Mio PostgreSQL, trois passages de 15 s. ' + note, values)

    fio_source = 'comparison-raw.json'
    runs = load(fio_source)['runs']
    for key, title, direction, metric, unit, divisor, note in [
        ('cold-read', 'Lecture aléatoire 4 Kio · cache réduit', 'read', 'iops', 'IOPS', 1, 'RAM 64 Mio / SSD 128 Mio, jeu 256 Mio.'),
        ('warm-read', 'Lecture aléatoire 4 Kio · SSD préchauffé', 'read', 'iops', 'IOPS', 1, 'RAM 64 Mio / SSD 512 Mio, jeu 256 Mio préchargé.'),
        ('buffered-write', 'Écriture aléatoire 4 Kio · sans fsync', 'write', 'iops', 'IOPS', 1, 'RAM 64 Mio ; aucune garantie de durabilité à chaque écriture.'),
        ('fsync-write', 'Écriture aléatoire 4 Kio + fsync', 'write', 'iops', 'IOPS', 1, 'InfiniDisk2 : fsync local ; ZeroFS : publication S3. Natif : référence antérieure, un passage de 10 s et jeu 256 Mio au total (4 jobs × 64 Mio), contre trois de 15 s pour les moteurs. Ce n’est pas un essai simultané.'),
        ('seq-read', 'Lecture séquentielle 1 Mio · SSD préchauffé', 'read', 'bw_bytes', 'Mio/s', 1024**2, 'RAM 64 Mio / SSD 512 Mio, jeu 256 Mio préchargé.')]:
        values = [series('InfiniDisk2 initial' if engine == 'infinidisk2' else 'ZeroFS', engine,
                         [runs[f'{engine}-{key}-{i}'][direction][metric] / divisor for i in range(3)], fio_source)
                  for engine in ['infinidisk2', 'zerofs']]
        native = [load('baseline-randwrite-fsync.json')['jobs'][0]['write']['iops']] if key == 'fsync-write' else []
        values.append(series('Natif · référence antérieure' if native else 'Natif', 'native', native,
                             'baseline-randwrite-fsync.json' if native else '', 'Un seul passage' if native else 'Non mesuré avec ce protocole'))
        add('fio · accès bloc', title, unit, 'O_DIRECT, 4 jobs ; profondeur 32 en aléatoire, 1 avec fsync. ZeroFS : compression LZ4 et chiffrement applicatif ; InfiniDisk2 : aucun des deux. ' + note, values)

    esc = html.escape
    out = '<section id="comparatifs"><h2>Comparaisons de performances par outil</h2><p>Médianes mesurées ; plus haut = plus rapide. Les graphiques initiaux et optimisés correspondent à des campagnes distinctes. « Non mesuré » ne signifie pas zéro.</p>'
    out += '<div class="legend">' + ''.join(f'<span><i style="background:{color}"></i>{label}</span>' for label, color in [('InfiniDisk2', COLORS['infinidisk2']), ('ZeroFS', COLORS['zerofs']), ('Natif', COLORS['native'])]) + '</div>'
    out += '<p class="chart-note"><b>Durabilité :</b> InfiniDisk2 confirme fsync après persistance locale et réplique S3 ensuite ; ZeroFS « fsync S3 » attend S3 ; ZeroFS « fsync ignoré » ne fournit pas la même garantie. Le natif confirme sur le disque VM. Aucun ratio entre ces contrats n’est une accélération à garantie égale.</p>'
    out += '<label class="scale-control">Échelle des barres <select id="chart-scale"><option value="linear">Linéaire, depuis zéro</option><option value="log">Logarithmique · log10(1 + valeur)</option></select><small id="scale-note">Chaque carte possède sa propre échelle.</small></label>'
    group = None
    for chart in charts:
        if chart['group'] != group:
            if group is not None: out += '</div>'
            group = chart['group']
            out += '<h3 class="chart-group">' + esc(group) + '</h3><div class="charts-grid">'
        maximum = max(s['value'] or 0 for s in chart['series'])
        out += '<article class="chart" data-max="%s"><h3>%s</h3><small>%s · médiane</small>' % (maximum, esc(chart['title']), esc(chart['unit']))
        for s in chart['series']:
            value = s['value']
            number = ('{:,.2f}'.format(value).replace(',', ' ').replace('.', ',') + ' ' + chart['unit']) if value is not None else 'Non mesuré'
            out += '<div class="chart-row"><div class="bar-label"><span>' + esc(s['label']) + '</span><strong>' + esc(number) + '</strong></div>'
            if value is not None:
                out += '<div class="bar-track"><div class="bar-fill" data-value="%s" style="width:%.6f%%;background:%s"></div></div>' % (value, 100 * value / maximum, COLORS[s['engine']])
            out += '</div>'
        out += '<p class="chart-note">' + esc(chart['note']) + '</p><details><summary>Passages et sources</summary><ul>'
        for s in chart['series']:
            samples = ', '.join('%.2f' % value for value in s['samples']) if s['samples'] else 'non mesuré'
            out += '<li>' + esc(s['label'] + ' : ' + samples + ' ' + chart['unit'] + ('. ' + s['note'] if s['note'] else ''))
            if s['source']: out += ' · <a href="../' + esc(s['source']) + '">JSON source</a>'
            out += '</li>'
        out += '</ul></details></article>'
    out += '</div></section><script>document.getElementById("chart-scale").addEventListener("change",function(){const logarithmic=this.value==="log";document.querySelectorAll(".chart").forEach(function(card){const maximum=Number(card.dataset.max);card.querySelectorAll(".bar-fill").forEach(function(bar){const value=Number(bar.dataset.value);bar.style.width=(100*(logarithmic?Math.log10(1+value)/Math.log10(1+maximum):value/maximum))+"%";});});document.getElementById("scale-note").textContent=logarithmic?"Échelle log10(1 + valeur) : les longueurs ne représentent pas des ratios de performance.":"Chaque carte possède sa propre échelle.";});</script>'
    return out, charts
