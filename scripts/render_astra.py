#!/usr/bin/env python3
"""Render only archived measurements, keeping durability contracts explicit."""
import argparse
import base64
import hashlib
import html
import json
import math
import os
import pathlib
import re
import statistics

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/astra'
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'test-output/matplotlib-cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from breakthrough_charts import CSS as OLD_CSS, render as historical_charts

COLORS = {'baseline': '#64748b', 'astra': '#0369a1', 'native': '#15803d',
          'zerofs': '#c2410c', 'generation': '#7e22ce'}
LABELS = {'read_only': 'Lecture', 'read_write': 'Lecture + écriture', 'write_only': 'Écriture'}


def load(path):
    return json.loads(path.read_text())


def table(headers, rows):
    esc = html.escape
    return '<div class="scroll"><table><thead><tr>' + ''.join('<th>' + esc(h) + '</th>' for h in headers) + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join('<td>' + esc(str(c)) + '</td>' for c in row) + '</tr>' for row in rows) + '</tbody></table></div>'


def plot(name, title, unit, series, subtitle, logarithmic=False):
    valid = [s for s in series if s['values']]
    fig, ax = plt.subplots(figsize=(10, max(3.4, len(valid) * .56 + 1.5)))
    labels = [s['label'] + ('\nnon qualifié' if s.get('unqualified') else '') for s in valid]
    values = [statistics.median(s['values']) for s in valid]
    lo = [v - min(s['values']) for s, v in zip(valid, values)]
    hi = [max(s['values']) - v for s, v in zip(valid, values)]
    bars = ax.barh(range(len(valid)), values, color=[COLORS[s['color']] for s in valid],
                   height=.64, xerr=[lo, hi], error_kw={'capsize': 3, 'lw': 1})
    for bar, item in zip(bars, valid):
        if item.get('unqualified'):
            bar.set_hatch('///')
            bar.set_edgecolor('#172554')
            bar.set_alpha(.6)
    ax.set_yticks(range(len(valid)), labels)
    ax.invert_yaxis()
    ax.set_xlabel(unit + (' · échelle logarithmique' if logarithmic else ''))
    positive = [value for item in valid for value in item['values'] if value > 0]
    left = max(1e-12, min(positive) * .4) if positive else .1
    right = max(positive) * (1.8 if logarithmic else 1.3) if positive else 1
    if logarithmic:
        ax.set_xscale('log')
        ax.set_xlim(left=left)
    else:
        ax.set_xlim(left=0)
    ax.set_xlim(right=right)
    for i, value in enumerate(values):
        position = max(valid[i]['values'])
        if logarithmic and position == 0:
            position = left
        digits = 4 if unit.startswith('USD') else 2
        ax.annotate(f'{value:,.{digits}f}'.replace(',', ' '), (position, i), xytext=(6, 0),
                    textcoords='offset points', va='center', fontsize=10)
    ax.set_title(title, loc='left', fontsize=14, pad=16, fontweight='bold')
    ax.grid(axis='x', alpha=.2)
    ax.set_axisbelow(True)
    for spine in ['top', 'right', 'left']:
        ax.spines[spine].set_visible(False)
    has_unqualified = any(s.get('unqualified') for s in valid)
    footnote = ('Un passage par série ; pas d’intervalle de variabilité mesuré.'
                if all(len(item['values']) == 1 for item in valid) else
                'Médiane ; barres d’erreur = minimum–maximum des passages. Aucune extrapolation.')
    if has_unqualified:
        footnote += '\nHachures : essai avec erreurs SQL, mesures brutes exclues des ratios de gain.'
    fig.text(.02, .015, footnote, fontsize=8, color='#475569')
    fig.tight_layout(rect=(0, .09 if has_unqualified else .05, 1, 1))
    directory = OUT / 'charts'
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (name + ('-log' if logarithmic else '') + '.svg')
    fig.savefig(path, format='svg', metadata={'Date': None})
    fig.savefig(path.with_suffix('.png'), dpi=150, metadata={'Software': 'matplotlib ' + matplotlib.__version__})
    plt.close(fig)
    embedded = 'data:image/svg+xml;base64,' + base64.b64encode(path.read_bytes()).decode()
    missing = ', '.join(s['label'] + ' (' + s.get('omitted_reason', 'non mesuré') + ')' for s in series if not s['values'])
    qualification_note = (' Les barres hachurées conservent les mesures brutes des essais complets avec erreurs SQL ; l’essai entier reste non qualifié, même si la charge représentée ne signale aucune erreur. Aucun ratio de gain n’en est déduit.' if has_unqualified else '')
    return '<figure><img alt="' + html.escape(title) + '" src="' + embedded + '"><figcaption>' + html.escape(subtitle + qualification_note) + ('. Données non présentées : ' + html.escape(missing) if missing else '') + ' <a href="charts/' + path.name + '">SVG</a> · <a href="charts/' + path.with_suffix('.png').name + '">PNG</a></figcaption></figure>'


def chart(*args):
    return '<div><div class="astra-linear">' + plot(*args) + '</div><div class="astra-log">' + plot(*args, logarithmic=True) + '</div></div>'


def metric_series(label, color, values, source):
    return {'label': label, 'color': color, 'values': [v for v in values if v is not None and math.isfinite(v)], 'source': source}


def checked_values(values, count, label):
    if (len(values) != count or any(type(value) not in (int, float)
            or not math.isfinite(value) or value <= 0 for value in values)):
        raise ValueError('Incomplete or invalid measurements: ' + label)
    return values


def recovery_charts(recoveries):
    """The fresh runs compare alignment only; never borrow a ZeroFS baseline."""
    output, exported = '', []
    metrics = [
        ('fio-fsync', 'fio · écritures aléatoires 4 Kio + fsync', 'IOPS', 'randwrite-fsync', 'write', 'iops'),
        ('fio-hot-read', 'fio · lectures aléatoires 4 Kio en cache', 'IOPS', 'randread-hot', 'read', 'iops'),
        ('fio-remote-read', 'fio · lectures aléatoires depuis S3', 'IOPS', 'randread-cold-remote', 'read', 'iops'),
        ('fio-full-cache', 'fio · lectures après remplissage du cache', 'IOPS', 'randread-fully-warm-cache', 'read', 'iops'),
        ('fio-sequential', 'fio · lectures séquentielles en cache', 'Mio / seconde', 'seqread-hot', 'read', 'bw_bytes'),
        ('pgbench', 'PostgreSQL · pgbench durable', 'transactions / seconde', 'pgbench-durable', None, None),
    ]
    for name, title, unit, key, direction, metric in metrics:
        series = []
        for run, report in recoveries:
            if not report.get('passed'):
                continue
            value = report.get('benchmarks', {}).get(key)
            if value is None:
                continue
            if key == 'pgbench-durable':
                match = re.search(r'tps\s*=\s*([\d.]+)\s*\(without initial connection time\)', value)
                number = float(match.group(1)) if match else None
            else:
                number = value.get(direction, {}).get(metric)
                if number is not None and metric == 'bw_bytes':
                    number /= 1024 ** 2
            label = 'Astra · ' + ('WAL aligné' if run.endswith('aligned') else 'WAL compact')
            series.append(metric_series(label, 'generation' if run.endswith('aligned') else 'astra', [number], run + '/report.json'))
            if key == 'randwrite-fsync':
                native = report.get('benchmarks', {}).get('baseline-randwrite-fsync', {}).get('write', {}).get('iops')
                series.append(metric_series('Natif · passage ' + run.removeprefix('recovery-'), 'native', [native], run + '/report.json'))
        if any(s['values'] for s in series):
            output += chart(name, title, unit, series, 'Qualification sur volumes neufs : un passage par profil, fio 10 s / pgbench 15 s. Référence native rejouée pour fsync seulement. Aucun ZeroFS contemporain dans cette campagne ; historique séparé ci-dessous.')
            exported.append({'name': name, 'series': series})
    return output, exported


def fio_comparison_charts(report):
    if not report or not report.get('complete') or not report.get('comparison', {}).get('complete'):
        return '', []
    runs = {**report['comparison']['runs'], **report['native_runs']}
    output, exported = '', []
    for key, title, direction, metric, unit in [
        ('buffered-write', 'fio · écritures aléatoires 4 Kio sans fsync individuel', 'write', 'iops', 'IOPS'),
        ('fsync-write', 'fio · écritures aléatoires 4 Kio + fsync', 'write', 'iops', 'IOPS'),
        ('cold-read', 'fio · lectures aléatoires depuis un cache moteur neuf', 'read', 'iops', 'IOPS'),
        ('warm-read', 'fio · lectures aléatoires après préchauffage', 'read', 'iops', 'IOPS'),
        ('seq-read', 'fio · lectures séquentielles après préchauffage', 'read', 'bw_bytes', 'Mio / seconde'),
    ]:
        series = []
        engines = [('infinidisk2', 'InfiniDisk2 · Astra', 'astra'),
                   ('zerofs', 'ZeroFS · fsync S3', 'zerofs')]
        if key == 'fsync-write':
            engines.append(('zerofs-async', 'ZeroFS · fsync ignoré', 'zerofs'))
        if key != 'cold-read':
            engines.append(('native', 'Natif · fichier sur disque VM', 'native'))
        for engine, label, color in engines:
            values = [runs.get(engine + '-' + key + '-' + str(i), {}).get(direction, {}).get(metric)
                      for i in range(3)]
            checked_values(values, 3, 'fio ' + engine + '/' + key)
            if metric == 'bw_bytes':
                values = [v / 1024 ** 2 if v is not None else None for v in values]
            series.append(metric_series(label, color, values, 'fio/report.json'))
        name = 'fio-comparison-' + key
        note = 'Trois passages de 15 s. RAM moteur 64 Mio ; SSD 128 Mio pour écritures et lectures froides, 512 Mio pour lectures préchauffées. ZeroFS async garde le budget 128 Mio des écritures durables, avec son propre cache neuf. Moteurs sur bloc brut ; natif sur fichier O_DIRECT du disque VM. Les moteurs bénéficient aussi du cache Linux. ZeroFS garde LZ4 et chiffrement ; InfiniDisk2 ne les implémente pas.'
        if key == 'cold-read':
            note += ' Cache moteur neuf par passage ; caches du fournisseur S3 non contrôlés. Aucun natif étiqueté artificiellement froid S3.'
        output += chart(name, title, unit, series, note)
        exported.append({'name': name, 'series': series})
    return output, exported


def postgres_comparison_charts(report):
    if not report or not report.get('complete'):
        return '', []
    output, exported = '', []
    for metric, suffix, title, unit in [
        ('tps', 'throughput', 'PostgreSQL · pgbench durable', 'transactions / seconde'),
        ('latency_ms', 'latency', 'PostgreSQL · latence moyenne des transactions', 'millisecondes · plus bas = mieux'),
    ]:
        series = []
        for key, label, color in [('baseline', 'InfiniDisk2 · avant Astra', 'baseline'),
                                  ('astra', 'InfiniDisk2 · Astra', 'astra'),
                                  ('zerofs-durable', 'ZeroFS · fsync S3', 'zerofs'),
                                  ('native', 'Natif · disque VM', 'native')]:
            samples = report['series'].get(key, {}).get('samples', [])
            if any(sample.get('failed_transactions') != 0 for sample in samples):
                raise ValueError('PostgreSQL failed transactions in ' + key)
            values = checked_values([sample.get(metric) for sample in samples], 3, 'PostgreSQL ' + key + '/' + metric)
            series.append(metric_series(label, color, values, 'postgres/report.json'))
        name = 'postgres-comparison-' + suffix
        output += chart(name, title, unit, series, 'PostgreSQL 16, scale 2, 4 clients, 1 CPU / 512 Mio ; trois passages de 15 s. Versions InfiniDisk2 avant/après rejouées dans cette campagne, avec les mêmes budgets. fsync, full_page_writes et synchronous_commit activés. InfiniDisk2 acquitte le disque local ; ZeroFS attend S3. Latence moyenne pgbench, pas p99.')
        exported.append({'name': name, 'series': series})
    return output, exported


def warm_charts(report, source):
    if not report:
        return '', []
    runs = sorted((run for run in report.get('runs', [])
                   if run.get('complete') and not run.get('optional')
                   and run.get('concurrency')), key=lambda run: run['concurrency'])
    if not runs:
        return '', []
    output, exported = '', []
    for metric, suffix, title, unit in [
        ('seconds', 'duration', 'Préchargement S3 · temps total à cache neuf', 'secondes · plus bas = mieux'),
        ('useful_mib_per_second', 'throughput', 'Préchargement S3 · débit utile', 'Mio / seconde'),
    ]:
        series = [metric_series(str(run['concurrency']) + ' téléchargements maximum', 'astra',
                                [run['cold'].get(metric)], source) for run in runs]
        if not any(value['values'] for value in series):
            continue
        name = 'warm-concurrency-' + suffix
        note = 'Même HEAD S3 et mêmes options ; cache applicatif neuf par niveau. Un passage exploratoire, dans l’ordre ' + ', '.join(map(str, report.get('concurrency_order', []))) + '. Durée incluant démarrage et index. Débit utile = pages × 4 Kio / durée ; caches Linux et fournisseur non purgés.'
        output += chart(name, title, unit, series, note)
        exported.append({'name': name, 'series': series})
    return output, exported


def compaction_control_charts(report):
    if not report or not report.get('complete'):
        return '', []
    before, after = report['reference'], report['control']
    expected = {**before['options'], 'compact_checkpoints': False}
    if not before['options'].get('compact_checkpoints') or after['options'] != expected:
        raise ValueError('Compaction diagnostic changed more than its single option')
    output, exported, resource_rows = '', [], []
    for workload in ('read_write', 'write_only'):
        for metric, suffix, unit in [('tps', 'throughput', 'transactions / seconde'),
                                    ('p99_ms', 'latency', 'millisecondes · plus bas = mieux')]:
            series = []
            for stage, label, color in [(before, 'Astra · compaction active', 'astra'),
                                         (after, 'Astra · compaction désactivée', 'generation')]:
                measurement = stage['summary'][workload]
                reasons = set(stage.get('comparison_exclusion_reasons', []))
                sql_error = bool(measurement.get('ignored_errors') or 'ignored_sql_errors' in reasons)
                if reasons - {'ignored_sql_errors'}:
                    raise ValueError('Invalid compaction diagnostic provenance or protocol')
                values = checked_values(measurement.get(metric, []), 3, 'compaction/' + label + '/' + workload + '/' + metric)
                series.append({**metric_series(label, color, values, 'mysql/' + stage['proof_report']),
                               'unqualified': sql_error,
                               'eligible_for_ratio': stage.get('eligible_for_comparison') is True and not sql_error})
            name = 'compaction-control-' + workload + '-' + suffix
            output += chart(name, LABELS[workload] + (' · débit' if metric == 'tps' else ' · p99'), unit,
                            series, 'Diagnostic distinct : seule compact_checkpoints passe de true à false ; index paginé et autres options conservés. 1 CPU MySQL, 3 × 30 s. La référence précède le contrôle sur une base évolutive ; les résultats ne remplacent pas la qualification initiale et ne prouvent pas seuls une causalité.')
            exported.append({'name': name, 'series': series})
        for stage, label in [(before, 'Compaction active'), (after, 'Compaction désactivée')]:
            resources = stage['resource_summary'][workload]
            resource_rows.append([label + ' / ' + LABELS[workload],
                                  round(resources['median_read_bytes'] / 1024**2, 2),
                                  round(resources['median_write_bytes'] / 1024**2, 2),
                                  round(resources['median_rss_after_kib'] / 1024, 2),
                                  round(resources['max_peak_rss_lifetime_kib'] / 1024, 2),
                                  stage['summary'][workload].get('ignored_errors', 0)])
    output = '<div class="figures">' + output + '</div><p>Les compteurs suivants encadrent les fenêtres sysbench, avec leur lancement et sortie. Le RSS est celui du moteur seul ; son maximum est un pic depuis le démarrage du processus, pas un pic limité au passage. Les volumes d’I/O ne sont pas normalisés par transaction.</p>' + table(
        ['Profil / charge', 'Lecture physique médiane Mio', 'Écriture physique médiane Mio', 'RSS final médian Mio', 'Pic RSS depuis démarrage Mio', 'Erreurs SQL'], resource_rows)
    return output, exported


def s3_accounting(report):
    """Recompute the accounting from the archived one-record-per-attempt traces."""
    if not report or not report.get('complete'):
        return '', '', [], {}
    from s3_request_pricing import project_events, sql_query_units
    directory = OUT / 's3-operations'
    archive = directory / report['id']
    manifest = load(directory / report['source_manifest'])
    for relative, expected in manifest['files_sha256'].items():
        path = archive / relative
        if not path.is_relative_to(archive) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('S3 accounting evidence changed: ' + relative)
    if load(directory / report['source_report']) != report:
        raise ValueError('S3 accounting canonical report differs from its archive')
    variants = [('baseline', 'InfiniDisk2 · avant Astra', 'baseline'),
                ('astra', 'InfiniDisk2 · Astra', 'astra'),
                ('zerofs-durable', 'ZeroFS · fsync S3', 'zerofs'),
                ('native', 'Natif', 'native')]
    if set(report['variants']) != {key for key, _, _ in variants}:
        raise ValueError('S3 accounting variant missing')
    restarts, totals, phases, objects, query_rows = {}, [], [], [], []
    for key, label, color in variants:
        variant = report['variants'][key]
        if not variant.get('complete'):
            raise ValueError('S3 accounting variant incomplete')
        restarts[key] = {}
        for workload in (('postgres',) if key == 'native' else ('postgres', 'fio')):
            fixture = variant['workloads'][workload]
            trace = archive / key / workload / 'requests.jsonl'
            events = [json.loads(line) for line in trace.read_text().splitlines() if line]
            if not fixture.get('complete') or fixture['cleanup']['failures']:
                raise ValueError('S3 fixture incomplete or cleanup failed')
            if not all(fixture['cleanup'].get(name) for name in ('nbd31_detached', 'mount_absent', 'container_removed')):
                raise ValueError('S3 fixture cleanup unproven')
            expected_checks = ('prepared', 'warm_restored', 'cold_restored') if workload == 'postgres' else ('warm_restored_crc32c', 'cold_restored_crc32c')
            if any(fixture['integrity'].get(name) != 'passed' for name in expected_checks):
                raise ValueError('S3 restart integrity unproven')
            if project_events(events) != fixture['summary']:
                raise ValueError('S3 total differs from the attempt trace')
            if key != 'native':
                proxy = fixture['proxy']
                if (proxy['local_rejections'] or proxy['closed_error'] or proxy['inflight']
                        or not proxy['journal_complete']
                        or proxy['forwarded'] != fixture['summary']['total_upstream_attempts']):
                    raise ValueError('S3 proxy evidence incomplete')
            if workload == 'postgres':
                if fixture.get('committed_history_rows') != {'prepared': 0, 'warm_restored': 256, 'cold_restored': 256}:
                    raise ValueError('S3 PostgreSQL restart transaction count unproven')
                normalized = project_events([event for event in events
                    if event['phase'] in ('postgres.load', 'postgres.drain')], 256)
                if fixture.get('transactions') != 256 or normalized != fixture['workload_normalized']:
                    raise ValueError('S3 transaction normalization mismatch')
                if normalized != variant['workload_normalized']:
                    raise ValueError('S3 variant normalization mismatch')
                units = sql_query_units(normalized, fixture['sql_accounting'])
                if units != fixture['query_normalized'] or units != variant['query_normalized']:
                    raise ValueError('SQL query normalization mismatch')
                query_rows.append([label, units['completed_business_queries'],
                    f"{units['estimated_usd_per_1000_queries']:.6f}",
                    f"{units['estimated_usd_per_10000_queries']:.6f}",
                    f"{normalized['estimated_usd_per_1000_transactions']:.6f}",
                    f"{normalized['estimated_usd_per_10000_transactions']:.6f}",
                    round(units['class_a_per_1000_queries'], 3), round(units['class_b_per_1000_queries'], 3)])
            total = fixture['summary']
            totals.append([label, workload, total['total_upstream_attempts'],
                total['request_classes']['A'], total['request_classes']['B'],
                total['request_classes']['FREE'], f"{total['estimated_gross_request_cost_usd']:.8f}",
                total['assumed_class_requests'], total['transport_uncertain_requests'] + total['unknown_class_requests']])
            for name, phase in fixture['phases'].items():
                counted = project_events([event for event in events if event['phase'] == name])
                if counted != phase['summary']:
                    raise ValueError('S3 phase differs from its trace: ' + name)
                phases.append([label, name, round(phase['seconds'], 3), counted['total_upstream_attempts'],
                    counted['request_classes']['A'], counted['request_classes']['B'],
                    round(counted['response_payload_bytes'] / 1024**2, 3),
                    f"{counted['estimated_gross_request_cost_usd']:.8f}"])
                if '.warm_' in name or '.cold_' in name:
                    objects.append([label, name, *[counted['by_object_type'].get(kind, 0)
                        for kind in ('infinidisk_head', 'infinidisk_index', 'segment', 'wal', 'sst', 'manifest', 'other')]])
            restarts[key][workload] = {}
            for temperature in ('warm', 'cold'):
                names = [workload + '.' + temperature + '_open']
                if workload == 'postgres':
                    names.append(workload + '.' + temperature + '_database_open')
                entry = project_events([event for event in events if event['phase'] in names])
                entry['seconds'] = sum(fixture['phases'][name]['seconds'] for name in names)
                entry['phases'] = names
                restarts[key][workload][temperature] = entry
    output, exported = '', []
    for cls in ('A', 'B'):
        series = [metric_series(label, color,
            [report['variants'][key]['workload_normalized']['request_classes'][cls]], 's3-operations/report.json')
            for key, label, color in variants]
        name = 's3-postgres-class-' + cls.lower()
        output += chart(name, 'PostgreSQL · appels de classe ' + cls, 'appels pour 256 transactions + drainage', series,
            'Travail fixé : 256 transactions terminées, puis arrêt propre de la base et publication distante. Préparation, attente et reprises exclues. Un passage instrumenté ; les contrats fsync local et S3 restent différents.')
        exported.append({'name': name, 'series': series})
    series = [metric_series(label, color,
        [report['variants'][key]['query_normalized']['estimated_usd_per_10000_queries']], 's3-operations/report.json')
        for key, label, color in variants]
    name = 's3-postgres-request-cost'
    output += chart(name, 'PostgreSQL · coût projeté des requêtes', 'USD / 10 000 requêtes SQL métier', series,
        'Normalisation arithmétique d’un seul lot de 256 transactions, soit 1 280 requêtes métier, avec drainage ; ce n’est pas une prévision de charge continue. Tarifs Tigris appliqués aux tentatives Elestio, avant franchise ; stockage et autres frais exclus.')
    exported.append({'name': name, 'series': series})
    for metric, suffix, unit in [('total_upstream_attempts', 'requests', 'appels par redémarrage'),
                                 ('seconds', 'seconds', 'secondes · plus bas = mieux')]:
        series = []
        for key, label, color in variants:
            for temperature, caption in [('warm', 'cache conservé'), ('cold', 'cache vide')]:
                if key == 'native':
                    caption = 'redémarrage ' + ('1' if temperature == 'warm' else '2') + ' · sans cache S3'
                series.append(metric_series(label + ' · ' + caption, color,
                    [restarts[key]['postgres'][temperature][metric]], 's3-operations/report.json'))
        name = 's3-postgres-restart-' + suffix
        output += chart(name, 'Redémarrage · PostgreSQL prêt', unit, series,
            'Ouverture du volume + démarrage PostgreSQL, avant le scan d’intégrité. Pour les moteurs S3 : processus neufs, répertoire WAL/SSD conservé à chaud, neuf à froid. Le natif conserve son disque et fournit deux redémarrages témoins, sans purge de cache. Proxy présent pour S3, une observation ; caches Linux non purgés. Le passage chaud peut modifier le checkpoint physique avant le froid.')
        exported.append({'name': name, 'series': series})
    series = []
    for key, label, color in variants:
        if key == 'native':
            continue
        for temperature, caption in [('warm', 'cache conservé'), ('cold', 'cache vide')]:
            value = report['variants'][key]['workloads']['fio']['phases']['fio.' + temperature + '_read']['summary']
            series.append(metric_series(label + ' · ' + caption, color, [value['total_upstream_attempts']], 's3-operations/report.json'))
    name = 's3-fio-restart-read'
    output += chart(name, 'Lecture vérifiée · 64 Mio après redémarrage', 'appels pendant la lecture CRC32C', series,
        'Même zone de données dans les deux cas ; ouverture du moteur comptée séparément. Cache SSD configuré à 128 Mio. Aucun résultat natif n’est présenté comme une lecture froide S3.')
    exported.append({'name': name, 'series': series})
    detail = '<section><h2>Opérations du bucket et redémarrages</h2><p>Chaque tentative HTTP amont est comptée, y compris les retries, les HEAD, les LIST et les métadonnées. Les nombres A/B ci-dessous sont les appels observés avant exonérations par statut ; les coûts tiennent compte des statuts explicitement gratuits. Les erreurs de transport, y compris pendant le corps après réception des en-têtes, et les API inconnues restent hors du sous-total tarifé ; cela ne signifie pas qu’elles ne seront pas facturées. Les classes multipart déduites de PUT/POST sont signalées comme hypothèses.</p>'
    detail += '<p>Projection selon <a href="https://www.tigrisdata.com/pricing/">le barème Tigris vérifié le 10 octobre 2026</a> : 5 USD par million de A et 0,50 USD par million de B. Franchise mensuelle globale non appliquée ; DELETE/CANCEL et egress gratuits. Backend réellement mesuré : Elestio. Le proxy perturbe les durées et la cadence des checkpoints : ces essais complètent les benchmarks directs, sans remplacer leurs TPS.</p>'
    detail += table(['Outil', 'Campagne', 'Appels totaux', 'A', 'B', 'Gratuits par API', 'USD bruts estimés', 'Classe supposée', 'Non tarifés / incertains'], totals)
    detail += '<h3>Coût pour 1 000 et 10 000 requêtes</h3><p>Le <a href="https://www.postgresql.org/docs/16/pgbench.html">scénario pgbench TPC-B-like</a> exécute trois UPDATE, un SELECT et un INSERT par transaction. Une requête métier désigne ici l’une de ces cinq instructions ; BEGIN/END sont exclus du dénominateur mais leur coût reste dans le lot mesuré. Aucun vacuum n’est lancé pendant la phase de charge. Le lot terminé contient 256 transactions, 1 280 requêtes métier et 1 792 commandes avec BEGIN/END, sans échec ni retry SQL.</p>' + table(
        ['Outil', 'Requêtes SQL mesurées', 'USD / 1K requêtes', 'USD / 10K requêtes', 'USD / 1K transactions', 'USD / 10K transactions', 'A / 1K requêtes', 'B / 1K requêtes'], query_rows)
    restart_rows = []
    for key, label, _ in variants:
        for temperature, caption in [('warm', 'Cache conservé'), ('cold', 'Cache vide')]:
            if key == 'native':
                caption = 'Sans cache S3 · redémarrage ' + ('1' if temperature == 'warm' else '2')
            value = restarts[key]['postgres'][temperature]
            restart_rows.append([label, caption, round(value['seconds'], 3), value['total_upstream_attempts'],
                value['request_classes']['A'], value['request_classes']['B'],
                round(value['response_payload_bytes'] / 1024**2, 3), f"{value['estimated_gross_request_cost_usd']:.8f}"])
    detail += '<h3>Coût par redémarrage jusqu’à PostgreSQL prêt</h3>' + table(
        ['Outil', 'Cache local', 'Secondes', 'Appels', 'A', 'B', 'Mio téléchargés', 'USD par reprise'], restart_rows)
    detail += '<p>La reprise à chaud conserve le WAL et le cache SSD, mais pas la RAM du processus. InfiniDisk2 relit actuellement HEAD et ses shards d’index ; avoir les pages en cache ne garantit donc pas zéro appel. Les phases de contrôle et d’arrêt restent comptabilisées séparément. Le natif n’a pas de cache S3 à vider : ses deux valeurs sont des redémarrages témoins sur les mêmes fichiers, sans purge du cache Linux.</p>'
    zero_warm = restarts['zerofs-durable']['postgres']['warm']
    zero_cold = restarts['zerofs-durable']['postgres']['cold']
    detail += '<p>Les deux reprises ZeroFS comprennent aussi des opérations de maintenance : ' + str(zero_warm['by_operation'].get('DeleteObjects', 0)) + ' appels DeleteObjects à chaud contre ' + str(zero_cold['by_operation'].get('DeleteObjects', 0)) + ' à froid dans cette observation. Ces appels sont gratuits dans la projection Tigris. Leur présence explique une grande partie de l’écart du nombre total ; un passage ne permet pas de conclure que le cache conservé coûte habituellement plus cher.</p>'
    detail += '<details><summary>Détail complet par phase</summary>' + table(
        ['Outil', 'Phase', 'Secondes', 'Appels', 'A', 'B', 'Mio téléchargés', 'USD estimés'], phases) + '</details>'
    detail += '<details><summary>Objets sollicités pendant les reprises</summary><p>Catégories physiques déduites du format des clés. Les WAL/SST de ZeroFS peuvent mêler données et métadonnées logiques ; cette répartition ne prétend pas les séparer à l’intérieur d’un objet.</p>' + table(
        ['Outil', 'Phase', 'HEAD InfiniDisk2', 'Index InfiniDisk2', 'Segments', 'WAL', 'SST', 'Manifests', 'Autres'], objects) + '</details>'
    detail += '<p><a href="s3-operations/report.json">Rapport machine</a> · <a href="s3-operations/' + html.escape(report['source_manifest']) + '">Empreintes des traces</a> · <a href="../../docs/astra-s3-operations.md">Protocole et limites</a></p></section>'
    return output, detail, exported, restarts


def s3_block_charts(report):
    if not report or not report.get('complete'):
        return '', '', [], {}
    from s3_request_pricing import project_events, operation_class, request_cost, PRICING_FILE
    pricing = load(PRICING_FILE)
    def with_uncertain_billed(events, counted):
        extra = {'A': 0, 'B': 0}
        for event in events:
            status = event.get('status')
            if not event.get('forwarded', type(status) is int):
                continue
            if type(status) is int and not event.get('transport_error'):
                continue
            if status in pricing['explicitly_nonbillable_http_statuses']:
                continue
            cls, _ = operation_class(event.get('operation', event.get('api', 'Unknown')))
            if cls in extra:
                extra[cls] += 1
        return counted['estimated_gross_request_cost_usd'] + request_cost(extra['A'], extra['B'], pricing=pricing)
    archive = OUT / 's3-blocks' / report['id']
    manifest = load(OUT / 's3-blocks' / report['source_manifest'])
    for name, expected in manifest['files_sha256'].items():
        if hashlib.sha256((archive / name).read_bytes()).hexdigest() != expected:
            raise ValueError('S3 block sweep evidence changed: ' + name)
    if load(OUT / 's3-blocks' / report['source_report']) != report:
        raise ValueError('S3 block canonical report differs from archive')
    expected = {'read-seed'} | {f'read-{extent}-r{repeat}' for extent in (16, 64, 256) for repeat in range(3)} | {
        f'{kind}-{segment}-r{repeat}' for kind in ('write', 'writefull') for segment in (8, 16, 32, 64) for repeat in range(3)}
    if set(report['variants']) != expected:
        raise ValueError('S3 block sweep has missing or extra cases')
    groups = {'random_read': {}, 'sequential_read': {}, 'write': {}, 'writefull': {}}
    for name, case in report['variants'].items():
        if not case.get('complete') or case['cleanup']['failures']:
            raise ValueError('S3 block case incomplete: ' + name)
        if not all(case['cleanup'].get(key) for key in ('nbd31_detached', 'mount_absent', 'container_removed')):
            raise ValueError('S3 block case not cleaned: ' + name)
        proxy = case['proxy']
        if proxy['inflight'] or proxy['closed_error'] or proxy['local_rejections'] or not proxy['journal_complete']:
            raise ValueError('S3 block proxy evidence incomplete: ' + name)
        events = [json.loads(line) for line in (archive / name / 'fio/requests.jsonl').read_text().splitlines() if line]
        if project_events(events) != case['summary'] or proxy['forwarded'] != case['summary']['total_upstream_attempts']:
            raise ValueError('S3 block accounting differs from trace')
        if name == 'read-seed':
            if case['integrity'].get('initial_crc32c') != 'passed':
                raise ValueError('S3 block seed unverified')
            continue
        kind, size, _ = name.split('-')
        size = int(size)
        option = 'read_extent_kib' if kind == 'read' else 'segment_mib'
        expected_options = {**report['protocol']['base_options'], option: size}
        if kind != 'read':
            expected_options['compact_checkpoints'] = kind == 'write'
        if case['options'] != expected_options:
            raise ValueError('S3 block sweep changed more than its declared option')
        if kind == 'read':
            for workload in ('random_read', 'sequential_read'):
                if case['integrity'].get(workload.split('_')[0] + '_crc32c') != 'passed':
                    raise ValueError('S3 block read CRC missing')
                selected = [event for event in events if event['phase'] == 'fio.' + workload]
                counted = project_events(selected)
                if counted != case['phases']['fio.' + workload]['summary']:
                    raise ValueError('S3 block read phase mismatch')
                job = case['samples'][workload.replace('_', '-')]['read']
                value = {**counted, 'iops': job['iops'], 'mib_per_second': job['bw_bytes'] / 1024**2,
                    'usd_if_uncertain_classified_attempts_billed': with_uncertain_billed(selected, counted),
                    'p99_ms': job['clat_ns']['percentile']['99.000000'] / 1e6,
                    'useful_bytes': job['io_bytes'],
                    'read_amplification': counted['response_payload_bytes'] / job['io_bytes'],
                    'seconds': case['phases']['fio.' + workload]['seconds'], 'source': name}
                groups[workload].setdefault(size, []).append(value)
        else:
            if any(case['integrity'].get(key) != 'passed' for key in ('initial_crc32c', 'remote_crc32c')):
                raise ValueError('S3 block write CRC missing')
            selected = [event for event in events if event['phase'] in case['write_accounting']['phases']]
            counted = project_events(selected)
            if counted != {k: v for k, v in case['write_accounting'].items() if k != 'phases'}:
                raise ValueError('S3 block write/drain mismatch')
            job = case['samples']['fsync-write']
            value = {**counted, 'iops': job['write']['iops'],
                'usd_if_uncertain_classified_attempts_billed': with_uncertain_billed(selected, counted),
                'p99_ms': job['sync']['lat_ns']['percentile']['99.000000'] / 1e6,
                'seconds': sum(case['phases'][phase]['seconds'] for phase in case['write_accounting']['phases']),
                'source': name}
            groups[kind].setdefault(size, []).append(value)
    for group in groups.values():
        for size, samples in group.items():
            if len(samples) != 3:
                raise ValueError('S3 block sample count differs')
    output, exported, rows = '', [], []
    source = 's3-blocks/report.json'
    metrics = [
        ('random_read', 'total_upstream_attempts', 'Lecture aléatoire · appels S3', 'appels pour 4 096 lectures de 4 Kio'),
        ('random_read', 'iops', 'Lecture aléatoire · débit', 'IOPS'),
        ('sequential_read', 'total_upstream_attempts', 'Lecture séquentielle · appels S3', 'appels pour 256 Mio'),
        ('write', 'class_a', 'Écritures + drainage · appels A', 'appels pour le lot complet'),
        ('write', 'p99_ms', 'Écritures · p99 des fsync', 'millisecondes · plus bas = mieux'),
        ('writefull', 'class_a', 'Sans compaction · appels A', 'appels pour le lot complet'),
        ('writefull', 'p99_ms', 'Sans compaction · p99 des fsync', 'millisecondes · plus bas = mieux'),
    ]
    for group, metric, title, unit in metrics:
        series = []
        for size, samples in sorted(groups[group].items()):
            values = [sample['request_classes']['A'] if metric == 'class_a' else sample[metric] for sample in samples]
            series.append(metric_series(str(size) + (' Mio / segment' if group.startswith('write') else ' Kio / plage S3'), 'astra', values, source))
        name = 's3-blocks-' + group + '-' + metric
        output += chart(name, title, unit, series,
            'Même binaire Astra ; trois passages par réglage, ordre tournant. Pages logiques 4 Kio, RAM 64 Mio / SSD 128 Mio. Proxy de comptage actif. Lecture à cache local neuf sur le même contenu ; écritures sur volumes neufs, drainage inclus dans les coûts. CRC vérifiés. Aucun optimum universel déduit.')
        exported.append({'name': name, 'series': series})
    medians = {}
    for workload, group in groups.items():
        medians[workload] = {}
        for size, samples in sorted(group.items()):
            median = lambda key: statistics.median(s[key] for s in samples)
            result = {key: median(key) for key in ('total_upstream_attempts', 'estimated_gross_request_cost_usd',
                'request_payload_bytes', 'response_payload_bytes', 'iops', 'p99_ms', 'seconds',
                'transport_uncertain_requests', 'usd_if_uncertain_classified_attempts_billed')}
            result['class_a'] = statistics.median(s['request_classes']['A'] for s in samples)
            result['class_b'] = statistics.median(s['request_classes']['B'] for s in samples)
            if not workload.startswith('write'):
                result['read_amplification'] = median('read_amplification')
                result['mib_per_second'] = median('mib_per_second')
            medians[workload][size] = result
            rows.append([workload, str(size) + (' Mio' if workload.startswith('write') else ' Kio'),
                result['class_a'], result['class_b'], f"{result['estimated_gross_request_cost_usd']:.8f}",
                f"{result['usd_if_uncertain_classified_attempts_billed']:.8f}",
                round(result['request_payload_bytes'] / 1024**2, 2),
                round(result['response_payload_bytes'] / 1024**2, 2),
                round(result['iops'], 2), round(result['p99_ms'], 3), round(result['seconds'], 3),
                result['transport_uncertain_requests']])
    detail = '<section><h2>Tailles des plages et segments : recherche du compromis</h2><p>Les plages de lecture sont variées séparément. Les tailles de segments sont ensuite testées avec compaction (write) et sans compaction (writefull). La taille logique reste 4 Kio. Les lectures utilisent le même contenu distant de 256 Mio, supérieur au budget SSD de 128 Mio, avec un répertoire local neuf avant chaque charge. Les écritures utilisent un volume neuf pour chaque passage. Le coût du lot d’écriture inclut 256 Mio séquentiels vérifiés, 4 096 écritures aléatoires avec fsync dans une autre zone et le drainage ; la restauration CRC finale est comptée dans la trace complète, mais séparée de ce lot. Le mode compact regroupe par shard de 4 096 pages, soit 16 Mio utiles au maximum : augmenter le WAL ne grossit pas directement ces objets S3.</p>'
    detail += table(['Charge', 'Réglage', 'A médian', 'B médian', 'Sous-total USD / lot', 'USD si incertains A/B facturés', 'Envoyé Mio', 'Reçu Mio',
        'IOPS médian', 'p99 ms', 'Durée phase(s) s', 'Tentatives incertaines'], rows)
    detail += '<p>Pour les lectures, le p99 est celui des complétions fio. Pour les écritures, les IOPS concernent les écritures aléatoires et le p99 concerne leurs fsync ; la durée et le prix englobent le lot complet. Chaque colonne est la médiane indépendante de ses trois observations. Les timings incluent le proxy et la vérification CRC ; ils complètent les benchmarks directs. Les traces permettent de distinguer segments, index et HEAD, qui ne grossissent pas automatiquement ensemble.</p>'
    if any(sample['transport_uncertain_requests'] for group in groups.values() for samples in group.values() for sample in samples):
        detail += '<p>Des transferts interrompus existent dans ce balayage : ils sont inclus dans les appels, mais exclus du sous-total tarifé. Une réponse HTTP 206 peut avoir été reçue avant l’interruption de son corps. Ces tentatives peuvent être facturées par le fournisseur ; le sous-total ne constitue donc pas le coût complet de ces lignes. La colonne voisine ajoute les tentatives incertaines classées A/B comme si elles étaient toutes facturées, sauf statut explicitement exonéré ; les API inconnues restent hors calcul. Les temps du proxy avec réessais restent des diagnostics, et la confirmation sans proxy est présentée séparément.</p>'
    detail += '<p><a href="s3-blocks/report.json">Mesures et protocole</a> · <a href="s3-blocks/' + html.escape(report['source_manifest']) + '">Empreintes des preuves</a> · <a href="../../docs/astra-s3-operations.md">Limites et reproduction</a></p></section>'
    return output, detail, exported, medians


def s3_direct_charts(report, accounting):
    """Validate direct timing evidence without inventing request counters."""
    if not report or not report.get('complete'):
        return '', '', [], {}
    directory = OUT / 's3-blocks-direct'
    archive = directory / report['id']
    manifest = load(directory / report['source_manifest'])
    for name, expected in manifest['files_sha256'].items():
        path = archive / name
        if not path.is_relative_to(archive) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError('Direct block evidence changed: ' + name)
    if load(directory / report['source_report']) != report:
        raise ValueError('Direct block canonical report differs from archive')
    protocol = report['protocol']
    if protocol['proxy'] is not False or protocol['http_operations'] != 'not measured':
        raise ValueError('Direct block timings must not claim HTTP accounting')
    if hashlib.sha256((OUT / 's3-blocks/report.json').read_bytes()).hexdigest() != protocol['source_accounting_report_sha256']:
        raise ValueError('Direct block selection source changed')
    selected = {(True, 8), (False, 32)}
    for kind, compact in (('write', True), ('writefull', False)):
        choices = []
        for size in (8, 16, 32, 64):
            cases = [accounting['variants'][f'{kind}-{size}-r{repeat}'] for repeat in range(3)]
            if any(case['write_accounting']['cost_has_unpriced_or_uncertain_requests'] for case in cases):
                raise ValueError('Direct block selection cannot use incomplete write pricing')
            costs = [case['write_accounting']['estimated_gross_request_cost_usd'] for case in cases]
            tails = [case['samples']['fsync-write']['sync']['lat_ns']['percentile']['99.000000'] for case in cases]
            choices.append((statistics.median(costs), statistics.median(tails), size))
        selected.add((compact, min(choices)[2]))
    declared = {(item['compact_checkpoints'], item['segment_mib']) for item in protocol['selected_write_profiles']}
    if declared != selected:
        raise ValueError('Direct block write selection differs from the declared cost rule')
    expected = {f'read-{size}-r{repeat}' for size in (16, 64, 256) for repeat in range(3)}
    expected |= {('write' if compact else 'writefull') + f'-{size}-r{repeat}' for compact, size in selected for repeat in range(3)}
    if set(report['variants']) != expected:
        raise ValueError('Direct block cases missing or extra')
    groups = {'random_read': {}, 'sequential_read': {}, 'write': {}}
    for name, case in report['variants'].items():
        if (not case.get('complete') or case['cleanup']['failures']
                or not all(case['cleanup'].get(key) for key in ('nbd31_detached', 'mount_absent', 'container_removed'))):
            raise ValueError('Direct block case incomplete or not cleaned: ' + name)
        if 'summary' in case or 'proxy' in case or any('summary' in phase for phase in case['phases'].values()):
            raise ValueError('Direct block case contains unmeasured HTTP counters')
        if case.get('http_accounting') != 'not measured: direct Rust to S3, no proxy':
            raise ValueError('Direct block HTTP scope is missing')
        kind, size, _ = name.split('-')
        size = int(size)
        options = {**protocol['base_options'], 'read_extent_kib' if kind == 'read' else 'segment_mib': size}
        if kind != 'read':
            options['compact_checkpoints'] = kind == 'write'
        if case['options'] != options:
            raise ValueError('Direct block changed an undeclared option')
        if kind == 'read':
            for workload, useful in (('random_read', 16 * 1024**2), ('sequential_read', 256 * 1024**2)):
                if case['integrity'].get(workload.split('_')[0] + '_crc32c') != 'passed':
                    raise ValueError('Direct read CRC missing')
                job = case['samples'][workload.replace('_', '-')]
                if job['error'] or job['read']['io_bytes'] != useful:
                    raise ValueError('Direct read workload differs')
                value = {'iops': job['read']['iops'], 'mib_per_second': job['read']['bw_bytes'] / 1024**2,
                    'p99_ms': job['read']['clat_ns']['percentile']['99.000000'] / 1e6,
                    'seconds': case['phases']['fio.' + workload]['seconds'], 'source': name}
                groups[workload].setdefault(str(size), []).append(value)
        else:
            if any(case['integrity'].get(key) != 'passed' for key in ('initial_crc32c', 'remote_crc32c')):
                raise ValueError('Direct write CRC missing')
            job = case['samples']['fsync-write']
            if job['error'] or job['write']['total_ios'] != 4096 or not job['sync']['total_ios']:
                raise ValueError('Direct fsync workload differs')
            if case['samples']['bulk-write']['write']['io_bytes'] != 256 * 1024**2 or case['samples']['restored-crc32c']['read']['io_bytes'] != 256 * 1024**2:
                raise ValueError('Direct write/restore byte count differs')
            value = {'iops': job['write']['iops'],
                'p99_ms': job['sync']['lat_ns']['percentile']['99.000000'] / 1e6,
                'seconds': sum(case['phases']['fio.' + phase]['seconds'] for phase in ('bulk_write', 'fsync_write', 'drain')),
                'source': name}
            groups['write'].setdefault(kind + '-' + str(size), []).append(value)
    output, exported, rows, medians = '', [], [], {}
    def label(group, key):
        if group != 'write':
            return key + ' Kio / plage S3'
        kind, size = key.split('-')
        return size + ' Mio / segment · ' + ('compact' if kind == 'write' else 'sans compaction')
    for group, metric, title, unit in (
        ('random_read', 'iops', 'Sans proxy · lecture aléatoire', 'IOPS'),
        ('random_read', 'p99_ms', 'Sans proxy · p99 lecture aléatoire', 'millisecondes · plus bas = mieux'),
        ('sequential_read', 'mib_per_second', 'Sans proxy · lecture séquentielle', 'Mio/s'),
        ('write', 'iops', 'Sans proxy · écritures avec fsync', 'IOPS'),
        ('write', 'p99_ms', 'Sans proxy · p99 fsync', 'millisecondes · plus bas = mieux')):
        series = [metric_series(label(group, key), 'astra', [sample[metric] for sample in samples],
            's3-blocks-direct/report.json') for key, samples in groups[group].items()]
        name = 's3-direct-' + group + '-' + metric
        output += chart(name, title, unit, series,
            'Rust accède directement à S3, sans proxy. Trois passages, CRC, binaire et budgets identiques au comptage. Lectures : même contenu distant. Écritures : volumes neufs. Aucun compteur S3 n’est mesuré pendant ces timings.')
        exported.append({'name': name, 'series': series})
    for group, values in groups.items():
        medians[group] = {}
        for key, samples in values.items():
            if len(samples) != 3:
                raise ValueError('Direct block sample count differs')
            result = {metric: statistics.median(sample[metric] for sample in samples)
                for metric in samples[0] if metric != 'source'}
            medians[group][key] = result
            rows.append([group, label(group, key), round(result['iops'], 2),
                round(result.get('mib_per_second', 0), 2) if group != 'write' else '—',
                round(result['p99_ms'], 3), round(result['seconds'], 3)])
    detail = '<section><h2>Confirmation directe des tailles S3</h2><p>Ces temps sont mesurés sans le proxy de comptage. Les mêmes trois plages de lecture utilisent le même contenu distant, avec un cache local neuf à chaque charge. Pour les écritures, les profils compact 8 Mio et sans compaction 32 Mio sont toujours conservés ; le profil de chaque mode ayant le plus petit coût médian instrumenté est ajouté, avec départage par p99 fsync puis taille. Le segment intermédiaire permet de confronter performance et économie marginale du plus gros segment. La sélection précède les mesures directes. Les appels observés dans l’autre campagne ne sont pas présentés comme des compteurs de ces passages.</p>'
    detail += table(['Charge', 'Réglage', 'IOPS médian', 'Mio/s médian', 'p99 ms', 'Durée phase(s) s'], rows)
    detail += '<p>La sélection des écritures vise le coût mesuré parmi quatre tailles ; elle n’établit pas l’optimum de performance de toutes les tailles sans proxy. Le p99 d’écriture concerne les fsync, la durée du lot inclut les écritures séquentielles, aléatoires et le drainage. Trois passages sur une VM partagée, sans purge globale des caches Linux/fournisseur.</p><p><a href="s3-blocks-direct/report.json">Preuves et protocole sans proxy</a> · <a href="s3-blocks-direct/' + html.escape(report['source_manifest']) + '">Empreintes</a></p></section>'
    return output, detail, exported, medians


def quota_rows(stages):
    rows = []
    for stage in stages:
        path = OUT / 'mysql' / stage.get('proof_report', 'missing')
        if not path.is_file():
            continue
        proof = load(path)
        entry = proof.get('mysql', {}).get(stage['engine'] + '-' + stage['phase'], {})
        for workload, samples in entry.get('samples', {}).items():
            for ordinal, sample in enumerate(samples, 1):
                mysql = sample.get('resources', {}).get('processes', {}).get('mysql', {})
                quota = mysql.get('cgroup_cpu', {})
                if not quota.get('comparable'):
                    continue
                rows.append([stage['label'] + ' / ' + workload + ' / ' + str(ordinal),
                             quota.get('direct_cpu_max', {}).get('quota_cores'),
                             round(quota.get('cpu_percent_of_one_core', 0), 2),
                             round(quota.get('throttled_periods_percent', 0), 2),
                             quota.get('counters_delta', {}).get('throttled_usec'),
                             sample.get('p99_ms')])
    return rows


def checkpoint_evidence(stages):
    result = []
    for stage in stages:
        if not stage.get('proof_report') or stage['engine'] != 'infinidisk2':
            continue
        directory = (OUT / 'mysql' / stage['proof_report']).parent
        source = directory / 'raw' / ('infinidisk2-' + stage['phase'] + '-server.log')
        if not source.is_file():
            continue
        records = []
        for line in source.read_text().splitlines():
            start = line.find('{"volume"')
            if start >= 0:
                try:
                    records.append(json.JSONDecoder().raw_decode(line[start:])[0])
                except ValueError:
                    pass
        if records and records[-1].get('checkpoint_wal_bytes', 0) > 0:
            last = records[-1]
            result.append({'stage': stage['label'], 'source': str(source.relative_to(OUT)),
                           'checkpoint_wal_bytes': last['checkpoint_wal_bytes'],
                           'uploaded_segment_bytes': last['uploaded_segment_bytes'],
                           'uploaded_percent': 100 * last['uploaded_segment_bytes'] / last['checkpoint_wal_bytes'],
                           'remote_gets': last['remote_gets'], 'remote_bytes': last['remote_bytes'],
                           'cache_fills_skipped': last.get('cache_fills_skipped'),
                           'cache_fill_errors': last.get('cache_fill_errors')})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--partial', action='store_true')
    args = parser.parse_args()
    campaign = load(OUT / 'mysql/report.json')
    generation_path = OUT / 'generation/report.json'
    generation = load(generation_path) if generation_path.exists() else None
    fio_path = OUT / 'fio/report.json'
    fio_report = load(fio_path) if fio_path.exists() else None
    postgres_path = OUT / 'postgres/report.json'
    postgres_report = load(postgres_path) if postgres_path.exists() else None
    compaction_path = OUT / 'mysql/compact-tail-control-report.json'
    compaction_report = load(compaction_path) if compaction_path.exists() else None
    s3_path = OUT / 's3-operations/report.json'
    s3_report = load(s3_path) if s3_path.exists() else None
    blocks_path = OUT / 's3-blocks/report.json'
    blocks_report = load(blocks_path) if blocks_path.exists() else None
    direct_path = OUT / 's3-blocks-direct/report.json'
    direct_report = load(direct_path) if direct_path.exists() else None
    warm_paths = sorted(OUT.glob('warm/*/report.json'))
    warm_path = warm_paths[-1] if warm_paths else None
    warm = load(warm_path) if warm_path else None
    recoveries = [(path.parent.name, load(path)) for path in sorted(OUT.glob('recovery-*/report.json'))]
    confirmations = campaign.get('final_confirmations', [])
    confirmation = confirmations[-1] if confirmations else None
    controls = campaign.get('cpu_controls', [])
    control = controls[-1] if controls else None
    sql_diagnostics = campaign.get('sql_diagnostics', [])
    builds = [(path.parent.name, load(path)) for path in sorted(OUT.glob('builds/*/manifest.json'))]
    if not args.partial:
        assert campaign.get('complete'), 'MySQL campaign unfinished'
        assert confirmation and confirmation.get('complete'), 'final binary confirmation unfinished'
        assert control and control.get('complete'), 'CPU quota diagnostic unfinished'
        assert compaction_report and compaction_report.get('complete'), 'compaction latency diagnostic unfinished'
        assert compaction_report.get('integrity', {}).get('database_SIGKILL_recovery') == 'passed', 'compaction recovery missing'
        assert all(compaction_report.get('integrity', {}).get(key) == 'passed' for key in ('table_counts', 'check_table_extended')), 'compaction table checks missing'
        assert all(compaction_report.get('cleanup', {}).get(key) is True for key in ('checked', 'test_nbd31_detached', 'test_ublk31_absent', 'test_mysql_container_absent')), 'compaction cleanup incomplete'
        assert fio_report and fio_report.get('complete'), 'fio comparison unfinished'
        assert fio_report.get('native_dataset_crc32c_after_timings') == 'passed', 'native fio final checksum missing'
        assert all(fio_report.get('comparison', {}).get('tests', {}).get(engine + '-restored-256m-crc32c') == 'passed'
                   for engine in ('infinidisk2', 'zerofs')), 'restored fio checksum missing'
        assert postgres_report and postgres_report.get('complete'), 'PostgreSQL comparison unfinished'
        assert s3_report and s3_report.get('complete'), 'S3 operation/restart accounting unfinished'
        assert blocks_report and blocks_report.get('complete'), 'S3 block cost sweep unfinished'
        assert direct_report and direct_report.get('complete'), 'Direct block timing confirmation unfinished'
        assert generation and generation.get('complete'), 'generation qualification unfinished'
        assert generation.get('tests', {}).get('live_checkpoint_recovery', {}).get('passed'), 'live checkpoint recovery unfinished'
        assert generation.get('tests', {}).get('total_local_loss_adoption', {}).get('passed'), 'live checkpoint adoption unfinished'
        assert len(recoveries) >= 2 and all(r.get('passed') for _, r in recoveries), 'durable recovery qualification unfinished'
        assert warm and warm.get('passed') and warm.get('original_caches_restored'), 'cold warm/retention qualification unfinished'
        assert {run.get('concurrency') for run in warm['runs'] if run.get('complete') and not run.get('optional')} == {8, 16, 32, 64, 128}, 'parallel warm sweep unfinished'
        final_sha = confirmation['binary_sha256']
        assert campaign['binary_sha256']['astra'] == final_sha, 'MySQL qualification used another binary'
        assert control['binary_sha256'] == final_sha, 'CPU diagnostic used another binary'
        assert compaction_report['binary_sha256'] == final_sha, 'compaction diagnostic used another binary'
        for key in ('reference', 'control'):
            item = compaction_report[key]
            assert item['binary_sha256'] == final_sha, 'compaction stage used another binary'
            assert hashlib.sha256((OUT / 'mysql' / item['proof_report']).read_bytes()).hexdigest() == item['proof_report_sha256'], 'compaction evidence changed'
        assert len(control.get('transport_stages', [])) == 2, 'large ublk CPU comparison unfinished'
        assert fio_report['binary_sha256'] == final_sha, 'fio used another binary'
        assert postgres_report['binary_sha256']['astra'] == final_sha, 'PostgreSQL used another binary'
        assert postgres_report['binary_sha256']['baseline'] == campaign['binary_sha256']['baseline'], 'PostgreSQL baseline used another binary'
        assert s3_report['binary_sha256']['astra'] == final_sha, 'S3 accounting used another binary'
        assert s3_report['binary_sha256']['baseline'] == campaign['binary_sha256']['baseline'], 'S3 accounting baseline used another binary'
        for name, expected in s3_report['source_sha256'].items():
            assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, 'S3 accounting source changed: ' + name
        assert blocks_report['binary_sha256']['astra'] == final_sha, 'S3 block sweep used another binary'
        for name, expected in blocks_report['source_sha256'].items():
            assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, 'S3 block sweep source changed: ' + name
        assert direct_report['binary_sha256']['astra'] == final_sha, 'Direct block timing used another binary'
        for name, expected in direct_report['source_sha256'].items():
            assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, 'Direct block timing source changed: ' + name
        assert generation['binary_sha256'] == final_sha, 'generation mode used another binary'
        assert all(r['binary_sha256'] == final_sha for _, r in recoveries), 'recovery used another binary'
        assert all(run['binary_sha256'] == final_sha for run in warm['runs'] if not run.get('optional')), 'cold warm used another binary'
        for diagnostic in sql_diagnostics:
            assert diagnostic.get('complete') and diagnostic.get('clean_teardown'), 'SQL diagnostic unfinished'
            assert diagnostic.get('binary_sha256') == final_sha, 'SQL diagnostic used another binary'
            proof = OUT / 'mysql' / diagnostic['proof_report']
            assert hashlib.sha256(proof.read_bytes()).hexdigest() == diagnostic['proof_report_sha256'], 'SQL diagnostic evidence changed'
        selected_build = next((build for _, build in builds if build.get('binary_sha256') == final_sha), None)
        assert selected_build, 'final build manifest missing'
        for name, expected in selected_build['source_sha256'].items():
            assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, 'source changed since qualification: ' + name
        from run_astra_recovery import verify_existing
        validator_sha = hashlib.sha256((ROOT / 'scripts/validate_vm.py').read_bytes()).hexdigest()
        for name, _ in recoveries:
            options = load(ROOT / 'scripts/profiles' / ('astra-' + name + '.json'))
            assert verify_existing(OUT / name, options, final_sha, validator_sha), 'recovery archive failed validation: ' + name
        fio_archive = OUT / 'fio' / fio_report['evidence_directory']
        fio_manifest = load(fio_archive / 'manifest.json')
        assert load(fio_archive / 'report.json') == fio_report, 'fio published report differs from archive'
        assert fio_report['comparison']['complete'], 'fio comparison incomplete'
        assert fio_manifest['binary_sha256'] == final_sha, 'fio archive binary mismatch'
        assert fio_manifest['script_sha256'] == hashlib.sha256((ROOT / 'scripts/run_astra_fio_compare.py').read_bytes()).hexdigest(), 'fio launcher changed'
        assert fio_report['helper_sha256'] == hashlib.sha256((ROOT / 'scripts/compare_zerofs.py').read_bytes()).hexdigest(), 'fio helper changed'
        for name, expected in fio_manifest['files_sha256'].items():
            assert hashlib.sha256((fio_archive / name).read_bytes()).hexdigest() == expected, 'fio evidence changed: ' + name
        for execution in fio_report['comparison']['executions']:
            assert execution['binary_sha256'] == final_sha and execution['engine_options'] == fio_report['options'], 'fio executed profile mismatch'
            assert execution['script_sha256'] == fio_report['helper_sha256'], 'fio executed helper mismatch'
        for stage in campaign['stages']:
            if stage.get('proof_report_sha256'):
                assert hashlib.sha256((OUT / 'mysql' / stage['proof_report']).read_bytes()).hexdigest() == stage['proof_report_sha256'], 'MySQL evidence changed: ' + stage['phase']
    stages = [s for s in campaign['stages'] if s.get('returncode') == 0 and s.get('complete') and not s.get('superseded')]
    if not args.partial:
        final_phases = set(confirmation.get('stages', []) + control.get('stages', []) + control.get('transport_stages', []))
        for stage in stages:
            if stage['engine'] == 'infinidisk2' and ('-qualified-' in stage['label'] or stage['phase'] in final_phases):
                expected = campaign['binary_sha256']['baseline'] if stage['profile'] == 'baseline' else final_sha
                assert stage.get('binary_sha256') == expected, 'qualified stage used another binary: ' + stage['label']
                assert stage.get('engine_metadata', {}).get('binary_sha256') == expected, 'executed engine SHA differs: ' + stage['label']
                assert stage.get('engine_metadata', {}).get('options') == stage.get('options'), 'executed engine options differ: ' + stage['label']
    checkpoints = checkpoint_evidence(stages)
    chosen = campaign.get('selection', {}).get('profile')
    by_label = {s['label']: s for s in stages}
    summary = {'complete': not args.partial, 'campaign': campaign['campaign'], 'selected_profile': chosen,
               'binary_sha256': campaign['binary_sha256'], 'build_revisions': campaign.get('build_revisions', []),
               'final_confirmation': confirmation,
               'cpu_control': control,
               'sql_diagnostics': sql_diagnostics,
               'compaction_control': compaction_report,
               's3_operations': s3_report,
               's3_blocks': blocks_report,
               's3_blocks_direct': direct_report,
               'warm': warm,
               'builds': [{'directory': name, **build} for name, build in builds],
               'checkpoint_evidence': checkpoints,
               'charts': [], 'ratios': {}, 'qualified_mysql_deltas': {},
               'tests': {}, 'limitations': campaign['protocol']['limitations']}
    def stage_series(label, title, color, workload, metric='tps', *, exploration=False):
        stage = by_label.get(label)
        measurements = stage.get('summary', {}).get(workload, {}) if stage else {}
        reasons = set(stage.get('comparison_exclusion_reasons', [])) if stage else set()
        allowed_reasons = {'ignored_sql_errors'}
        if exploration and '-screen-' in label:
            allowed_reasons.add('exploratory_screening_only')
        unqualified = bool('ignored_sql_errors' in reasons or measurements.get('ignored_errors', 0))
        excluded = bool(stage and stage.get('eligible_for_comparison') is False
                        and (not reasons or not reasons.issubset(allowed_reasons)))
        values = measurements.get(metric, [])
        if values and not excluded:
            checked_values(values, stage['samples'], stage['label'] + '/' + workload + '/' + metric)
        return {'label': title, 'color': color,
                'values': [] if excluded else values,
                'source': 'mysql/' + stage['proof_report'] if stage else None,
                'unqualified': unqualified,
                'eligible_for_ratio': bool(values and not excluded and not unqualified
                                           and stage.get('eligible_for_comparison') is True),
                'comparison_exclusion_reasons': sorted(reasons),
                'omitted_reason': 'essai exclu : protocole ou screening' if excluded else 'non mesuré'}
    chart_html = ''
    for size, caption in [('small', 'Petite base · 4 × 25 000 lignes'), ('large', 'Grande base · 4 × 1 000 000 lignes')]:
        if not any(s['label'].startswith(size + '-qualified-') for s in stages):
            continue
        chart_html += '<h2>MySQL · ' + caption + '</h2><div class="figures">'
        for workload, title in LABELS.items():
            for metric, suffix, unit in [('tps', '', 'transactions / seconde'),
                                         ('p99_ms', '-p99', 'millisecondes · plus bas = mieux')]:
                series = [stage_series(size + '-qualified-baseline', 'InfiniDisk2 · avant Astra', 'baseline', workload, metric),
                          stage_series(size + '-qualified-' + str(chosen), 'InfiniDisk2 · Astra ' + str(chosen), 'astra', workload, metric)]
                if size == 'small':
                    series += [stage_series('small-zerofs-durable', 'ZeroFS · fsync S3', 'zerofs', workload, metric),
                               stage_series('small-zerofs-async', 'ZeroFS · fsync ignoré', 'zerofs', workload, metric)]
                else:
                    series += [{'label': 'ZeroFS', 'color': 'zerofs', 'values': [], 'source': None}]
                series.append(stage_series(size + '-qualified-native', 'Natif · disque VM', 'native', workload, metric))
                if any(s['values'] for s in series):
                    note = 'MySQL 1 CPU / 1 Gio, buffer pool 256 Mio, 8 threads. InfiniDisk2 et natif : 3 × 30 s. ZeroFS : 1 × 30 s. Contrats de durabilité différents ; caches et page cache Linux décrits ci-dessous.'
                    if metric == 'p99_ms':
                        note += ' Médiane des p99 de chaque passage, pas p99 global recomposé.'
                    name = size + '-' + workload + suffix
                    chart_html += chart(name, title + (' · p99' if suffix else ' · débit'), unit, series, note)
                    summary['charts'].append({'name': name, 'series': series})
                if metric == 'tps' and all(s.get('eligible_for_ratio') for s in series[:2]):
                    before = by_label[size + '-qualified-baseline']['summary'][workload]
                    after = by_label[size + '-qualified-' + str(chosen)]['summary'][workload]
                    key = size + '/' + workload
                    summary['ratios'][key] = statistics.median(series[1]['values']) / statistics.median(series[0]['values'])
                    summary['qualified_mysql_deltas'][key] = {
                        'baseline_tps': statistics.median(before['tps']),
                        'astra_tps': statistics.median(after['tps']),
                        'baseline_p99_ms': statistics.median(before['p99_ms']),
                        'astra_p99_ms': statistics.median(after['p99_ms']),
                        'tps_ratio': summary['ratios'][key],
                        'p99_ratio': statistics.median(after['p99_ms']) / statistics.median(before['p99_ms']),
                        'source_reports': [s['source'] for s in series[:2]],
                    }
        chart_html += '</div>'
    new_fio_html, new_fio_series = fio_comparison_charts(fio_report)
    if new_fio_html:
        chart_html += '<h2>fio · comparaison actuelle des trois outils</h2><div class="figures">' + new_fio_html + '</div>'
        summary['charts'].extend(new_fio_series)
    new_postgres_html, new_postgres_series = postgres_comparison_charts(postgres_report)
    if new_postgres_html:
        chart_html += '<h2>PostgreSQL · trois outils et référence avant Astra</h2><div class="figures">' + new_postgres_html + '</div>'
        summary['charts'].extend(new_postgres_series)
        pg_before = postgres_report['series']['baseline']['samples']
        pg_after = postgres_report['series']['astra']['samples']
        summary['ratios']['postgres/astra-over-baseline'] = statistics.median(sample['tps'] for sample in pg_after) / statistics.median(sample['tps'] for sample in pg_before)
    new_warm_html, new_warm_series = warm_charts(warm, warm_path.relative_to(OUT).as_posix() if warm_path else None)
    if new_warm_html:
        chart_html += '<h2>Téléchargements S3 parallèles</h2><div class="figures">' + new_warm_html + '</div>'
        summary['charts'].extend(new_warm_series)
        complete_warm = [run for run in warm['runs'] if run.get('complete') and not run.get('optional')]
        baseline_warm = next((run for run in complete_warm if run.get('concurrency') == 8), None)
        if baseline_warm:
            for run in complete_warm:
                summary['ratios']['warm/concurrency-' + str(run['concurrency']) + '-over-8'] = baseline_warm['cold']['seconds'] / run['cold']['seconds']
    if confirmation:
        prefix = 'small-final-' + confirmation['binary_sha256'][:12]
        reused = next((s for s in stages if s['phase'] == confirmation.get('reused_nbd_qualification')), None)
        if reused:
            by_label[prefix + '-nbd'] = reused
        final_stages = [by_label.get(prefix + '-' + transport) for transport in ('nbd', 'ublk')]
        final_stages = [stage for stage in final_stages if stage]
        chart_html += '<h2>Binaire final · NBD et ublk</h2><p>Vérification des dernières corrections de reprise et de bornes mémoire. Même petite base, même profil sélectionné, 3 × 30 s. La qualification NBD est réutilisée lorsqu’elle porte déjà sur ce binaire exact. Le transport ublk reçoit en plus ses workers persistants.</p><div class="figures">'
        for workload, title in LABELS.items():
            for metric, suffix, unit in [('tps', 'débit', 'transactions / seconde'), ('p99_ms', 'p99', 'millisecondes · plus bas = mieux')]:
                series = [stage_series(prefix + '-nbd', 'Astra final · NBD', 'astra', workload, metric),
                          stage_series(prefix + '-ublk', 'Astra final · ublk', 'generation', workload, metric)]
                if any(s['values'] for s in series):
                    name = 'final-' + workload + '-' + suffix
                    chart_html += chart(name, title + ' · ' + suffix, unit, series,
                                        'Trois passages de 30 s par transport. Comparaison séquentielle ; le p99 affiché est la médiane des p99 de chaque passage.')
                    summary['charts'].append({'name': name, 'series': series})
                if metric == 'tps' and all(s.get('eligible_for_ratio') for s in series):
                    summary['ratios']['final-ublk/nbd/' + workload] = statistics.median(series[1]['values']) / statistics.median(series[0]['values'])
        chart_html += '</div>'
    else:
        final_stages = []
    control_stages = []
    control_transport_stages = []
    if control:
        control_stages = [s for s in stages if s['phase'] in control.get('stages', [])
                          or s['label'].startswith('cpu2-' + control['binary_sha256'][:12])]
        chart_html += '<h2>Diagnostic CPU · quota MySQL de 1 à 2 cœurs</h2><p>Le quota est la variable modifiée. Les profils de cache, durées et distributions restent identiques ; la base évolue entre les passages. Ces mesures sont présentées séparément de la comparaison principale à un CPU.</p><div class="figures">'
        prefix = 'cpu2-' + control['binary_sha256'][:12]
        for size, workload in [('small', 'read_write'), ('large', 'read_only')]:
            for metric, suffix, unit in [('tps', 'débit', 'transactions / seconde'), ('p99_ms', 'p99', 'millisecondes · plus bas = mieux')]:
                series = []
                for engine, label, color in [('baseline', 'Avant Astra', 'baseline'),
                                              (chosen, 'Astra', 'astra'), ('native', 'Natif', 'native')]:
                    for cpus, key in [(1, size + '-qualified-' + str(engine)),
                                      (2, prefix + '-' + size + '-' + str(engine))]:
                        series.append(stage_series(key, label + ' · ' + str(cpus) + ' CPU', color, workload, metric))
                if any(s['values'] for s in series):
                    name = 'cpu-control-' + size + '-' + suffix
                    chart_html += chart(name, ('Petite base' if size == 'small' else 'Grande base') + ' · ' + LABELS[workload] + ' · ' + suffix,
                                        unit, series, 'Médianes de 3 × 30 s. Variation de quota sur une VM de quatre vCPU partagée. Une amélioration peut dépendre à la fois du quota et des ressources disponibles sur l’hôte.')
                    summary['charts'].append({'name': name, 'series': series})
        chart_html += '</div>'
        control_transport_stages = [s for s in stages if s['phase'] in control.get('transport_stages', [])]
        if control_transport_stages:
            chart_html += '<h2>Grande base · transport et quota CPU</h2><p>Même profil Astra sélectionné, mêmes budgets de cache, lecture uniforme et trois passages de 30 s. Le chemin ublk ajoute ses workers persistants. Les deux quotas restent séparés ; le screening ublk antérieur utilisait le profil core et ne constitue pas cette comparaison appariée.</p><div class="figures">'
            for metric, suffix, unit in [('tps', 'débit', 'transactions / seconde'), ('p99_ms', 'p99', 'millisecondes · plus bas = mieux')]:
                series = []
                for cpus in (1, 2):
                    nbd_label = 'large-qualified-' + str(chosen) if cpus == 1 else prefix + '-large-' + str(chosen)
                    ublk_label = 'large-ublk-cpu' + str(cpus) + '-' + control['binary_sha256'][:12]
                    nbd = stage_series(nbd_label, 'NBD · ' + str(cpus) + ' CPU MySQL', 'astra', 'read_only', metric)
                    ublk = stage_series(ublk_label, 'ublk · ' + str(cpus) + ' CPU MySQL', 'generation', 'read_only', metric)
                    series += [nbd, ublk]
                    if metric == 'tps' and nbd.get('eligible_for_ratio') and ublk.get('eligible_for_ratio'):
                        summary['ratios']['large-ublk/nbd/cpu' + str(cpus)] = statistics.median(ublk['values']) / statistics.median(nbd['values'])
                if any(s['values'] for s in series):
                    name = 'large-transport-cpu-' + suffix
                    chart_html += chart(name, 'Grande base · lecture · ' + suffix, unit, series, 'Comparaison séquentielle sur la même VM ; médianes et étendue des passages. Le quota MySQL modifie les ressources attribuées à l’application.')
                    summary['charts'].append({'name': name, 'series': series})
            chart_html += '</div>'
    compaction_html, compaction_series = compaction_control_charts(compaction_report)
    if compaction_html:
        chart_html += '<h2>Grande base · contrôle de la compaction</h2>' + compaction_html
        summary['charts'].extend(compaction_series)
    s3_html, s3_detail, s3_series, s3_restarts = s3_accounting(s3_report)
    if s3_html:
        chart_html += '<h2>Appels S3, coût et redémarrage</h2><div class="figures">' + s3_html + '</div>'
        summary['charts'].extend(s3_series)
        summary['s3_restarts'] = s3_restarts
    blocks_html, blocks_detail, blocks_series, blocks_medians = s3_block_charts(blocks_report)
    if blocks_html:
        chart_html += '<h2>Tailles S3 : coût et performances</h2><div class="figures">' + blocks_html + '</div>'
        summary['charts'].extend(blocks_series)
        summary['s3_block_medians'] = blocks_medians
    direct_html, direct_detail, direct_series, direct_medians = s3_direct_charts(direct_report, blocks_report)
    if direct_html:
        chart_html += '<h2>Tailles S3 : performances sans proxy</h2><div class="figures">' + direct_html + '</div>'
        summary['charts'].extend(direct_series)
        summary['s3_direct_medians'] = direct_medians
    new_recovery_html, new_recovery_series = recovery_charts(recoveries)
    if new_recovery_html:
        chart_html += '<h2>fio et PostgreSQL · qualification du binaire final</h2><div class="figures">' + new_recovery_html + '</div>'
        summary['charts'].extend(new_recovery_series)
    if generation and generation.get('complete'):
        chart_html += '<h2>Reprise par générations · contrat distinct</h2><div class="figures">'
        generation_sql_errors = sum(sample.get('ignored_errors', 0)
            for samples in generation.get('benchmark', {}).get('samples', {}).values() for sample in samples)
        if generation_sql_errors:
            summary.setdefault('excluded_measurements', []).append({'campaign': 'generation', 'reason': 'ignored SQL errors; entire performance series remains raw-only', 'count': generation_sql_errors})
        for workload, title in LABELS.items():
            samples = generation.get('benchmark', {}).get('samples', {}).get(workload, [])
            if not samples:
                continue
            checked_values([sample.get('tps') for sample in samples], 3, 'generation/' + workload)
            series = [stage_series('small-qualified-' + str(chosen), 'Astra · fsync local', 'astra', workload),
                      {'label': 'Astra · retour à HEAD S3', 'color': 'generation', 'values': [s['tps'] for s in samples], 'source': 'generation/report.json', 'unqualified': bool(generation_sql_errors), 'eligible_for_ratio': False},
                      stage_series('small-qualified-native', 'Natif · disque VM', 'native', workload)]
            chart_html += chart('generation-' + workload, title, 'transactions / seconde', series,
                               'Transactions acquittées potentiellement perdues en génération. Volume neuf et profil complet, journal aligné : cette comparaison combine plusieurs choix et ne mesure pas isolément fsync. Les tests de reprise restent distincts de la qualification des débits.')
            summary['charts'].append({'name': 'generation-' + workload, 'series': series})
        chart_html += '</div>'
    chart_html += '<h2>Exploration des options · un passage par profil</h2><div class="figures">'
    for size, workload in [('small', 'write_only'), ('small', 'read_write'), ('large', 'read_only')]:
        profiles = [('baseline-before', 'Avant · référence', 'baseline'),
                    ('core', 'Cache async + lectures', 'astra')]
        profiles += ([('pipeline', '+ segments préparés', 'astra'),
                      ('compact', '+ pages finales / index paginé', 'astra')]
                     if size == 'small' else [('ublk', '+ ublk persistant', 'astra')])
        profiles.append(('baseline-after', 'Après · référence', 'baseline'))
        series = [stage_series(size + '-screen-' + key, label, color, workload, exploration=True)
                  for key, label, color in profiles]
        if any(s['values'] for s in series):
            chart_html += chart('screen-' + size + '-' + workload,
                               ('Petite base · ' if size == 'small' else 'Grande base · ') + LABELS[workload],
                               'transactions / seconde', series,
                               'Exploration : un passage de 30 s, références avant/après. Les erreurs SQL et les p99 sont conservés dans le tableau ; les variantes ne sont pas toutes retenues.')
            summary['charts'].append({'name': 'screen-' + size + '-' + workload, 'series': series})
    chart_html += '</div>'
    compact_screen = [value for value in checkpoints if value['stage'] in ('small-screen-pipeline', 'small-screen-compact')]
    if compact_screen:
        series = [metric_series('Astra · ' + ('pages finales' if value['stage'].endswith('compact') else 'WAL complet'), 'astra', [value['uploaded_percent']], value['source']) for value in compact_screen]
        chart_html += '<h2>Coût des checkpoints S3</h2>' + chart('checkpoint-upload-ratio', 'Données de segments envoyées / WAL traité', '% · plus bas = moins d’octets S3', series, 'Compteurs cumulés de la dernière ligne de statut après checkpoint ; hors index, HEAD et protocole réseau. Chaque profil est rapporté à son propre WAL, pas aux transactions d’un autre profil.')
        summary['charts'].append({'name': 'checkpoint-upload-ratio', 'series': series})
    rows = []
    for stage in stages:
        for workload, metric in stage.get('summary', {}).items():
            rows.append([stage['label'] + ' / ' + workload, f"{metric['median_tps']:.2f}",
                         ', '.join(f'{v:.2f}' for v in metric['tps']), f"{metric['median_p99_ms']:.2f}",
                         metric.get('median_engine_cpu_percent_of_one_core'), metric['ignored_errors'],
                         'admis' if stage.get('eligible_for_comparison') is True else ', '.join(stage.get('comparison_exclusion_reasons', [])) or 'non qualifié',
                         stage.get('database_SIGKILL_recovery')])
    preparation_rows = []
    for stage in stages:
        path = OUT / 'mysql' / stage.get('proof_report', 'missing')
        proof = load(path) if path.is_file() else {}
        entry_label = stage['engine'] + '-' + stage['phase']
        entry = proof.get('mysql', {}).get(entry_label, {})
        if stage['engine'] == 'native':
            storage_sha = '— (disque natif)'
        elif stage['engine'] == 'zerofs':
            storage_sha = proof.get('binary_sha256', {}).get('zerofs', 'non archivé')
        else:
            storage_sha = stage.get('binary_sha256')
        preparation_rows.append([stage['label'], stage.get('transport'), round(stage.get('seconds', 0), 1),
                                 proof.get('preparation', {}).get(entry_label, {}).get('warm_seconds', '—'),
                                 entry.get('startup_seconds', '—'), entry.get('database_recovery_seconds', '—'),
                                 entry.get('ssd_cache_bytes_before_samples', '—'),
                                 entry.get('engine_rss_kib_before_samples', '—'), storage_sha])
    checks = []
    for name, data in recoveries:
        checks.append([name, data.get('passed'), '; '.join(key + ': ' + str(value) for key, value in data.get('tests', {}).items()), data.get('binary_sha256')])
    if generation:
        generation_checks = generation.get('tests', {})
        scenarios = ['retour au checkpoint après SIGKILL : ' + str(sum(bool(case.get('passed')) for case in generation.get('crashes', []))) + ' scénarios']
        for key, label in [('live_checkpoint_recovery', 'checkpoint sous transactions'),
                           ('live_root_scrub', 'scrub de la racine active'),
                           ('total_local_loss_adoption', 'adoption sans état local')]:
            value = generation_checks.get(key, {})
            scenarios.append(label + ' : ' + ('validé' if value.get('passed') else 'non validé'))
        checks.append(['génération', generation.get('complete'), '; '.join(scenarios), generation.get('binary_sha256')])
    for stage in stages:
        if stage.get('storage_crash'):
            checks.append([stage['label'], stage.get('storage_engine_SIGKILL_recovery'), 'SIGKILL moteur ublk pendant écritures MySQL ; ext4 + InnoDB', stage.get('binary_sha256')])
    for name in ('tests.log', 'clippy.log'):
        path = OUT / name
        if path.exists():
            summary['tests'][name] = path.read_text()
    ratio_rows = [[key, f'{value:.2f}×'] for key, value in summary['ratios'].items()]
    esc = html.escape
    css = '''body{margin:0;background:#edf2f7;color:#172839;font:16px/1.6 system-ui}main{max-width:1300px;margin:auto;padding:24px}section{background:white;border-radius:12px;padding:24px;margin:20px 0}h1{font-size:32px}h2{margin-top:24px}figure{margin:0;padding:12px;border:1px solid #dbe3eb;border-radius:10px;background:white;min-width:0}figure img{width:100%;height:auto}figcaption{font-size:13px;color:#475569}.figures{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,540px),1fr));gap:16px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:9px;border-bottom:1px solid #dde3eb;text-align:left}a{color:#0369a1}.contract{border-left:4px solid #7e22ce;padding-left:16px}pre{white-space:pre-wrap;font-size:12px}@media print{figure{break-inside:avoid}}'''
    css += '.astra-log{display:none}body.astra-logscale .astra-log{display:block}body.astra-logscale .astra-linear{display:none}.report-controls{position:sticky;top:0;z-index:10;background:#fff;padding:12px;border:1px solid #dbe3eb;border-radius:10px;display:flex;gap:12px;flex-wrap:wrap}.report-controls label{min-width:0}.report-controls select{max-width:100%;padding:6px}h2{scroll-margin-top:110px}@media print{.report-controls{display:none}}'
    page = '<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>InfiniDisk2 — rapport complet Astra</title><style>' + css + OLD_CSS + '</style><main><h1>InfiniDisk2 — rapport complet Astra</h1>'
    page += '<p>' + ('Campagne en cours : aucune conclusion finale.' if args.partial else 'Mesures archivées, choix techniques et reprise après panne.') + '</p>'
    page += '<nav class="report-controls" aria-label="Navigation du rapport"><label>Aller à <select id="report-jump"><option value="">Choisir une section</option></select></label><label>Échelle <select id="astra-scale"><option value="linear">Linéaire depuis zéro</option><option value="log">Logarithmique</option></select></label></nav>'
    page += '<section><p class="contract">InfiniDisk2 durable attend son disque local, puis réplique sur S3. ZeroFS durable attend S3. ZeroFS asynchrone ignore fsync. Le mode génération InfiniDisk2 peut perdre des transactions acquittées et exige une reprise complète de l’application. Ces garanties doivent rester distinctes.</p><p>En échelle logarithmique, les longueurs ne représentent pas des ratios de performance.</p>' + chart_html + '</section><script>document.getElementById("astra-scale").addEventListener("change",function(){document.body.classList.toggle("astra-logscale",this.value==="log")});document.addEventListener("DOMContentLoaded",function(){const jump=document.getElementById("report-jump");document.querySelectorAll("h2").forEach(function(heading,index){heading.id="section-"+index;const option=document.createElement("option");option.value=heading.id;option.textContent=heading.textContent;jump.appendChild(option)});jump.addEventListener("change",function(){const target=document.getElementById(this.value);if(target)target.scrollIntoView({behavior:"smooth",block:"start"})})});</script>'
    page += '<section><h2>Effet mesuré de la sélection Astra</h2>' + table(['Comparaison / charge', 'Rapport des débits médians'], ratio_rows) + '<p>Rapports des médianes mesurées, sans garantie universelle ni intervalle de confiance. La sélection est issue du screening de la petite base ; elle n’est pas nécessairement optimale pour chaque charge. Les paramètres restent expérimentaux et désactivés par défaut.</p></section>'
    page += s3_detail
    page += blocks_detail
    page += direct_detail
    if blocks_medians and direct_medians:
        best_random = max(direct_medians['random_read'], key=lambda key: direct_medians['random_read'][key]['iops'])
        best_sequential = max(direct_medians['sequential_read'], key=lambda key: direct_medians['sequential_read'][key]['mib_per_second'])
        full = blocks_medians['writefull']
        saving_32 = (1 - full[32]['class_a'] / full[8]['class_a']) * 100
        saving_64 = (1 - full[64]['class_a'] / full[8]['class_a']) * 100
        page += '<section><h2>Quel compromis retenir pour les tailles ?</h2><p>Sur ces passages directs, le meilleur débit aléatoire froid vient de ' + best_random + ' Kio, et le meilleur séquentiel de ' + best_sequential + ' Kio. La variabilité reste forte ; les barres montrent l’étendue des trois passages. Le séquentiel 16 Kio reste lent sans proxy, avec CRC corrects : sa cause précise entre moteur, SDK, réseau et fournisseur n’est pas isolée ici.</p>'
        page += '<p>Sans compaction, 32 Mio retire ' + f'{saving_32:.2f}' + ' % des appels A face à 8 Mio, contre ' + f'{saving_64:.2f}' + ' % pour 64 Mio. Le minimum de coût observé est 64 Mio ; 32 Mio offre un compromis avec une réserve WAL plus petite. La confirmation directe n’observe pas de gain d’IOPS à 64 Mio face à 32 Mio. Avec compaction, grossir le WAL ne réduit pas les appels de ce lot : le regroupement par shard domine.</p>'
        page += '<p>Pour une DB mêlant petits accès et scans, une future sélection adaptative des plages paraît plus prometteuse qu’une taille globale unique. Elle reste à implémenter et à tester. Les profils SQL qualifiés conservent leurs paramètres : ce balayage fio ne constitue pas une qualification sous crash de toutes les tailles, ni une mesure de leur TPS SQL. La faible économie d’octets de la compaction dans ce lot ne prédit pas celle d’une base qui réécrit souvent les mêmes pages.</p></section>'
    page += '<section><h2>Méthode et limites</h2><p>' + esc(campaign['protocol']['limitations']) + '</p><p>Les références avant/après encadrent le screening ; la qualification est séquentielle sur une VM partagée. La base évolue pendant les charges en écriture. Le cache SSD InfiniDisk2 bénéficie du cache Linux. Les essais natifs MySQL et fio utilisent O_DIRECT ; PostgreSQL garde ses I/O habituelles et le cache du système. Les p99 ci-dessous sont les percentiles rapportés pour chaque passage, pas un percentile recomposé de toute la campagne. Les compteurs CPU brackettent le processus sysbench, y compris son lancement et sa sortie.</p><p>Le quota de un ou deux CPU concerne le conteneur de la base. Le service de stockage tourne hors de ce conteneur : ce quota ne borne donc pas le coût CPU total de la base et du moteur. Les budgets de cache configurés ne rendent pas non plus identique leur consommation totale de RAM, notamment face au natif. Les mesures décrivent ces configurations, et non une égalité de ressources physiques consommées.</p><p>Les contrôles SIGKILL démontrent les scénarios exécutés, pas la résistance à toute panne électrique, matérielle ou du fournisseur S3.</p></section>'
    page += '<section><h2>Mesures détaillées</h2><p>La lecture de la petite base travaille surtout dans le buffer pool MySQL : la baseline qualifiée utilise environ 100 % d’un cœur côté base et 0,03 % côté moteur de stockage. Son débit ne mesure donc pas celui des téléchargements S3. Les écritures et la grande base exercent des chemins différents.</p>' + table(['Profil / charge', 'TPS médian', 'Passages TPS', 'p99 médian ms', 'CPU moteur % d’un cœur', 'Erreurs SQL ignorées', 'Admission aux ratios', 'Reprise MySQL'], rows) + '</section>'
    page += '<section><h2>Préchauffage et provenance</h2><p>Le passage de une à seize partitions invalide le cache SSD jetable. Le rechargement depuis S3 est un coût réel de migration. Les TPS mesurent le régime après préparation ; ils ne décrivent pas la première ouverture à froid. Les durées complètes incluent préparation, mesures, reprise et arrêt. Démarrage et reprise MySQL sont des observations uniques par essai, distinctes des trois fenêtres de débit.</p>' + table(['Profil', 'Transport', 'Durée totale s', 'Warm S3 s', 'Démarrage MySQL s', 'Reprise MySQL s', 'Cache SSD avant mesures (octets)', 'RSS moteur avant mesures (Kio)', 'SHA-256 du binaire'], preparation_rows) + '</section>'
    if warm:
        warm_rows = []
        for run in warm.get('runs', []):
            for phase in ('cold', 'retention'):
                sample = run.get(phase)
                if sample:
                    warm_rows.append([run['label'] + ' / ' + phase, round(sample.get('seconds', 0), 3), sample.get('pages'), sample.get('remote_gets'), sample.get('remote_bytes'), sample.get('max_range_inflight'), round(sample.get('useful_mib_per_second', 0), 3), run['binary_sha256']])
        warm_source = warm_path.relative_to(OUT).as_posix()
        warm_incidents = warm.get('interrupted_runs', [])
        if warm_incidents:
            page += '<section><h2>Préchargements interrompus et repris</h2><p>Le budget global de la première tentative a arrêté une passe avant son terme. Elle reste archivée, sans débit complet ni ratio. La consolidation retient la première paire froid/rétention terminée pour chaque niveau ; HEAD, binaire, options et volumes transférés sont identiques entre les tentatives.</p>' + table(['Niveau', 'Temps avant interruption s', 'Motif'], [[run.get('concurrency'), round(run.get('elapsed_seconds_until_timeout', 0), 3), run.get('limitation')] for run in warm_incidents]) + '<p><a href="' + esc(warm_source) + '">Rapport consolidé et références des deux archives</a>.</p></section>'
        page += '<section><h2>Préchauffage S3 depuis un cache vide</h2><p>Mesure hors ligne sur un HEAD figé de la grande fixture. Les caches applicatifs du test démarrent vides ; les caches Linux, du disque et du fournisseur ne sont pas purgés. Le deuxième passage vérifie que toutes les pages restent présentes, sans nouveau GET de données. Les compteurs excluent les métadonnées HEAD/index. Le pic mesure les appels de plage au backend, pas les connexions HTTP. Le débit utile inclut les pages trouvées dans le cache au passage de rétention : il ne représente alors aucun débit réseau. Les caches d’origine sont restaurés après le test.</p>' + table(['Binaire / passage', 'Secondes', 'Pages 4 Kio', 'GET de données', 'Octets de données', 'Pic de plages actives', 'Débit utile Mio/s', 'SHA-256'], warm_rows) + '<p>Validation : ' + esc(str(warm.get('passed'))) + ' ; caches restaurés : ' + esc(str(warm.get('original_caches_restored'))) + '. <a href="' + esc(warm_source) + '">Preuves du préchauffage</a>. Les préchargements historiques de 1 322 s et 1 185 s portaient sur d’autres HEAD ; aucun facteur d’accélération contrôlé n’en est déduit.</p></section>'
    if checkpoints:
        page += '<section><h2>Transferts et cache</h2><p>Compteurs moteur cumulés jusqu’au dernier statut périodique archivé, incluant démarrage et préchauffage applicatif. Ils ne sont pas limités aux fenêtres sysbench et ne comptent pas les uploads du dernier arrêt si celui-ci suit le dernier statut.</p>' + table(['Profil', 'WAL traité Mio', 'Segments envoyés Mio', 'Envoyé / WAL %', 'GET S3', 'Lectures S3 Mio', 'Fills sautés', 'Erreurs de fill'], [[value['stage'], round(value['checkpoint_wal_bytes'] / 1024 ** 2, 2), round(value['uploaded_segment_bytes'] / 1024 ** 2, 2), round(value['uploaded_percent'], 2), value['remote_gets'], round(value['remote_bytes'] / 1024 ** 2, 2), value['cache_fills_skipped'], value['cache_fill_errors']] for value in checkpoints]) + '</section>'
    quota_stages = final_stages + control_stages + control_transport_stages + [s for s in stages if '-qualified-' in s['label']]
    quotas = quota_rows(list({s['phase']: s for s in quota_stages}.values()))
    if quotas:
        page += '<section><h2>Quota CPU MySQL · compteurs des passages</h2><p>Un quota de un CPU peut retarder des threads même lorsque les I/O deviennent plus rapides. Ces compteurs indiquent les périodes touchées par le quota ; ils ne mesurent pas le temps d’attente de chaque requête et ne prouvent pas à eux seuls la cause d’un p99 élevé. Le diagnostic à deux CPU ci-dessus sert à éprouver cette hypothèse. Les compteurs de limitation locale ne couvrent pas toutes les contraintes ancestrales du cgroup.</p>' + table(['Profil / charge / passage', 'Quota cœurs', 'CPU % d’un cœur', 'Périodes limitées %', 'throttled_usec', 'p99 ms'], quotas) + '</section>'
    excluded = [s for s in campaign['stages'] if s.get('superseded') or s.get('returncode') not in (0, None)]
    if excluded:
        page += '<section><h2>Essais écartés et incidents</h2><p>Un essai incomplet ne reçoit aucun débit inventé. Les anciens résultats et diagnostics sont conservés pour expliquer les corrections et la provenance du binaire retenu.</p>' + table(['Essai', 'Code sortie', 'Remplacé', 'Binaire', 'Motif / observation'], [[s['phase'], s.get('returncode'), s.get('superseded', False), s.get('binary_sha256'), s.get('failure', s.get('error', s.get('superseded_reason', 'Voir les preuves et logs de cet essai.')))] for s in excluded]) + '</section>'
    sql_excluded = [[stage['label'], workload, metric.get('ignored_errors')]
                    for stage in stages for workload, metric in stage.get('summary', {}).items()
                    if metric.get('ignored_errors')]
    if sql_excluded:
        page += '<section><h2>Mesures avec erreurs SQL</h2><p>Les valeurs brutes restent dans les preuves, le tableau détaillé et les barres hachurées des graphiques. Tout essai complet signalant des erreurs SQL ignorées conserve son exclusion de qualification et des ratios annoncés, y compris ses charges sans erreur. Cet affichage ne requalifie aucun essai et ne remplace aucun passage. Le compteur, à lui seul, ne signifie pas une corruption du stockage.</p>' + table(['Essai', 'Charge', 'Erreurs ignorées'], sql_excluded) + '</section>'
    if sql_diagnostics:
        page += '<section><h2>Diagnostic des erreurs SQL</h2><p>Une campagne distincte journalise les codes ignorés, les erreurs de performance_schema et les compteurs de verrouillage InnoDB autour de chaque passage. Elle conserve les réglages durables et ajoute les logs détaillés ; ses débits ne remplacent aucun échantillon de comparaison. Les événements reproduits sont identifiés, tandis que les événements historiques sans trace individuelle restent non identifiés.</p>' + table(['Phase d’origine', 'Codes reproduits / occurrences', 'Compteurs concordants', 'Reprise et tables', 'Arrêt propre'], [[item['source_phase'], json.dumps(item.get('ignored_error_codes', {}), ensure_ascii=False), item.get('counters_concordant'), item.get('database_SIGKILL_recovery'), item.get('clean_teardown')] for item in sql_diagnostics])
        if any('1213' in item.get('ignored_error_codes', {}) for item in sql_diagnostics):
            page += '<p>Le code 1213 / SQLSTATE 40001 observé correspond à un deadlock entre transactions InnoDB. Les logs conservés donnent le cycle de verrous et la transaction choisie pour être annulée. Cette observation ne transforme pas les erreurs historiques non tracées en deadlocks prouvés.</p>'
        page += '<p>' + ' · '.join('<a href="mysql/' + esc(item['proof_report']) + '">Preuve ' + esc(item['phase']) + '</a>' for item in sql_diagnostics) + '</p></section>'
    if (OUT / 'mysql/diagnostic-3b97cc836e8c/flush-descriptors.json').exists():
        page += '<section><h2>Diagnostic du blocage ublk</h2><p>La trace noyau a confirmé une requête FLUSH avec <code>start_sector = 18446744073709551615</code> et <code>nr_sectors = 0</code>. Le décodage calculait à tort une adresse en octets avant de distinguer les commandes ; son overflow quittait le tag sans réponse, laissant MySQL attendre une barrière. Le nouveau décodage ignore le secteur pour FLUSH et rend une erreur de protocole pour les requêtes invalides sans perdre le tag. Une autre anomalie de fermeture a ensuite été isolée : le poll du worker de complétion restait armé lors de la destruction du runtime. Les essais qui ont révélé ces défauts restent exclus des chiffres qualifiés.</p><p><a href="mysql/diagnostic-3b97cc836e8c/flush-descriptors.json">Descripteurs FLUSH observés</a>. Les essais de la version retenue et leur SHA figurent dans les campagnes ci-dessus.</p></section>'
    proof_links = '<a href="tests.log">Tests Rust</a> · <a href="clippy.log">Clippy</a> · <a href="mysql/report.json">Campagne MySQL et sources</a>'
    if compaction_report:
        proof_links += ' · <a href="mysql/compact-tail-control-report.json">Diagnostic compaction</a>'
    for name, title in [('generation', 'Générations'), ('fio', 'fio'), ('postgres', 'PostgreSQL'), ('recovery-core', 'Reprise WAL compact'), ('recovery-aligned', 'Reprise WAL aligné')]:
        if (OUT / name / 'report.json').exists():
            proof_links += ' · <a href="' + name + '/report.json">' + title + '</a>'
    page += '<section><h2>Tests et reprises</h2>' + table(['Campagne', 'Terminée avec succès', 'Scénarios', 'SHA binaire'], checks) + '<p>' + proof_links + '</p></section>'
    if builds:
        page += '<section><h2>Versions compilées et contrôles</h2>' + table(['Preuves', 'Objet du build', 'Tests passés', 'Test io_uring explicite', 'SHA-256'], [[name, build.get('purpose'), build.get('tests_passed'), build.get('explicit_io_uring_test'), build.get('binary_sha256')] for name, build in builds]) + '<p>Les manifestes sous <code>builds/</code> conservent les empreintes des sources et des journaux de compilation. Les versions de diagnostic expliquent les incidents ; chaque campagne qualifiée indique le binaire réellement exécuté.</p></section>'
    pg_interrupted = [(path, load(path)) for path in sorted((OUT / 'postgres').glob('*/report.json'))]
    pg_interrupted = [(path, data) for path, data in pg_interrupted if not data.get('complete')]
    if pg_interrupted:
        page += '<section><h2>Essais PostgreSQL interrompus</h2><p>Les mesures des essais incomplets ne remplacent aucun échantillon du comparatif final. Le premier précontrôle a confondu des connexions TCP TIME_WAIT avec un port occupé ; après correction du contrôle et vérification du refus d’un vrai listener, les quatre variantes ont été rejouées sur des fixtures neuves.</p>' + table(['Archive', 'Cause conservée'], [[path.parent.name, data.get('error', 'Voir le rapport archivé')] for path, data in pg_interrupted]) + '<p>' + ' · '.join('<a href="' + esc(path.relative_to(OUT).as_posix()) + '">Preuve ' + esc(path.parent.name) + '</a>' for path, _ in pg_interrupted) + '</p></section>'
    s3_interrupted = [(path, load(path)) for path in sorted((OUT / 's3-operations').glob('*/report.json'))]
    s3_interrupted = [(path, data) for path, data in s3_interrupted if not data.get('complete')]
    if s3_interrupted:
        page += '<section><h2>Essai de comptage S3 interrompu</h2><p>La première tentative a bloqué le proxy asyncio pendant un accès synchrone au répertoire de la base sur NBD froid. L’outil de mesure a été corrigé : opérations de fichiers déportées dans un thread et export limité aux diagnostics directs, sans parcours des bases ou caches. Un test de réentrance vérifie que le proxy continue de répondre pendant cet accès. La campagne entière a ensuite été reprise sur des fixtures neuves. L’arrêt forcé du moteur de test pour débloquer ce harnais ne constitue pas une qualification de reprise du moteur ; les temps incomplets sont exclus.</p>'
        page += table(['Archive', 'Erreur conservée'], [[path.parent.name, data.get('error', 'Voir archive')] for path, data in s3_interrupted])
        page += '<p>' + ' · '.join('<a href="' + esc(path.relative_to(OUT).as_posix()) + '">Preuve ' + esc(path.parent.name) + '</a>' for path, _ in s3_interrupted) + '</p></section>'
    page += '<section><h2>Review technique et recommandations</h2><p>Le chemin chaud conserve une autorité locale claire : le WAL avant l’index, puis une frontière durable explicite. Les caches sont jetables et vérifiés. Sur S3, les objets et leurs index précèdent le remplacement conditionnel de HEAD. Les optimisations conservent cet ordre ; le mode génération change explicitement la garantie de durabilité.</p>'
    if fio_report and fio_report.get('complete'):
        fio_runs = {**fio_report['comparison']['runs'], **fio_report['native_runs']}
        def fio_median(engine, workload):
            direction = 'write' if 'write' in workload else 'read'
            return statistics.median(fio_runs[f'{engine}-{workload}-{i}'][direction]['iops'] for i in range(3))
        page += '<p>fio confirme la valeur du cache : ' + f"{fio_median('infinidisk2', 'warm-read'):.0f} IOPS chaudes pour Astra contre {fio_median('zerofs', 'warm-read'):.0f} pour ZeroFS. À froid, les médianes sont {fio_median('infinidisk2', 'cold-read'):.0f} et {fio_median('zerofs', 'cold-read'):.0f} IOPS, avec une forte dispersion côté ZeroFS." + ' Aucun gain universel à froid ne peut en être déduit. Le débit chaud des moteurs bénéficie de caches que le fichier natif O_DIRECT contourne ; dépasser cette référence sur les petites lectures ne signifie pas dépasser le matériel à ressources identiques.</p>'
        page += '<p>En écritures avec fsync, Astra atteint ' + f"{fio_median('infinidisk2', 'fsync-write'):.0f} IOPS contre {fio_median('native', 'fsync-write'):.0f} pour le natif. ZeroFS durable obtient {fio_median('zerofs', 'fsync-write'):.2f} IOPS en attendant S3." + ' Cette différence de contrat interdit de présenter leur rapport comme un gain à durabilité identique. Les CRC des données natives et des restaurations S3 des deux moteurs passent après les fenêtres chronométrées.</p>'
    if postgres_report and postgres_report.get('complete'):
        pg = postgres_report['series']
        before_tps = statistics.median(s['tps'] for s in pg['baseline']['samples'])
        after_tps = statistics.median(s['tps'] for s in pg['astra']['samples'])
        before_latency = statistics.median(s['latency_ms'] for s in pg['baseline']['samples'])
        after_latency = statistics.median(s['latency_ms'] for s in pg['astra']['samples'])
        page += '<p>PostgreSQL, au même contrat fsync local et avec les mêmes budgets configurés : ' + f'{before_tps:.2f} → {after_tps:.2f} transactions/s, soit {(after_tps / before_tps - 1) * 100:+.2f} %. La médiane des latences moyennes passe de {before_latency:.3f} à {after_latency:.3f} ms.' + ' Les trois passages de chaque série ne signalent aucune transaction échouée ; les contrôles SQL et les reprises après SIGKILL de PostgreSQL sont archivés. Cette comparaison mesure le changement de version et de profil, sans attribuer le gain à une option unique.</p>'
    if summary['qualified_mysql_deltas']:
        page += '<p>Les comparaisons MySQL admises ci-dessous rapprochent systématiquement débit et latence. Un débit supérieur avec un p99 supérieur reste un compromis, pas une amélioration générale pour une base interactive. Les essais exclus pour erreurs SQL ne contribuent pas à ce tableau.</p>' + table(
            ['Charge', 'TPS avant → Astra', 'Variation TPS', 'p99 avant → Astra (ms)', 'Rapport p99 · plus bas = mieux'],
            [[('Petite base' if key.startswith('small/') else 'Grande base') + ' / ' + LABELS[key.split('/')[1]],
              f"{values['baseline_tps']:.2f} → {values['astra_tps']:.2f}",
              f"{(values['tps_ratio'] - 1) * 100:+.2f} %",
              f"{values['baseline_p99_ms']:.2f} → {values['astra_p99_ms']:.2f}",
              f"{values['p99_ratio']:.2f}×"]
             for key, values in summary['qualified_mysql_deltas'].items()])
    page += table(['Choix', 'Apport', 'Compromis / recommandation'], [
        ['Cache asynchrone borné', 'Retire le remplissage SSD du chemin d’acquittement ; saturation mesurée.', 'Une file pleine laisse un miss de cache. Dimensionner le working set et examiner le p99, pas seulement les TPS.'],
        ['Segments préparés et sync sélective', 'Déplace les allocations et évite des synchronisations de fichiers immuables.', 'Réserve physique incluse dans le budget. Un disque qui ne respecte pas fsync reste hors des preuves SIGKILL.'],
        ['Lectures groupées et ublk', 'Réduit tâches temporaires, copies et contention du cache.', 'Choisir le transport selon ses mesures finales et sa reprise réelle ; ublk exige le support du noyau.'],
        ['WAL aligné', 'Format explicite à frontières 4 Kio, validé séparément.', 'Amplification des petits records ; aucune activation par défaut sans gain adapté à la charge.'],
        ['Index paginé et publication des versions finales', 'Borne les cartes résidentes et évite l’envoi des versions déjà dépassées.', 'Misses d’index sous verrou, coût de copie des shards, PUT supplémentaires possibles, racine encore limitée à 64 Mio.'],
        ['Reprise par générations', 'Permet de différer la durabilité transactionnelle en revenant à un HEAD complet.', 'Volume neuf de format 2 ; perte d’acquittements acceptée ; arrêter, détacher et redémarrer toute l’application.'],
    ])
    page += '<p>Pour un premier déploiement pilote, retenir le contrat fsync local, un cache couvrant les pages actives et les options effectivement qualifiées pour la charge. La sélection automatique privilégie le débit ; une application sensible au p99 doit appliquer son propre seuil de latence. Les options restent explicites et désactivées par défaut.</p><p>Le mode génération demande une décision de produit distincte : accepter les transactions perdues après redémarrage et exploiter les métriques de retard. L’intervalle de cinq secondes est une cadence nominale ; le blocage au seuil de retard protège contre une accumulation illimitée, sans promettre un RPO de cinq secondes lors d’une panne S3.</p><p>Avant de qualifier ce stockage pour des données critiques, les prochaines expériences utiles sont une coupure de VM dans un environnement dédié, les erreurs disque et pertes réseau pendant publication, puis une charge longue dont les données dépassent RAM et SSD. Les tests actuels couvrent les scénarios enregistrés. Ils ne démontrent ni toutes les pannes physiques ni un volume multi-To rempli.</p><p>Le préchauffage hors ligne regroupe les pages par bloc physique, garde chaque téléchargement jusqu’au remplissage du cache et règle la concurrence avec warm --concurrency. Les contrôles CRC précèdent les remplissages. Son temps et ses octets sont mesurés séparément des TPS ; le meilleur niveau observé lors d’un passage doit être confirmé sur le fournisseur et le volume cibles.</p>'
    page += '<p>La capacité logique creuse ne qualifie pas un volume de même taille entièrement rempli. Le format actuel impose encore, par calcul, 65 536 GET et 13 Gio d’index à chaque ouverture d’un volume dense de 1 Tio ; les commandes hors ligne peuvent matérialiser au moins 12 Gio de références pour ce volume. Le cache SSD possède son propre annuaire RAM, en plus du cache de données. Ces valeurs décrivent les structures du format, pas un benchmark multi-Tio ; les hypothèses et le plafond variable de HEAD sont détaillés dans <a href="../../docs/astra-index.md">le dimensionnement de l’index</a>.</p>'
    page += table(['Suite prioritaire', 'Problème concret', 'Mesure de réussite'], [
        ['Réemploi des données vérifiées pendant la compaction', 'Le checkpoint valide les WAL puis relit les pages retenues. Le p99 de la grande base augmente ; le contrôle sans compaction évalue cette piste sans changer les autres options.', 'p99, octets locaux lus/écrits et RSS, à débit comparable. Garder la vérification CRC avant publication et des buffers bornés ; aucune fuite mémoire n’est établie.'],
        ['Plages S3 adaptatives et regroupement en ligne', 'Le warm parallèle conserve une surlecture mesurée de 2,188× ; un miss isolé peut charger beaucoup plus que sa page utile.', 'Octets et GET par page utile, débit froid et p99, avec la même vérification CRC et une borne globale des buffers.'],
        ['Regroupement des petits objets compacts entre shards', 'Le compactage actuel produit un objet par shard modifié : augmenter segment_mib ne regroupe pas ces objets. Les économies d’octets peuvent ainsi augmenter les PUT.', 'Taille cible des objets distants séparée de celle du WAL local ; mesurer PUT, espace stocké, surlecture et p99, en gardant les index et HEAD publiés après les objets. Piste non implémentée.'],
        ['Fusion des lectures locales', 'Les lectures groupées effectuent encore des accès de 4 Kio sous les verrous du cache.', 'Appels système par requête, CPU total moteur + base, débit et p99 ; contrôler séparément le quota de la base.'],
        ['Index hiérarchique chargé à la demande', 'Chargement complet au démarrage, HEAD monolithique et réécriture entière de chaque shard modifié.', 'Temps de reprise à froid, GET/PUT et octets de métadonnées, RAM totale et latence sous checkpoints.'],
        ['Index local vérifié conservé au redémarrage', 'Le cache de données évite les GET de pages à chaud, mais l’ouverture recharge HEAD et les index. L’adoption puis serve chargent actuellement deux fois les index à froid.', 'Réutiliser les shards locaux seulement après validation de l’identité du HEAD distant et des checksums ; compter les appels évités et injecter des corruptions locales. Piste non implémentée dans cette campagne.'],
        ['Collecte des objets et sessions abandonnés', 'Les anciennes versions S3 restent jusqu’au GC hors ligne ; les SIGKILL peuvent laisser du scratch local.', 'Espace physique borné en charge prolongée, sans supprimer un objet ou snapshot encore référencé.'],
    ])
    page += '<p>La compaction des données ne mesure pas toute l’amplification d’écriture : modifier 4 Kio dans un shard plein peut republier 213 000 octets d’index, puis HEAD. Les compteurs de segments affichés excluent ces métadonnées. Aucun facteur de gain n’est promis pour les suites proposées.</p></section>'
    previous = ROOT / 'validation/breakthroughs/summary.json'
    if previous.exists():
        old_html, _ = historical_charts(ROOT, load(previous)['cases'])
        page += '<section><h2>Campagnes antérieures · PostgreSQL, fio et premières versions</h2><p>Les mesures suivantes précèdent Astra. Les budgets et durées varient ; elles complètent l’historique et ne doivent pas être utilisées comme une comparaison simultanée avec les nouveaux graphiques.</p>' + old_html + '</section>'
    page += '<section><h2>Spécifications et choix</h2><p><a href="../../docs/astra-optimizations.md">Les six optimisations et leurs limites</a> · <a href="../../docs/astra-index.md">Index paginé</a> · <a href="../../docs/astra-ublk.md">Transport ublk</a> · <a href="../../docs/astra-warm-parallel.md">Préchargement parallèle</a> · <a href="../../docs/astra-warm-measurement.md">Mesures du préchargement</a> · <a href="../../docs/astra-postgres-comparison.md">Comparatif PostgreSQL avant/après</a> · <a href="../../docs/generation-mode.md">Reprise par génération</a> · <a href="../breakthroughs/rapport.html">Rapport précédent</a> · <a href="summary.json">Synthèse exploitable</a></p></section></main></html>'
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n')
    (OUT / 'rapport.html').write_text(page)
    print(OUT / 'rapport.html')


if __name__ == '__main__':
    main()
