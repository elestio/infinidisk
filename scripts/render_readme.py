#!/usr/bin/env python3
"""Render README figures from frozen evidence. No network or benchmark execution."""
import hashlib
import json
import os
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'docs/assets'
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'test-output/matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator
from matplotlib.patches import FancyBboxPatch

BG, PANEL, INK, MUTED = '#0b1220', '#121e30', '#edf5ff', '#aabbd0'
TEAL, PURPLE, BLUE, GREY = '#58dfc6', '#ae98f4', '#75bef0', '#91a6bc'
SOURCES = {}

def read(name):
    raw = (ROOT / name).read_bytes()
    SOURCES[name] = hashlib.sha256(raw).hexdigest()
    return json.loads(raw)

astra = read('validation/astra/summary.json')
assert astra['complete']
charts = {x['name']: x for x in astra['charts']}

def series(chart, index, label, color, qualified=False):
    row = charts[chart]['series'][index]
    if qualified:
        assert row['eligible_for_ratio'] and not row['unqualified'], row
    values = row['values']
    assert len(values) == 3 and all(v > 0 for v in values)
    source = 'validation/astra/' + row['source']
    read(source)
    return dict(label=label, color=color, samples=values, median=median(values), source=source)

panels = [
    dict(title='PostgreSQL', subtitle='pgbench · scale 2 · read / write', unit='transactions / second',
         note='1 CPU database · 3 × 15 s\nLocal WAL commits vs. S3 commits vs. VM disk.',
         rows=[series('postgres-comparison-throughput', 1, 'InfiniDisk · NBD', TEAL),
               series('postgres-comparison-throughput', 2, 'ZeroFS · S3 fsync', PURPLE),
               series('postgres-comparison-throughput', 3, 'Native VM disk', GREY)]),
    dict(title='fio · cached reads', subtitle='4 KiB random · depth 32 × 4 jobs', unit='IOPS',
         note='Warm data · 3 × 15 s\nHost page cache benefits InfiniDisk; native uses O_DIRECT.',
         rows=[series('fio-comparison-warm-read', 0, 'InfiniDisk · NBD', TEAL),
               series('fio-comparison-warm-read', 1, 'ZeroFS', PURPLE),
               series('fio-comparison-warm-read', 2, 'Native VM file', GREY)]),
    dict(title='MySQL · read-only', subtitle='Large fixture · uniform access · warm data', unit='transactions / second',
         note='2 CPU database · 3 × 30 s\nZeroFS: not measured in this fixture. Cache paths differ.',
         rows=[series('large-transport-cpu-débit', 2, 'InfiniDisk · NBD', TEAL, True),
               series('large-transport-cpu-débit', 3, 'InfiniDisk · ublk', BLUE, True),
               series('cpu-control-large-débit', 5, 'Native VM disk', GREY, True)]),
    dict(title='fio · synchronized writes', subtitle='4 KiB write + fsync · depth 1 × 4 jobs', unit='IOPS',
         note='3 × 15 s · different durability boundaries\nInfiniDisk: local WAL. ZeroFS: S3. Native: VM disk.',
         rows=[series('fio-comparison-fsync-write', 0, 'InfiniDisk · NBD', TEAL),
               series('fio-comparison-fsync-write', 1, 'ZeroFS · S3 fsync', PURPLE),
               series('fio-comparison-fsync-write', 3, 'Native VM file', GREY)])
]

plt.rcParams.update({'font.family': 'DejaVu Sans', 'text.color': INK,
                     'axes.labelcolor': MUTED, 'xtick.color': MUTED,
                     'ytick.color': INK, 'font.size': 11, 'svg.fonttype': 'none'})

def save(fig, name):
    for fmt in ('png', 'svg'):
        options = {'metadata': {'Date': None}} if fmt == 'svg' else {}
        fig.savefig(OUT / f'{name}.{fmt}', dpi=145, facecolor=BG, **options)
    plt.close(fig)

def number(value):
    return f'{value:,.0f}' if value >= 10 else f'{value:.3f}'

def tick(value, _):
    return f'{value / 1000:g}k' if value >= 1000 else f'{value:g}'

OUT.mkdir(parents=True, exist_ok=True)
fig = plt.figure(figsize=(16, 10.5), facecolor=BG)
fig.text(.043, .95, 'THE PERFORMANCE RECORD', fontsize=12, color=TEAL, weight='bold')
fig.text(.043, .904, 'Real workloads. Traceable results.', fontsize=28, weight='bold')
fig.text(.043, .87, 'Historical reference • 10 Oct 2026 • medians of 3 runs • higher is better', fontsize=13, color=MUTED)
for idx, panel in enumerate(panels):
    col, row = idx % 2, idx // 2
    left, bottom, width, height = .032 + col * .489, .473 - row * .401, .47, .368
    fig.patches.append(FancyBboxPatch((left, bottom), width, height,
                      boxstyle='round,pad=0.0,rounding_size=0.012',
                      facecolor=PANEL, edgecolor='#26364b', linewidth=.7,
                      transform=fig.transFigure, zorder=-1))
    fig.text(left+.018, bottom+height-.043, panel['title'], fontsize=19, weight='bold')
    fig.text(left+.018, bottom+height-.071, panel['subtitle'], fontsize=11, color=MUTED)
    ax = fig.add_axes([left+.139, bottom+.119, width-.172, .154], facecolor=PANEL)
    values = [r['median'] for r in panel['rows']]
    cap = max(values) * 1.27
    ax.barh(range(3), values, height=.45, color=[r['color'] for r in panel['rows']], zorder=3)
    ax.set_yticks(range(3), [r['label'] for r in panel['rows']], fontsize=11)
    ax.invert_yaxis()
    ax.set_xlim(0, cap)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=4))
    ax.xaxis.set_major_formatter(FuncFormatter(tick))
    ax.set_xlabel(panel['unit'], fontsize=10, labelpad=7)
    ax.tick_params(axis='both', length=0, pad=9)
    ax.grid(axis='x', color='#2b3a4e', linewidth=.7, zorder=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    for y, v in enumerate(values):
        ax.text(v+cap*.025, y, number(v), va='center', fontsize=12, weight='bold', color=INK)
    fig.text(left+.018, bottom+.029, panel['note'], fontsize=10.5, color=MUTED, linespacing=1.5)
fig.text(.043, .032, 'Different durability and cache paths: read the methodology before comparing. Current defaults were qualified separately.', fontsize=11, color=MUTED)
save(fig, 'performance')

adaptive = read('validation/adaptive/summary.json')
indexes = read('validation/index-cache/summary.json')
downloads = read('validation/downloads/summary.json')
assert adaptive['complete'] and indexes['complete'] and downloads['complete']
fixed = adaptive['reads']['fixed64']['sequential-read']['median']['gets']
adapt = adaptive['reads']['adaptive']['sequential-read']['median']['gets']
cache_off = median(x['B'] for x in indexes['metadata_ABBA'] if x['label'].startswith('off'))
cache_on = median(x['B'] for x in indexes['metadata_ABBA'] if x['label'].startswith('on'))
p99 = downloads['medians']['mixed-random']
efficiency = [
    dict(title='Sequential data GETs', before=fixed, after=adapt, unit='requests',
         labels=['Fixed 64 KiB', 'Adaptive'], headline='75% fewer',
         foot='Fresh-cache fixture · 2 samples / variant\nSequential p99 increased 19%.', source='validation/adaptive/summary.json'),
    dict(title='Warm-open metadata reads', before=cache_off, after=cache_on, unit='Class-B reads',
         labels=['Index cache off', 'Index cache on'], headline='15 → 1',
         foot='Unchanged HEAD · metadata-only ABBA\nHEAD is still read; timing gain unproven.', source='validation/index-cache/summary.json'),
    dict(title='Mixed random-read p99', before=p99['off']['p99_ms'], after=p99['bounded']['p99_ms'], unit='milliseconds',
         labels=['Budget off', '8 MiB budget'], headline='14.8% lower',
         foot='Direct HTTPS · 2 samples / variant\nSequential-only p99 increased 1%.', source='validation/downloads/summary.json')
]
fig = plt.figure(figsize=(16, 5.2), facecolor=BG)
fig.text(.035, .918, 'EVERY REQUEST COUNTS', color=TEAL, fontsize=12, weight='bold')
fig.text(.035, .82, 'Three targeted optimizations.', fontsize=26, weight='bold')
for idx, e in enumerate(efficiency):
    left = .032 + idx * .327
    fig.text(left+.012, .697, e['title'], fontsize=13, color=MUTED)
    fig.text(left+.012, .584, e['headline'], fontsize=30, color=TEAL, weight='bold')
    ax=fig.add_axes([left+.105, .287, .196, .204], facecolor=BG)
    vals=[e['before'],e['after']]
    cap=max(vals)*1.30
    ax.barh([0,1],vals,color=[GREY,TEAL],height=.39)
    ax.set_yticks([0,1], e['labels'], fontsize=10)
    ax.invert_yaxis();ax.set_xlim(0,cap);ax.set_xticks([])
    ax.tick_params(length=0, pad=8)
    for sp in ax.spines.values(): sp.set_visible(False)
    for y,v in enumerate(vals):
        label=f'{v:,.0f}' if idx<2 else f'{v:.2f}'
        ax.text(v+cap*.025,y,label,va='center',fontsize=11,color=INK)
    fig.text(left+.012,.193,e['foot'],fontsize=10.5,color=MUTED,linespacing=1.5)
    if idx<2: fig.add_artist(plt.Line2D([left+.316]*2,[.17,.71],transform=fig.transFigure,color='#28364a',linewidth=.8))
fig.text(.045,.069,'Separate experiments • lower is better • results are not additive • exact sources and limitations in docs/benchmarks.md',fontsize=11,color=MUTED)
save(fig,'efficiency')
manifest={'schema':'infinidisk.readme-figures.v1','aggregation':'median; no normalization across panels',
          'reference_binary_sha256':'b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151',
          'comparisons':panels,'efficiency':efficiency,'source_files_sha256':SOURCES}
(OUT/'benchmark-data.json').write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+'\n')
print('Rendered docs/assets/{performance,efficiency}.{png,svg} and benchmark-data.json from',len(SOURCES),'source files.')
