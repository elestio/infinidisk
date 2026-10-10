#!/usr/bin/env python3
"""Render checked measurements, individual points and the predeclared decision."""
import base64
import hashlib
import html
import json
import os
from pathlib import Path
import statistics
import tomllib

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/downloads'
os.environ.setdefault('MPLCONFIGDIR', str(ROOT / 'test-output/matplotlib'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table(headers, rows):
    def cells(tag, values):
        return ''.join(f'<{tag}>{html.escape(str(value))}</{tag}>' for value in values)
    return '<div class="table"><table><thead><tr>' + cells('th', headers) + '</tr></thead><tbody>' + ''.join(
        '<tr>' + cells('td', row) + '</tr>' for row in rows) + '</tbody></table></div>'


def main():
    build = read(OUT / 'build/manifest.json')
    final = read(OUT / 'build-final/manifest.json')
    report = read(OUT / 'reads/report.json')
    recovery = read(OUT / 'recovery/report.json')
    promotion = read(OUT / 'default-profile/manifest.json')
    count = 0
    for doc in (build, final, report, recovery, promotion):
        assert doc['complete']
        for name, expected in doc['source_sha256'].items():
            path = ROOT / name
            if doc is build and name in final['changed_sources']:
                path = OUT / 'measured-source' / name
            if doc is final and name in final['changed_sources']:
                path = OUT / 'post-measurement-source' / name
            assert sha(path) == expected, str(path)
    assert final['inherited_manifest_sha256'] == sha(OUT / 'build/manifest.json')
    assert build['binary_sha256'] == report['binary_sha256']['astra']
    assert final['binary_sha256'] == recovery['binary_sha256']
    measured_config = (OUT / 'measured-source/src/config.rs').read_text()
    assert measured_config == (ROOT / 'src/config.rs').read_text()
    assert measured_config.replace('            download_budget_mib: 8,\n', '') == (OUT / 'post-measurement-source/src/config.rs').read_text()
    assert promotion['binary_sha256'] == build['binary_sha256'] == promotion['checkout_binary_sha256']
    assert promotion['reused_build_manifest_sha256'] == sha(OUT / 'build/manifest.json')
    for label, expected in [('recommended', 8), ('legacy', 0)]:
        check = promotion['checks'][label]
        assert check['download_budget_mib'] == expected and check['download_max_requests'] == 64 and check['overwrite_refused']
        assert sha(OUT / 'default-profile' / (label + '.toml')) == check['generated_config_sha256']
        count += 1
    for directory, doc in [('build', build), ('build-final', final)]:
        for name, check in doc['checks'].items():
            assert check['returncode'] == 0 and sha(OUT / directory / name) == check['sha256']
            count += 1
    manifest = read(OUT / 'reads' / report['source_manifest'])
    for name, expected in manifest['files_sha256'].items():
        assert sha(OUT / 'reads' / report['id'] / name) == expected, name
        count += 1
    for name, expected in recovery['evidence_sha256'].items():
        assert sha(OUT / 'recovery/raw' / name) == expected, name
        count += 1
    assert recovery['returncode'] == 0 and recovery['cleanup']
    assert len([k for k, v in recovery['result']['tests'].items() if v == 'passed']) == 7
    assert recovery['options']['download_budget_mib'] == 8
    config = tomllib.loads((ROOT / 'configs/recommended.toml').read_text())
    assert config['download_budget_mib'] == 8 and config['download_max_requests'] == 64
    for k, value in report['protocol']['base_options'].items():
        assert {'memory_cache_mib': 64, 'disk_cache_mib': 128, 'max_index_mib': 64,
                'download_budget_mib': 8}.get(k, config[k]) == value, k

    jobs = ['sequential-read', 'mixed-sequential', 'mixed-random']
    names = ['Séquentiel seul', 'Séquentiel en mixte', 'Aléatoire en mixte']
    points = []
    summaries = {}
    counters = []
    for label in report['protocol']['order']:
        case = report['variants'][label]
        assert case['complete'] and not case['cleanup']['failures']
        assert all(case['cleanup'][k] for k in ('nbd31_detached', 'mount_absent', 'container_removed'))
        assert set(case['integrity'].values()) == {'passed'}
        assert case['options'] == {**report['protocol']['base_options'],
                                  'download_budget_mib': 8 if label.startswith('bounded') else 0}
        for job in jobs:
            sample = case['samples'][job]
            assert not sample['error']
            sample = sample['read']
            points.append({'variant': label, 'job': job, 'MiB_s': sample['bw_bytes'] / 1024**2,
                           'IOPS': sample['iops'], 'p99_ms': sample['clat_ns']['percentile']['99.000000'] / 1e6})
        for workload, value in case['engine_data_counters'].items():
            d = value['downloads']
            if d['enabled']:
                assert d['peak_reserved_bytes'] <= d['budget_bytes'] and d['peak_active'] <= d['max_requests']
                assert d['waiting'] == d['reserved_bytes'] == d['active'] == 0
                assert d['admissions'] == value['remote_gets']
            counters.append({'variant': label, 'workload': workload, **value})
    for job in jobs:
        summaries[job] = {}
        for mode in ['off', 'bounded']:
            selected = [p for p in points if p['job'] == job and p['variant'].startswith(mode)]
            assert len(selected) == 2
            summaries[job][mode] = {key: statistics.median(p[key] for p in selected)
                                    for key in ['MiB_s', 'IOPS', 'p99_ms']}
        summaries[job]['change_pct'] = {key: (summaries[job]['bounded'][key] / summaries[job]['off'][key] - 1) * 100
                                        for key in ['MiB_s', 'IOPS', 'p99_ms']}
    checks = {
        'sequential_p99_improves_5pct': summaries['sequential-read']['change_pct']['p99_ms'] <= -5,
        'sequential_throughput_loss_at_most_5pct': summaries['sequential-read']['change_pct']['MiB_s'] >= -5,
        'mixed_random_p99_rises_at_most_5pct': summaries['mixed-random']['change_pct']['p99_ms'] <= 5,
    }
    assert not all(checks.values()), 'selection policy requires review if results change'
    summary = {'complete': True, 'measured_binary_sha256': build['binary_sha256'],
               'final_binary_sha256': promotion['binary_sha256'], 'proof_files_verified': count,
               'intermediate_binary_sha256': final['binary_sha256'],
               'points': points, 'medians': summaries, 'engine_counters': counters,
               'selection_checks': checks, 'selected_download_budget_mib': 8,
               'initial_selected_download_budget_mib': 0,
               'decision': promotion['decision']}
    (OUT / 'summary.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n')
    cleanup = read(OUT / 'disk-cleanup.json')
    assert cleanup['complete'] and cleanup['production_nbd_pids'] == {'nbd0': '5299', 'nbd1': '9216'}
    freed_gib = cleanup['freed_available_bytes'] / 1024**3
    available_gib = cleanup['after']['available_bytes'] / 1024**3

    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11,
                         'axes.spines.top': False, 'axes.spines.right': False})
    charts = []
    colors = ['#7890a8', '#009e8d']
    for metric, title in [('MiB_s', 'Débit — plus haut = mieux'), ('p99_ms', 'Latence p99 — plus bas = mieux')]:
        fig, axes = plt.subplots(1, 3, figsize=(14, 4.3), constrained_layout=True)
        for ax, job, name in zip(axes, jobs, names):
            key = 'IOPS' if metric == 'MiB_s' and job == 'mixed-random' else metric
            vals = [summaries[job][mode][key] for mode in ['off', 'bounded']]
            ax.bar([0, 1], vals, color=colors, width=.57)
            for i, mode in enumerate(['off', 'bounded']):
                samples = [p[key] for p in points if p['job'] == job and p['variant'].startswith(mode)]
                ax.scatter([i - .075, i + .075], samples, c='#182d3e', s=27, zorder=4)
                ax.text(i, max(samples) + max(vals) * .045, f'{vals[i]:.1f}', ha='center', weight='bold')
            max_sample = max(p[key] for p in points if p['job'] == job)
            ax.set_ylim(0, max_sample * 1.24)
            ax.set_xticks([0, 1], ['Sans limiteur', 'Budget 8 Mio'])
            ax.set_ylabel({'MiB_s': 'Mio/s', 'IOPS': 'IOPS', 'p99_ms': 'ms'}[key])
            ax.set_title(name + f"\n{summaries[job]['change_pct'][key]:+.1f} %")
            ax.grid(axis='y', alpha=.2)
            ax.set_axisbelow(True)
        fig.suptitle(title + ' · médianes, points = mesures individuelles', fontsize=14)
        filename = 'throughput' if metric == 'MiB_s' else 'latency'
        for extension in ['png', 'svg']:
            destination = OUT / (filename + '.' + extension)
            fig.savefig(destination, dpi=150, facecolor='white')
            if extension == 'svg':
                destination.write_text('\n'.join(line.rstrip() for line in destination.read_text().splitlines()) + '\n')
        plt.close(fig)
        charts.append('<figure><img alt="' + title + '" src="data:image/png;base64,' +
                      base64.b64encode((OUT / (filename + '.png')).read_bytes()).decode() +
                      '"><figcaption>' + title + ' — <a href="' + filename + '.svg">SVG</a> · <a href="' + filename + '.png">PNG</a></figcaption></figure>')

    rows = [[p['variant'], names[jobs.index(p['job'])], f"{p['MiB_s']:.2f}", f"{p['IOPS']:.2f}", f"{p['p99_ms']:.2f}"] for p in points]
    counter_rows = [[c['variant'], c['workload'], c['remote_gets'], f"{c['remote_bytes']/1024**2:.3f}",
                     c['downloads']['peak_active'] if c['downloads']['enabled'] else 'non mesuré',
                     f"{c['downloads']['peak_reserved_bytes']/1024**2:.3f}" if c['downloads']['enabled'] else 'non mesuré',
                     c['downloads']['small_reserve_admissions'] if c['downloads']['enabled'] else '—'] for c in counters]
    document = f'''<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>InfiniDisk2 — budget des téléchargements S3</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#f2f6fa;color:#1a2e40;font:16px/1.6 system-ui,sans-serif}}main{{max-width:1180px;margin:auto;padding:32px 20px 70px}}h1{{line-height:1.15;font-size:clamp(28px,4vw,44px)}}h2{{margin-top:38px}}a{{color:#006b84}}figure,section{{background:white;border-radius:14px;padding:18px;margin:22px 0;box-shadow:0 3px 18px #1433540a}}img{{display:block;width:100%;height:auto}}figcaption{{font-size:14px;color:#536b7b}}.callout{{background:#fff2d6;border-left:5px solid #b37700;padding:18px;border-radius:8px}}.table{{overflow-x:auto}}table{{border-collapse:collapse;min-width:630px;width:100%}}th,td{{border-bottom:1px solid #dfe6eb;padding:9px 12px;text-align:left;font-size:14px}}th{{background:#eaf1f6}}code{{overflow-wrap:anywhere}}p{{max-width:95ch}}.meta{{color:#546c7d}}
</style><main><p class="meta">10 octobre 2026 · S3 direct HTTPS · comparaison A–B–B–A · 2 mesures par mode</p>
<h1>Limiter les téléchargements simultanés : bénéfice en charge mixte, objectif p99 séquentiel non atteint</h1>
<div class="callout"><strong>Décision révisée avec Joseph : 8 Mio / 64 requêtes par défaut pour les nouvelles configurations.</strong> Le budget de 8 Mio améliore le débit séquentiel médian de 20,3 % et le p99 des petites lectures en charge mixte de 14,8 %. Le p99 séquentiel seul passe de 408,9 à 413,1 ms (+1,0 %) ; le seuil d’amélioration de 5 % fixé avant les mesures n’est pas atteint. Le défaut privilégie désormais le compromis global et les transferts bornés ; ce changement de politique ne modifie aucun résultat.</div>
{''.join(charts)}
<p>Les barres représentent la médiane de deux passages ; les points montrent chaque passage. Le premier contrôle séquentiel est nettement plus lent (71,9 contre 100,1 Mio/s au second passage). Ces mesures courtes sur VM partagée ne prouvent ni un gain universel ni sa significativité statistique.</p>
<section><h2>Ce qui est livré</h2><p>Un budget de payload partagé par toutes les connexions d’un volume, un plafond de groupes admis et une réserve dédiée aux petites lectures. À 8 Mio/64 groupes, les grandes plages de 260 Kio plafonnent à 27 simultanément ; les petites plages peuvent utiliser la capacité restante et la réserve. Le test mixte atteint 35 groupes admis et 7,012 Mio réservés. Aucun dépassement ni permis conservé après arrêt.</p>
<p>Cette adaptation dépend de la taille et de la concurrence des requêtes. <strong>Il n’y a pas de contrôleur qui ajuste les seuils d’après la latence.</strong> Les opérations S3 sont asynchrones ; 64 groupes ne signifie pas 64 threads. Les caches conservent leurs propres budgets, distincts du budget réseau ; ce n’est pas une limite du RSS total.</p>
<p>Le modèle recommandé et le CLI génèrent <code>download_budget_mib = 8</code> et <code>download_max_requests = 64</code>. La valeur 0 désactive le limiteur et reste le défaut des anciens fichiers qui omettent le champ. Le format, le CRC, le journal local, les barrières de durabilité et la publication conditionnelle de HEAD restent les mêmes.</p>
<p><a href="../../configs/recommended.toml">Configuration recommandée</a> · <a href="../../configs/downloads-experimental.toml">Ancien modèle expérimental, désormais recommandé</a> · <a href="../../docs/download-admission.md">Spécifications et choix</a></p></section>
<h2>Mesures individuelles</h2>{table(['Passage', 'Charge', 'Mio/s', 'IOPS', 'p99 ms'], rows)}
<h2>Appels et octets de données</h2>{table(['Passage', 'Charge', 'GET', 'Mio reçus', 'Pic groupes admis', 'Pic réservé Mio', 'Admissions réserve'], counter_rows)}
<p>Le séquentiel conserve exactement 1 024 GET de données et 259,94 Mio reçus dans chaque passage. Le mixte passe de 1 487 à 1 489–1 490 GET (+0,17 % sur la médiane), avec un ordre de remplissage/éviction différent. Ces compteurs couvrent les GET de données réussis, pas HEAD, les index, les erreurs ou les retries. Ils ne constituent pas une mesure complète des opérations facturables A/B ; aucune nouvelle projection de facture n’est déduite ici.</p>
<h2>Protocole et décision</h2><p>Même binaire, même seed immutable de 256 Mio avec CRC32C toutes les pages, même cache RAM 64 Mio/SSD 128 Mio/index 64 Mio. Le mode à 8 Mio est la seule option modifiée. Cache local et processus neufs avant chaque charge ; pas de proxy ni de purge globale des caches Linux. Ordre : sans limiteur, borné, borné, sans limiteur.</p>
<p>Le séquentiel lit 256 Mio en blocs de 1 Mio à QD16. Le mixte démarre deux jobs fio : 128 Mio séquentiels à QD16 et 4 Mio de lectures aléatoires de 4 Kio à QD8 dans une autre zone de 128 Mio. Les jobs ont une quantité de travail fixe ; ils se chevauchent partiellement et ne terminent pas ensemble. Le p99 est celui de la latence de complétion fio, pas celui d’un GET S3 isolé.</p>
{table(['Critère déclaré avant mesure', 'Résultat'], [['p99 séquentiel : au moins −5 %', 'NON : +1,0 %'], ['Débit séquentiel : perte maximale 5 %', 'OUI : +20,3 %'], ['p99 aléatoire mixte : hausse maximale 5 %', 'OUI : −14,8 %']])}
<p>La première sélection laissait le mécanisme optionnel, car elle exigeait une baisse du p99 séquentiel. Après réexamen avec Joseph, le profil général retient 8 Mio / 64 groupes pour borner les transferts et privilégier le compromis global. Le critère initial reste explicitement non satisfait ; deux passages ne prouvent pas un optimum universel. Avant une régulation par latence, la prochaine mesure utile est de séparer temps d’attente dans la file et temps réel de réponse S3 : réduire la concurrence ne supprime pas les GET lents du fournisseur.</p>
<h2>Intégrité et provenance</h2><p>30 tests Rust ciblés au premier build, vérifications fmt/clippy et release avec ublk, puis contrôle du profil final. Une seule campagne de récupération S3 : 7/7 contrôles réussis, notamment arrêt brutal du moteur pendant PostgreSQL, contrôles PostgreSQL/ext4 et restauration distante avec CRC sur 256 Mio. Cela ne certifie ni une coupure électrique réelle ni tous les fournisseurs.</p>
<p>Binaire mesuré : <code>{build['binary_sha256']}</code>.<br>Binaire recommandé : <code>{promotion['binary_sha256']}</code>, identique au binaire mesuré et testé. Le build intermédiaire <code>{final['binary_sha256']}</code> avait seulement ramené le défaut généré à 0 ; les 7 contrôles de récupération y forçaient déjà 8 Mio. Les sources intermédiaires sont archivées. Toutes les sources Rust/Cargo du profil rétabli correspondent exactement au build mesuré ; les sorties du CLI recommandé et legacy ont été revérifiées sans répéter la matrice de performances.</p>
<p>{count} fichiers de preuve vérifiés par SHA256. <a href="reads/report.json">Lectures brutes</a> · <a href="recovery/report.json">Récupération</a> · <a href="build/manifest.json">Build mesuré</a> · <a href="build-final/manifest.json">Build intermédiaire</a> · <a href="default-profile/manifest.json">Promotion du défaut 8 Mio et contrôles CLI</a> · <a href="summary.json">Synthèse JSON</a></p>
<p>Les services de production ne sont pas reconfigurés ; les tests utilisent nbd31. Aucun nouveau benchmark PostgreSQL/MySQL/natif/ZeroFS n’est présenté comme exécuté : <a href="../astra/rapport.html">rapport comparatif historique complet</a>. Voir aussi <a href="../index-cache/rapport.html">le redémarrage et son coût</a>, <a href="../adaptive/rapport.html">les lectures adaptatives 16/256 Kio</a>.</p>
<h2>Espace disque de la VM</h2><p>À la demande de Joseph : {freed_gib:.1f} Gio libérés, occupation de 92 % à 54 %, {available_gib:.1f} Gio disponibles après nettoyage. Suppression de 31 répertoires de compilation debug et de données/caches locaux de cinq anciennes campagnes terminées. Les {len(cleanup['preserved_evidence_sha256'])} fichiers de logs/résultats concernés ont été conservés et vérifiés. Sources, rapports, binaires release qualifiés, objets S3, Docker et volumes de production préservés. Les tests suivants recompileront les dépendances debug si nécessaire. <a href="disk-cleanup.json">Inventaire des suppressions et contrôle avant/après</a>.</p>
</main></html>'''
    (OUT / 'rapport.html').write_text(document)
    print(json.dumps({'proof_files_verified': count, 'selection_checks': checks, 'report': str(OUT / 'rapport.html')}))


if __name__ == '__main__':
    main()
