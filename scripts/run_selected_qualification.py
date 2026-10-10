#!/usr/bin/env python3
"""One old/new PostgreSQL comparison and one selected-profile recovery run.

Uses existing isolated VM helpers. No native/ZeroFS/MySQL matrix, global cache
drop, automatic retries, or production reconfiguration.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib

from run_astra_recovery import Redactor, REQUIRED_TESTS, atomic_json, digest, load_profiles, utc
from run_astra_s3_operations import preflight

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/adaptive/selected'
OLD_SHA = 'b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    args = parser.parse_args()
    if OUT.exists():
        raise RuntimeError('refusing to overwrite prior qualification evidence')
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        lock = (ROOT / 'validation/astra' / name).open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    binaries = {'previous': ROOT / 'target/release/infinidisk2-astra-b39b705f43b6',
                'selected': args.binary.resolve()}
    expected = {'previous': OLD_SHA, 'selected': args.expected_sha256}
    if any(digest(binary) != expected[name] for name, binary in binaries.items()):
        raise RuntimeError('binary differs from frozen qualification candidate')
    OUT.mkdir(parents=True)
    redactor = Redactor.from_credentials(args.credentials)
    read_report = json.loads((ROOT / 'validation/adaptive/reads/report.json').read_text())
    if not read_report['complete'] or read_report['binary_sha256']['astra'] != expected['selected']:
        raise RuntimeError('adaptive read candidate has not passed direct read qualification')
    options = {'previous': load_profiles()['core'],
               'selected': read_report['protocol']['base_options']}
    if not options['selected']['adaptive_reads'] or options['selected']['compact_checkpoints']:
        raise RuntimeError('selected profile differs from the proposed selection')
    for name, value in options.items():
        atomic_json(OUT / (name + '-options.json'), value)
    sources = ['scripts/run_selected_qualification.py', 'scripts/compare_zerofs.py',
               'scripts/validate_vm.py', 'scripts/run_astra_recovery.py',
               'scripts/run_astra_s3_operations.py', 'scripts/profiles/astra-recovery-core.json',
               'scripts/profiles/astra-recovery-aligned.json',
               'validation/adaptive/reads/report.json', 'validation/adaptive/build/manifest.json']
    report = {'complete': False, 'started_utc': utc(), 'binary_sha256': expected,
              'source_sha256': {name: digest(ROOT / name) for name in sources},
              'options': options, 'cases': {}, 'protocol': {
                  'postgres': 'Previous Astra compact8 versus selected noncompact32/adaptive, fresh scale2 DBs, 3x15s each, 4 clients, 1 CPU and 512MiB for PostgreSQL.',
                  'cache_budgets': 'Equal 64MiB RAM, 128MiB SSD, 64MiB resident index. Recommended deployment budgets are larger and not part of the comparison.',
                  'recovery': 'One selected profile: local fsync + ext4/PG SIGKILL, remote scrub, fresh S3 restore and CRC32C.',
                  'limits': 'Sequential short samples on shared VM. No new native, ZeroFS or MySQL timings; no claim of isolated attribution or power-loss certification.',
                  'http_accounting': 'No complete S3 A/B accounting in this short qualification.'}}

    def save():
        atomic_json(OUT / 'report.json', redactor.object(report))

    def verify():
        if any(digest(ROOT / name) != value for name, value in report['source_sha256'].items()):
            raise RuntimeError('qualification source changed during run')
        if any(digest(binary) != expected[name] for name, binary in binaries.items()):
            raise RuntimeError('binary changed during run')

    def execute(label, command, profile, recovery=False):
        verify()
        preflight()
        target = OUT / label
        target.mkdir()
        case = {'complete': False, 'started_utc': utc(), 'command': list(map(str, command))}
        report['cases'][label] = case
        save()
        print('START', label, flush=True)
        # Child helpers always run their own finally cleanup. Never capture
        # credentials or recursively traverse a mounted database directory.
        proc = subprocess.Popen(list(map(str, command)), cwd=ROOT, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        output = []
        try:
            for line in proc.stdout:
                clean = redactor.text(line)
                output.append(clean)
                print(clean, end='', flush=True)
            code = proc.wait()
            case['returncode'] = code
            matches = re.findall(r'^(?:REPORT |Report: )(/root/infinidisk2/test-output/[^\s]+/report.json)$',
                                 ''.join(output), re.M)
            if len(matches) != 1:
                raise RuntimeError('helper did not identify one result directory')
            path = Path(matches[0]).resolve()
            wanted = r'run-[a-f0-9]{12}' if recovery else r'comparison-[a-f0-9]{12}'
            if path.parent.parent != ROOT / 'test-output' or not re.fullmatch(wanted, path.parent.name):
                raise RuntimeError('helper result is outside its isolated directory')
            case['work_directory'] = str(path.parent)
            files = {}
            for file in sorted(path.parent.iterdir()):
                if file.is_symlink() or not file.is_file() or file.suffix not in {'.log', '.json', '.toml'}:
                    continue
                if file.stat().st_size > 64 * 1024**2:
                    raise RuntimeError('oversized text proof')
                exported = target / file.name
                exported.write_text(redactor.text(file.read_text()))
                files[file.name] = digest(exported)
            case['evidence_sha256'] = files
            data = json.loads(path.read_text())
            case['result'] = redactor.object(data)
            if code or not data.get('passed' if recovery else 'complete'):
                raise RuntimeError('helper failed: ' + label)
            if recovery:
                required = REQUIRED_TESTS | {'remote_fio_crc32c'}
                if any(data['tests'].get(key) != 'passed' for key in required):
                    raise RuntimeError('missing recovery check')
                manifest = data['validation_executions'][-1]
                starts = [entry['effective_engine_options'] for entry in manifest['engine_starts']]
                if manifest['applied_engine_options'] != options[profile]:
                    raise RuntimeError('recovery options differ')
                if data['binary_sha256'] != expected[profile]:
                    raise RuntimeError('recovery binary differs')
            else:
                pg = data['postgres']['infinidisk2']
                if (len(pg['samples']) != 3 or any(x['failed_transactions'] for x in pg['samples'])
                        or pg['settings'] != ['on'] * 4 or pg['database_SIGKILL_recovery'] != 'passed'):
                    raise RuntimeError('PostgreSQL qualification incomplete')
                starts = [entry['options'] for entry in data['engine_starts'].values()]
                if data['binary_sha256']['infinidisk2'] != expected[profile]:
                    raise RuntimeError('PostgreSQL binary differs')
            if not starts or any(any(entry.get(k) != v for k, v in options[profile].items()) for entry in starts):
                raise RuntimeError('effective profile changed between starts')
            preflight()
            if os.path.ismount(path.parent / 'mount'):
                raise RuntimeError('test mount still active')
            case['cleanup'] = 'reserved ports/devices free; test mount absent'
            case['complete'] = True
            # Delete only this completed helper's disposable local state, after
            # preserving text proofs. Immutable S3 fixture remains auditable.
            for config in path.parent.glob('*.toml'):
                local = tomllib.loads(config.read_text()).get('local_dir')
                if not local:
                    continue
                local = Path(local)
                if local.parent == path.parent and local.is_dir() and not local.is_symlink() and local.name != 'mount':
                    shutil.rmtree(local)
        finally:
            (target / 'controller.log').write_text(''.join(output))
            case['ended_utc'] = utc()
            save()

    try:
        for profile in ('previous', 'selected'):
            execute('postgres-' + profile,
                    [sys.executable, ROOT / 'scripts/compare_zerofs.py', '--postgres-only',
                     '--engine', 'infinidisk2', '--binary', binaries[profile],
                     '--engine-options', OUT / (profile + '-options.json')], profile)
        execute('recovery-selected',
                [sys.executable, ROOT / 'scripts/validate_vm.py', '--s3', '--credentials', args.credentials,
                 '--postgres', '--checks-only', '--binary', binaries['selected'],
                 '--engine-options', OUT / 'selected-options.json'], 'selected', recovery=True)
        verify()
        report['complete'] = True
    except BaseException as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        report['ended_utc'] = utc()
        save()
        print('REPORT', OUT / 'report.json', flush=True)


if __name__ == '__main__':
    main()
