#!/usr/bin/env python3
"""One checks-only S3 recovery on the bounded data-read path, no perf repeats."""
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib

from run_astra_recovery import Redactor, REQUIRED_TESTS, atomic_json, digest, utc
from run_astra_s3_operations import ROOT, preflight


def main():
    output = ROOT / 'validation/downloads/recovery'
    if output.exists():
        raise RuntimeError('refusing to overwrite recovery evidence')
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        handle = (ROOT / 'validation/astra' / name).open('a')
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(handle)
    preflight()
    build = json.loads((ROOT / 'validation/downloads/build-final/manifest.json').read_text())
    account = json.loads((ROOT / 'validation/downloads/reads/report.json').read_text())
    binary = Path(build['binary_path'])
    if not build['complete'] or not account['complete'] or digest(binary) != build['binary_sha256']:
        raise RuntimeError('unqualified or changed candidate')
    output.mkdir()
    options = {**account['protocol']['base_options'], 'download_budget_mib': 8}
    atomic_json(output / 'options.json', options)
    credentials = Path('/opt/elestio/infinidisk/bench.env')
    redactor = Redactor.from_credentials(credentials)
    command = [sys.executable, ROOT / 'scripts/validate_vm.py', '--s3', '--postgres', '--checks-only',
               '--credentials', credentials, '--binary', binary, '--engine-options', output / 'options.json']
    sources = ['scripts/run_download_recovery.py', 'scripts/validate_vm.py',
               'scripts/run_astra_recovery.py', 'scripts/run_astra_s3_operations.py']
    report = {'complete': False, 'started_utc': utc(), 'binary_sha256': build['binary_sha256'],
              'command': list(map(str, command)), 'options': options,
              'source_sha256': {name: digest(ROOT / name) for name in sources}}
    lines = []
    try:
        child = subprocess.Popen(list(map(str, command)), cwd=ROOT, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True)
        for line in child.stdout:
            clean = redactor.text(line)
            print(clean, end='', flush=True)
            lines.append(clean)
        report['returncode'] = child.wait()
        paths = re.findall(r'^Report: (/root/infinidisk2/test-output/run-[a-f0-9]{12}/report.json)$', ''.join(lines), re.M)
        if len(paths) != 1:
            raise RuntimeError('missing isolated result path')
        path = Path(paths[0])
        report['work'] = str(path.parent)
        raw = output / 'raw'
        raw.mkdir()
        report['evidence_sha256'] = {}
        for file in sorted(path.parent.iterdir()):
            if file.is_symlink() or not file.is_file() or file.suffix not in {'.log', '.json', '.toml'}:
                continue
            if file.stat().st_size > 64 * 1024**2:
                raise RuntimeError('oversized proof')
            export = raw / file.name
            export.write_text(redactor.text(file.read_text()))
            report['evidence_sha256'][file.name] = digest(export)
        data = json.loads(path.read_text())
        report['result'] = redactor.object(data)
        if report['returncode'] or not data['passed'] or data['binary_sha256'] != build['binary_sha256']:
            raise RuntimeError('recovery failed or candidate differs')
        if any(data['tests'].get(key) != 'passed' for key in REQUIRED_TESTS | {'remote_fio_crc32c'}):
            raise RuntimeError('recovery check missing')
        manifest = data['validation_executions'][-1]
        if manifest['applied_engine_options'] != options:
            raise RuntimeError('recovery options differ')
        if any(any(start['effective_engine_options'].get(k) != v for k, v in options.items())
               for start in manifest['engine_starts']):
            raise RuntimeError('effective settings changed on recovery')
        if any(digest(ROOT / name) != expected for name, expected in report['source_sha256'].items()):
            raise RuntimeError('helper source changed during recovery')
        preflight()
        if os.path.ismount(path.parent / 'mount'):
            raise RuntimeError('test filesystem remains mounted')
        report['cleanup'] = 'test mount absent; nbd31/ublk31/ports free'
        report['complete'] = True
        for config in path.parent.glob('*.toml'):
            local = tomllib.loads(config.read_text()).get('local_dir')
            if local:
                local = Path(local)
                if local.parent == path.parent and local.is_dir() and not local.is_symlink() and local.name != 'mount':
                    shutil.rmtree(local)
    except BaseException as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        (output / 'controller.log').write_text(''.join(lines))
        report['ended_utc'] = utc()
        atomic_json(output / 'report.json', report)
        print('REPORT', output / 'report.json', flush=True)


if __name__ == '__main__':
    main()
