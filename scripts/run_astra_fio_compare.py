#!/usr/bin/env python3
"""Fresh Astra/ZeroFS raw-device fio comparison plus a matched native file.

Run sequentially on the isolated benchmark VM, after the database campaigns.
No existing mount or object prefix is reused and no global cache is dropped.
"""
import argparse
import fcntl
import json
import os
import pathlib
import re
import signal
import shutil
import socket
import subprocess
import sys
import time
import uuid

from run_astra_recovery import ROOT, Redactor, atomic_json, digest, load_profiles, utc

HELPER = ROOT / 'scripts/compare_zerofs.py'


def preflight():
    if not pathlib.Path('/dev/nbd31').exists() or pathlib.Path('/sys/class/block/nbd31/pid').exists():
        raise RuntimeError('reserved nbd31 is active')
    if pathlib.Path('/dev/ublkc31').exists():
        raise RuntimeError('reserved ublk31 is active')
    for port in (11990, 11991, 12991):
        with socket.socket() as sock:
            sock.settimeout(.2)
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                raise RuntimeError('test listener active: ' + str(port))
    for comm in pathlib.Path('/proc').glob('[0-9]*/comm'):
        try:
            if comm.read_text().strip() in {'cargo', 'rustc', 'cc1', 'ld.lld'}:
                raise RuntimeError('compiler active during comparison')
        except FileNotFoundError:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=pathlib.Path, default=ROOT / 'target/release/infinidisk2-astra')
    parser.add_argument('--expected-binary-sha256', required=True)
    parser.add_argument('--credentials', type=pathlib.Path, default=pathlib.Path('/opt/elestio/infinidisk/bench.env'))
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    options = load_profiles()['core']
    # compare_zerofs owns the 128 MiB cold / 512 MiB warm cache phases.
    options.pop('disk_cache_mib')
    options['hot_wal_mib'] = 0
    if args.plan_only:
        print(json.dumps({'options': options, 'samples': 3, 'seconds': 15,
                          'native': '256 MiB regions in a new regular file; direct libaio, same offsets and jobs; native reads warm at the host/provider level, not labelled cold S3'}, indent=2))
        return
    if not re.fullmatch('[0-9a-f]{64}', args.expected_binary_sha256) or digest(args.binary) != args.expected_binary_sha256:
        raise RuntimeError('frozen binary SHA mismatch')
    if args.credentials.resolve() != pathlib.Path('/opt/elestio/infinidisk/bench.env').resolve():
        raise RuntimeError('compare_zerofs currently requires the VM benchmark credentials path')
    locks = []
    for name in ('mysql/campaign.lock', 'fio-comparison.lock'):
        path = ROOT / 'validation/astra' / name
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    zerofs = pathlib.Path('/usr/local/bin/zerofs').resolve()
    found_zerofs = shutil.which('zerofs')
    if not found_zerofs or pathlib.Path(found_zerofs).resolve() != zerofs:
        raise RuntimeError('PATH would execute a different ZeroFS binary')
    attempt = 'fio-' + uuid.uuid4().hex[:12]
    work = ROOT / 'test-output' / attempt
    work.mkdir(mode=0o700, parents=True)
    option_path = work / 'options.json'
    atomic_json(option_path, options)
    report = {'complete': False, 'attempt': attempt, 'started_utc': utc(),
              'binary_sha256': args.expected_binary_sha256, 'options': options,
              'helper_sha256': digest(HELPER), 'zerofs_sha256': digest(zerofs),
              'zerofs_path': str(zerofs), 'native_runs': {},
              'protocol': '3 x 15 s; libaio direct=1; shared 256 MiB regions; 4 KiB random depth32 x4 jobs, fsync depth1 x4 jobs, 1 MiB sequential depth16. Native uses a regular file on the VM filesystem; engines use raw block devices. No global cache drop. ZeroFS lz4/encryption remain enabled. Durability contracts reported separately.'}
    fixture = None
    controller_log = work / 'controller.log'
    redactor = Redactor.from_credentials(args.credentials)

    def run_helper(command, log, timeout):
        found = shutil.which('zerofs')
        if (not found or pathlib.Path(found).resolve() != zerofs
                or digest(zerofs) != report['zerofs_sha256']
                or digest(args.binary) != args.expected_binary_sha256
                or digest(HELPER) != report['helper_sha256']):
            raise RuntimeError('binary/helper identity changed before helper execution')
        with log.open('w') as output:
            child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                return child.wait(timeout=timeout)
            except BaseException:
                # Let the coordinator detach clients and publish/stop engines
                # in its own order. Its cumulative cleanup timeouts can exceed
                # nine minutes; do not interrupt every child at once.
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=600)
                except subprocess.TimeoutExpired:
                    report.setdefault('cleanup', []).append({'log': log.name, 'forced_group_kill_after_seconds': 600})
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                raise

    try:
        command = [sys.executable, str(HELPER), '--binary', str(args.binary),
                   '--engine', 'both', '--engine-options', str(option_path)]
        code = run_helper(command, controller_log, 7200)
        matches = re.findall(r'^REPORT (.+/report\.json)$', controller_log.read_text(), re.M)
        if matches:
            fixture = pathlib.Path(matches[-1]).resolve().parent
            if fixture.parent != (ROOT / 'test-output').resolve() or not re.fullmatch('comparison-[0-9a-f]+', fixture.name):
                raise RuntimeError('unexpected comparison fixture path')
            report['comparison'] = json.loads((fixture / 'report.json').read_text())
        if code or not report.get('comparison', {}).get('complete'):
            raise RuntimeError('raw-device comparison incomplete; see archived controller log')
        if report['comparison'].get('binary_sha256') != {
                'infinidisk2': args.expected_binary_sha256, 'zerofs': report['zerofs_sha256']}:
            raise RuntimeError('raw-device comparison binary identities differ')
        preflight()
        native = work / 'native-fio.bin'
        with native.open('xb') as output:
            output.truncate(768 * 1024 * 1024)

        def fio(label, **settings):
            target = work / ('native-' + label + '.json')
            opts = {'name': 'native-' + label, 'filename': str(native), 'ioengine': 'libaio',
                    'direct': 1, 'group_reporting': 1, 'size': '256m', 'offset': '16m',
                    'output-format': 'json', 'output': str(target), **settings}
            if opts.get('rw') == 'randwrite':
                opts['offset'] = '512m'
            done = subprocess.run(['fio'] + [f'--{k}={v}' for k, v in opts.items()],
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=300)
            (work / ('native-' + label + '.log')).write_text(done.stdout)
            if done.returncode:
                raise RuntimeError('native fio failed: ' + label)
            data = json.loads(target.read_text())
            if any(job['error'] for job in data['jobs']):
                raise RuntimeError('native fio I/O error: ' + label)
            report['native_runs']['native-' + label] = {key: data['jobs'][0][key]
                                                      for key in ('read', 'write', 'sync') if key in data['jobs'][0]}

        fio('verified-write', rw='write', bs='128k', iodepth=16, verify='crc32c',
            verify_fatal=1, do_verify=1, refill_buffers=1, fsync_on_close=1)
        for i in range(3):
            fio(f'buffered-write-{i}', rw='randwrite', bs='4k', iodepth=32, numjobs=4,
                runtime=15, time_based=1, refill_buffers=1)
            fio(f'fsync-write-{i}', rw='randwrite', bs='4k', iodepth=1, numjobs=4,
                fsync=1, runtime=15, time_based=1, refill_buffers=1)
        fio('prefetch', rw='read', bs='1m', iodepth=16)
        for i in range(3):
            fio(f'warm-read-{i}', rw='randread', bs='4k', iodepth=32, numjobs=4, runtime=15, time_based=1)
            fio(f'seq-read-{i}', rw='read', bs='1m', iodepth=16, runtime=15, time_based=1)
        fio('verified-after-timings', rw='read', bs='128k', iodepth=16,
            verify='crc32c', verify_fatal=1, verify_only=1)
        report['native_dataset_crc32c_after_timings'] = 'passed'
        native.unlink()
        # The verified read dataset lives at 16 MiB; random writes use the
        # disjoint region at 512 MiB. Check that dataset again after the helper's
        # fresh-local S3 adoptions and all timing windows, so verification cannot
        # prewarm a measured cold read.
        verify_log = work / 'verify-controller.log'
        verify_command = [sys.executable, str(HELPER), '--binary', str(args.binary),
                          '--engine', 'both', '--engine-options', str(option_path),
                          '--verify-report', str(fixture / 'report.json')]
        code = run_helper(verify_command, verify_log, 1800)
        report['comparison'] = json.loads((fixture / 'report.json').read_text())
        tests = report['comparison'].get('tests', {})
        if code or not report['comparison'].get('complete') or any(
                tests.get(engine + '-restored-256m-crc32c') != 'passed'
                for engine in ('infinidisk2', 'zerofs')):
            raise RuntimeError('restored fio dataset checksum verification failed')
        report['restored_dataset_crc32c'] = 'passed for InfiniDisk2 and ZeroFS after timing windows'
        if (digest(args.binary) != args.expected_binary_sha256 or digest(HELPER) != report['helper_sha256']
                or digest(zerofs) != report['zerofs_sha256']):
            raise RuntimeError('binary/helper changed during measurements')
        report['complete'] = True
    except BaseException as error:
        report['error'] = str(error)
        raise
    finally:
        # A timed-out helper can still finish cleanup and print REPORT while
        # run_helper is propagating the timeout. Recover its raw proof path too.
        if fixture is None and controller_log.exists():
            matches = re.findall(r'^REPORT (.+/report\.json)$', controller_log.read_text(), re.M)
            if matches:
                candidate = pathlib.Path(matches[-1]).parent
                if (not candidate.is_symlink() and candidate.resolve().parent == (ROOT / 'test-output').resolve()
                        and re.fullmatch('comparison-[0-9a-f]+', candidate.name)):
                    fixture = candidate.resolve()
        if fixture:
            redactor = Redactor([*redactor.values, *(path.read_text().strip() for path in fixture.glob('*.secret'))])
        report['ended_utc'] = utc()
        out = ROOT / 'validation/astra/fio'
        archive = out / attempt
        archive.mkdir(parents=True, mode=0o700)
        entries = {}
        for directory in [work, *([fixture] if fixture else [])]:
            for path in directory.iterdir():
                if path.is_symlink() or not path.is_file() or path.suffix not in {'.log', '.json'}:
                    continue
                if path.stat().st_size > 64 * 1024 * 1024:
                    report['complete'] = False
                    report.setdefault('evidence_errors', []).append({'name': path.name, 'reason': 'file exceeds export limit'})
                    continue
                name = directory.name + '-' + path.name
                try:
                    content = path.read_text(errors='replace')
                except OSError as error:
                    report['complete'] = False
                    report.setdefault('evidence_errors', []).append({'name': name, 'reason': str(error)})
                    continue
                if path.suffix == '.json':
                    try:
                        content = json.dumps(redactor.object(json.loads(content)), indent=2)
                    except json.JSONDecodeError:
                        # Interrupted fio may leave an empty/truncated JSON.
                        # Keep that diagnostic without losing the failure report.
                        report.setdefault('evidence_warnings', []).append({'name': name, 'reason': 'incomplete JSON preserved as text'})
                        name += '.invalid.log'
                        content = redactor.text(content)
                else:
                    content = redactor.text(content)
                (archive / name).write_text(content)
                entries[name] = digest(archive / name)
        report['evidence_directory'] = attempt
        atomic_json(archive / 'report.json', redactor.object(report))
        entries['report.json'] = digest(archive / 'report.json')
        atomic_json(archive / 'manifest.json', {'files_sha256': entries, 'binary_sha256': args.expected_binary_sha256,
                                             'zerofs_sha256': report['zerofs_sha256'],
                                             'script_sha256': digest(pathlib.Path(__file__)),
                                             'export': 'flat JSON/log text only; redacted credentials; no config, secret file, binary, database, WAL or cache'})
        atomic_json(out / 'report.json', redactor.object(report))
        print('REPORT ' + str(out / 'report.json'), flush=True)
    if not report['complete']:
        raise RuntimeError('fio qualification/evidence export is incomplete')


if __name__ == '__main__':
    main()
