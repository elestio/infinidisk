#!/usr/bin/env python3
"""Fresh PostgreSQL comparison: native, pre-Astra, final Astra, durable ZeroFS.

No VM action occurs with --plan-only or --self-test. Execution needs the frozen
Astra SHA256 and a coordinated VM slot. Every database/volume is created anew;
the comparison helper owns its fresh NBD/S3 fixtures and their cleanup.
"""
import argparse
import ast
import fcntl
import json
import math
import os
import pathlib
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid

from run_astra_recovery import Redactor, atomic_json, digest, load_profiles, read_regular, utc

ROOT = pathlib.Path(__file__).resolve().parents[1]
HELPER = ROOT / 'scripts/compare_zerofs.py'
PROFILE = ROOT / 'scripts/profiles/astra-recovery-core.json'
MYSQL_CAMPAIGN = ROOT / 'scripts/run_astra_mysql.py'
OUT = ROOT / 'validation/astra/postgres'
CREDENTIALS = pathlib.Path('/opt/elestio/infinidisk/bench.env')
IMAGE = 'postgres:16'
ORDER = ('native', 'baseline', 'astra', 'zerofs-durable')
CONTRACTS = {
    'native': 'PostgreSQL fsync on the VM filesystem; acknowledged commits rely on the local disk.',
    'baseline': 'Pre-Astra: FLUSH/FUA durable in the local WAL; S3 publication asynchronous. Previous logical cache, fixed WAL, commit markers, writev and sync_data_only retained; legacy format without aligned WAL or generation rollback.',
    'astra': 'FLUSH/FUA durable in the local WAL; S3 publication asynchronous. recovery-core, aligned_wal=false, generation_mode=false.',
    'zerofs-durable': 'ignore_fsync=false: FLUSH/FUA publishes extents and metadata to S3; stronger remote commit contract than local fsync.',
}
INSPECT_FORMAT = ('{"image":{{json .Image}},"nano_cpus":{{.HostConfig.NanoCpus}},'
                  '"memory_bytes":{{.HostConfig.Memory}},"mounts":{{json .Mounts}}}')
HELPER_LOG = re.compile(
    r'(?:host|fio-version|zerofs-version|id2-version|id2-init|'
    r'(?:infinidisk2|zerofs)-(?:initial-(?:server|attach|nfs|nfs-umount)|mkfs|mount|'
    r'pg-(?:start|ready|settings|init|crash|restart|sums|amcheck|stop|remove)|pgbench-[0-2]|unmount)|'
    r'detach-\d+|cleanup-(?:ext4|pg)-\d+|cleanup-nfs)\.log')
NATIVE_LOG = re.compile(r'native-(?:start|ready|settings|init|pgbench-[0-2]|sums|amcheck|stop|remove|cleanup|filesystem)\.log')


def competing_test_process(comm, argv):
    # Production storage, database and Docker processes are deliberately absent.
    if comm in {'cargo', 'rustc', 'cc1', 'ld.lld', 'fio', 'sysbench', 'pgbench'}:
        return True
    controllers = {'run_astra_mysql.py', 'run_astra_recovery.py', 'run_astra_fio_compare.py',
                   'run_astra_postgres_compare.py', 'validate_generation.py', 'validate_vm.py',
                   'compare_zerofs.py', 'measure_astra_warm.py', 'run_breakthroughs.py',
                   'run_fixed_wal.py', 'probe_fsync_layout.py'}
    return any(pathlib.Path(argument).name in controllers for argument in argv)


def baseline_options(options):
    """Reuse the MySQL pre-Astra profile without importing its campaign."""
    tree = ast.parse(MYSQL_CAMPAIGN.read_text())
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'options']
    if len(functions) != 1:
        raise RuntimeError('MySQL baseline profile API changed; review before benchmarking')
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(MYSQL_CAMPAIGN), 'exec'), namespace)
    baseline = namespace['options']('small', 'baseline')
    for key in ('memory_cache_mib', 'disk_cache_mib', 'max_index_mib', 'hot_wal_mib'):
        baseline[key] = options[key]
    if any(options.get(key) != value for key, value in baseline.items()):
        raise RuntimeError('pre-Astra and Astra common options differ beyond matched budgets')
    for key in ('logical_cache', 'wal_fixed_size', 'wal_commit_records', 'wal_writev', 'sync_data_only'):
        if baseline.get(key) is not True:
            raise RuntimeError('previously enabled pre-Astra optimization was disabled: ' + key)
    if 'aligned_wal' in baseline or 'generation_mode' in baseline or options['aligned_wal'] or options['generation_mode']:
        raise RuntimeError('before/after PostgreSQL requires legacy-compatible local durability')
    return baseline


def option_delta(baseline, astra):
    absent = 'not supported by the legacy binary'
    return {key: {'baseline': baseline.get(key, absent), 'astra': astra.get(key, absent)}
            for key in sorted(set(baseline) | set(astra)) if baseline.get(key) != astra.get(key)}


def protocol(options, baseline=None):
    baseline = baseline_options(options) if baseline is None else baseline
    return {
        'image_reference': IMAGE, 'scale': 2, 'clients': 4, 'jobs': 4,
        'samples': 3, 'sample_seconds': 15, 'cpu_limit': 1, 'memory_mib': 512,
        'fsync': 'on', 'synchronous_commit': 'on', 'full_page_writes': 'on', 'data_checksums': 'on',
        'warmup': 'none, matching compare_zerofs.py --postgres-only; initialization precedes samples',
        'transport': 'nbd', 'fresh_fixture_per_series': True, 'order': list(ORDER),
        'astra_options': options, 'baseline_options': baseline, 'options_delta': option_delta(baseline, options),
        'baseline_profile_source': 'scripts/run_astra_mysql.py:options(small, baseline), matching only RAM/SSD/index/hot-WAL budgets to Astra',
        'legacy_equivalent_modes': {'aligned_wal': False, 'generation_mode': False},
        'measurement_window_seconds': len(ORDER) * 3 * 15,
        'zerofs_cache_memory_mib': 64, 'zerofs_cache_disk_mib': 128,
        'limitations': 'Sequential tests on a shared VM; fixed order, no host page-cache drop. '
                       'Configured caches do not bound total RAM. Both InfiniDisk2 binaries retain 64 MiB of hot WAL. '
                       'pgbench reports mean latency, not p99. The three storage durability contracts differ.',
        'zerofs_async': 'not_run: the current helper only exposes ignored fsync through the roomy-cache repeat '
                        '(1 GiB RAM/4 GiB SSD), which would change two variables. A fresh-fixture ignore_fsync flag is needed.',
    }


def parse_pgbench(text):
    tps = re.search(r'tps = ([\d.]+)', text)
    latency = re.search(r'latency average = ([\d.]+)', text)
    failed = re.search(r'number of failed transactions: (\d+)', text)
    if not tps or not latency or not failed:
        raise ValueError('incomplete pgbench result')
    result = {'tps': float(tps[1]), 'latency_ms': float(latency[1]), 'failed_transactions': int(failed[1])}
    validate_samples([result], count=1)
    return result


def validate_samples(samples, count=3):
    if not isinstance(samples, list) or len(samples) != count:
        raise ValueError('incorrect pgbench sample count')
    for sample in samples:
        if sample.get('failed_transactions') != 0:
            raise ValueError('pgbench reported failed transactions')
        for key in ('tps', 'latency_ms'):
            value = sample.get(key)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('invalid pgbench metric: ' + key)


def container_errors(observed, image_id, datadir):
    errors = []
    if not observed or observed.get('image') != image_id:
        errors.append('actual PostgreSQL container image was not observed or changed')
    if not observed or observed.get('nano_cpus') != 1_000_000_000 or observed.get('memory_bytes') != 512 * 1024**2:
        errors.append('actual PostgreSQL CPU/RAM limits differ')
    mounts = observed.get('mounts', []) if observed else []
    if not any(m.get('Type') == 'bind' and m.get('Destination') == '/var/lib/postgresql/data'
               and pathlib.Path(m.get('Source', '')).resolve() == datadir.resolve() for m in mounts):
        errors.append('PostgreSQL datadir is not the fresh owned fixture')
    return errors


def validate_helper(report, label, work, options, hashes, image_id, container):
    errors = []
    engine = 'infinidisk2' if label in ('baseline', 'astra') else 'zerofs'
    binary_label = 'baseline' if label == 'baseline' else 'astra'
    legacy = label == 'baseline'
    expected_options = options if engine == 'infinidisk2' else {}
    if report.get('complete') is not True:
        errors.append('comparison helper did not complete')
    if report.get('binary_sha256') != {'infinidisk2': hashes[binary_label], 'zerofs': hashes['zerofs']}:
        errors.append('comparison binary SHA256 differs')
    execution = (report.get('executions') or [{}])[-1]
    if execution.get('phase') != 'postgres' or execution.get('script_sha256') != hashes['helper']:
        errors.append('comparison was not the frozen fresh PostgreSQL helper')
    if report.get('requested_engine_options') != expected_options:
        errors.append('requested engine options differ')
    if report.get('legacy_config') is not legacy or execution.get('legacy_config') is not legacy:
        errors.append('legacy configuration mode differs from the selected binary')
    if execution.get('binary_sha256') != hashes[binary_label]:
        errors.append('helper execution used a different InfiniDisk2 binary')
    prefix = 's3://testperf-6czebk/infinidisk2-' + work.name + '/' + engine
    if report.get('work') != str(work) or report.get('device') != '/dev/nbd31' or report.get('prefixes', {}).get(engine) != prefix:
        errors.append('comparison did not use the expected isolated fixture')
    entry = report.get('postgres', {}).get(engine, {})
    if any(entry.get(key) != value for key, value in {
        'scale': 2, 'clients': 4, 'cpu_limit': 1, 'memory_mib': 512, 'settings': ['on'] * 4,
        'database_SIGKILL_recovery': 'passed',
    }.items()):
        errors.append('PostgreSQL protocol or helper integrity checks differ')
    try:
        validate_samples(entry.get('samples'))
    except ValueError as error:
        errors.append(str(error))
    config = tomllib.loads(read_regular(work / (engine + '.toml')))
    if engine == 'infinidisk2':
        starts = report.get('engine_starts', {})
        if not starts or any(start.get('binary_sha256') != hashes[binary_label] or start.get('options') != options
                             or start.get('legacy_config') is not legacy
                             for start in starts.values()):
            errors.append('InfiniDisk2 effective startup options/SHA/legacy mode differ from the selected profile')
        if any(config.get(key) != value for key, value in options.items()):
            errors.append('InfiniDisk2 actual configuration differs from the selected profile')
    elif (config.get('filesystem', {}).get('ignore_fsync') is not False
          or config.get('cache', {}).get('memory_size_gb') != 64 / 1024
          or config.get('cache', {}).get('disk_size_gb') != 128 / 1024):
        errors.append('ZeroFS durability/cache configuration differs')
    errors.extend(container_errors(container, image_id, work / 'mount/postgres'))
    return errors


def inspect_container(name):
    result = subprocess.run(['docker', 'inspect', '--format', INSPECT_FORMAT, name],
                            text=True, capture_output=True, timeout=10)
    return json.loads(result.stdout) if result.returncode == 0 else None


class Campaign:
    def __init__(self, args, options):
        self.args, self.options = args, options
        self.binary = args.binary.resolve()
        self.baseline_binary = args.baseline.resolve()
        self.baseline_options = baseline_options(options)
        self.zerofs = pathlib.Path('/usr/local/bin/zerofs').resolve()
        self.run_id = uuid.uuid4().hex[:12]
        self.work = ROOT / 'test-output' / ('astra-postgres-' + self.run_id)
        self.work.mkdir(mode=0o700)
        self.redactor = Redactor.from_credentials(CREDENTIALS)
        self.hashes = {'astra': digest(self.binary), 'baseline': digest(self.baseline_binary),
                       'zerofs': digest(self.zerofs), 'helper': digest(HELPER)}
        if self.hashes['astra'] != args.expected_binary_sha256:
            raise RuntimeError('Astra binary changed after the initial SHA256 check')
        if self.hashes['baseline'] != args.expected_baseline_sha256:
            raise RuntimeError('pre-Astra binary changed after the initial SHA256 check')
        sources = [HELPER, PROFILE, pathlib.Path(__file__).resolve(), ROOT / 'scripts/run_astra_recovery.py',
                   ROOT / 'scripts/validate_vm.py', MYSQL_CAMPAIGN]
        self.sources = {str(path.relative_to(ROOT)): digest(path) for path in sources}
        self.native_name = 'id2-astra-pg-native-' + self.run_id
        self.native_running = False
        self.helper_work = {}
        self.image = self.image_identity()
        self.report = {'schema': 'infinidisk2.astra.postgres-comparison.v1', 'id': self.run_id,
                       'started_utc': utc(), 'work': str(self.work), 'complete': False,
                       'binary_sha256': self.hashes, 'source_files_sha256': self.sources,
                       'binary_paths': {'astra': str(self.binary), 'baseline': str(self.baseline_binary), 'zerofs': str(self.zerofs)},
                       'image': self.image, 'protocol': protocol(options, self.baseline_options), 'contracts': CONTRACTS,
                       'series': {}, 'stages': [], 'source_snapshot_is_build_attestation': False}
        atomic_json(self.work / 'engine-options.json', options)
        atomic_json(self.work / 'baseline-options.json', self.baseline_options)
        self.save()

    def save(self):
        atomic_json(self.work / 'report.json', self.redactor.object(self.report))

    def image_identity(self):
        result = subprocess.run(['docker', 'image', 'inspect', IMAGE], check=True,
                                text=True, capture_output=True, timeout=30)
        image = json.loads(result.stdout)[0]
        return {key: image.get(key) for key in ('Id', 'RepoDigests', 'Architecture', 'Os', 'Created')}

    def preflight(self):
        if (digest(self.binary) != self.hashes['astra'] or digest(self.zerofs) != self.hashes['zerofs']
                or digest(self.baseline_binary) != self.hashes['baseline']):
            raise RuntimeError('a frozen binary changed during the campaign')
        for name, expected in self.sources.items():
            if digest(ROOT / name) != expected:
                raise RuntimeError('campaign source changed: ' + name)
        if self.image_identity()['Id'] != self.image['Id']:
            raise RuntimeError('postgres:16 changed during the campaign')
        if not pathlib.Path('/dev/nbd31').exists() or pathlib.Path('/sys/class/block/nbd31/pid').exists():
            raise RuntimeError('reserved nbd31 is unavailable')
        if pathlib.Path('/dev/ublkc31').exists() or pathlib.Path('/sys/class/block/ublkb31').exists():
            raise RuntimeError('reserved ublk31 is occupied')
        for port in (11990, 11991, 12991):
            with socket.socket() as check:
                # Closed NBD connections can leave this port in TIME_WAIT.
                # A listening server still conflicts with this bind.
                check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                check.bind(('127.0.0.1', port))
        for proc in pathlib.Path('/proc').glob('[0-9]*'):
            if int(proc.name) == os.getpid():
                continue
            try:
                state = (proc / 'stat').read_text().rsplit(')', 1)[1].split()[0]
                comm = (proc / 'comm').read_text().strip()
                argv = (proc / 'cmdline').read_bytes().decode(errors='replace').split('\x00')
            except FileNotFoundError:
                continue
            if state != 'Z' and competing_test_process(comm, argv):
                raise RuntimeError(f'competing test or compiler active (PID {proc.name}, {comm})')

    def command(self, argv, label, timeout=300, check=True):
        with (self.work / (label + '.log')).open('w') as output:
            process = subprocess.Popen(list(map(str, argv)), stdout=output, stderr=subprocess.STDOUT,
                                       stdin=subprocess.DEVNULL, start_new_session=True)
            try:
                process.wait(timeout=timeout)
            except BaseException:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
        text = read_regular(self.work / (label + '.log'))
        if check and process.returncode:
            raise RuntimeError(label + ' failed; inspect the private controller log')
        return subprocess.CompletedProcess(argv, process.returncode, text)

    def native(self):
        datadir = self.work / 'native-data'
        datadir.mkdir()
        settings_query = 'SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes; SHOW data_checksums;'
        self.command(['findmnt', '-no', 'SOURCE,FSTYPE,OPTIONS', '--target', datadir], 'native-filesystem')
        # Set before docker run: cleanup also covers a daemon-side start whose
        # CLI response was interrupted. The container name belongs to this UUID.
        self.native_running = True
        self.command(['docker', 'run', '-d', '--name', self.native_name, '--network', 'none',
                      '--memory', '512m', '--cpus', '1', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust',
                      '-e', 'POSTGRES_INITDB_ARGS=--data-checksums', '-v',
                      str(datadir) + ':/var/lib/postgresql/data', self.image['Id']], 'native-start', timeout=600)
        container = inspect_container(self.native_name)
        errors = container_errors(container, self.image['Id'], datadir)
        if errors:
            raise RuntimeError('; '.join(errors))
        deadline = time.monotonic() + 450
        while time.monotonic() < deadline:
            result = self.command(['docker', 'exec', self.native_name, 'sh', '-c',
                                   '[ "$(cat /proc/1/comm)" = postgres ] && pg_isready -U postgres'],
                                  'native-ready', timeout=30, check=False)
            if result.returncode == 0:
                break
            time.sleep(.5)
        else:
            raise RuntimeError('native PostgreSQL startup timed out')
        settings = self.command(['docker', 'exec', self.native_name, 'psql', '-U', 'postgres', '-Atc', settings_query],
                                'native-settings').stdout.strip().splitlines()
        if settings != ['on'] * 4:
            raise RuntimeError('native PostgreSQL durability/checksums are not enabled')
        self.command(['docker', 'exec', self.native_name, 'pgbench', '-U', 'postgres', '-i', '-s', '2', 'postgres'],
                     'native-init', timeout=600)
        samples = []
        for iteration in range(3):
            result = self.command(['docker', 'exec', self.native_name, 'pgbench', '-U', 'postgres', '-c', '4',
                                   '-j', '4', '-T', '15', 'postgres'], 'native-pgbench-' + str(iteration))
            samples.append(parse_pgbench(result.stdout))
        query = ('SELECT (SELECT sum(abalance) FROM pgbench_accounts)=(SELECT sum(bbalance) FROM pgbench_branches) '
                 'AND (SELECT sum(abalance) FROM pgbench_accounts)=(SELECT sum(tbalance) FROM pgbench_tellers);')
        if self.command(['docker', 'exec', self.native_name, 'psql', '-U', 'postgres', '-Atc', query],
                        'native-sums').stdout.strip() != 't':
            raise RuntimeError('native PostgreSQL transaction sums differ')
        self.command(['docker', 'exec', self.native_name, 'pg_amcheck', '-U', 'postgres', '--database', 'postgres',
                      '--install-missing'], 'native-amcheck', timeout=600)
        self.command(['docker', 'stop', '-t', '120', self.native_name], 'native-stop', timeout=150)
        self.command(['docker', 'rm', self.native_name], 'native-remove')
        self.native_running = False
        return {'settings': settings, 'samples': samples, 'scale': 2, 'clients': 4, 'cpu_limit': 1,
                'memory_mib': 512, 'container': container, 'integrity': 'sums and pg_amcheck passed',
                'database_SIGKILL_recovery': 'not_run: comparison only; helper crash checks are separate evidence'}

    def comparison(self, label):
        engine = 'infinidisk2' if label in ('baseline', 'astra') else 'zerofs'
        binary = self.baseline_binary if label == 'baseline' else self.binary
        options = self.baseline_options if label == 'baseline' else self.options
        argv = [sys.executable, str(HELPER), '--postgres-only', '--engine', engine, '--binary', str(binary)]
        if engine == 'infinidisk2':
            argv.extend(['--engine-options', str(self.work / ('baseline-options.json' if label == 'baseline' else 'engine-options.json'))])
        if label == 'baseline':
            argv.append('--legacy-config')
        before = set((ROOT / 'test-output').glob('comparison-*'))
        log = self.work / (label + '-runner.log')
        container = None
        owned_work = None
        with log.open('x') as output:
            child = subprocess.Popen(argv, cwd=ROOT, stdin=subprocess.DEVNULL, stdout=output,
                                     stderr=subprocess.STDOUT, start_new_session=True)
            started, next_notice = time.monotonic(), time.monotonic() + 30
            try:
                while child.poll() is None:
                    candidates = set((ROOT / 'test-output').glob('comparison-*')) - before
                    if len(candidates) > 1:
                        raise RuntimeError('ambiguous fresh comparison fixtures; another campaign may be active')
                    if candidates and owned_work is None:
                        candidate = next(iter(candidates))
                        if not re.fullmatch(r'comparison-[0-9a-f]{12}', candidate.name) or candidate.is_symlink():
                            raise RuntimeError('unexpected comparison fixture path')
                        owned_work = candidate
                        self.helper_work[label] = owned_work
                    if owned_work and container is None and (owned_work / (engine + '-pg-start.log')).exists():
                        container = inspect_container('id2-compare-pg-' + owned_work.name.removeprefix('comparison-'))
                    now = time.monotonic()
                    if now - started > self.args.stage_timeout:
                        raise TimeoutError('PostgreSQL comparison stage exceeded its time limit')
                    if now >= next_notice:
                        print(f'RUNNING {label}: {int(now - started)} s', flush=True)
                        next_notice = now + 30
                    time.sleep(.5)
            except BaseException:
                child.send_signal(signal.SIGINT)  # helper finally cleans up its own fresh fixture
                try:
                    child.wait(timeout=360)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
                    raise RuntimeError('helper cleanup exceeded 360 s; inspect its private fixture before reusing the VM')
                raise
        if not owned_work:
            raise RuntimeError('comparison exited without an identified fresh fixture')
        result_path = owned_work / 'report.json'
        result = json.loads(read_regular(result_path)) if result_path.exists() else {}
        errors = validate_helper(result, label, owned_work, options, self.hashes, self.image['Id'], container)
        if child.returncode:
            errors.append('comparison process exited with code ' + str(child.returncode))
        if errors:
            raise RuntimeError('; '.join(errors))
        entry = dict(result['postgres'][engine])
        entry.update(container=container, source_work=str(owned_work),
                     binary_sha256=self.hashes[label if engine == 'infinidisk2' else 'zerofs'],
                     engine_options=options if engine == 'infinidisk2' else {},
                     legacy_config=label == 'baseline', engine_starts=result.get('engine_starts', {}),
                     source_report=self.run_id + '/' + label + '/report.json')
        return entry

    def execute(self):
        for label in ORDER:
            self.preflight()
            stage_sha = None if label == 'native' else self.hashes[label if label in ('astra', 'baseline') else 'zerofs']
            stage = {'label': label, 'started_utc': utc(), 'complete': False, 'binary_sha256': stage_sha}
            self.report['stages'].append(stage)
            self.save()
            print('STAGE ' + label + ' binary_sha256=' + str(stage_sha), flush=True)
            entry = self.native() if label == 'native' else self.comparison(label)
            self.preflight()
            validate_samples(entry['samples'])
            entry.update(contract=CONTRACTS[label], median_tps=statistics.median(s['tps'] for s in entry['samples']),
                         median_mean_latency_ms=statistics.median(s['latency_ms'] for s in entry['samples']))
            self.report['series'][label] = entry
            stage.update(finished_utc=utc(), complete=True)
            self.save()
        series = self.report['series']
        self.report['ratios'] = {name: series['astra']['median_tps'] / series[target]['median_tps']
                                for name, target in [('astra_over_baseline_tps', 'baseline'),
                                                     ('astra_over_native_tps', 'native'),
                                                     ('astra_over_zerofs_durable_tps', 'zerofs-durable')]}
        self.report['ratios']['baseline_over_astra_mean_latency'] = (
            series['baseline']['median_mean_latency_ms'] / series['astra']['median_mean_latency_ms'])
        self.report['complete'] = True

    def cleanup(self):
        if self.native_running:
            result = self.command(['docker', 'rm', '-f', self.native_name], 'native-cleanup', timeout=180, check=False)
            remains = inspect_container(self.native_name) is not None
            self.report['native_cleanup'] = {'returncode': result.returncode, 'container_remains': remains}
            if remains:
                self.report['complete'] = False
            self.native_running = remains
        self.report['finished_utc'] = utc()
        self.save()

    def export(self):
        OUT.mkdir(parents=True, exist_ok=True)
        pending = OUT / ('.pending-' + self.run_id)
        pending.mkdir(mode=0o700)
        files = []

        def write(relative, contents):
            target = pending / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(self.redactor.text(contents))
            files.append({'name': relative, 'sha256': digest(target), 'bytes': target.stat().st_size})

        write('report.json', json.dumps(self.redactor.object(self.report), indent=2) + '\n')
        write('engine-options.json', json.dumps(self.options, indent=2) + '\n')
        write('baseline-options.json', json.dumps(self.baseline_options, indent=2) + '\n')
        for path in sorted(self.work.iterdir()):
            if NATIVE_LOG.fullmatch(path.name) or path.name in ('baseline-runner.log', 'astra-runner.log', 'zerofs-durable-runner.log'):
                write(path.name, read_regular(path))
        for label, work in self.helper_work.items():
            for path in sorted(work.iterdir()):
                if path.name == 'report.json':
                    write(label + '/report.json', json.dumps(self.redactor.object(json.loads(read_regular(path))), indent=2) + '\n')
                elif HELPER_LOG.fullmatch(path.name):
                    write(label + '/' + path.name, read_regular(path))
        atomic_json(pending / 'manifest.json', {
            'schema': 'infinidisk2.astra.postgres-evidence.v1', 'id': self.run_id,
            'complete': self.report['complete'], 'binary_sha256': self.hashes,
            'source_files_sha256': self.sources, 'files': files,
            'export_policy': 'Allowlisted bounded regular UTF-8 logs/JSON only; redacted credentials/signatures; '
                             'no database, WAL, cache, raw config, .secret, binary, symlink, or recursive fixture copy.',
        })
        destination = OUT / self.run_id
        pending.rename(destination)
        canonical = self.redactor.object(self.report)
        canonical.update(source_report=self.run_id + '/report.json', source_manifest=self.run_id + '/manifest.json')
        atomic_json(OUT / 'report.json', canonical)
        print('EXPORTED ' + str(OUT / 'report.json'), flush=True)


def self_test():
    global OUT
    # Reusing a closed connection's address must not permit a live listener.
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('127.0.0.1', 0))
        server.listen()
        address = server.getsockname()
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(address)
            except OSError as error:
                assert error.errno == 98, 'expected EADDRINUSE for an actual listener'
            else:
                raise AssertionError('preflight accepted a live listener')
        with socket.create_connection(address) as client:
            accepted, _ = server.accept()
            accepted.close()
            assert client.recv(1) == b''
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(address)
    for comm, argv in [('infinidisk2', ['infinidisk2', '-c', '/opt/elestio/infinidisk/bench.toml', 'serve']),
                       ('zerofs', ['zerofs', '-c', '/opt/elestio/infinidisk/data.toml']),
                       ('nbd-client', ['nbd-client', '/dev/nbd0']), ('nbd-client', ['nbd-client', '/dev/nbd1']),
                       ('mysqld', ['mysqld']), ('redis-server', ['redis-server']),
                       ('docker', ['docker', 'start', 'app-wordpress-1', 'phpmyadmin', 'database', 'redis', 'elestio'])]:
        assert not competing_test_process(comm, argv), 'production process must remain allowed'
    for comm, argv in [('cargo', ['cargo', 'build']), ('sysbench', ['sysbench']), ('fio', ['fio']),
                       ('python3', ['python3', '/root/infinidisk2/scripts/run_astra_mysql.py'])]:
        assert competing_test_process(comm, argv), 'concurrent benchmark/build must be detected'
    options = load_profiles()['core']
    baseline = baseline_options(options)
    assert not options['aligned_wal'] and not options['generation_mode']
    assert protocol(options)['astra_options'] == options
    assert protocol(options)['baseline_options'] == baseline
    assert protocol(options)['measurement_window_seconds'] == 180
    assert ORDER == ('native', 'baseline', 'astra', 'zerofs-durable')
    assert all(options[key] == value for key, value in baseline.items())
    assert all(baseline[key] for key in ('logical_cache', 'wal_fixed_size', 'wal_commit_records',
                                       'wal_writev', 'sync_data_only'))
    assert {key for key, change in option_delta(baseline, options).items() if change['astra'] is True} == {
        'async_cache', 'checkpoint_pipeline', 'compact_checkpoints', 'fast_local_reads', 'paged_index', 'selective_sync'}
    for bad_options in ({**options, 'wal_writev': False}, {**options, 'aligned_wal': True},
                        {**options, 'generation_mode': True}):
        try:
            baseline_options(bad_options)
        except RuntimeError:
            pass
        else:
            raise AssertionError('invalid before/after PostgreSQL configuration accepted')
    valid = 'tps = 50.12 (without initial connection time)\nlatency average = 79.8 ms\nnumber of failed transactions: 0 (0.000%)\n'
    assert parse_pgbench(valid)['tps'] == 50.12
    for bad in (valid.replace('transactions: 0', 'transactions: 1'), valid.replace('latency average', 'latency p99'), ''):
        try:
            parse_pgbench(bad)
        except ValueError:
            pass
        else:
            raise AssertionError('invalid pgbench result accepted')
    assert HELPER_LOG.fullmatch('zerofs-pgbench-2.log')
    assert not any(HELPER_LOG.fullmatch(name) or NATIVE_LOG.fullmatch(name)
                   for name in ('password.secret', 'database.log', 'wal.wal', 'volume.toml', '../host.log'))
    temporary_root = ROOT / 'test-output/astra-tmp'
    temporary_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=temporary_root) as temporary:
        work = pathlib.Path(temporary) / 'comparison-0123456789ab'
        work.mkdir()
        hashes = {'astra': 'a' * 64, 'baseline': 'd' * 64, 'zerofs': 'b' * 64, 'helper': 'c' * 64}
        container = {'image': 'sha256:image', 'nano_cpus': 1_000_000_000, 'memory_bytes': 512 * 1024**2,
                     'mounts': [{'Type': 'bind', 'Source': str(work / 'mount/postgres'), 'Destination': '/var/lib/postgresql/data'}]}
        report = {'complete': True, 'work': str(work), 'device': '/dev/nbd31',
                  'binary_sha256': {'infinidisk2': hashes['astra'], 'zerofs': hashes['zerofs']},
                  'executions': [{'phase': 'postgres', 'script_sha256': hashes['helper'],
                                  'binary_sha256': hashes['astra'], 'legacy_config': False}],
                  'legacy_config': False,
                  'requested_engine_options': options, 'engine_starts': {'initial': {
                      'binary_sha256': hashes['astra'], 'options': options, 'legacy_config': False}},
                  'prefixes': {'infinidisk2': 's3://testperf-6czebk/infinidisk2-comparison-0123456789ab/infinidisk2'},
                  'postgres': {'infinidisk2': {'scale': 2, 'clients': 4, 'cpu_limit': 1, 'memory_mib': 512,
                                             'settings': ['on'] * 4, 'database_SIGKILL_recovery': 'passed',
                                             'samples': [parse_pgbench(valid)] * 3}}}
        (work / 'infinidisk2.toml').write_text('\n'.join(key + ' = ' + json.dumps(value) for key, value in options.items()))
        assert not validate_helper(report, 'astra', work, options, hashes, 'sha256:image', container)
        assert validate_helper(report, 'astra', work, options, {**hashes, 'astra': 'd' * 64}, 'sha256:image', container)
        baseline_report = json.loads(json.dumps(report))
        baseline_report['binary_sha256']['infinidisk2'] = hashes['baseline']
        baseline_report['legacy_config'] = True
        baseline_report['executions'][0].update(binary_sha256=hashes['baseline'], legacy_config=True)
        baseline_report['requested_engine_options'] = baseline
        baseline_report['engine_starts']['initial'].update(binary_sha256=hashes['baseline'], options=baseline, legacy_config=True)
        (work / 'infinidisk2.toml').write_text('\n'.join(key + ' = ' + json.dumps(value) for key, value in baseline.items()))
        assert not validate_helper(baseline_report, 'baseline', work, baseline, hashes, 'sha256:image', container)
        assert validate_helper(baseline_report, 'baseline', work, baseline,
                               {**hashes, 'baseline': hashes['astra']}, 'sha256:image', container)
        wrong_mode = json.loads(json.dumps(baseline_report))
        wrong_mode['engine_starts']['initial']['legacy_config'] = False
        assert validate_helper(wrong_mode, 'baseline', work, baseline, hashes, 'sha256:image', container)
        wrong_options = json.loads(json.dumps(baseline_report))
        wrong_options['engine_starts']['initial']['options']['wal_commit_records'] = False
        assert validate_helper(wrong_options, 'baseline', work, baseline, hashes, 'sha256:image', container)
        assert container_errors({**container, 'image': 'other-image'}, 'sha256:image', work / 'mount/postgres')
        assert container_errors(container, 'sha256:image', work / 'somewhere-else')
        secret = 'postgres-test-secret'
        redactor = Redactor([secret])
        controller = pathlib.Path(temporary) / 'controller'
        controller.mkdir()
        (work / 'report.json').write_text(json.dumps(report))
        (work / 'infinidisk2-initial-server.log').write_text('diagnostic ' + secret)
        (work / 'password.secret').write_text(secret)
        (work / 'database.log').write_text(secret)
        campaign = object.__new__(Campaign)
        campaign.work, campaign.options, campaign.hashes = controller, options, hashes
        campaign.baseline_options = baseline
        campaign.sources, campaign.redactor = {}, redactor
        baseline_work = pathlib.Path(temporary) / 'comparison-fedcba987654'
        baseline_work.mkdir()
        (baseline_work / 'report.json').write_text(json.dumps(baseline_report))
        (baseline_work / 'infinidisk2-pg-crash.log').write_text('test crash\n')
        campaign.helper_work = {'astra': work, 'baseline': baseline_work}
        campaign.run_id = 'self-test'
        campaign.report = {'id': 'self-test', 'complete': False, 'diagnostic': secret}
        original_out = OUT
        OUT = pathlib.Path(temporary) / 'archive'
        try:
            campaign.export()
            canonical = json.loads((OUT / 'report.json').read_text())
            assert canonical['complete'] is False
            assert canonical['source_report'] == 'self-test/report.json'
            assert json.loads((OUT / 'self-test/baseline-options.json').read_text()) == baseline
            archived_baseline = json.loads((OUT / 'self-test/baseline/report.json').read_text())
            assert archived_baseline['binary_sha256']['infinidisk2'] == hashes['baseline']
            assert (OUT / 'self-test/baseline/infinidisk2-pg-crash.log').is_file()
            manifest = json.loads((OUT / canonical['source_manifest']).read_text())
            for entry in manifest['files']:
                file = OUT / 'self-test' / entry['name']
                assert digest(file) == entry['sha256']
                assert secret not in file.read_text()
            assert not list(OUT.rglob('*.secret')) and not list(OUT.rglob('database.log'))
            (work / 'infinidisk2-pgbench-0.log').symlink_to(work / 'password.secret')
            campaign.run_id = 'second-attempt'
            try:
                campaign.export()
            except ValueError:
                pass
            else:
                raise AssertionError('allowed-name symlink escaped the export policy')
            assert json.loads((OUT / 'report.json').read_text()) == canonical
            assert (OUT / 'self-test/report.json').is_file()
        finally:
            OUT = original_out
    print('PostgreSQL comparison self-test passed: matched baseline/Astra options, old optimizations retained, parser, protocol/SHA/legacy/image/path rejection, separate baseline evidence, redacted manifest export and symlink refusal; no VM touched.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=pathlib.Path, default=ROOT / 'target/release/infinidisk2-astra')
    parser.add_argument('--expected-binary-sha256')
    parser.add_argument('--baseline', type=pathlib.Path, default=ROOT / 'target/release/infinidisk2-pre-astra')
    parser.add_argument('--expected-baseline-sha256')
    parser.add_argument('--stage-timeout', type=int, default=5400)
    parser.add_argument('--plan-only', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    options = load_profiles()['core']
    if args.plan_only:
        print(json.dumps({'protocol': protocol(options), 'contracts': CONTRACTS, 'helper': str(HELPER),
                          'output': str(OUT / 'report.json'), 'required_binary_sha256': True,
                          'required_baseline_sha256': True,
                          'credentials_path_fixed_by_helper': str(CREDENTIALS), 'starts_no_process': True}, indent=2))
        return 0
    for value, flag in [(args.expected_binary_sha256, '--expected-binary-sha256'),
                        (args.expected_baseline_sha256, '--expected-baseline-sha256')]:
        if not value or not re.fullmatch(r'[0-9a-f]{64}', value):
            parser.error('execution requires ' + flag + ' from the frozen binary')
    if not 60 <= args.stage_timeout <= 10800:
        parser.error('stage timeout must be 60..10800 seconds')
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK) or digest(args.binary) != args.expected_binary_sha256:
        parser.error('Astra binary is missing, not executable or has a different SHA256')
    if not args.baseline.is_file() or not os.access(args.baseline, os.X_OK) or digest(args.baseline) != args.expected_baseline_sha256:
        parser.error('pre-Astra binary is missing, not executable or has a different SHA256')
    if args.expected_baseline_sha256 == args.expected_binary_sha256:
        parser.error('pre-Astra and final Astra must be distinct frozen binaries')
    if os.geteuid() != 0:
        parser.error('execution requires root on the reserved test VM')
    for tool in ('docker', 'zerofs', 'fio', 'mount', 'umount', 'mountpoint', 'mkfs.ext4', 'findmnt'):
        if not shutil.which(tool):
            parser.error('missing comparison dependency: ' + tool)
    if pathlib.Path(shutil.which('zerofs')).resolve() != pathlib.Path('/usr/local/bin/zerofs').resolve():
        parser.error('ZeroFS PATH executable differs from the binary hashed by compare_zerofs.py')
    (ROOT / 'test-output').mkdir(exist_ok=True)
    locks = []
    for path in (ROOT / 'test-output/astra-postgres.lock', ROOT / 'test-output/astra-recovery.lock', OUT.parent / 'mysql/campaign.lock'):
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    campaign = Campaign(args, options)
    try:
        campaign.execute()
    except BaseException as error:
        campaign.report['complete'] = False
        campaign.report['error'] = campaign.redactor.text(type(error).__name__ + ': ' + str(error))
        raise
    finally:
        try:
            campaign.cleanup()
        except BaseException as error:
            campaign.report['complete'] = False
            campaign.report['cleanup_error'] = campaign.redactor.text(type(error).__name__ + ': ' + str(error))
            campaign.save()
            raise
        finally:
            campaign.export()
    return 0 if campaign.report['complete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
