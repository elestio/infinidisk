#!/usr/bin/env python3
"""Dedicated S3/MySQL generation-mode benchmark and complete-application crashes.

Run only on the coordinated test VM. Uses a new 2 GiB volume, unique S3 prefix,
/dev/nbd31 and port 11990; refuses an occupied device/port. Never resumes or
reformats an existing fixture. --self-test needs neither root nor the VM.
"""
import argparse
import ast
import datetime
import fcntl
import hashlib
import json
import os
import pathlib
import random
import re
import resource
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import tomllib
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
DEVICE = pathlib.Path('/dev/nbd31')
PIDFILE = pathlib.Path('/sys/class/block/nbd31/pid')
PORT = 11990
LIVE_FIRST_ID = 2000000
LIVE_TRANSACTIONS = 20000


def competing_test_process(comm, argv):
    # Production storage, database and Docker processes are deliberately absent.
    if comm in {'cargo', 'rustc', 'cc1', 'ld.lld', 'fio', 'sysbench', 'pgbench'}:
        return True
    controllers = {'run_astra_mysql.py', 'run_astra_recovery.py', 'run_astra_fio_compare.py',
                   'run_astra_postgres_compare.py', 'validate_generation.py', 'validate_vm.py',
                   'compare_zerofs.py', 'measure_astra_warm.py', 'run_breakthroughs.py',
                   'run_fixed_wal.py', 'probe_fsync_layout.py'}
    return any(pathlib.Path(argument).name in controllers for argument in argv)


def check_test_slot():
    if not DEVICE.exists() or PIDFILE.exists():
        raise RuntimeError('reserved nbd31 is unavailable')
    if pathlib.Path('/dev/ublkc31').exists() or pathlib.Path('/sys/class/block/ublkb31').exists():
        raise RuntimeError('reserved ublk31 is occupied')
    for port in (11990, 11991, 12991):
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(('127.0.0.1', port))
            except OSError as error:
                raise RuntimeError(f'reserved test port {port} is occupied') from error
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


def comparison_helpers():
    """Reuse the established parsers without executing its top-level campaign."""
    names = {'parse_sysbench_sample', 'process_snapshot', 'process_window',
             'cgroup_cpu_snapshot', 'cgroup_cpu_window',
             'host_cpu_snapshot', 'host_cpu_window'}
    tree = ast.parse((ROOT / 'scripts/compare_zerofs.py').read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in nodes} != names:
        raise RuntimeError('comparison helper API changed; inspect before running')
    namespace = {'re': re, 'os': os, 'time': time, 'pathlib': pathlib}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'comparison-helpers', 'exec'), namespace)
    return {name: namespace[name] for name in names}


def read_credentials(path, inherited):
    env = inherited.copy()
    allowed = {'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN'}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.removeprefix('export ').split('=', 1)
        if key.strip() in allowed:
            values = shlex.split(value, comments=True)
            if len(values) != 1:
                raise ValueError('invalid credential field: ' + key.strip())
            env[key.strip()] = values[0]
    if not all(env.get(key) for key in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY')):
        raise ValueError('AWS credentials are missing')
    return env


def parse_head(output):
    # status is a pretty-printed JSON object; do not accept a log fragment as HEAD.
    head = json.loads(output.strip())
    if head.get('format') != 2 or not isinstance(head.get('seq'), int):
        raise RuntimeError('expected a verified format-2 HEAD')
    return head


def status_records(text):
    decoder = json.JSONDecoder()
    records = []
    for match in re.finditer(r'\{"volume"', text):
        try:
            record, _ = decoder.raw_decode(text[match.start():])
            records.append(record)
        except json.JSONDecodeError:
            # A process may still be completing the last log line while polled.
            continue
    return records


def same_root(left, right):
    return all(left[key] == right[key] for key in ('volume', 'seq', 'generation', 'writer', 'shards'))


def acknowledged_transfers(output):
    # The writer can be halfway through its last line when we inspect this log.
    ids = [int(match[1]) for match in re.finditer(r'^ACK:(\d+)\r?\n', output, re.M)]
    if len(ids) > LIVE_TRANSACTIONS or any(value != LIVE_FIRST_ID + position for position, value in enumerate(ids)):
        raise RuntimeError('transfer acknowledgements are not one contiguous committed prefix')
    return {'count': len(ids), 'last_id': ids[-1] if ids else None}


def validate_live_invariants(lines):
    if len(lines) != 2:
        raise RuntimeError('live checkpoint transaction invariants are missing')
    a, b, total = map(int, lines[0].split('\t'))
    count, minimum, maximum, outside = map(int, lines[1].split('\t'))
    transfers = count - 1  # Marker zero belongs to the clean initial baseline.
    if (not 1 <= transfers <= LIVE_TRANSACTIONS or minimum != 0 or outside != 0
            or maximum != LIVE_FIRST_ID + transfers - 1
            or (a, b, total) != (1000000 - transfers, 1000000 + transfers, 2000000)):
        raise RuntimeError('live checkpoint balances and journal are not the same complete transaction prefix')
    return {'recovered_transfers': transfers, 'last_id': maximum,
            'balances': [a, b], 'constant_sum': total, 'journal_is_contiguous': True}


def publish_canonical_report(run_directory):
    """Publish renderer-ready metrics while retaining the immutable run evidence."""
    report = json.loads((run_directory / 'report.json').read_text())
    if report['id'] != run_directory.name or not (run_directory / 'manifest.json').is_file():
        raise RuntimeError('cannot publish an incomplete or mismatched evidence export')
    report['source_report'] = run_directory.name + '/report.json'
    report['source_manifest'] = run_directory.name + '/manifest.json'
    destination = run_directory.parent / 'report.json'
    temporary = destination.with_name('report.json.' + run_directory.name + '.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(destination)
    return destination


class Campaign:
    def __init__(self, args):
        self.args = args
        self.binary = args.binary.resolve()
        self.helpers = comparison_helpers()
        self.env = read_credentials(args.credentials, os.environ)
        self.env['RUST_LOG'] = 'infinidisk=info,infinidisk2=info'
        self.run_id = uuid.uuid4().hex[:12]
        self.work = ROOT / 'test-output' / ('generation-' + self.run_id)
        self.work.mkdir(parents=True, mode=0o700)
        self.mountpoint = self.work / 'mount'
        self.mountpoint.mkdir()
        self.local_dir = self.work / 'local'
        self.config = self.work / 'volume.toml'
        self.name = 'id2-generation-mysql-' + self.run_id
        self.password = uuid.uuid4().hex
        self.env['MYSQL_ROOT_PASSWORD'] = self.password
        self.env['MYSQL_PWD'] = self.password
        self.store = f's3://{args.bucket}/infinidisk2-generation-{self.run_id}'
        self.server = self.client = self.workload = self.transfer = None
        self.server_log = None
        self.attached = self.mounted = self.mysql_running = False
        self.ordinal = 0
        self.write_config(paused=False)
        self.sysbench_secret = self.work / 'sysbench.secret'
        with os.fdopen(os.open(self.sysbench_secret, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as file:
            file.write('mysql-password=' + self.password + '\n')
        self.report = {
            'id': self.run_id, 'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'work': str(self.work), 'device': str(DEVICE), 'store': self.store,
            'binary_sha256': hashlib.sha256(self.binary.read_bytes()).hexdigest(),
            'script_sha256': hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
            'contract': 'FLUSH/FUA order writes; acknowledged transactions may disappear on complete restart to the last S3 HEAD. This is not local-fsync durability.',
            'benchmark': {'tables': 4, 'rows_per_table': 25000, 'threads': 8, 'cpu_limit': 1,
                          'mysql_memory_mib': 1024, 'mysql_buffer_pool_mib': 256,
                          'sample_count': args.samples, 'sample_seconds': args.seconds,
                          'warmup_seconds': args.warm_seconds, 'latency_percentile': 99,
                          'rand_type': 'special', 'samples': {}, 'checkpoint_seconds': 5,
                          'generation_max_lag_seconds': 30},
            'crashes': [], 'tests': {}, 'complete': False,
            'crash_control': 'After the benchmark and a clean published baseline, periodic publication is intentionally paused for 3600 s. Each application writes for only a few seconds before SIGKILL. This isolates coherent rollback; it does not measure the production RPO.',
            'live_checkpoint_control': 'A separate case restores 5 s periodic publication, keeps MySQL/ext4 and two transaction clients active through two newer HEAD publications, freezes only the engine, then kills engine and DB. Recovery checks a shared balance/journal transaction prefix; no graceful database stop precedes that HEAD.',
            'online_s3_outage_backpressure': 'not_run: no host firewall, credential or shared-service changes; deterministic lag backpressure is covered by tests/astra_engine.rs',
        }
        self.verify_binary()
        self.save()

    def verify_binary(self):
        if hashlib.sha256(self.binary.read_bytes()).hexdigest() != self.args.expected_binary_sha256:
            raise RuntimeError('InfiniDisk2 binary differs from the required frozen SHA256')

    def write_config(self, paused):
        values = {
            'local_dir': str(self.local_dir), 'store': self.store, 'endpoint': self.args.endpoint,
            'region': 'auto', 'listen': '127.0.0.1:11990',
            'checkpoint_seconds': 3600 if paused else 5,
            'generation_max_lag_seconds': 3600 if paused else 30,
            'memory_cache_mib': 1024, 'disk_cache_mib': 4096, 'hot_wal_mib': 0,
            'max_pending_mib': 1024, 'segment_mib': 8, 'max_index_mib': 64,
            'logical_cache': True, 'async_cache': True, 'cache_queue_mib': 16,
            'fast_local_reads': True, 'wal_commit_records': True, 'wal_fixed_size': True,
            'checkpoint_pipeline': True, 'selective_sync': True, 'aligned_wal': True,
            'compact_checkpoints': True, 'paged_index': True, 'generation_mode': True,
        }
        text = '\n'.join(key + ' = ' + json.dumps(value) for key, value in values.items()) + '\n'
        tomllib.loads(text)
        self.config.write_text(text)

    def save(self):
        destination = self.work / 'report.json'
        temporary = self.work / 'report.json.tmp'
        temporary.write_text(json.dumps(self.report, indent=2) + '\n')
        temporary.replace(destination)

    def redact(self, output):
        for key in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'MYSQL_ROOT_PASSWORD'):
            value = self.env.get(key)
            if value:
                output = output.replace(value, '[REDACTED]')
        return output

    def command(self, argv, label, timeout=180, check=True):
        self.ordinal += 1
        filename = self.work / f'{self.ordinal:04d}-{label}.log'
        if not label.endswith('ready'):
            print(label, flush=True)
        proc = subprocess.Popen(list(map(str, argv)), env=self.env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, start_new_session=True)
        try:
            output, _ = proc.communicate(timeout=timeout)
        except BaseException:
            os.killpg(proc.pid, signal.SIGKILL)
            output, _ = proc.communicate()
            filename.write_text(self.redact(output))
            raise
        output = self.redact(output)
        filename.write_text(output)
        if check and proc.returncode:
            raise RuntimeError(f'{label} failed ({proc.returncode}); see {filename}')
        return subprocess.CompletedProcess(argv, proc.returncode, output)

    def cli(self, *argv, label, timeout=180, check=True):
        return self.command([self.binary, '-c', self.config, *argv], label, timeout, check)

    def head(self, label):
        return parse_head(self.cli('status', label=label).stdout)

    def start_engine(self, label):
        self.verify_binary()
        if self.server is not None or self.client is not None or PIDFILE.exists():
            raise RuntimeError('device/process already occupied; refusing attachment')
        before = self.head(label + '-head-before-open')
        self.server_log = self.work / (label + '-server.log')
        with self.server_log.open('w') as log:
            self.server = subprocess.Popen([str(self.binary), '-c', str(self.config), 'serve'],
                                           env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                raise RuntimeError('engine startup failed; see ' + str(self.server_log))
            # Wait for the initial checkpoint tick before attaching/mounting.
            # During paused crash tests this ensures no unexpected first tick
            # can publish the acknowledged-loss witness after attachment.
            records = status_records(self.server_log.read_text())
            if records and records[-1]['sequence'] == before['seq'] and records[-1]['remote_sequence'] == before['seq']:
                with socket.create_connection(('127.0.0.1', PORT), timeout=1):
                    break
            time.sleep(.1)
        else:
            raise RuntimeError('engine initial checkpoint/status did not settle')
        with (self.work / (label + '-attach.log')).open('w') as log:
            self.client = subprocess.Popen([str(self.binary), '-c', str(self.config), 'attach',
                                            '--device', str(DEVICE), '--connections', '8'],
                                           env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
        self.attached = True
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.client.poll() is not None:
                raise RuntimeError('NBD attachment exited')
            if PIDFILE.exists():
                return
            time.sleep(.1)
        raise RuntimeError('NBD attachment timed out')

    def mount(self, label, fsck=False):
        if fsck:
            result = self.command(['e2fsck', '-f', '-p', DEVICE], label + '-fsck', check=False, timeout=300)
            if result.returncode not in (0, 1):
                raise RuntimeError('filesystem recovery required manual repair')
        self.command(['mount', '-o', 'noatime', DEVICE, self.mountpoint], label + '-mount')
        self.mounted = True

    def mysql_start(self, label):
        datadir = self.mountpoint / 'mysql'
        datadir.mkdir(exist_ok=True)
        self.command(['docker', 'run', '-d', '--name', self.name, '--network', 'none',
                      '--memory', '1g', '--cpus', '1', '-e', 'MYSQL_ROOT_PASSWORD',
                      '-v', str(datadir) + ':/var/lib/mysql', 'mysql:8.0',
                      '--socket=/var/lib/mysql/mysql.sock', '--innodb-buffer-pool-size=268435456',
                      '--innodb-redo-log-capacity=134217728', '--innodb-flush-log-at-trx-commit=1',
                      '--innodb-doublewrite=ON', '--innodb-flush-method=O_DIRECT',
                      '--log-bin=mysql-bin', '--sync-binlog=1', '--binlog-expire-logs-seconds=3600'],
                     label + '-mysql-start', timeout=180)
        self.mysql_running = True
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            result = self.sql('SELECT 1', label + '-mysql-ready', check=False)
            if result.returncode == 0:
                ready = self.command(['docker', 'exec', self.name, 'sh', '-c',
                                      '[ "$(cat /proc/1/comm)" = mysqld ]'], label + '-pid-ready', check=False)
                if ready.returncode == 0:
                    break
            time.sleep(.5)
        else:
            raise RuntimeError('MySQL did not become ready')
        settings = self.sql('SELECT @@innodb_flush_log_at_trx_commit,@@sync_binlog,@@innodb_doublewrite,@@innodb_flush_method,@@innodb_buffer_pool_size,@@version,@@log_bin', label + '-mysql-settings').stdout.strip().split('\t')
        if settings[:5] != ['1', '1', 'ON', 'O_DIRECT', '268435456'] or settings[-1] != '1':
            raise RuntimeError('unexpected MySQL durability or memory settings')
        return settings

    def sql(self, query, label, check=True, timeout=300):
        return self.command(['docker', 'exec', '-e', 'MYSQL_PWD', self.name, 'mysql',
                             '--socket=/var/lib/mysql/mysql.sock', '-uroot', '-NBe', query],
                            label, timeout=timeout, check=check)

    def bench_command(self, workload, seconds, seed=42, action='run'):
        return ['sysbench', '--config-file=' + str(self.sysbench_secret), '--db-driver=mysql',
                '--mysql-socket=' + str(self.mountpoint / 'mysql/mysql.sock'), '--mysql-user=root',
                '--mysql-db=bench', '--tables=4', '--table-size=25000', '--threads=8',
                '--rand-seed=' + str(seed), '--rand-type=special', '--percentile=99',
                '--time=' + str(seconds), '/usr/share/sysbench/oltp_' + workload + '.lua', action]

    def stop_mysql(self, label, crash=False):
        if self.mysql_running:
            if crash:
                self.command(['docker', 'kill', '--signal', 'KILL', self.name], label + '-mysql-kill', check=False, timeout=60)
            else:
                self.command(['docker', 'stop', '-t', '120', self.name], label + '-mysql-stop', timeout=150)
            self.command(['docker', 'rm', '-f', self.name], label + '-mysql-remove', timeout=90)
            self.mysql_running = False

    def unmount_detach(self, label, lazy=False):
        if self.mounted:
            self.command(['umount', *(['-l'] if lazy else []), self.mountpoint], label + '-unmount', timeout=90)
            self.mounted = False
        if self.attached:
            if PIDFILE.exists():
                self.cli('detach', '--device', DEVICE, label=label + '-detach', timeout=90)
            if self.client is not None:
                self.client.wait(timeout=90)
                self.client = None
            self.attached = False
        if PIDFILE.exists():
            raise RuntimeError('old kernel attachment still exists; refusing generation rollback')

    def kill_engine(self, label):
        if self.server is not None:
            pid = self.server.pid
            if self.server.poll() is None:
                self.server.kill()
            self.server.wait(timeout=30)
            result = {'pid': pid, 'returncode': self.server.returncode}
            self.server = None
            return result
        return None

    def stop_clean(self, label):
        self.stop_mysql(label)
        self.unmount_detach(label)
        if self.server is not None:
            self.server.send_signal(signal.SIGTERM)
            self.server.wait(timeout=180)
            if self.server.returncode:
                raise RuntimeError('clean shutdown did not publish the final generation')
            self.server = None

    def stop_without_publication(self, label):
        # Order is deliberate: application shutdown completes, then its mount is
        # removed, but the engine is killed instead of publishing recovery writes.
        self.stop_mysql(label)
        self.unmount_detach(label)
        self.kill_engine(label)

    def integrity(self, label, marker_must_be_absent=None, live=False):
        rows = self.sql(' UNION ALL '.join(f"SELECT 'sbtest{i}',COUNT(*) FROM bench.sbtest{i}" for i in range(1, 5)), label + '-row-counts').stdout.strip().splitlines()
        if len(rows) != 4 or any(int(row.split('\t')[1]) != 25000 for row in rows):
            raise RuntimeError('sysbench table row-count invariant failed')
        checks = self.sql('CHECK TABLE ' + ','.join('bench.sbtest' + str(i) for i in range(1, 5)) + ',bench.witness,bench.commits EXTENDED', label + '-check-tables', timeout=1800).stdout.strip().splitlines()
        if len(checks) != 6 or any(not row.endswith('\tOK') for row in checks):
            raise RuntimeError('CHECK TABLE failed')
        query = ('SELECT a,b,a+b FROM bench.witness WHERE id=1; SELECT COUNT(*),MIN(id),MAX(id)'
                 + (f',SUM(id<>0 AND id<{LIVE_FIRST_ID})' if live else '') + ' FROM bench.commits;')
        witness = self.sql(query, label + '-atomic-invariants').stdout.strip().splitlines()
        live_checks = validate_live_invariants(witness) if live else None
        if not live and witness != ['1000000\t1000000\t2000000', '1\t0\t0']:
            raise RuntimeError('recovered data is not the complete baseline transaction state')
        if marker_must_be_absent is not None:
            result = self.sql(f'SELECT COUNT(*) FROM bench.commits WHERE id={marker_must_be_absent}', label + '-lost-acknowledgement').stdout.strip()
            if result != '0':
                raise RuntimeError('controlled unpublished acknowledgement unexpectedly survived')
        checksum_settings = self.sql('SELECT @@innodb_checksum_algorithm,@@innodb_force_recovery',
                                     label + '-checksum-settings').stdout.strip().split('\t')
        if len(checksum_settings) != 2 or checksum_settings[0] not in ('crc32', 'strict_crc32') or checksum_settings[1] != '0':
            raise RuntimeError('page checksums must be enabled and forced InnoDB recovery disabled')
        result = {'rows': rows, 'checks': checks, 'transaction_invariants': witness,
                  'innodb_checksum_algorithm': checksum_settings[0], 'innodb_force_recovery': 0}
        if live:
            result['live_transaction_prefix'] = live_checks
        return result

    def benchmark(self):
        self.verify_binary()
        self.command(self.bench_command('read_only', self.args.warm_seconds), 'mysql-warm', timeout=180)
        mysql_pid = int(self.command(['docker', 'inspect', '--format', '{{.State.Pid}}', self.name], 'mysql-host-pid').stdout.strip())
        pids = {'engine': self.server.pid, 'mysql': mysql_pid, 'nbd_client': self.client.pid}
        sample_parser = self.helpers['parse_sysbench_sample']
        for workload in ('read_write', 'read_only', 'write_only'):
            samples = self.report['benchmark']['samples'].setdefault(workload, [])
            for iteration in range(self.args.samples):
                before = {name: self.helpers['process_snapshot'](pid) for name, pid in pids.items()}
                host_before = self.helpers['host_cpu_snapshot']()
                usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
                started = time.monotonic()
                result = self.command(self.bench_command(workload, self.args.seconds), f'mysql-{workload}-{iteration}', timeout=self.args.seconds + 180)
                elapsed = time.monotonic() - started
                usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
                after = {name: self.helpers['process_snapshot'](pid) for name, pid in pids.items()}
                sample = sample_parser(result.stdout, 99)
                user = usage_after.ru_utime - usage_before.ru_utime
                system = usage_after.ru_stime - usage_before.ru_stime
                sample['resources'] = {
                    'processes': {name: self.helpers['process_window'](before[name], after[name]) for name in pids},
                    'host_cpu': self.helpers['host_cpu_window'](host_before, self.helpers['host_cpu_snapshot']()),
                    'sysbench': {'window_seconds': elapsed, 'user_cpu_seconds': user, 'system_cpu_seconds': system},
                }
                samples.append(sample)
                self.save()
        self.verify_binary()

    def crash_case(self, seed, baseline):
        label = 'crash-' + str(seed)
        self.start_engine(label)
        self.mount(label, fsck=True)
        self.mysql_start(label)
        marker = seed + 10000
        with (self.work / (label + '-active-sysbench.log')).open('w') as log:
            self.workload = subprocess.Popen(self.bench_command('read_write', 120, seed=seed),
                                             env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                             start_new_session=True)
        time.sleep(.4)
        ack = self.sql(f'START TRANSACTION; UPDATE bench.witness SET a=a-1,b=b+1 WHERE id=1; INSERT INTO bench.commits VALUES ({marker}); COMMIT; SELECT id FROM bench.commits WHERE id={marker};', label + '-acknowledged-transaction').stdout.strip()
        if ack != str(marker):
            raise RuntimeError('lost-transaction witness was not acknowledged')
        acknowledged_at = time.monotonic()
        time.sleep(random.Random(seed).uniform(.3, 1.8))
        if self.workload.poll() is not None:
            raise RuntimeError('transaction workload exited before engine SIGKILL')
        before = self.head(label + '-head-at-crash')
        if not same_root(before, baseline):
            raise RuntimeError('controlled crash unexpectedly published a newer generation')
        if self.server is None or self.server.poll() is not None:
            raise RuntimeError('engine was not running at the planned crash point')
        engine_death = self.kill_engine(label)
        if engine_death['returncode'] != -signal.SIGKILL:
            raise RuntimeError('engine did not terminate through SIGKILL')
        ack_to_kill = time.monotonic() - acknowledged_at
        self.stop_mysql(label, crash=True)
        try:
            self.workload.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(self.workload.pid, signal.SIGKILL)
            self.workload.wait(timeout=10)
        self.workload = None
        self.unmount_detach(label, lazy=True)
        after = self.head(label + '-head-after-crash')
        if not same_root(before, after):
            raise RuntimeError('HEAD changed after process death')
        self.start_engine(label + '-recovery')
        self.mount(label + '-recovery', fsck=True)
        self.mysql_start(label + '-recovery')
        checks = self.integrity(label + '-recovery', marker_must_be_absent=marker)
        self.report['crashes'].append({'seed': seed, 'acknowledged_marker': marker,
                                      'marker_acknowledged_before_sigkill': True,
                                      'engine_death': engine_death,
                                      'ack_to_engine_sigkill_seconds': ack_to_kill,
                                      'ack_to_checks_seconds': time.monotonic() - acknowledged_at,
                                      'head_before': before, 'head_after': after,
                                      'acknowledged_marker_survived': False,
                                      'whole_application_restart': True, 'checks': checks, 'passed': True})
        self.save()
        self.stop_without_publication(label + '-recovered-stop')

    def finish_workloads(self):
        for attribute in ('workload', 'transfer'):
            process = getattr(self, attribute)
            if process is None:
                continue
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=10)
            setattr(self, attribute, None)

    def live_checkpoint_case(self):
        """Recover a root published with a running DB, not the clean baseline."""
        label = 'live-checkpoint'
        self.write_config(paused=False)
        configuration = tomllib.loads(self.config.read_text())
        (self.work / 'live-checkpoint.toml').write_text(self.config.read_text())
        case = {'passed': False, 'stage': 'starting', 'configuration': configuration,
                'whole_application_restart': True, 'database_stopped_before_publication': False}
        self.report['tests']['live_checkpoint_recovery'] = case
        self.save()
        self.start_engine(label)
        self.mount(label, fsck=True)
        self.mysql_start(label)
        mysql_pid = int(self.command(['docker', 'inspect', '--format', '{{.State.Pid}}', self.name],
                                    label + '-mysql-pid').stdout.strip())
        mysql_before = self.helpers['process_snapshot'](mysql_pid)
        if not mysql_before['available']:
            raise RuntimeError('cannot establish the live MySQL process identity')

        # Independent clients mutate the regular workload and the transaction
        # witness concurrently. The witness has two UPDATE statements and a
        # journal INSERT in the same transaction, followed by an explicit ACK.
        transfer_input = self.work / 'live-transfers.sql'
        with transfer_input.open('w') as commands:
            for marker in range(LIVE_FIRST_ID, LIVE_FIRST_ID + LIVE_TRANSACTIONS):
                commands.write('START TRANSACTION;\n'
                               'UPDATE bench.witness SET a=a-1 WHERE id=1;\n'
                               'UPDATE bench.witness SET b=b+1 WHERE id=1;\n'
                               f'INSERT INTO bench.commits VALUES ({marker});\n'
                               f"COMMIT;\nSELECT 'ACK:{marker}';\nDO SLEEP(0.01);\n")
        transfer_log = self.work / (label + '-transfers.log')
        with (self.work / (label + '-sysbench.log')).open('w') as log:
            self.workload = subprocess.Popen(self.bench_command('read_write', 300, seed=123),
                                             env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                             start_new_session=True)
        with transfer_input.open('r') as commands, transfer_log.open('w') as log:
            self.transfer = subprocess.Popen(['docker', 'exec', '-i', '-e', 'MYSQL_PWD', self.name,
                                              'mysql', '--socket=/var/lib/mysql/mysql.sock', '-uroot',
                                              '--batch', '--skip-column-names', '--unbuffered'],
                                             env=self.env, stdin=commands, stdout=log,
                                             stderr=subprocess.STDOUT, start_new_session=True)
        started = time.monotonic()
        client_pids = {'sysbench': self.workload.pid, 'transfers': self.transfer.pid}

        def live_proof():
            if any(process is None or process.poll() is not None
                   for process in (self.server, self.workload, self.transfer)):
                raise RuntimeError('engine or transaction client exited before the live checkpoint crash')
            mysql = self.helpers['process_snapshot'](mysql_pid)
            if (not mysql['available'] or mysql['start_ticks'] != mysql_before['start_ticks']
                    or not os.path.ismount(self.mountpoint) or not PIDFILE.exists()):
                raise RuntimeError('MySQL identity or the active mount changed before the live checkpoint crash')
            return {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    'elapsed_seconds': time.monotonic() - started,
                    'mysql': mysql, 'engine_pid': self.server.pid, 'client_pids': client_pids,
                    'mount_active': True, 'nbd_attached': True,
                    'acknowledgements': acknowledged_transfers(transfer_log.read_text())}

        deadline = started + 180
        while time.monotonic() < deadline:
            proof = live_proof()
            if proof['acknowledgements']['count'] >= 20:
                break
            time.sleep(.1)
        else:
            raise RuntimeError('live transfers did not acknowledge an initial transaction prefix')

        initial_head = self.head(label + '-head-after-initial-acks')
        initial_proof = live_proof()
        observations = []
        case.update(stage='awaiting_live_publications', initial_head=initial_head,
                    initial_proof=initial_proof, publications=observations)
        self.save()
        previous_head = initial_head
        previous_acks = initial_proof['acknowledgements']['count']
        # A first in-flight checkpoint could have captured before the first ACK.
        # A second publication after that root necessarily captures later work.
        while time.monotonic() < deadline and len(observations) < 2:
            current = self.head(label + '-head-poll')
            proof = live_proof()
            records = status_records(self.server_log.read_text())
            completed = next((record for record in reversed(records)
                              if record.get('remote_sequence') == current['seq']), None)
            if (current['seq'] > previous_head['seq'] and current['generation'] > previous_head['generation']
                    and proof['acknowledgements']['count'] > previous_acks and completed is not None):
                observations.append({'head': current, 'proof': proof, 'completed_checkpoint_status': completed})
                previous_head = current
                previous_acks = proof['acknowledgements']['count']
                self.save()
            time.sleep(.25)
        if len(observations) != 2:
            raise RuntimeError('two complete HEAD publications during active transfers were not observed')

        # Pause only the engine to keep the chosen root fixed while killing the
        # complete application. Clients/DB are still running; none was quiesced.
        before_freeze = live_proof()
        case.update(stage='freezing_engine', before_engine_freeze=before_freeze)
        self.save()
        self.server.send_signal(signal.SIGSTOP)
        frozen = self.head(label + '-head-frozen')
        for attempt in range(4):
            time.sleep(.3)
            next_head = self.head(label + f'-head-settle-{attempt}')
            if same_root(frozen, next_head):
                break
            frozen = next_head
        else:
            raise RuntimeError('HEAD did not settle after the engine was frozen')
        if frozen['seq'] < observations[-1]['head']['seq']:
            raise RuntimeError('frozen HEAD is older than the live publication observed')
        at_kill = live_proof()
        death = self.kill_engine(label)
        if death['returncode'] != -signal.SIGKILL:
            raise RuntimeError('live engine did not terminate through SIGKILL')
        self.stop_mysql(label, crash=True)
        self.finish_workloads()
        self.unmount_detach(label, lazy=True)
        after_kill = self.head(label + '-head-after-sigkill')
        case.update(stage='recovering', at_engine_sigkill=at_kill, frozen_head=frozen,
                    head_after_sigkill=after_kill, engine_death=death)
        self.save()
        if not same_root(frozen, after_kill):
            raise RuntimeError('HEAD changed after the frozen engine was killed')

        # Recovery itself writes journals. Keep publication paused so the same
        # selected root can subsequently be tested after total local-state loss.
        self.write_config(paused=True)
        self.start_engine(label + '-recovery')
        self.mount(label + '-recovery', fsck=True)
        self.mysql_start(label + '-recovery')
        checks = self.integrity(label + '-recovery', live=True)
        if not same_root(frozen, self.head(label + '-head-after-recovery')):
            raise RuntimeError('the selected live root changed during recovery checks')
        self.report['tests']['live_checkpoint_recovery'] = {
            'passed': True, 'stage': 'complete', 'configuration': configuration, 'initial_head': initial_head,
            'initial_proof': initial_proof, 'publications': observations,
            'before_engine_freeze': before_freeze, 'at_engine_sigkill': at_kill,
            'frozen_head': frozen, 'head_after_sigkill': after_kill, 'engine_death': death,
            'whole_application_restart': True, 'database_stopped_before_publication': False,
            'checks': checks,
            'acknowledgements_are_not_a_durability_bound': 'Recovery may lose acknowledged transfers and may retain a committed transfer whose ACK was not read before SIGKILL.',
        }
        self.save()
        self.stop_without_publication(label + '-recovered-stop')
        return frozen, checks

    def execute(self):
        self.cli('init', '--size', '2GiB', label='new-volume-init')
        self.report['initial_head'] = self.head('new-volume-head')
        self.start_engine('initial')
        with DEVICE.open('rb', buffering=0) as device:
            if any(device.read(4096)):
                raise RuntimeError('new export is not blank; refusing mkfs')
        self.command(['mkfs.ext4', '-F', '-E', 'lazy_itable_init=0,lazy_journal_init=0', DEVICE], 'new-volume-mkfs', timeout=600)
        self.mount('initial')
        self.report['benchmark']['mysql_settings'] = self.mysql_start('initial')
        self.sql('CREATE DATABASE bench', 'mysql-create-bench')
        self.command(self.bench_command('read_write', 1, action='prepare'), 'mysql-prepare', timeout=1200)
        self.sql('CREATE TABLE bench.witness(id INT PRIMARY KEY,a BIGINT NOT NULL,b BIGINT NOT NULL) ENGINE=InnoDB; INSERT INTO bench.witness VALUES(1,1000000,1000000); CREATE TABLE bench.commits(id BIGINT PRIMARY KEY) ENGINE=InnoDB; INSERT INTO bench.commits VALUES(0);', 'create-transaction-witness')
        self.benchmark()
        self.report['tests']['pre_crash_integrity'] = self.integrity('before-crashes')
        self.stop_clean('publish-clean-baseline')
        baseline = self.head('published-clean-baseline')
        if baseline['seq'] == 0 or not baseline['shards']:
            raise RuntimeError('database baseline was not published')
        self.report['published_baseline'] = baseline
        self.report['benchmark']['configuration'] = tomllib.loads(self.config.read_text())
        (self.work / 'benchmark.toml').write_text(self.config.read_text())
        self.write_config(paused=True)
        self.report['crash_configuration'] = tomllib.loads(self.config.read_text())
        (self.work / 'crash.toml').write_text(self.config.read_text())
        self.save()
        for seed in self.args.seeds:
            self.crash_case(seed, baseline)
        live_head, live_checks = self.live_checkpoint_case()
        self.cli('scrub', label='remote-live-root-scrub', timeout=1800)
        self.report['tests']['live_root_scrub'] = {'passed': True, 'head': live_head}
        # No byte from the original local directory is consulted by adoption.
        # Retain it as evidence instead of deleting WAL/cache diagnostics.
        archived = self.work / 'lost-local-evidence'
        self.local_dir.rename(archived)
        self.local_dir = self.work / 'restored-local'
        self.write_config(paused=True)
        self.cli('adopt', '--takeover', label='adopt-after-total-local-loss', timeout=300)
        adopted_head = self.head('adopted-head')
        if any(adopted_head[key] != live_head[key] for key in ('volume', 'seq', 'shards')):
            raise RuntimeError('adoption changed the committed data generation')
        self.start_engine('adopted')
        self.mount('adopted', fsck=True)
        self.mysql_start('adopted')
        adopted_checks = self.integrity('adopted', live=True)
        if adopted_checks['transaction_invariants'] != live_checks['transaction_invariants']:
            raise RuntimeError('adoption recovered a different live transaction prefix')
        self.report['tests']['total_local_loss_adoption'] = {'old_local_directory_consulted': False,
                                                           'same_live_transaction_prefix': True,
                                                           'head': adopted_head, 'checks': adopted_checks,
                                                           'passed': True}
        self.stop_without_publication('adopted-stop')
        self.report['complete'] = True
        self.save()

    def cleanup(self):
        failures = []
        for attribute in ('workload', 'transfer'):
            process = getattr(self, attribute)
            if process is None:
                continue
            try:
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.wait(timeout=15)
                setattr(self, attribute, None)
            except Exception as error:
                failures.append(attribute + ' cleanup: ' + type(error).__name__)
        for action in [lambda: self.kill_engine('cleanup'),
                       lambda: self.stop_mysql('cleanup', crash=True),
                       lambda: self.unmount_detach('cleanup', lazy=True)]:
            try:
                action()
            except Exception as error:
                failures.append(type(error).__name__ + ': ' + self.redact(str(error)))
        self.report['cleanup'] = {'failures': failures, 'nbd31_attached': PIDFILE.exists(),
                                  'mount_present': os.path.ismount(self.mountpoint)}
        if failures or PIDFILE.exists() or os.path.ismount(self.mountpoint):
            self.report['complete'] = False
        self.save()

    def export(self):
        """Allowlist text evidence; never recurse into private fixture state."""
        destination = ROOT / 'validation/astra/generation' / self.run_id
        destination.mkdir(parents=True, exist_ok=False)
        names = ['report.json', 'benchmark.toml', 'crash.toml', 'live-checkpoint.toml', 'volume.toml']
        names.extend(path.name for path in sorted(self.work.glob('*.log')))
        manifest = {'source_work': str(self.work), 'complete': self.report['complete'],
                    'secret_files_included': False, 'files': []}
        for name in names:
            source = self.work / name
            if not source.is_file():
                continue
            contents = self.redact(source.read_text(errors='replace'))
            (destination / name).write_text(contents)
            manifest['files'].append({'name': name, 'sha256': hashlib.sha256(contents.encode()).hexdigest()})
        (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        canonical = publish_canonical_report(destination)
        print('Export: ' + str(destination / 'report.json'), flush=True)
        print('Canonical report: ' + str(canonical), flush=True)


def self_test():
    for comm, argv in [('infinidisk2', ['infinidisk2', '-c', '/opt/elestio/infinidisk/bench.toml', 'serve']),
                       ('zerofs', ['zerofs', '-c', '/opt/elestio/infinidisk/data.toml']),
                       ('nbd-client', ['nbd-client', '/dev/nbd0']), ('nbd-client', ['nbd-client', '/dev/nbd1']),
                       ('mysqld', ['mysqld']), ('redis-server', ['redis-server']),
                       ('docker', ['docker', 'start', 'app-wordpress-1', 'phpmyadmin', 'database', 'redis', 'elestio'])]:
        assert not competing_test_process(comm, argv), 'production process must remain allowed'
    for comm, argv in [('cargo', ['cargo', 'build']), ('sysbench', ['sysbench']), ('fio', ['fio']),
                       ('python3', ['python3', '/root/infinidisk2/scripts/run_astra_mysql.py'])]:
        assert competing_test_process(comm, argv), 'concurrent benchmark/build must be detected'
    helpers = comparison_helpers()
    sample = helpers['parse_sysbench_sample']('transactions: 600 (20.00 per sec.)\n avg: 2.3\n 99th percentile: 7.1\n ignored errors: 0\n', 99)
    assert sample['tps'] == 20 and sample['p99_ms'] == 7.1 and sample['p95_ms'] is None
    try:
        helpers['parse_sysbench_sample']('transactions: 1 (1 per sec.)\n avg: 1\n 95th percentile: 1\n', 99)
    except RuntimeError:
        pass
    else:
        raise AssertionError('wrong percentile accepted')
    # Exercise the extracted dependency chain, not only the public function names.
    before = helpers['process_snapshot'](os.getpid())
    after = helpers['process_snapshot'](os.getpid())
    assert before['available'] and after['available'], 'self-test requires readable local /proc'
    assert 'cgroup_cpu' in before and 'cgroup_cpu' in after
    window = helpers['process_window'](before, after)
    assert window['comparable'] and 'cgroup_cpu' in window
    cgroup_before = {'available': True, 'path': '/test', 'identity': [1, 2],
                     'direct_cpu_max': {'quota_usec': 100000, 'period_usec': 100000, 'quota_cores': 1},
                     'counters': {'usage_usec': 100, 'nr_periods': 10, 'nr_throttled': 1}}
    cgroup_after = {**cgroup_before,
                    'counters': {'usage_usec': 500100, 'nr_periods': 20, 'nr_throttled': 3}}
    synthetic_before = {**before, 'monotonic_ns': 1000000000, 'cgroup_cpu': cgroup_before}
    synthetic_after = {**before, 'monotonic_ns': 2000000000, 'cgroup_cpu': cgroup_after}
    window = helpers['process_window'](synthetic_before, synthetic_after)
    assert window['cgroup_cpu']['comparable']
    assert window['cgroup_cpu']['cpu_percent_of_one_core'] == 50
    assert window['cgroup_cpu']['throttled_periods_percent'] == 20
    synthetic_after['cgroup_cpu'] = {**cgroup_after, 'identity': [1, 3]}
    window = helpers['process_window'](synthetic_before, synthetic_after)
    assert window['comparable'] and not window['cgroup_cpu']['comparable']
    assert parse_head('{"format":2,"seq":7}')['seq'] == 7
    assert status_records('log status={"volume":"x","sequence":3,"remote_sequence":3} done')[0]['sequence'] == 3
    assert status_records('log status={"volume":"x","index":{"resident_shards":2},"sequence":3} done')[0]['index']['resident_shards'] == 2
    assert status_records('log status={"volume":"x","index":') == []
    assert same_root(dict(volume=1, seq=2, generation=3, writer=4, shards={}), dict(volume=1, seq=2, generation=3, writer=4, shards={}))
    acknowledgements = acknowledged_transfers(f'ACK:{LIVE_FIRST_ID}\nACK:{LIVE_FIRST_ID + 1}\r\nACK:20')
    assert acknowledgements == {'count': 2, 'last_id': LIVE_FIRST_ID + 1}
    assert acknowledged_transfers('') == {'count': 0, 'last_id': None}
    for bad in (f'ACK:{LIVE_FIRST_ID + 1}\n', f'ACK:{LIVE_FIRST_ID}\nACK:{LIVE_FIRST_ID + 2}\n',
                f'ACK:{LIVE_FIRST_ID}\nACK:{LIVE_FIRST_ID}\n'):
        try:
            acknowledged_transfers(bad)
        except RuntimeError:
            pass
        else:
            raise AssertionError('invalid transfer acknowledgement prefix accepted')
    good = ['999997\t1000003\t2000000', f'4\t0\t{LIVE_FIRST_ID + 2}\t0']
    prefix = validate_live_invariants(good)
    assert prefix['recovered_transfers'] == 3 and prefix['journal_is_contiguous']
    for bad in (['1000000\t1000000\t2000000', '1\t0\t0\t0'],
                ['999997\t1000002\t1999999', good[1]],
                ['999997\t1000003\t2000000', f'3\t0\t{LIVE_FIRST_ID + 2}\t0'],
                ['999997\t1000003\t2000000', f'4\t0\t{LIVE_FIRST_ID + 2}\t1'],
                ['999997\t1000003\t2000000', f'4\t1\t{LIVE_FIRST_ID + 2}\t0'],
                good[:1]):
        try:
            validate_live_invariants(bad)
        except RuntimeError:
            pass
        else:
            raise AssertionError('incoherent live transaction state accepted')
    # Only disposable local fixtures: ensure the renderer sees the latest report,
    # including an incomplete attempt, while earlier run evidence remains intact.
    temporary_root = ROOT / 'test-output' / 'astra-tmp'
    temporary_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='generation-export-', dir=temporary_root) as directory:
        root = pathlib.Path(directory)
        reports = []
        for run_id, complete in [('first-run', True), ('second-run', False)]:
            run = root / run_id
            run.mkdir()
            report = {'id': run_id, 'complete': complete, 'benchmark': {'samples': {}}}
            reports.append(report)
            (run / 'report.json').write_text(json.dumps(report))
            (run / 'manifest.json').write_text('{}')
            canonical = json.loads(publish_canonical_report(run).read_text())
            assert canonical['id'] == run_id and canonical['complete'] is complete
            assert canonical['benchmark'] == report['benchmark']
            assert json.loads((root / canonical['source_report']).read_text()) == report
            assert (root / canonical['source_manifest']).is_file()
        assert json.loads((root / 'first-run/report.json').read_text()) == reports[0]
        assert not list(root.glob('*.tmp'))
    print('Generation harness parser/resource/transaction-prefix/export self-test passed; no device or VM touched.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=pathlib.Path, default=ROOT / 'target/release/infinidisk2-astra')
    parser.add_argument('--expected-binary-sha256', help='required frozen SHA256 for every real campaign')
    parser.add_argument('--credentials', type=pathlib.Path, default=pathlib.Path('/opt/elestio/infinidisk/bench.env'))
    parser.add_argument('--bucket', default='testperf-6czebk')
    parser.add_argument('--endpoint', default='https://storage.elestio.com')
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--warm-seconds', type=int, default=10)
    parser.add_argument('--seeds', type=int, nargs='+', default=[17, 42, 91])
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.expected_binary_sha256 or not re.fullmatch(r'[0-9a-f]{64}', args.expected_binary_sha256):
        parser.error('--expected-binary-sha256 is mandatory and must contain 64 lowercase hex digits')
    if not 1 <= args.samples <= 5 or not 5 <= args.seconds <= 120 or not 1 <= args.warm_seconds <= 120:
        parser.error('invalid benchmark limits')
    if not 1 <= len(args.seeds) <= 5 or len(set(args.seeds)) != len(args.seeds) or any(not 1 <= seed <= 1000000 for seed in args.seeds):
        parser.error('use one to five distinct positive seeds <= 1000000')
    if os.geteuid() != 0:
        parser.error('requires root on the coordinated test VM')
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        parser.error('executable InfiniDisk2 binary is required')
    if hashlib.sha256(args.binary.read_bytes()).hexdigest() != args.expected_binary_sha256:
        parser.error('executable SHA256 does not match --expected-binary-sha256')
    for program in ('docker', 'sysbench', 'mkfs.ext4', 'e2fsck', 'mount', 'umount'):
        if shutil.which(program) is None:
            parser.error('required program missing: ' + program)
    locks = []
    for path in (ROOT / 'test-output/astra-generation.lock', ROOT / 'test-output/astra-recovery.lock',
                 ROOT / 'validation/astra/mysql/campaign.lock'):
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.open('a')
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            parser.error('another test campaign holds ' + str(path))
        locks.append(lock)
    try:
        check_test_slot()
    except (OSError, RuntimeError) as error:
        parser.error(str(error))
    campaign = Campaign(args)
    print('Report: ' + str(campaign.work / 'report.json'), flush=True)
    try:
        campaign.execute()
    except BaseException as error:
        campaign.report['error'] = type(error).__name__ + ': ' + campaign.redact(str(error))
        campaign.save()
        raise
    finally:
        campaign.cleanup()
        campaign.export()
    if not campaign.report['complete']:
        raise RuntimeError('campaign cleanup is incomplete; inspect its report')


if __name__ == '__main__':
    main()
