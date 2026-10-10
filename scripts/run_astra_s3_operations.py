#!/usr/bin/env python3
"""Dedicated HTTP-operation accounting; its timings are not performance scores."""
import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import time
import uuid

from run_astra_recovery import Redactor, atomic_json, digest, load_profiles, utc
from run_astra_postgres_compare import baseline_options, parse_pgbench, container_errors, INSPECT_FORMAT
from s3_counting_proxy import CountingProxy, consolidated_events
from s3_request_pricing import classify_s3_request, project_events, sql_query_units, PRICING_FILE

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/astra/s3-operations'
ASTRA_SHA = 'b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151'
BASELINE_SHA = '6096fff8d5b69e9e3f21c8a500cb7a1c98d737a876a018d2faf8d8a1b64046d1'
DEVICE = Path('/dev/nbd31')
PORT = 11990
NFS_PORT = 12991
ORDER = ('native', 'baseline', 'astra', 'zerofs-durable')


def preflight():
    if not DEVICE.exists() or Path('/sys/class/block/nbd31/pid').exists():
        raise RuntimeError('reserved nbd31 unavailable')
    if Path('/dev/ublkc31').exists():
        raise RuntimeError('reserved ublk31 occupied')
    for port in (11990, 11991, 12991):
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(('127.0.0.1', port))
    for file in Path('/proc').glob('[0-9]*/comm'):
        try:
            if file.read_text().strip() in {'cargo', 'rustc', 'cc1', 'ld.lld', 'fio', 'sysbench', 'pgbench'}:
                raise RuntimeError('benchmark or compiler active')
        except FileNotFoundError:
            pass
    if shutil.disk_usage(ROOT).free < 3 * 1024**3:
        raise RuntimeError('less than 3 GiB free before isolated fixture')


def credentials(path):
    result = {}
    for line in path.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            key, value = line.split('=', 1)
            if key in {'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'INFINIDISK_PASSWORD'}:
                values = shlex.split(value)
                if values:
                    result[key] = values[0]
    if not {'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'INFINIDISK_PASSWORD'} <= result.keys():
        raise RuntimeError('required benchmark credentials missing')
    return result


class Fixture:
    def __init__(self, campaign, variant, workload):
        self.campaign, self.variant, self.workload = campaign, variant, workload
        self.directory = campaign.work / variant / workload
        self.directory.mkdir(parents=True, mode=0o700)
        self.mount = self.directory / 'mount'
        self.mount.mkdir()
        self.cfg = self.directory / 'volume.toml'
        self.attach_cfg = self.directory / 'attach.toml'
        self.attach_cfg.write_text(f'local_dir = "{self.directory}/unused"\n'
                                   f'store = "file://{self.directory}/unused-objects"\n'
                                   f'listen = "127.0.0.1:{PORT}"\n')
        self.server = self.client = self.proxy = None
        self.container = None
        self.mounted = self.nfs_mounted = False
        self.local_number = 0
        self.command_number = 0
        self.events = []
        self.phases = {}
        self.phase_name = None
        self.phase_started = None
        self.report = {'complete': False, 'integrity': {}, 'phases': self.phases}
        self.env = dict(os.environ)
        for key in ('AWS_SESSION_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
            self.env.pop(key, None)
        self.env['ZEROFS_PASSWORD'] = campaign.private['INFINIDISK_PASSWORD']
        self.binary = campaign.binaries['baseline' if variant == 'baseline' else 'astra']
        self.prefix = 'infinidisk2-s3-operations-' + campaign.identity + '/' + variant + '/' + workload

    def phase(self, name):
        now = time.monotonic()
        if self.phase_name:
            self.phases[self.phase_name]['seconds'] = now - self.phase_started
        self.phase_name = self.workload + '.' + name
        if self.phase_name in self.phases:
            raise RuntimeError('phase name reused')
        self.phase_started = now
        self.phases[self.phase_name] = {'started_utc': utc()}
        if self.proxy:
            self.proxy.set_phase(self.phase_name)
        print('PHASE', self.variant, self.phase_name, flush=True)

    async def command(self, argv, label, timeout=600, check=True, env=None):
        self.command_number += 1
        log = self.directory / f'{self.command_number:04d}-{label}.log'
        with log.open('w') as output:
            proc = await asyncio.create_subprocess_exec(*map(str, argv), stdout=output,
                    stderr=asyncio.subprocess.STDOUT, stdin=asyncio.subprocess.DEVNULL,
                    env=env or self.env, start_new_session=True)
            try:
                await asyncio.wait_for(proc.wait(), timeout)
            except BaseException:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                await proc.wait()
                raise
        text = log.read_text(errors='replace')
        if check and proc.returncode:
            raise RuntimeError(f'{self.variant}/{self.workload}/{label} exited {proc.returncode}')
        return proc.returncode, text

    async def background(self, argv, label):
        file = (self.directory / (label + '.log')).open('w')
        try:
            return await asyncio.create_subprocess_exec(*map(str, argv), stdout=file,
                    stderr=asyncio.subprocess.STDOUT, stdin=asyncio.subprocess.DEVNULL,
                    env=self.env, start_new_session=True)
        finally:
            file.close()

    async def open_proxy(self):
        self.proxy = CountingProxy(endpoint='https://storage.elestio.com',
                bucket='testperf-6czebk', prefix=self.prefix, region='auto',
                credentials=self.campaign.private, journal_path=self.directory / 'events.jsonl',
                scratch=self.directory / 'spool', classify=classify_s3_request,
                max_inflight=128, max_body_bytes=128 * 1024**2, max_requests=200_000)
        await self.proxy.start()
        self.env.update(self.proxy.client_environment())
        self.campaign.secret_values.extend([self.proxy.client_key, self.proxy.client_secret])

    def configuration(self):
        local = self.directory / ('local-' + str(self.local_number))
        if self.variant != 'zerofs-durable':
            opts = self.campaign.options[self.variant]
            text = '\n'.join([
                'local_dir = ' + json.dumps(str(local)),
                'store = ' + json.dumps('s3://testperf-6czebk/' + self.prefix),
                'endpoint = ' + json.dumps(self.proxy.listen),
                'region = "auto"', f'listen = "127.0.0.1:{PORT}"',
                *[key + ' = ' + json.dumps(value) for key, value in opts.items()]]) + '\n'
        else:
            # Credentials are environment placeholders; allow_http applies only
            # to the local measurement hop. Upstream TLS stays verified.
            variable = lambda name: '"$' + '{' + name + '}"'
            text = (f'[cache]\ndir = {json.dumps(str(local))}\n'
                    'memory_size_gb = 0.0625\ndisk_size_gb = 0.125\n'
                    f'[storage]\nurl = "s3://testperf-6czebk/{self.prefix}"\n'
                    'encryption_password = ' + variable('ZEROFS_PASSWORD') + '\n'
                    f'[servers.nbd]\naddresses = ["127.0.0.1:{PORT}"]\n'
                    f'[servers.nfs]\naddresses = ["127.0.0.1:{NFS_PORT}"]\n'
                    '[aws]\nendpoint = ' + json.dumps(self.proxy.listen) + '\n'
                    'access_key_id = ' + variable('AWS_ACCESS_KEY_ID') + '\n'
                    'secret_access_key = ' + variable('AWS_SECRET_ACCESS_KEY') + '\n'
                    'default_region = "auto"\nallow_http = "true"\n'
                    '[filesystem]\nignore_fsync = false\ncompression = "lz4"\n'
                    '[lsm]\nflush_interval_secs = 5\n')
        self.cfg.write_text(text)

    async def start_storage(self, fresh=False, cold=False):
        if cold:
            self.local_number += 1
        self.configuration()
        if self.variant != 'zerofs-durable':
            if fresh:
                await self.command([self.binary, '-c', self.cfg, 'init', '--size', '2GiB'], 'init')
            elif cold:
                await self.command([self.binary, '-c', self.cfg, 'adopt', '--takeover'], 'adopt')
            self.server = await self.background([self.binary, '-c', self.cfg, 'serve'], 'server-' + str(self.command_number))
        else:
            self.server = await self.background([self.campaign.binaries['zerofs'], 'run', '-c', self.cfg],
                                                'server-' + str(self.command_number))
        for _ in range(900):
            if self.server.returncode is not None:
                raise RuntimeError('storage server exited during startup')
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', PORT)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                await asyncio.sleep(.2)
        else:
            raise TimeoutError('storage server did not listen')
        if fresh and self.variant == 'zerofs-durable':
            nfs = self.directory / 'nfs'
            nfs.mkdir()
            await self.command(['mount', '-t', 'nfs', '-o',
                f'nolock,vers=3,tcp,port={NFS_PORT},mountport={NFS_PORT}',
                '127.0.0.1:/', nfs], 'nfs-mount')
            self.nfs_mounted = True
            def create_export():
                (nfs / '.nbd').mkdir(exist_ok=True)
                with (nfs / '.nbd/infinidisk2').open('wb') as target:
                    target.truncate(2 * 1024**3)
                    target.flush()
                    os.fsync(target.fileno())
            await asyncio.to_thread(create_export)
            await self.command(['umount', nfs], 'nfs-unmount')
            self.nfs_mounted = False
        self.client = await self.background([self.campaign.binaries['astra'], '-c', self.attach_cfg,
                    'attach', '--device', DEVICE, '--connections', '8'], 'attach-' + str(self.command_number))
        for _ in range(200):
            if self.client.returncode is not None:
                raise RuntimeError('NBD client exited during attachment')
            if Path('/sys/class/block/nbd31/pid').exists():
                break
            await asyncio.sleep(.1)
        else:
            raise TimeoutError('NBD attachment timeout')
        if fresh:
            def blank():
                with DEVICE.open('rb', buffering=0) as stream:
                    return not any(stream.read(4096))
            if not await asyncio.to_thread(blank):
                raise RuntimeError('new test device was not blank')

    async def stop_storage(self):
        if await asyncio.to_thread(os.path.ismount, self.mount) or self.nfs_mounted:
            raise RuntimeError('refusing to detach a mounted test filesystem')
        if self.client:
            if Path('/sys/class/block/nbd31/pid').exists():
                # Attach/detach only needs the listen address, irrespective of
                # whether the server-side config is an InfiniDisk2 or ZeroFS one.
                await self.command([self.campaign.binaries['astra'], '-c', self.attach_cfg,
                                    'detach', '--device', DEVICE], 'detach', timeout=90)
            await asyncio.wait_for(self.client.wait(), 90)
            self.client = None
        if self.server:
            if self.server.returncode is None:
                self.server.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(self.server.wait(), 300)
            except TimeoutError:
                self.server.kill()
                await self.server.wait()
                raise RuntimeError('storage shutdown timed out; remote publication unproven')
            code = self.server.returncode
            self.server = None
            if code:
                raise RuntimeError('storage exited with error')

    async def mount_pg(self, fresh=False):
        if self.variant != 'native':
            if fresh:
                await self.command(['mkfs.ext4', '-F', '-E', 'lazy_itable_init=0,lazy_journal_init=0', DEVICE], 'mkfs')
            await self.command(['mount', '-o', 'noatime', DEVICE, self.mount], 'mount')
            self.mounted = True
        datadir = self.mount / 'postgres'
        # This filesystem is itself served through the async counting proxy.
        # A cold lookup can require S3: blocking this event loop would deadlock.
        await asyncio.to_thread(datadir.mkdir, exist_ok=True)
        self.container = 'id2-s3ops-' + self.campaign.identity + '-' + self.variant.replace('-', '')
        await self.command(['docker', 'run', '-d', '--name', self.container, '--network', 'none',
            '--memory', '512m', '--cpus', '1', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust',
            '-e', 'POSTGRES_INITDB_ARGS=--data-checksums', '-v',
            str(datadir) + ':/var/lib/postgresql/data', self.campaign.image_id], 'pg-start')
        _, raw = await self.command(['docker', 'inspect', '--format', INSPECT_FORMAT, self.container], 'pg-inspect')
        if errors := await asyncio.to_thread(container_errors, json.loads(raw), self.campaign.image_id, datadir):
            raise RuntimeError('; '.join(errors))
        for _ in range(900):
            code, _ = await self.command(['docker', 'exec', self.container, 'sh', '-c',
                '[ "$(cat /proc/1/comm)" = postgres ] && pg_isready -U postgres'], 'pg-ready', timeout=30, check=False)
            if not code:
                break
            await asyncio.sleep(.5)
        else:
            raise TimeoutError('PostgreSQL startup timeout')
        _, settings = await self.command(['docker', 'exec', self.container, 'psql', '-U', 'postgres', '-Atc',
            'SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes; SHOW data_checksums;'], 'pg-settings')
        if settings.strip().splitlines() != ['on'] * 4:
            raise RuntimeError('PostgreSQL durability/checksums disabled')

    async def check_pg(self, label):
        _, result = await self.command(['docker', 'exec', self.container, 'psql', '-U', 'postgres', '-Atc',
            'SELECT (SELECT sum(abalance) FROM pgbench_accounts)=(SELECT sum(bbalance) FROM pgbench_branches) '
            'AND (SELECT sum(abalance) FROM pgbench_accounts)=(SELECT sum(tbalance) FROM pgbench_tellers); '
            'SELECT count(*) FROM pgbench_history;'], label + '-sums')
        expected_history = self.report.get('transactions', 0)
        if result.strip().splitlines() != ['t', str(expected_history)]:
            raise RuntimeError('PostgreSQL sums or committed transaction history differ')
        await self.command(['docker', 'exec', self.container, 'pg_amcheck', '-U', 'postgres',
                           '--database', 'postgres', '--install-missing'], label + '-amcheck')
        self.report['integrity'][label] = 'passed'
        self.report.setdefault('committed_history_rows', {})[label] = expected_history

    async def stop_pg(self):
        if self.container:
            await self.command(['docker', 'stop', '-t', '120', self.container], 'pg-stop', timeout=180)
            await self.command(['docker', 'rm', self.container], 'pg-remove')
            self.container = None
        if self.mounted or await asyncio.to_thread(os.path.ismount, self.mount):
            await self.command(['umount', self.mount], 'unmount')
            self.mounted = False

    async def stop_nfs(self):
        if self.nfs_mounted:
            await self.command(['umount', self.directory / 'nfs'], 'cleanup-nfs')
            self.nfs_mounted = False

    async def postgres(self):
        self.phase('init')
        if self.variant != 'native':
            await self.start_storage(fresh=True)
        await self.mount_pg(fresh=True)
        await self.command(['docker', 'exec', self.container, 'pgbench', '-U', 'postgres',
                           '-i', '-s', '2', 'postgres'], 'pg-init', timeout=900)
        await self.check_pg('prepared')
        self.phase('prepare_drain')
        await self.stop_pg()
        await self.stop_storage()
        self.phase('open')
        if self.variant != 'native':
            await self.start_storage()
        await self.mount_pg()
        self.phase('idle')
        await asyncio.sleep(10)
        self.phase('load')
        _, text = await self.command(['docker', 'exec', self.container, 'pgbench', '-U', 'postgres',
                '-n', '-r', '-b', 'tpcb-like', '--max-tries=1', '--random-seed=42',
                '-c', '4', '-j', '4', '-t', '64', 'postgres'], 'pg-fixed-transactions', timeout=1800)
        parsed = parse_pgbench(text)
        count = re.search(r'number of transactions actually processed:\s*(\d+)/(\d+)', text)
        if not count or tuple(map(int, count.groups())) != (256, 256):
            raise RuntimeError('pgbench did not finish exactly 256 transactions')
        self.report['transactions'] = 256
        self.report['instrumented_pgbench'] = parsed
        statements = re.findall(r'^\s*[\d.]+\s+0\s+(BEGIN;|END;|UPDATE .+|SELECT .+|INSERT .+)$', text, re.M)
        if len(statements) != 7 or sum(s.startswith(('UPDATE ', 'SELECT ', 'INSERT ')) for s in statements) != 5:
            raise RuntimeError('pgbench statement accounting differs from the documented built-in script')
        self.report['sql_accounting'] = {'business_queries': 256 * 5, 'all_commands_including_begin_end': 256 * 7,
            'queries_per_transaction': 5, 'commands_per_transaction': 7, 'statement_failures': 0,
            'basis': 'TPC-B-like built-in script: three UPDATE, one SELECT, one INSERT; BEGIN and END excluded from query unit. No vacuum during load; max-tries=1, zero failed transactions.',
            'source': 'https://www.postgresql.org/docs/16/pgbench.html'}
        self.phase('drain')
        await self.stop_pg()
        await self.stop_storage()
        self.phase('warm_open')
        if self.variant != 'native':
            await self.start_storage()
        self.phase('warm_database_open')
        await self.mount_pg()
        self.phase('warm_check')
        await self.check_pg('warm_restored')
        self.phase('warm_drain')
        await self.stop_pg()
        await self.stop_storage()
        self.phase('cold_open')
        if self.variant != 'native':
            await self.start_storage(cold=True)
        self.phase('cold_database_open')
        await self.mount_pg()
        self.phase('cold_check')
        await self.check_pg('cold_restored')
        self.phase('cold_drain')
        await self.stop_pg()
        await self.stop_storage()

    async def fio(self):
        self.phase('init')
        await self.start_storage(fresh=True)
        async def run_fio(label, **options):
            output = self.directory / (label + '.json')
            opts = {'name': label, 'filename': str(DEVICE), 'ioengine': 'libaio', 'direct': 1,
                    'group_reporting': 1, 'size': '64m', 'offset': '16m',
                    'output-format': 'json', 'output': str(output), **options}
            await self.command(['fio', *['--' + key + '=' + str(value) for key, value in opts.items()]],
                               label, timeout=1200)
            jobs = json.loads(output.read_text())['jobs']
            if any(job['error'] for job in jobs):
                raise RuntimeError('fio data verification or I/O error')
            self.report.setdefault('fio_results', {})[label] = jobs
        self.phase('verified_write')
        await run_fio('fio-verified-write', rw='write', bs='128k', iodepth=16,
                      verify='crc32c', verify_fatal=1, do_verify=1, fsync_on_close=1, refill_buffers=1)
        self.phase('fsync_write')
        await run_fio('fio-fixed-fsync', rw='randwrite', bs='4k', iodepth=1, numjobs=4,
                      io_size='64k', offset='128m', fsync=1, refill_buffers=1)
        fixed = self.report['fio_results']['fio-fixed-fsync']
        if sum(job['write']['total_ios'] for job in fixed) != 64 or not sum(job['sync']['total_ios'] for job in fixed):
            raise RuntimeError('fixed fio write/fsync count differs from the protocol')
        self.phase('drain')
        await self.stop_storage()
        self.phase('warm_open')
        await self.start_storage()
        self.phase('warm_read')
        await run_fio('fio-warm-crc32c', rw='read', bs='128k', iodepth=16,
                      verify='crc32c', verify_fatal=1, verify_only=1)
        self.report['integrity']['warm_restored_crc32c'] = 'passed'
        self.phase('warm_drain')
        await self.stop_storage()
        self.phase('cold_open')
        await self.start_storage(cold=True)
        self.phase('cold_read')
        await run_fio('fio-restored-crc32c', rw='read', bs='128k', iodepth=16,
                      verify='crc32c', verify_fatal=1, verify_only=1)
        self.report['integrity']['cold_restored_crc32c'] = 'passed'
        self.phase('cold_drain')
        await self.stop_storage()

    async def execute(self):
        failure = None
        try:
            if self.variant != 'native':
                await self.open_proxy()
            await (self.postgres() if self.workload == 'postgres' else self.fio())
            self.report['complete'] = True
        except BaseException as error:
            failure = error
            self.report['error'] = type(error).__name__ + ': ' + str(error)
        finally:
            cleanup = []
            for action in (self.stop_pg, self.stop_nfs, self.stop_storage):
                try:
                    await action()
                except BaseException as error:
                    cleanup.append(type(error).__name__ + ': ' + str(error))
            if self.proxy:
                await self.proxy.close()
                self.report['proxy'] = self.proxy.snapshot()
                self.events = consolidated_events(self.directory / 'events.jsonl')
                forwarded = sum(bool(item['forwarded']) for item in self.events)
                if (forwarded != self.proxy.forwarded or self.proxy.closed_error
                        or self.proxy.local_rejections or not self.proxy.snapshot()['journal_complete']):
                    cleanup.append('proxy trace incomplete, rejected request or counter mismatch')
            if self.phase_name:
                self.phases[self.phase_name]['seconds'] = time.monotonic() - self.phase_started
            for name, phase in self.phases.items():
                phase['summary'] = project_events([event for event in self.events if event['phase'] == name])
            self.report['summary'] = project_events(self.events)
            if self.workload == 'postgres' and self.report.get('transactions') == 256:
                selected = [event for event in self.events if event['phase'] in ('postgres.load', 'postgres.drain')]
                self.report['workload_normalized'] = project_events(selected, 256)
                self.report['query_normalized'] = sql_query_units(self.report['workload_normalized'], self.report['sql_accounting'])
            self.report['cleanup'] = {'failures': cleanup, 'nbd31_detached': not Path('/sys/class/block/nbd31/pid').exists(),
                                      'mount_absent': not await asyncio.to_thread(os.path.ismount, self.mount), 'container_removed': self.container is None}
            if cleanup or not all(self.report['cleanup'][key] for key in ('nbd31_detached', 'mount_absent', 'container_removed')):
                self.report['complete'] = False
            (self.directory / 'requests.jsonl').write_text(''.join(json.dumps(event) + '\n' for event in self.events))
            atomic_json(self.directory / 'report.json', self.report)
        if failure:
            raise failure
        if not self.report['complete']:
            raise RuntimeError('fixture cleanup or evidence validation failed')
        return self.report


class Campaign:
    def __init__(self, args):
        self.args = args
        self.output = OUT
        self.identity = uuid.uuid4().hex[:12]
        self.work = ROOT / 'test-output' / ('s3-operations-' + self.identity)
        self.work.mkdir(mode=0o700)
        self.private = credentials(args.credentials)
        self.secret_values = list(self.private.values())
        self.binaries = {'astra': args.binary.resolve(), 'baseline': args.baseline.resolve(),
                         'zerofs': Path('/usr/local/bin/zerofs').resolve()}
        self.hashes = {name: digest(path) for name, path in self.binaries.items()}
        if self.hashes['astra'] != ASTRA_SHA or self.hashes['baseline'] != BASELINE_SHA:
            raise RuntimeError('measurement requires the qualified frozen binaries')
        self.options = {'astra': load_profiles()['core']}
        self.options['baseline'] = baseline_options(self.options['astra'])
        import subprocess
        self.image_id = subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Id}}', 'postgres:16'], text=True).strip()
        sources = [Path(__file__), ROOT / 'scripts/s3_counting_proxy.py', ROOT / 'scripts/s3_request_pricing.py',
                   ROOT / 'scripts/run_astra_postgres_compare.py', ROOT / 'scripts/run_astra_recovery.py',
                   ROOT / 'scripts/run_astra_mysql.py', ROOT / 'scripts/profiles/astra-recovery-core.json', PRICING_FILE]
        self.sources = {str(path.relative_to(ROOT)): digest(path) for path in sources}
        self.report = {'schema_version': 1, 'complete': False, 'id': self.identity, 'started_utc': utc(),
            'binary_sha256': self.hashes, 'source_sha256': self.sources, 'variants': {},
            'recorder_runtime': {'python': platform.python_version(),
                'aiohttp': importlib.metadata.version('aiohttp'), 'botocore': importlib.metadata.version('botocore')},
            'protocol': {'order': list(ORDER), 'postgres': {'scale': 2, 'clients': 4, 'transactions_per_client': 64,
                'transactions': 256, 'cpu_limit': 1, 'memory_mib': 512, 'idle_seconds': 10, 'image_id': self.image_id},
                'fio': {'verified_bytes': 64 * 1024**2, 'verified_offset': 16 * 1024**2,
                        'random_fsync_writes': 64, 'random_write_offset': 128 * 1024**2, 'block_size': 4096},
                'options': self.options, 'backend': 'Elestio S3 over verified HTTPS through a loopback accounting proxy',
                'samples': 1, 'timings_are_performance_comparison': False,
                'normalization': 'postgres.load + postgres.drain only; setup, idle, warm/cold recovery and fio excluded',
                'restart_comparison': 'Warm: fresh processes, same local WAL/SSD directory. Cold: new empty local directory, remote adoption. PostgreSQL storage open, database readiness and integrity scan measured separately. Warm PostgreSQL startup/shutdown can change the physical checkpoint before cold restart; logical dataset unchanged. Fio verifies the same CRC32C data area in both restarts. RAM process caches are not retained; Linux page caches are not globally purged.',
                'limitations': 'Sequential fixed-work diagnostics; proxy affects timing and checkpoint cadence. No global page-cache drop. Different local/remote fsync contracts. Request fees projected onto Tigris, not billed on Tigris. No claim of power-loss qualification.'}}

    def verify(self):
        preflight()
        for name, path in self.binaries.items():
            if digest(path) != self.hashes[name]:
                raise RuntimeError('binary changed during accounting')
        for name, expected in self.sources.items():
            if digest(ROOT / name) != expected:
                raise RuntimeError('accounting source changed')

    def export(self):
        redactor = Redactor(self.secret_values)
        archive = self.output / self.identity
        archive.mkdir(parents=True, mode=0o700, exist_ok=True)
        files = {}
        candidates = [path for fixture in self.work.glob('*/*')
                      if fixture.name in ('postgres', 'fio') and fixture.is_dir() and not fixture.is_symlink()
                      for path in fixture.iterdir()]
        for path in sorted(candidates):
            relative = path.relative_to(self.work)
            # Only direct fixture-level diagnostics; never DBs, local stores,
            # configs, spooled bodies or binary datasets.
            if (len(relative.parts) != 3 or path.suffix not in {'.log', '.json', '.jsonl'}
                    or path.is_symlink() or not path.is_file()):
                continue
            if path.stat().st_size > 128 * 1024**2:
                raise RuntimeError('evidence exceeds export bound')
            content = redactor.text(path.read_text(errors='replace'))
            destination = archive / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(content)
            files[str(relative)] = digest(destination)
        self.report['source_report'] = self.identity + '/report.json'
        self.report['source_manifest'] = self.identity + '/manifest.json'
        atomic_json(archive / 'report.json', redactor.object(self.report))
        files['report.json'] = digest(archive / 'report.json')
        atomic_json(archive / 'manifest.json', {'files_sha256': files, 'source_sha256': self.sources,
                    'binary_sha256': self.hashes, 'complete': self.report['complete']})
        atomic_json(self.output / 'report.json', redactor.object(self.report))

    async def execute(self):
        try:
            for variant in ORDER:
                entry = {'complete': False, 'workloads': {}, 'phases': {}, 'integrity': {}}
                self.report['variants'][variant] = entry
                all_events = []
                for workload in (('postgres',) if variant == 'native' else ('postgres', 'fio')):
                    self.verify()
                    fixture = Fixture(self, variant, workload)
                    try:
                        result = await fixture.execute()
                    finally:
                        entry['workloads'][workload] = fixture.report
                        all_events.extend(fixture.events)
                        entry['phases'].update(fixture.phases)
                        self.export()
                    entry['integrity'][workload] = result['integrity']
                    removed = []
                    candidates = list(fixture.directory.glob('local-*'))
                    if variant == 'native':
                        candidates.append(fixture.mount / 'postgres')
                    for path in candidates:
                        if path.is_dir() and not path.is_symlink():
                            shutil.rmtree(path)
                            removed.append(str(path.relative_to(fixture.directory)))
                    result['local_test_data_removed_after_verified_restarts'] = removed
                    atomic_json(fixture.directory / 'report.json', result)
                    self.export()
                entry['summary'] = project_events(all_events)
                entry['workload_normalized'] = entry['workloads']['postgres']['workload_normalized']
                entry['query_normalized'] = entry['workloads']['postgres']['query_normalized']
                entry['complete'] = True
                self.export()
            self.verify()
            self.report['complete'] = True
        except BaseException as error:
            self.report['error'] = type(error).__name__ + ': ' + str(error)
            raise
        finally:
            self.report['ended_utc'] = utc()
            self.export()
            print('REPORT', OUT / 'report.json', flush=True)


async def self_test():
    """A filesystem lookup must leave the proxy event loop able to respond."""
    import tempfile
    import threading
    from types import SimpleNamespace

    gate = threading.Event()
    loop_thread = threading.get_ident()
    with tempfile.TemporaryDirectory(prefix='s3-accounting-reentry-') as temporary:
        directory = Path(temporary) / 'postgres'
        class MountedDirectory:
            def mkdir(self, **_kwargs):
                if threading.get_ident() == loop_thread or not gate.wait(1):
                    raise AssertionError('mounted filesystem lookup blocked the proxy event loop')
            def __str__(self):
                return str(directory)
            def resolve(self):
                return directory.resolve()
        class MountedRoot:
            def __truediv__(self, name):
                assert name == 'postgres'
                return MountedDirectory()
        fixture = Fixture.__new__(Fixture)
        fixture.variant = 'native'
        fixture.mount = MountedRoot()
        fixture.campaign = SimpleNamespace(image_id='self-test-image', identity='selftest')
        async def command(_argv, label, **_kwargs):
            if label == 'pg-inspect':
                return 0, json.dumps({'image': 'self-test-image', 'nano_cpus': 1_000_000_000,
                    'memory_bytes': 512 * 1024**2, 'mounts': [{'Type': 'bind',
                    'Source': str(directory), 'Destination': '/var/lib/postgresql/data'}]})
            return 0, 'on\non\non\non\n' if label == 'pg-settings' else ''
        fixture.command = command
        async def respond():
            await asyncio.sleep(.02)
            gate.set()
        responder = asyncio.create_task(respond())
        await asyncio.wait_for(fixture.mount_pg(), 2)
        await responder
    print(json.dumps({'mounted_filesystem_proxy_reentrancy': 'passed',
                      'docker_calls_mocked': True, 'no_vm_or_credentials_used': True}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/infinidisk2-astra-b39b705f43b6')
    parser.add_argument('--baseline', type=Path, default=ROOT / 'target/release/infinidisk2-pre-astra')
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    parser.add_argument('--plan-only', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        asyncio.run(self_test())
        return
    if args.plan_only:
        print(json.dumps({'order': ORDER, 'postgres_fixed_transactions': 256,
            'fio_verified_bytes': 64 * 1024**2, 'fio_fixed_fsync_writes': 64,
            'normalization': 'only postgres.load + postgres.drain',
            'pricing': str(PRICING_FILE), 'native_s3_requests': 0}, indent=2))
        return
    OUT.mkdir(parents=True, exist_ok=True)
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        path = OUT.parent / name
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    campaign = Campaign(args)
    asyncio.run(campaign.execute())


if __name__ == '__main__':
    main()
