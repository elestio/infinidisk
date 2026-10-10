#!/usr/bin/env python3
"""One focused ABBA comparison: same binary, unlimited versus byte admission."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib
import uuid

from run_astra_s3_operations import Campaign, ROOT, DEVICE, credentials, preflight
from run_astra_s3_block_direct import DirectFixture
from run_astra_recovery import atomic_json, digest, option_helpers, utc

OUT = ROOT / 'validation/downloads/reads'


class ReadFixture(DirectFixture):
    async def fio(self):
        self.phase('sequential_open')
        await self.start_storage(cold=True)
        self.phase('sequential_read')
        result = await self.measure('sequential-read', rw='read', bs='1m', iodepth=16,
            verify='crc32c', verify_interval='4k', verify_fatal=1, verify_only=1)
        if result['read']['io_bytes'] != 256 * 1024**2:
            raise RuntimeError('sequential byte count differs')
        self.report['integrity']['sequential_crc32c'] = 'passed'
        self.phase('sequential_drain')
        await self.stop_storage()
        self.phase('mixed_open')
        await self.start_storage(cold=True)
        self.phase('mixed_read')
        path = self.directory / 'mixed.fio'
        path.write_text('\n'.join([
            '[global]', f'filename={DEVICE}', 'ioengine=libaio', 'direct=1',
            'numjobs=1', 'size=128m', 'randseed=42', 'randrepeat=1',
            'verify=crc32c', 'verify_interval=4k', 'verify_fatal=1', 'verify_only=1',
            '[mixed-sequential]', 'rw=read', 'bs=1m', 'iodepth=16', 'offset=16m',
            '[mixed-random]', 'rw=randread', 'bs=4k', 'iodepth=8', 'offset=144m',
            'io_size=4m', ''])
        )
        output = self.directory / 'mixed-read.json'
        await self.command(['fio', path, '--output-format=json', '--output=' + str(output)],
                           'mixed-read', timeout=180)
        jobs = json.loads(output.read_text())['jobs']
        if len(jobs) != 2 or any(job['error'] for job in jobs):
            raise RuntimeError('mixed fio error or unexpected grouping')
        for job in jobs:
            expected = {'mixed-sequential': 128, 'mixed-random': 4}[job['jobname']] * 1024**2
            if job['read']['io_bytes'] != expected:
                raise RuntimeError('mixed byte count differs')
            self.report['samples'][job['jobname']] = job
        self.report['integrity']['mixed_crc32c'] = 'passed'
        self.phase('mixed_drain')
        await self.stop_storage()


class DownloadReads(Campaign):
    def __init__(self, args):
        self.args = args
        self.output = OUT
        self.identity = uuid.uuid4().hex[:12]
        self.work = ROOT / 'test-output' / ('downloads-' + self.identity)
        self.work.mkdir(mode=0o700)
        self.private = credentials(args.credentials)
        self.secret_values = list(self.private.values())
        build = json.loads((OUT.parent / 'build/manifest.json').read_text())
        binary = Path(build['binary_path'])
        if not build['complete'] or digest(binary) != build['binary_sha256']:
            raise RuntimeError('unqualified candidate')
        self.binaries = {'astra': binary}
        self.hashes = {'astra': digest(binary)}
        seed_report = ROOT / 'validation/astra/s3-blocks/report.json'
        seed = json.loads(seed_report.read_text())
        if not seed['complete']:
            raise RuntimeError('incomplete read seed')
        self.seed_prefix = seed['variants']['read-seed']['immutable_read_dataset']['prefix']
        if not re.fullmatch(r'infinidisk2-s3-operations-[a-f0-9]{12}/read-seed/fio', self.seed_prefix):
            raise RuntimeError('only the existing owned read seed may be reused')
        generated = self.work / 'recommended.toml'
        subprocess.run([str(binary), '-c', str(generated), 'config'], check=True)
        helpers = option_helpers()
        allowed = helpers['ENGINE_BOOLEAN_OPTIONS'] | helpers['ENGINE_INTEGER_OPTIONS'].keys()
        self.base = {k: v for k, v in tomllib.loads(generated.read_text()).items() if k in allowed}
        self.base.update(memory_cache_mib=64, disk_cache_mib=128, max_index_mib=64)
        self.options = {}
        names = ['scripts/run_download_reads.py', 'scripts/run_astra_s3_operations.py',
                 'scripts/run_astra_s3_block_direct.py', 'scripts/run_astra_s3_block_sweep.py',
                 'scripts/run_astra_recovery.py', 'scripts/validate_vm.py',
                 'validation/downloads/build/manifest.json', str(seed_report.relative_to(ROOT))]
        self.sources = {name: digest(ROOT / name) for name in names}
        self.report = {'schema_version': 1, 'complete': False, 'id': self.identity,
            'started_utc': utc(), 'binary_sha256': self.hashes, 'source_sha256': self.sources,
            'variants': {}, 'protocol': {'order': ['off-r0', 'bounded-r0', 'bounded-r1', 'off-r1'],
                'base_options': self.base, 'proxy': False, 'backend': 'Elestio S3 direct HTTPS',
                'fixture': 'Existing immutable CRC32C seed, new local cache and process per workload.',
                'sequential': '256 MiB, 1 MiB reads QD16; CRC on every page.',
                'mixed': 'Concurrent fio jobs on disjoint zones: sequential128MiB 1MiB QD16; random4MiB 4KiB QD8 in128MiB. Both fixed work, partial overlap; no stonewall.',
                'selection_rule': 'Enable by default only if median sequential p99 improves at least5%, sequential throughput loses at most5%, mixed random p99 rises at most5%; all integrity checks and budget invariants must pass. Short measurements are not statistical proof.',
                'counter_scope': 'Successful engine data range GETs/bytes only; excludes metadata, retries and failures. Not total billable operations.',
                'limits': 'Two ABBA repetitions on shared VM/provider caches; no global cache purge. No new native, ZeroFS or DB throughput comparison.'}}

    async def execute(self):
        try:
            for name in self.report['protocol']['order']:
                self.verify()
                self.options[name] = {**self.base, 'download_budget_mib': 8 if name.startswith('bounded') else 0}
                fixture = ReadFixture(self, name, 'read')
                try:
                    await fixture.execute()
                    counters = []
                    logs = sorted(fixture.directory.glob('server-*.log'), key=lambda p: int(p.stem.split('-')[-1]))
                    for log in logs:
                        final = [line for line in log.read_text().splitlines() if 'volume final status' in line]
                        if len(final) != 1:
                            raise RuntimeError('missing final engine counters')
                        value = json.JSONDecoder().raw_decode(final[0][final[0].index('{"volume"'):])[0]
                        counters.append({k: value[k] for k in ('remote_gets', 'remote_bytes',
                            'adaptive_small_gets', 'adaptive_large_gets', 'range_cache_bytes', 'downloads')})
                        stats = value['downloads']
                        if stats['enabled']:
                            if stats['active'] or stats['waiting'] or stats['reserved_bytes']:
                                raise RuntimeError('admission leak after drain')
                            if stats['peak_reserved_bytes'] > stats['budget_bytes'] or stats['peak_active'] > stats['max_requests']:
                                raise RuntimeError('download admission bound violated')
                    if len(counters) != 2:
                        raise RuntimeError('expected sequential and mixed process counters')
                    fixture.report['engine_data_counters'] = dict(zip(('sequential', 'mixed'), counters))
                finally:
                    fixture.report.pop('summary', None)
                    for phase in fixture.report['phases'].values():
                        phase.pop('summary', None)
                    (fixture.directory / 'requests.jsonl').unlink(missing_ok=True)
                    self.report['variants'][name] = fixture.report
                    atomic_json(fixture.directory / 'report.json', fixture.report)
                    self.export()
                for path in fixture.directory.glob('local-*'):
                    if path.is_dir() and not path.is_symlink():
                        shutil.rmtree(path)
                print('COMPLETE', name, flush=True)
            self.verify()
            self.report['complete'] = True
        except BaseException as error:
            self.report['error'] = type(error).__name__ + ': ' + str(error)
            raise
        finally:
            self.report['ended_utc'] = utc()
            self.export()
            print('REPORT', OUT / 'report.json', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    args = parser.parse_args()
    if OUT.exists():
        raise RuntimeError('refusing to overwrite read evidence')
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        lock = (ROOT / 'validation/astra' / name).open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    OUT.mkdir(parents=True)
    asyncio.run(DownloadReads(args).execute())


if __name__ == '__main__':
    main()
