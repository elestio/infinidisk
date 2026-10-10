#!/usr/bin/env python3
"""Focused direct S3 comparison of fixed and adaptive reads; no full matrix."""
import argparse
import asyncio
import fcntl
import json
import re
import shutil
import subprocess
import tomllib
import uuid
from pathlib import Path

from run_astra_s3_operations import Campaign, ROOT, credentials, preflight
from run_astra_s3_block_direct import DirectFixture
from run_astra_recovery import atomic_json, digest, option_helpers, utc

OUT = ROOT / 'validation/adaptive/reads'


class FocusedReads(Campaign):
    def __init__(self, args):
        self.args = args
        self.output = OUT
        self.identity = uuid.uuid4().hex[:12]
        self.work = ROOT / 'test-output' / ('adaptive-' + self.identity)
        self.work.mkdir(mode=0o700)
        self.private = credentials(args.credentials)
        self.secret_values = list(self.private.values())
        self.binaries = {'astra': args.binary.resolve()}
        self.hashes = {'astra': digest(args.binary)}
        if self.hashes['astra'] != args.expected_sha256:
            raise RuntimeError('candidate binary differs from build manifest')
        seed_report = ROOT / 'validation/astra/s3-blocks/report.json'
        seed = json.loads(seed_report.read_text())
        if not seed['complete']:
            raise RuntimeError('prior immutable read fixture is incomplete')
        self.seed_prefix = seed['variants']['read-seed']['immutable_read_dataset']['prefix']
        if not re.fullmatch(r'infinidisk2-s3-operations-[a-f0-9]{12}/read-seed/fio', self.seed_prefix):
            raise RuntimeError('only the previous owned test seed may be reused')
        generated = self.work / 'recommended.toml'
        subprocess.run([str(args.binary), '-c', str(generated), 'config'], check=True)
        helpers = option_helpers()
        allowed = helpers['ENGINE_BOOLEAN_OPTIONS'] | helpers['ENGINE_INTEGER_OPTIONS'].keys()
        self.base = {k: v for k, v in tomllib.loads(generated.read_text()).items() if k in allowed}
        self.base.update(memory_cache_mib=64, disk_cache_mib=128, max_index_mib=64)
        self.options = {}
        names = ['scripts/run_adaptive_reads.py', 'scripts/run_astra_s3_operations.py',
                 'scripts/run_astra_s3_block_direct.py', 'scripts/run_astra_s3_block_sweep.py',
                 'scripts/run_astra_recovery.py', 'scripts/validate_vm.py']
        self.sources = {name: digest(ROOT / name) for name in names}
        self.sources[str(seed_report.relative_to(ROOT))] = digest(seed_report)
        self.report = {'schema_version': 1, 'complete': False, 'id': self.identity,
            'started_utc': utc(), 'binary_sha256': self.hashes, 'source_sha256': self.sources,
            'variants': {}, 'protocol': {'order': ['fixed64-r0', 'adaptive-r0', 'adaptive-r1', 'fixed64-r1'],
                'base_options': self.base, 'proxy': False, 'backend': 'Elestio S3 direct HTTPS',
                'fixture': 'Existing CRC32C seed, unchanged user data; fresh local cache per workload.',
                'workloads': '4096 random 4 KiB reads QD32; 256 MiB sequential reads 1 MiB QD16.',
                'counter_scope': 'Engine successful data range GETs and returned bytes only. Excludes HEAD, index, retries and failed requests; not total billable S3 operations.',
                'limits': 'Two ABBA ordered repetitions, shared VM/provider caches, no global cache drop. This isolates adaptive policy on the same candidate binary; historical native/ZeroFS benchmarks are not rerun.'}}

    async def execute(self):
        try:
            for name in self.report['protocol']['order']:
                self.verify()
                self.options[name] = {**self.base, 'adaptive_reads': name.startswith('adaptive')}
                fixture = DirectFixture(self, name, 'read')
                try:
                    await fixture.execute()
                    logs = sorted(fixture.directory.glob('server-*.log'),
                                  key=lambda p: int(p.stem.split('-')[-1]))
                    counters = []
                    for log in logs:
                        final = [line for line in log.read_text().splitlines() if 'volume final status' in line]
                        if len(final) != 1:
                            raise RuntimeError('missing unambiguous final engine counters')
                        line = final[0]
                        value = json.JSONDecoder().raw_decode(line[line.index('{"volume"'):])[0]
                        counters.append({k: value[k] for k in ('remote_gets', 'remote_bytes',
                            'adaptive_small_gets', 'adaptive_large_gets', 'range_cache_bytes')})
                    if len(counters) != 2:
                        raise RuntimeError('expected random and sequential fresh-process counters')
                    fixture.report['engine_data_counters'] = dict(zip(('random-read', 'sequential-read'), counters))
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
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        lock = (ROOT / 'validation/astra' / name).open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    asyncio.run(FocusedReads(args).execute())


if __name__ == '__main__':
    main()
