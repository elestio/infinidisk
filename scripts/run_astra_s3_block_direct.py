#!/usr/bin/env python3
"""Confirm block sweep timings without the HTTP accounting proxy."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import re
import shutil
import statistics
from types import SimpleNamespace

from run_astra_s3_operations import Campaign, ROOT, preflight
from run_astra_s3_block_sweep import BlockFixture, READ_EXTENTS
from run_astra_recovery import atomic_json, digest, utc

OUT = ROOT / 'validation/astra/s3-blocks-direct'
INPUT = ROOT / 'validation/astra/s3-blocks/report.json'


def selected_write_profiles(report):
    """Declare selection before direct measurements, using cost, then fsync p99."""
    selected = {(True, 8), (False, 32)}
    for kind, compact in (('write', True), ('writefull', False)):
        choices = []
        for size in (8, 16, 32, 64):
            cases = [report['variants'][f'{kind}-{size}-r{repeat}'] for repeat in range(3)]
            if any(case['write_accounting']['cost_has_unpriced_or_uncertain_requests'] for case in cases):
                raise RuntimeError('cannot select a write cost winner from incomplete pricing')
            costs = [case['write_accounting']['estimated_gross_request_cost_usd'] for case in cases]
            p99 = [case['samples']['fsync-write']['sync']['lat_ns']['percentile']['99.000000'] for case in cases]
            choices.append((statistics.median(costs), statistics.median(p99), size))
        selected.add((compact, min(choices)[2]))
    return sorted(selected, key=lambda value: (not value[0], value[1]))


class DirectFixture(BlockFixture):
    async def open_proxy(self):
        self.env.update({key: value for key, value in self.campaign.private.items() if key.startswith('AWS_')})
        self.report['http_accounting'] = 'not measured: direct Rust to S3, no proxy'

    def configuration(self):
        # Reuse the same config encoder, without changing the active benchmark
        # source or creating a listener. No actual credentials enter the TOML.
        self.proxy = SimpleNamespace(listen='https://storage.elestio.com')
        try:
            super().configuration()
        finally:
            self.proxy = None


class DirectCampaign(Campaign):
    def __init__(self, args):
        super().__init__(args)
        self.output = OUT
        source = json.loads(INPUT.read_text())
        if not source.get('complete') or source['binary_sha256']['astra'] != self.hashes['astra']:
            raise RuntimeError('instrumented block sweep must finish on the same binary first')
        for name, expected in source['source_sha256'].items():
            if digest(ROOT / name) != expected:
                raise RuntimeError('block sweep source changed before direct confirmation')
        self.seed_prefix = source['variants']['read-seed']['immutable_read_dataset']['prefix']
        if not re.fullmatch(r'infinidisk2-s3-operations-[a-f0-9]{12}/read-seed/fio', self.seed_prefix):
            raise RuntimeError('direct reads require the dedicated immutable test seed')
        self.base_options = source['protocol']['base_options']
        self.selected = selected_write_profiles(source)
        for path in (Path(__file__), ROOT / 'scripts/run_astra_s3_block_sweep.py', INPUT):
            self.sources[str(path.relative_to(ROOT))] = digest(path)
        self.report['kind'] = 'direct-block-timing-confirmation'
        self.report['protocol'] = {
            'engine': 'Astra only', 'proxy': False, 'http_operations': 'not measured',
            'source_accounting_report': str(INPUT.relative_to(ROOT)),
            'source_accounting_report_sha256': digest(INPUT),
            'read_extent_kib': READ_EXTENTS, 'repeats': 3, 'base_options': self.base_options,
            'selected_write_profiles': [{'compact_checkpoints': compact, 'segment_mib': size} for compact, size in self.selected],
            'selection_rule': 'Always include compact 8 MiB and noncompact 32 MiB to compare the intermediate segment size with the fee winner. Within each compaction mode select the lowest median measured request fee; tie-break by median fsync p99, then segment size. Selection occurs before direct timing.',
            'read': 'Same immutable 256 MiB dataset as the accounting sweep; new local directory before each workload. 4096 random 4 KiB reads QD32 and complete sequential 1 MiB reads QD16; CRC on all reads.',
            'write': 'Same fixed workload as accounting on fresh volumes; clean publication drain and fresh-local remote CRC verification.',
            'limits': 'Sequential shared VM; Linux/provider caches not purged. Accounting and direct timings are separate runs: do not represent the instrumented request count as a count observed in the direct run. Three repeats do not establish a universal optimum.',
        }

    async def case(self, name, kind, changes):
        self.verify()
        self.options[name] = {**self.base_options, **changes}
        fixture = DirectFixture(self, name, kind)
        try:
            await fixture.execute()
        finally:
            result = fixture.report
            # The common fixture emits empty accounting for native/no-proxy
            # runs. Here zero would be misleading, so remove those counters.
            result.pop('summary', None)
            for phase in result['phases'].values():
                phase.pop('summary', None)
            (fixture.directory / 'requests.jsonl').unlink(missing_ok=True)
            self.report['variants'][name] = result
            atomic_json(fixture.directory / 'report.json', result)
            self.export()
        removed = []
        for path in fixture.directory.glob('local-*'):
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
                removed.append(path.name)
        result['local_disposable_directories_removed_after_success'] = removed
        atomic_json(fixture.directory / 'report.json', result)
        self.export()
        print('COMPLETE', name, flush=True)

    async def execute(self):
        try:
            for repeat in range(3):
                for extent in READ_EXTENTS[repeat:] + READ_EXTENTS[:repeat]:
                    await self.case(f'read-{extent}-r{repeat}', 'read', {'read_extent_kib': extent})
            for repeat in range(3):
                rotated = self.selected[repeat % len(self.selected):] + self.selected[:repeat % len(self.selected)]
                for compact, size in rotated:
                    name = ('write' if compact else 'writefull') + f'-{size}-r{repeat}'
                    await self.case(name, 'write', {'compact_checkpoints': compact, 'segment_mib': size})
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
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/infinidisk2-astra-b39b705f43b6')
    parser.add_argument('--baseline', type=Path, default=ROOT / 'target/release/infinidisk2-pre-astra')
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    if args.plan_only:
        print(json.dumps({'proxy': False, 'read_extents_kib': READ_EXTENTS,
            'read_repeats': 3, 'write_profiles': 'compact 8 MiB + noncompact 32 MiB + fee winner of each compaction mode',
            'write_repeats': 3, 'accounting': 'not measured; separately archived'}, indent=2))
        return
    OUT.mkdir(parents=True, exist_ok=True)
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        lock = (OUT.parent / name).open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    asyncio.run(DirectCampaign(args).execute())


if __name__ == '__main__':
    main()
