#!/usr/bin/env python3
"""Cost/performance sweep of existing S3 extent and WAL segment options."""
import argparse
import asyncio
import fcntl
import json
from pathlib import Path
import shutil

from run_astra_s3_operations import Campaign, Fixture, ROOT, DEVICE, preflight
from run_astra_recovery import atomic_json, digest, utc
from s3_request_pricing import project_events

OUT = ROOT / 'validation/astra/s3-blocks'
READ_EXTENTS = (16, 64, 256)
WRITE_SEGMENTS = (8, 16, 32, 64)
REPEATS = 3


class BlockFixture(Fixture):
    def __init__(self, campaign, name, kind):
        super().__init__(campaign, name, 'fio')
        self.kind = kind
        if kind == 'read':
            self.prefix = campaign.seed_prefix
        self.report['kind'] = kind
        self.report['options'] = campaign.options[name]
        self.report['samples'] = {}

    async def measure(self, label, **options):
        output = self.directory / (label + '.json')
        opts = {'name': label, 'filename': str(DEVICE), 'ioengine': 'libaio', 'direct': 1,
                'numjobs': 1, 'group_reporting': 1, 'size': '256m', 'offset': '16m',
                'randseed': 42, 'randrepeat': 1, 'output-format': 'json',
                'output': str(output), **options}
        await self.command(['fio', *['--' + key + '=' + str(value) for key, value in opts.items()]],
                           label, timeout=1800)
        jobs = json.loads(output.read_text())['jobs']
        if len(jobs) != 1 or jobs[0]['error']:
            raise RuntimeError('fio error or unexpected grouping in ' + label)
        result = jobs[0]
        self.report['samples'][label] = result
        return result

    async def fio(self):
        if self.kind in ('seed', 'write'):
            self.phase('init')
            await self.start_storage(fresh=True)
            self.phase('bulk_write')
            result = await self.measure('bulk-write', rw='write', bs='128k', iodepth=16,
                verify='crc32c', verify_interval='4k', verify_fatal=1, do_verify=1,
                refill_buffers=1, fsync_on_close=1)
            if result['write']['io_bytes'] != 256 * 1024**2:
                raise RuntimeError('bulk write byte count differs from protocol')
            self.report['integrity']['initial_crc32c'] = 'passed'
            if self.kind == 'write':
                self.phase('fsync_write')
                result = await self.measure('fsync-write', rw='randwrite', bs='4k', iodepth=1,
                    offset='512m', io_size='16m', fsync=1, refill_buffers=1)
                if result['write']['total_ios'] != 4096 or not result['sync']['total_ios']:
                    raise RuntimeError('write/fsync count differs from protocol')
            self.phase('drain')
            await self.stop_storage()
            if self.kind == 'seed':
                self.report['immutable_read_dataset'] = {
                    'bytes': 256 * 1024**2, 'offset_bytes': 16 * 1024**2,
                    'crc_granularity_bytes': 4096, 'prefix': self.prefix}
                return
            self.phase('restore_open')
            await self.start_storage(cold=True)
            self.phase('restore_check')
            result = await self.measure('restored-crc32c', rw='read', bs='128k', iodepth=16,
                verify='crc32c', verify_interval='4k', verify_fatal=1, verify_only=1)
            if result['read']['io_bytes'] != 256 * 1024**2:
                raise RuntimeError('restored checksum byte count differs')
            self.report['integrity']['remote_crc32c'] = 'passed'
            self.phase('restore_drain')
            await self.stop_storage()
            return
        self.phase('random_open')
        await self.start_storage(cold=True)
        self.phase('random_read')
        result = await self.measure('random-read', rw='randread', bs='4k', iodepth=32,
            io_size='16m', verify='crc32c', verify_interval='4k',
            verify_fatal=1, verify_only=1)
        if result['read']['io_bytes'] != 16 * 1024**2:
            raise RuntimeError('random read byte count differs')
        self.report['integrity']['random_crc32c'] = 'passed'
        self.phase('random_drain')
        await self.stop_storage()
        self.phase('sequential_open')
        await self.start_storage(cold=True)
        self.phase('sequential_read')
        result = await self.measure('sequential-read', rw='read', bs='1m', iodepth=16,
            verify='crc32c', verify_interval='4k', verify_fatal=1, verify_only=1)
        if result['read']['io_bytes'] != 256 * 1024**2:
            raise RuntimeError('sequential read byte count differs')
        self.report['integrity']['sequential_crc32c'] = 'passed'
        self.phase('sequential_drain')
        await self.stop_storage()


class Sweep(Campaign):
    def __init__(self, args):
        super().__init__(args)
        self.output = OUT
        self.sources[str(Path(__file__).relative_to(ROOT))] = digest(Path(__file__))
        self.base_options = dict(self.options['astra'])
        self.seed_prefix = None
        self.report['kind'] = 'block-cost-sweep'
        self.report['protocol'] = {
            'read_extent_kib': READ_EXTENTS, 'segment_mib': WRITE_SEGMENTS,
            'repeats': REPEATS, 'base_options': self.base_options,
            'data_bytes': 256 * 1024**2, 'random_io_bytes': 16 * 1024**2,
            'read': 'Same immutable seeded data; 4096 random 4 KiB reads QD32, then 256 MiB sequential 1 MiB reads QD16. Fresh local cache before each workload; checksum on every read.',
            'write': 'Both compact_checkpoints=true and false, with segment sizes 8/16/32/64 MiB. Fresh volume for each sample; 256 MiB sequential 128 KiB writes + CRC verification, then 4096 random 4 KiB writes with fsync in a disjoint zone. Count through clean drain, then verify the 256 MiB dataset from S3 with a new local directory.',
            'order': 'One shared seed. Balanced read orders: 16/64/256, 64/256/16, 256/16/64. Rotated segment orders, alternating compaction order per round, three rounds.',
            'cache': '64 MiB RAM, 128 MiB SSD, 64 MiB hot WAL; Linux/provider caches not purged. Process restart empties RAM; cold adopt creates a new local directory.',
            'contract': 'Local fsync, S3 asynchronous, 5 second checkpoint cadence; final drain included in write cost.',
            'limits': 'Instrumented sequential diagnostics on shared VM, not a production forecast. Proxy changes timings. Read range tested separately from the segment-size/compaction matrix. Logical pages remain 4 KiB. Compact output is grouped per 4096-page index shard, so WAL segment size does not directly size compact S3 objects. Read-only adoption can update ownership metadata but does not change the seeded user data. No new crash-safety claim for each tuning value.',
        }

    async def case(self, name, kind, changes):
        self.verify()
        self.options[name] = {**self.base_options, **changes}
        fixture = BlockFixture(self, name, kind)
        try:
            result = await fixture.execute()
        finally:
            self.report['variants'][name] = fixture.report
            self.export()
        if kind == 'seed':
            self.seed_prefix = fixture.prefix
        windows = (('fio.bulk_write', 'fio.fsync_write', 'fio.drain'),) if kind == 'write' else ()
        for phases in windows:
            result['write_accounting'] = project_events([event for event in fixture.events if event['phase'] in phases])
            result['write_accounting']['phases'] = list(phases)
        # Only this completed fixture's disposable local directories are removed.
        # Remote data, configs, raw logs and exported proofs remain available.
        removed = []
        for path in fixture.directory.glob('local-*'):
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
                removed.append(path.name)
        result['local_disposable_directories_removed_after_success'] = removed
        self.report['variants'][name] = result
        atomic_json(fixture.directory / 'report.json', result)
        self.export()
        print('COMPLETE', name, flush=True)

    async def execute(self):
        try:
            await self.case('read-seed', 'seed', {})
            for repeat in range(REPEATS):
                order = READ_EXTENTS[repeat:] + READ_EXTENTS[:repeat]
                for extent in order:
                    await self.case(f'read-{extent}-r{repeat}', 'read', {'read_extent_kib': extent})
            for repeat in range(REPEATS):
                order = WRITE_SEGMENTS[repeat:] + WRITE_SEGMENTS[:repeat]
                for segment in order:
                    for compact in ((True, False) if repeat % 2 == 0 else (False, True)):
                        prefix = 'write' if compact else 'writefull'
                        await self.case(f'{prefix}-{segment}-r{repeat}', 'write',
                                        {'segment_mib': segment, 'compact_checkpoints': compact})
            self.verify()
            self.report['complete'] = True
        except BaseException as error:
            self.report['error'] = type(error).__name__ + ': ' + str(error)
            raise
        finally:
            self.report['ended_utc'] = utc()
            self.export()
            print('REPORT', self.output / 'report.json', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/infinidisk2-astra-b39b705f43b6')
    parser.add_argument('--baseline', type=Path, default=ROOT / 'target/release/infinidisk2-pre-astra')
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    if args.plan_only:
        print(json.dumps({'read_extent_kib': READ_EXTENTS, 'segment_mib': WRITE_SEGMENTS,
                          'compact_checkpoints': [True, False],
                          'repeats': REPEATS, 'output': str(OUT)}, indent=2))
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
    asyncio.run(Sweep(args).execute())


if __name__ == '__main__':
    main()
