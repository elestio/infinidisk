#!/usr/bin/env python3
"""Focused complete S3 accounting: selected profile, hot/cold PG and index ABBA.

Reuses the already validated proxy/fixture, without the native/ZeroFS/fio matrix.
"""
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

from run_astra_recovery import atomic_json, digest, option_helpers, utc
from run_astra_s3_operations import Campaign, Fixture, ROOT, credentials, preflight
from s3_request_pricing import PRICING_FILE

OUT = ROOT / 'validation/index-cache/restart'


class RestartFixture(Fixture):
    async def postgres(self):
        self.report['selected_options'] = dict(self.campaign.options['astra'])
        await super().postgres()
        self.phase('metadata_prepare')
        _, text = await self.command([self.binary, '-c', self.cfg, 'status'], 'head-before-metadata')
        before = json.loads(text)
        self.report['metadata_shards'] = len(before['shards'])
        if not before['shards']:
            raise RuntimeError('no remote index to qualify')
        self.report['metadata_repeats'] = []
        for label, budget in [('off-r0', 0), ('on-r0', 128), ('on-r1', 128), ('off-r1', 0)]:
            self.campaign.options['astra']['remote_index_cache_mib'] = budget
            logs = set(self.directory.glob('server-*.log'))
            self.phase(label + '_open')
            await self.start_storage()
            self.phase(label + '_drain')
            await self.stop_storage()
            new = set(self.directory.glob('server-*.log')) - logs
            if len(new) != 1:
                raise RuntimeError('ambiguous server metrics')
            final = [line for line in next(iter(new)).read_text().splitlines() if 'volume final status' in line]
            if len(final) != 1:
                raise RuntimeError('missing final index cache metrics')
            line = final[0]
            stats = json.JSONDecoder().raw_decode(line[line.index('{"volume"'):])[0]
            cache = stats['index_object_cache']
            expected = self.report['metadata_shards']
            if budget and (cache['hits'] != expected or cache['remote_gets']):
                raise RuntimeError('hot metadata restart did not use verified cache')
            if not budget and (cache['hits'] or cache['remote_gets'] != expected):
                raise RuntimeError('bypass reference did not fetch every index')
            if cache['errors'] or cache['corruptions'] or stats['remote_gets']:
                raise RuntimeError('unexpected data GET or cache error during metadata-only restart')
            self.report['metadata_repeats'].append({'label': label, 'budget_mib': budget,
                                                     'index_object_cache': cache})
        self.campaign.options['astra']['remote_index_cache_mib'] = 128
        self.phase('metadata_check')
        _, text = await self.command([self.binary, '-c', self.cfg, 'status'], 'head-after-metadata')
        if json.loads(text) != before:
            raise RuntimeError('HEAD changed between controlled restarts')
        self.report['integrity']['metadata_restarts_same_HEAD'] = 'passed'


class Restarts(Campaign):
    def __init__(self, args):
        self.args = args
        self.output = OUT
        self.identity = uuid.uuid4().hex[:12]
        self.work = ROOT / 'test-output' / ('s3-operations-' + self.identity)
        self.work.mkdir(mode=0o700)
        self.private = credentials(args.credentials)
        self.secret_values = list(self.private.values())
        self.binaries = {'astra': args.binary.resolve()}
        self.hashes = {'astra': digest(args.binary)}
        if self.hashes['astra'] != args.expected_sha256:
            raise RuntimeError('binary differs from build manifest')
        generated = self.work / 'recommended.toml'
        subprocess.run([str(args.binary), '-c', str(generated), 'config'], check=True)
        helpers = option_helpers()
        allowed = helpers['ENGINE_BOOLEAN_OPTIONS'] | helpers['ENGINE_INTEGER_OPTIONS'].keys()
        options = {k: v for k, v in tomllib.loads(generated.read_text()).items() if k in allowed}
        options.update(memory_cache_mib=64, disk_cache_mib=128, max_index_mib=64)
        self.options = {'astra': options}
        self.image_id = subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Id}}', 'postgres:16'], text=True).strip()
        names = ['scripts/run_index_cache_restart.py', 'scripts/run_astra_s3_operations.py',
                 'scripts/run_astra_recovery.py', 'scripts/validate_vm.py',
                 'scripts/s3_counting_proxy.py', 'scripts/s3_request_pricing.py',
                 'scripts/run_astra_postgres_compare.py', 'scripts/run_astra_mysql.py',
                 str(PRICING_FILE.relative_to(ROOT)), 'validation/index-cache/build/manifest.json']
        self.sources = {name: digest(ROOT / name) for name in names}
        self.report = {'complete': False, 'id': self.identity, 'started_utc': utc(),
            'binary_sha256': self.hashes, 'source_sha256': self.sources, 'variants': {},
            'protocol': {'options': dict(options), 'backend': 'Elestio S3 through verified HTTPS accounting proxy',
                'postgres': 'One fresh scale2 DB, 256 transactions, 1280 business SQL statements; CPU1, memory512MiB, all fsync/checksum settings enabled.',
                'restarts': 'Warm same local directory; cold fresh directory + adoption. Database integrity checked after both. Both use cache enabled128MiB.',
                'metadata': 'ABBA off/on/on/off on identical HEAD and same local files, two repeats each; only index cache budget differs 0/128MiB. No application I/O between these four opens.',
                'cost_scope': 'All HTTP attempts including retries. Query costs cover load + orderly drain; setup/restarts separate. Pricing projected on Tigris, not provider invoice.',
                'timings': 'Instrumented diagnostic durations; proxy and shared VM caches affect them, not a direct performance benchmark.'}}

    async def execute(self):
        fixture = RestartFixture(self, 'astra', 'postgres')
        try:
            self.verify()
            await fixture.execute()
            self.verify()
            if fixture.report['summary']['cost_has_unpriced_or_uncertain_requests']:
                raise RuntimeError('request accounting contains uncertainty')
            self.report['complete'] = True
        except BaseException as error:
            self.report['error'] = type(error).__name__ + ': ' + str(error)
            raise
        finally:
            self.report['variants']['astra'] = fixture.report
            self.report['ended_utc'] = utc()
            self.export()
            print('REPORT', OUT / 'report.json', flush=True)
        # Keep all text proofs, remove only this completed fixture's local states.
        for path in fixture.directory.glob('local-*'):
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--expected-sha256', required=True)
    parser.add_argument('--credentials', type=Path, default=Path('/opt/elestio/infinidisk/bench.env'))
    args = parser.parse_args()
    if OUT.exists():
        raise RuntimeError('refusing to overwrite a prior campaign')
    OUT.mkdir(parents=True)
    locks = []
    for name in ('mysql/campaign.lock', 's3-operations/campaign.lock'):
        lock = (ROOT / 'validation/astra' / name).open('a')
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        locks.append(lock)
    preflight()
    asyncio.run(Restarts(args).execute())


if __name__ == '__main__':
    main()
