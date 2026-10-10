#!/usr/bin/env python3
"""Reuse the exact tested 8 MiB build, verify generated profiles, preserve history."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tomllib

from run_astra_recovery import atomic_json, utc

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'validation/downloads/default-profile'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if OUT.exists():
        raise RuntimeError('refusing to overwrite promotion evidence')
    build_path = ROOT / 'validation/downloads/build/manifest.json'
    build = json.loads(build_path.read_text())
    assert build['complete']
    assert all(sha(ROOT / name) == expected for name, expected in build['source_sha256'].items())
    binary = Path(build['binary_path'])
    assert sha(binary) == build['binary_sha256']
    intermediate = json.loads((ROOT / 'validation/downloads/build-final/manifest.json').read_text())
    for name in intermediate['changed_sources']:
        assert sha(ROOT / 'validation/downloads/post-measurement-source' / name) == intermediate['source_sha256'][name]
    OUT.mkdir()
    result = {'complete': False, 'started_utc': utc(), 'binary_path': str(binary),
              'binary_sha256': build['binary_sha256'], 'source_sha256': build['source_sha256'],
              'reused_build_manifest_sha256': sha(build_path), 'checks': {},
              'decision': 'Promote 8MiB/64 for the overall tradeoff after Joseph questioned the default. The original sequential p99 criterion remains failed; measurements and thresholds are unchanged.',
              'scope': 'New generated configs only; omitted legacy fields retain0. Exact previously measured Rust sources and binary; no repeat perf matrix or debug rebuild.'}
    try:
        for legacy in (False, True):
            label = 'legacy' if legacy else 'recommended'
            path = OUT / (label + '.toml')
            command = [str(binary), '-c', str(path), 'config', *(['--legacy'] if legacy else [])]
            run = subprocess.run(command, capture_output=True, text=True, check=True)
            (OUT / (label + '.log')).write_text(run.stdout + run.stderr)
            config = tomllib.loads(path.read_text())
            assert config['download_budget_mib'] == (0 if legacy else 8)
            assert config['download_max_requests'] == 64
            if not legacy:
                example = tomllib.loads((ROOT / 'configs/recommended.toml').read_text())
                for key, value in example.items():
                    if key not in {'local_dir', 'store', 'endpoint', 'region', 'listen'}:
                        assert config[key] == value, key
            before = sha(path)
            repeated = subprocess.run(command, capture_output=True, text=True)
            assert repeated.returncode != 0 and sha(path) == before
            result['checks'][label] = {'generated_config_sha256': before,
                'download_budget_mib': config['download_budget_mib'],
                'download_max_requests': config['download_max_requests'],
                'overwrite_refused': True}
        # This only updates the checkout's executable; no service is restarted.
        target = ROOT / 'target/release/infinidisk2'
        result['previous_checkout_binary_sha256'] = sha(target)
        temporary = target.with_name('infinidisk2.promoting')
        shutil.copy2(binary, temporary)
        assert sha(temporary) == build['binary_sha256']
        temporary.replace(target)
        result['checkout_binary_sha256'] = sha(target)
        result['complete'] = True
    finally:
        result['ended_utc'] = utc()
        atomic_json(OUT / 'manifest.json', result)
        print(json.dumps({'complete': result['complete'], 'binary_sha256': result['binary_sha256'],
                          'checks': result['checks']}))


if __name__ == '__main__':
    main()
