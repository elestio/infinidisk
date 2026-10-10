#!/usr/bin/env python3
"""Build and target the read-admission changes; archive source and binary hashes."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

from run_astra_recovery import atomic_json, digest, utc

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final', action='store_true', help='Qualify only the default-policy change after measurements')
    args = parser.parse_args()
    out = ROOT / 'validation/downloads' / ('build-final' if args.final else 'build')
    if out.exists():
        raise RuntimeError('refusing to overwrite build evidence')
    out.mkdir(parents=True)
    env = dict(os.environ, CARGO_HOME=str(ROOT / '.cargo'),
               RUSTUP_HOME=str(ROOT / '.rustup'), TMPDIR=str(ROOT / 'test-output/astra-tmp'))
    cargo = str(ROOT / '.cargo/bin/cargo')
    commands = {
        'fmt': [cargo, 'fmt', '--all', '--', '--check'],
        'downloads': [cargo, 'test', '--locked', '--features', 'ublk', '--lib', 'download'],
        'adaptive': [cargo, 'test', '--locked', '--features', 'ublk', '--lib', 'adaptive'],
        'profile': [cargo, 'test', '--locked', '--features', 'ublk', '--test', 'selected_profile'],
        'engine': [cargo, 'test', '--locked', '--features', 'ublk', '--test', 'astra_engine'],
        'clippy': [cargo, 'clippy', '--locked', '--features', 'ublk', '--all-targets', '--', '-D', 'warnings'],
        'release': [cargo, 'build', '--locked', '--release', '--features', 'ublk', '-j', '2'],
    }
    paths = [ROOT / 'Cargo.toml', ROOT / 'Cargo.lock', *ROOT.glob('src/**/*.rs'), *ROOT.glob('tests/**/*.rs')]
    sources = {str(p.relative_to(ROOT)): digest(p) for p in sorted(paths)}
    report = {'complete': False, 'started_utc': utc(), 'source_sha256': sources,
              'commands': commands, 'checks': {}, 'scope': 'Online data range admission and recovery regressions.'}
    if args.final:
        previous = ROOT / 'validation/downloads/build/manifest.json'
        measured = json.loads(previous.read_text())
        changed = {name for name, sha in sources.items() if measured['source_sha256'].get(name) != sha}
        if not measured['complete'] or changed != {'src/config.rs', 'tests/selected_profile.rs'}:
            raise RuntimeError('final qualification may only change the generated default and its contract test')
        # Preserve exact measured sources and make the sole behavior difference reviewable.
        for name in changed:
            saved = ROOT / 'validation/downloads/measured-source' / name
            if digest(saved) != measured['source_sha256'][name]:
                raise RuntimeError('measured source archive differs')
        commands = {key: value for key, value in commands.items() if key in {'fmt', 'profile', 'clippy', 'release'}}
        report.update(commands=commands, inherited_checks_from=str(previous.relative_to(ROOT)),
                      inherited_manifest_sha256=digest(previous), changed_sources=sorted(changed))
    try:
        for name, command in commands.items():
            log = out / (name + '.log')
            with log.open('w') as handle:
                result = subprocess.run(command, env=env, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT)
            report['checks'][log.name] = {'returncode': result.returncode, 'sha256': digest(log)}
            print(name, result.returncode, flush=True)
            if result.returncode:
                raise RuntimeError('failed build check: ' + name)
        if any(digest(ROOT / name) != sha for name, sha in sources.items()):
            raise RuntimeError('source changed during build')
        binary = ROOT / 'target/release/infinidisk'
        sha = digest(binary)
        frozen = binary.with_name('infinidisk2-downloads-' + sha[:12])
        shutil.copy2(binary, frozen)
        report.update(complete=True, binary_sha256=sha, binary_path=str(frozen))
    finally:
        report['ended_utc'] = utc()
        atomic_json(out / 'manifest.json', report)
        print(json.dumps({'complete': report['complete'], 'binary': report.get('binary_path')}), flush=True)


if __name__ == '__main__':
    main()
