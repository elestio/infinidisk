#!/usr/bin/env python3
"""Export only flat benchmark evidence, excluding WAL, cache, data and secrets."""
import argparse,json,pathlib,shlex,sys,tarfile
ROOT=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--raw',required=True,type=pathlib.Path)
p.add_argument('--postgres',required=True,type=pathlib.Path)
p.add_argument('--mysql',type=pathlib.Path)
p.add_argument('--crash',type=pathlib.Path)
a=p.parse_args()
secrets=[]
for line in pathlib.Path('/opt/elestio/infinidisk/bench.env').read_text().splitlines():
    if '=' not in line or line.startswith('#'):continue
    k,v=line.split('=',1)
    if k in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN','INFINIDISK_PASSWORD'):
        secrets.extend(s.encode() for s in shlex.split(v) if len(s)>8)
files=[]
for label,path in [('raw',a.raw),('postgres',a.postgres),('mysql',a.mysql),('crash',a.crash)]:
    if path is None:continue
    work=path.resolve().parent
    if work.parent!=ROOT/'test-output' or not work.name.startswith('run-' if label=='crash' else 'comparison-'):raise RuntimeError('not an isolated comparison')
    if label!='crash' and not json.loads(path.read_text()).get('complete'):raise RuntimeError('comparison unfinished')
    secrets.extend(f.read_bytes().strip() for f in work.glob('*.secret') if len(f.read_bytes().strip())>8)
    for f in sorted(work.iterdir()):
        if f.is_file() and f.suffix in ('.json','.log','.toml'):
            if any(s in f.read_bytes() for s in secrets):raise RuntimeError('secret in evidence: '+f.name)
            files.append((label,f))
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz') as tar:
    for label,f in files:tar.add(f,arcname=label+'/'+f.name,recursive=False)
