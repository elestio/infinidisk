#!/usr/bin/env python3
"""Export flat evidence only; never export cache, WAL, databases or credentials."""
import argparse,json,pathlib,re,shlex,sys,tarfile
root=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('report',type=pathlib.Path);p.add_argument('--extra-report',type=pathlib.Path);p.add_argument('--allow-incomplete',action='store_true');a=p.parse_args()
secrets=[]
for line in pathlib.Path('/opt/elestio/infinidisk/bench.env').read_text().splitlines():
    if '=' in line and not line.startswith('#'):
        k,v=line.split('=',1)
        if k in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN','INFINIDISK_PASSWORD'):secrets.extend(s.encode() for s in shlex.split(v) if len(s)>8)
folders={}
def add(label,path):
    path=path.resolve()
    if path.parent!=root/'test-output' or not path.name.startswith(('breakthrough-','comparison-','run-','fsync-layout-')):raise RuntimeError('not an isolated test path')
    if path in folders.values():return
    folders[label]=path
for label,report in [('campaign',a.report),('fixed-campaign',a.extra_report)]:
    if report is None:continue
    d=json.loads(report.read_text())
    if not a.allow_incomplete and not d.get('complete'):raise RuntimeError('campaign incomplete')
    add(label,report.parent)
    for stage in d['stages']:
        args=stage['arguments']
        for flag in ('--mysql-repeat-report','--mysql-recovery-report'):
            if flag in args:
                fixture=pathlib.Path(args[args.index(flag)+1]);add('mysql-small' if '323bb1097b47' in str(fixture) else 'mysql-large',fixture.parent)
    for log in report.parent.glob('*.log'):
        for match in re.finditer(r'(?:REPORT|Report:)\s+(/root/infinidisk2/test-output/[^\s]+\.json)',log.read_text()):
            f=pathlib.Path(match[1]);add(f.parent.name,f.parent)
    probe=report.parent/'fsync-layout.log'
    if probe.exists():
        for line in probe.read_text().splitlines():
            if line.startswith(str(root/'test-output'/'fsync-layout-')):add('fsync-layout',pathlib.Path(line).parent)
for work in folders.values():secrets.extend(f.read_bytes().strip() for f in work.glob('*.secret') if len(f.read_bytes().strip())>8)
files=[]
for label,work in folders.items():
    for f in sorted(work.iterdir()):
        if f.is_file() and f.suffix in ('.json','.log','.toml'):
            if any(s in f.read_bytes() for s in secrets):raise RuntimeError('secret in evidence: '+f.name)
            files.append((label,f))
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz') as t:
    for label,f in files:t.add(f,arcname=label+'/'+f.name,recursive=False)
