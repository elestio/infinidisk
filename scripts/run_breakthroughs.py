#!/usr/bin/env python3
"""Sequential, isolated experiment matrix. Does not touch production volumes."""
import argparse,hashlib,json,pathlib,subprocess,sys,time,uuid
root=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--resume',type=pathlib.Path);a=p.parse_args()
work=a.resume.resolve().parent if a.resume else root/'test-output'/('breakthrough-'+uuid.uuid4().hex[:12])
if a.resume:
    if work.parent!=root/'test-output' or not work.name.startswith('breakthrough-'):raise RuntimeError('not an isolated campaign')
else:work.mkdir(mode=0o700)
small=root/'test-output/comparison-323bb1097b47/report.json'
large=root/'test-output/comparison-be4ba51a4a38/report.json'
for p in (small,large):
    if not json.loads(p.read_text()).get('complete'):raise RuntimeError('fixture is incomplete')
binary=hashlib.sha256((root/'target/release/infinidisk').read_bytes()).hexdigest()
report=json.loads(a.resume.read_text()) if a.resume else {'binary_sha256':binary,'work':str(work),'stages':[],'complete':False}
if a.resume and report.get('complete'):raise RuntimeError('campaign already complete')
files=sorted([root/'Cargo.toml',root/'Cargo.lock',*root.glob('src/*.rs'),*root.glob('tests/*.rs')])
source={str(f.relative_to(root)):{'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'content':f.read_text()} for f in files}
suffix='-resume' if a.resume else ''
(work/('source-snapshot'+suffix+'.json')).write_text(json.dumps(source,indent=2))
(work/('build-manifest'+suffix+'.json')).write_text(json.dumps({'binary_sha256':binary,'source_files_sha256':{k:v['sha256'] for k,v in source.items()},'feature':'ublk'},indent=2))
if a.resume:report.setdefault('resumed_binaries',[]).append(binary)
def save():(work/'report.json').write_text(json.dumps(report,indent=2))
def run(label,args):
    if any(s['label']==label and s['returncode']==0 for s in report['stages']):return
    old=work/(label+'.log')
    if old.exists():old.rename(work/(label+'-failed-'+str(time.time_ns())+'.log'))
    print('STAGE '+label,flush=True);started=time.monotonic()
    with (work/(label+'.log')).open('w') as log:
        p=subprocess.run(args,cwd=root,stdout=log,stderr=subprocess.STDOUT)
    report['stages'].append({'label':label,'returncode':p.returncode,'seconds':time.monotonic()-started,'arguments':list(map(str,args)),'binary_sha256':binary});save()
    if p.returncode:raise RuntimeError(label+' failed; '+str(work/(label+'.log')))
def mysql(label,fixture,options,works=('read_only',),skip=True):
    args=[sys.executable,'scripts/compare_zerofs.py','--mysql-repeat-report',str(fixture),'--phase-label',label,'--engine','infinidisk2','--mysql-workloads',*works,'--mysql-seconds','15','--mysql-samples','3','--mysql-warm-seconds','60' if fixture==large else '10','--mysql-rand-type','uniform' if fixture==large else 'special',*options]
    if skip:args+=['--mysql-skip-crash-check']
    run(label,args)
    source=fixture.parent/('optimization-'+label+'.json')
    (work/(label+'.json')).write_text(source.read_text())
try:
    save()
    run('fsync-layout',[sys.executable,'scripts/probe_fsync_layout.py'])
    for label,commit,batch in [('wal-reference','off','0'),('wal-one-barrier','on','0'),('wal-batch-100','on','100'),('wal-batch-500','on','500'),('wal-reference-after','off','0')]:
        mysql(label,small,['--logical-cache','off','--wal-commit-records',commit,'--flush-batch-us',batch],works=('write_only',),skip=False)
    for label,logical,budget in [('extent-128','off','128'),('logical-128','on','128'),('logical-2048','on','2048')]:
        mysql(label,large,['--logical-cache',logical,'--wal-commit-records','off','--flush-batch-us','0','--read-extent-kib','64','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib',budget])
    mysql('compact-extent-128',large,['--logical-cache','off','--wal-commit-records','off','--flush-batch-us','0','--read-extent-kib','64','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','128','--offline-compact'])
    mysql('compact-logical-128',large,['--logical-cache','on','--wal-commit-records','off','--flush-batch-us','0','--read-extent-kib','64','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','128'])
    mysql('local-active-nbd',large,['--logical-cache','on','--wal-commit-records','off','--flush-batch-us','0','--read-extent-kib','64','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','4096','--offline-warm'])
    if a.resume:
        mysql('local-active-nbd-recheck',large,['--logical-cache','on','--wal-fixed-size','off','--wal-commit-records','off','--flush-batch-us','0','--read-extent-kib','64','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','4096'])
    mysql('local-active-ublk',large,['--logical-cache','on','--wal-commit-records','off','--flush-batch-us','0','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','4096','--transport','ublk'])
    mysql('local-active-mixed',large,['--logical-cache','on','--wal-commit-records','on','--flush-batch-us','0','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','4096'],works=('read_write','write_only'),skip=False)
    run('s3-postgres-recovery',[sys.executable,'scripts/validate_vm.py','--s3','--credentials','/opt/elestio/infinidisk/bench.env','--postgres','--logical-cache','--wal-commit-records'])
    # Existing small fixture was updated with the experimental config by the repeats.
    # Recovery uses a phase label and explicit flags to avoid relying on defaults.
    run('mysql-engine-recovery',[sys.executable,'scripts/compare_zerofs.py','--mysql-recovery-report',str(small),'--phase-label','breakthrough-recovery','--engine','infinidisk2','--logical-cache','on','--wal-commit-records','on'])
    report['complete']=True;save()
finally:print('CAMPAIGN '+str(work/'report.json'),flush=True)
