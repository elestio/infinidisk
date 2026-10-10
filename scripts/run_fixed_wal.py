#!/usr/bin/env python3
"""Build current sources, then measure factorial WAL layout and crash recovery."""
import argparse,hashlib,json,os,pathlib,subprocess,sys,time,uuid
root=pathlib.Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser();p.add_argument('--resume',type=pathlib.Path);a=p.parse_args()
work=a.resume.resolve().parent if a.resume else root/'test-output'/('breakthrough-fixed-'+uuid.uuid4().hex[:12])
if a.resume:
    if work.parent!=root/'test-output' or not work.name.startswith('breakthrough-fixed-'):raise RuntimeError('not an isolated campaign')
else:work.mkdir(mode=0o700)
suffix='-resume' if a.resume else ''
build_env=os.environ.copy();build_env.update(CARGO_HOME=str(root/'.cargo'),RUSTUP_HOME=str(root/'.rustup'))
build_env['PATH']=str(root/'.cargo/bin')+':'+build_env.get('PATH','')
checks=[]
with (work/('build-checks'+suffix+'.log')).open('w') as log:
    for command in [['cargo','fmt','--all','--check'],['cargo','test','--features','ublk','--locked','-j','2'],['cargo','clippy','--features','ublk','--locked','--all-targets','-j','2','--','-D','warnings'],['nice','-n','10','cargo','build','--features','ublk','--release','--locked','-j','2']]:
        print('CHECK '+' '.join(command),flush=True);start=time.monotonic()
        result=subprocess.run(command,cwd=root,env=build_env,stdout=log,stderr=subprocess.STDOUT)
        checks.append({'command':command,'returncode':result.returncode,'seconds':time.monotonic()-start})
        (work/('build-checks'+suffix+'.json')).write_text(json.dumps(checks,indent=2))
        if result.returncode:raise RuntimeError('build/check failed: '+str(work/'build-checks.log'))
small=root/'test-output/comparison-323bb1097b47/report.json';large=root/'test-output/comparison-be4ba51a4a38/report.json'
binary=hashlib.sha256((root/'target/release/infinidisk2').read_bytes()).hexdigest()
report=json.loads(a.resume.read_text()) if a.resume else {'work':str(work),'binary_sha256':binary,'stages':[],'complete':False}
if a.resume:
    if report.get('complete'):raise RuntimeError('campaign already complete')
    report.setdefault('resumed_binaries',[]).append(binary)
files=sorted([root/'Cargo.toml',root/'Cargo.lock',*root.glob('src/*.rs'),*root.glob('tests/*.rs')])
source={str(f.relative_to(root)):{'sha256':hashlib.sha256(f.read_bytes()).hexdigest(),'content':f.read_text()} for f in files}
(work/('source-snapshot'+suffix+'.json')).write_text(json.dumps(source,indent=2))
(work/('build-manifest'+suffix+'.json')).write_text(json.dumps({'binary_sha256':binary,'source_files_sha256':{k:v['sha256'] for k,v in source.items()},'feature':'ublk','base_commit':'17aeec7278b188fa3de7ad0d58b212e126106736'},indent=2))
def save():(work/'report.json').write_text(json.dumps(report,indent=2))
def run(label,args):
    if any(s['label']==label and s['returncode']==0 for s in report['stages']):return
    old=work/(label+'.log')
    if old.exists():old.rename(work/(label+'-failed-'+str(time.time_ns())+'.log'))
    print('STAGE '+label,flush=True);start=time.monotonic()
    with (work/(label+'.log')).open('w') as log:p=subprocess.run(args,cwd=root,stdout=log,stderr=subprocess.STDOUT)
    report['stages'].append({'label':label,'returncode':p.returncode,'seconds':time.monotonic()-start,'arguments':list(map(str,args)),'binary_sha256':binary});save()
    if p.returncode:raise RuntimeError(label+' failed: '+str(work/(label+'.log')))
try:
    save()
    if a.resume and pathlib.Path('/dev/ublkc31').exists():
        run('ublk-stale-control-cleanup',[str(root/'target/release/infinidisk2'),'-c',str(small.parent/'infinidisk2.toml'),'ublk-delete','--id','31'])
    for label,fixed,commit in [('fixed-reference','off','off'),('fixed-only','on','off'),('fixed-growing-one-barrier','off','on'),('fixed-one-barrier','on','on'),('fixed-reference-after','off','off')]:
        run(label,[sys.executable,'scripts/compare_zerofs.py','--mysql-repeat-report',str(small),'--phase-label',label,'--engine','infinidisk2','--mysql-workloads','write_only','--logical-cache','off','--wal-fixed-size',fixed,'--wal-commit-records',commit,'--flush-batch-us','0','--mysql-warm-seconds','10'])
    run('native-final',[sys.executable,'scripts/compare_zerofs.py','--mysql-repeat-report',str(large),'--phase-label','native-final','--engine','native','--mysql-workloads','read_only','read_write','write_only','--mysql-rand-type','uniform','--mysql-warm-seconds','60'])
    run('s3-fixed-recovery',[sys.executable,'scripts/validate_vm.py','--s3','--credentials','/opt/elestio/infinidisk/bench.env','--postgres','--logical-cache','--wal-commit-records','--wal-fixed-size'])
    run('mysql-fixed-engine-recovery',[sys.executable,'scripts/compare_zerofs.py','--mysql-recovery-report',str(small),'--phase-label','breakthrough-fixed-recovery','--engine','infinidisk2','--logical-cache','on','--wal-fixed-size','on','--wal-commit-records','on'])
    run('mysql-ublk-engine-recovery',[sys.executable,'scripts/compare_zerofs.py','--mysql-recovery-report',str(small),'--phase-label','breakthrough-ublk-recovery','--engine','infinidisk2','--logical-cache','on','--wal-fixed-size','on','--wal-commit-records','on','--transport','ublk'])
    run('local-active-fixed-mixed',[sys.executable,'scripts/compare_zerofs.py','--mysql-repeat-report',str(large),'--phase-label','local-active-fixed-mixed','--engine','infinidisk2','--mysql-workloads','read_only','read_write','write_only','--mysql-rand-type','uniform','--mysql-warm-seconds','60','--logical-cache','on','--wal-fixed-size','on','--wal-commit-records','on','--flush-batch-us','0','--mysql-repeat-memory-cache-mib','64','--mysql-repeat-disk-cache-mib','4096'])
    report['complete']=True;save()
finally:print('CAMPAIGN '+str(work/'report.json'),flush=True)
