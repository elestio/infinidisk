#!/usr/bin/env python3
"""Isolated, destructive ONLY to a newly-created test volume. No existing mount is used.
Run on the target VM. --s3 uses only AWS credentials from the explicitly given env file.
All logs, configs and results live under test-output/run-<uuid>. Objects use a unique prefix.
"""
import argparse, datetime, hashlib, json, os, pathlib, re, shlex, signal, socket, subprocess, time, tomllib, uuid

# Pure allowlist/type validation shared in intent with compare_zerofs.py. These
# definitions are copied from its AST; never import that executable's top-level.
# Paths, endpoints, listen addresses, and all credentials are fixture-controlled.
ENGINE_BOOLEAN_OPTIONS={
    'sync_data_only','wal_preallocate','wal_writev','logical_cache',
    'wal_commit_records','wal_fixed_size','async_cache','fast_local_reads',
    'checkpoint_pipeline','selective_sync','ublk_fast_path','generation_mode',
    'paged_index','compact_checkpoints','aligned_wal','adaptive_reads',
}

ENGINE_INTEGER_OPTIONS={
    'cache_queue_mib':(1,128),'max_index_mib':(1,65536),
    'generation_max_lag_seconds':(1,3600),'checkpoint_seconds':(1,3600),
    'segment_mib':(1,64),'max_pending_mib':(2,1024*1024),
    'max_inflight':(1,4096),'hot_wal_mib':(0,1024*1024),
    'memory_cache_mib':(0,16384),'disk_cache_mib':(0,1024*1024),
    'read_extent_kib':(16,256),'flush_batch_us':(0,5000),
}

def load_engine_options(path):
    data=json.loads(path.read_text())
    if not isinstance(data,dict):raise ValueError('engine options must be a JSON object')
    for key,value in data.items():
        if key in ENGINE_BOOLEAN_OPTIONS:
            if type(value) is not bool:raise ValueError('engine boolean option has wrong type: '+key)
        elif key in ENGINE_INTEGER_OPTIONS:
            low,high=ENGINE_INTEGER_OPTIONS[key]
            if type(value) is not int or not low<=value<=high:raise ValueError('engine integer option outside bounds: '+key)
            if key=='read_extent_kib' and value not in (16,64,256):raise ValueError('invalid read_extent_kib')
        else:raise ValueError('unsupported engine option: '+key)
    return data

def patch_engine_options(text,options):
    for key,value in sorted(options.items()):
        # Existing fixture TOML contains indented keys; strip the old assignment
        # before appending the validated scalar to avoid duplicate keys.
        names=(re.escape(key),'"'+re.escape(key)+'"',"'"+re.escape(key)+"'")
        text=re.sub(r'^[ \t]*(?:'+'|'.join(names)+r')\s*=.*$', '',text,flags=re.M)
        text+='\n'+key+' = '+json.dumps(value)+'\n'
    parsed=tomllib.loads(text)
    if parsed.get('generation_mode',False):
        raise ValueError('generation_mode=true is unsupported by this strict-fsync validator; use a complete-generation rollback campaign')
    return text

def public_engine_options(text):
    return {key:value for key,value in tomllib.loads(text).items()
            if key in ENGINE_BOOLEAN_OPTIONS or key in ENGINE_INTEGER_OPTIONS}

ROOT=pathlib.Path(__file__).resolve().parents[1]
BIN=ROOT/'target/release/infinidisk2'
parser=argparse.ArgumentParser()
parser.add_argument('--s3',action='store_true')
parser.add_argument('--credentials',type=pathlib.Path)
parser.add_argument('--bucket',default='testperf-6czebk')
parser.add_argument('--endpoint',default='https://storage.elestio.com')
parser.add_argument('--logical-cache',action='store_true',default=None)
parser.add_argument('--wal-fixed-size',action='store_true',default=None)
parser.add_argument('--wal-commit-records',action='store_true',default=None)
parser.add_argument('--postgres',action='store_true')
parser.add_argument('--checks-only',action='store_true',help='Keep CRC and crash/recovery checks; skip redundant throughput benchmarks')
parser.add_argument('--binary',type=pathlib.Path,help='InfiniDisk2 executable; defaults to this checkout target/release/infinidisk2')
parser.add_argument('--engine-options',type=pathlib.Path,help='JSON object containing allowlisted non-secret settings; generation_mode=true is refused')
parser.add_argument('--resume-report',type=pathlib.Path,help='repeat remote recovery/cache tests of an existing completed run')
args=parser.parse_args()
try:
    ENGINE_OPTIONS=load_engine_options(args.engine_options) if args.engine_options else {}
    CLI_OPTIONS={key:value for key,value in (
        ('logical_cache',args.logical_cache),('wal_fixed_size',args.wal_fixed_size),
        ('wal_commit_records',args.wal_commit_records)) if value is not None}
    for key,value in CLI_OPTIONS.items():
        if key in ENGINE_OPTIONS and ENGINE_OPTIONS[key]!=value:
            raise ValueError('conflicting CLI and JSON engine option: '+key)
    APPLIED_OPTIONS={**ENGINE_OPTIONS,**CLI_OPTIONS}
    patch_engine_options('',APPLIED_OPTIONS)  # reject generation mode before any fixture changes
except (OSError,ValueError) as error:
    parser.error('invalid engine options: '+str(error))
BIN=(args.binary or BIN).resolve()
if not BIN.is_file() or not os.access(BIN,os.X_OK):
    parser.error('InfiniDisk2 binary is missing or not executable: '+str(BIN))
if args.resume_report:
    WORK=args.resume_report.resolve().parent
    if ROOT/'test-output' not in WORK.parents:raise RuntimeError('resume report must be under test-output')
    run_id=WORK.name.removeprefix('run-')
    try:
        # Inherited generation volumes must be rejected even without a JSON flag.
        existing_text=(WORK/'volume.toml').read_text()
        patch_engine_options(existing_text,{})
        patch_engine_options(existing_text,APPLIED_OPTIONS)
    except (OSError,ValueError) as error:
        parser.error('invalid resume configuration: '+str(error))
else:
    run_id=uuid.uuid4().hex[:12]
    WORK=ROOT/'test-output'/f'run-{run_id}'
    WORK.mkdir(parents=True,mode=0o700)
MOUNT=WORK/'mount';MOUNT.mkdir(exist_ok=True)
env=os.environ.copy()
if args.s3:
    if not args.credentials:parser.error('--s3 requires --credentials')
    for line in args.credentials.read_text().splitlines():
        if '=' not in line or line.startswith('#'):continue
        k,v=line.split('=',1)
        if k in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN'):
            env[k]=shlex.split(v)[0]
store=f's3://{args.bucket}/infinidisk2-validation-{run_id}' if args.s3 else f'file://{WORK}/objects'
config=WORK/'volume.toml'
if not args.resume_report:
    config.write_text(f'''local_dir = "{WORK}/local"
    store = "{store}"
    endpoint = "{args.endpoint}"
    region = "auto"
    listen = "127.0.0.1:11990"
    checkpoint_seconds = 5
    memory_cache_mib = 64
    disk_cache_mib = 128
    hot_wal_mib = 256
    max_pending_mib = 1024
    segment_mib = 8
    max_inflight = 128
    ''')
if not args.resume_report:
    with config.open('a') as f:f.write('\nlogical_cache = false\nwal_commit_records = false\nwal_fixed_size = false\n')
# Performance/format options are set before init, not only when starting serve.
config.write_text(patch_engine_options(config.read_text(),APPLIED_OPTIONS))
initial_config=config.read_text()
devices=[pathlib.Path(f'/dev/nbd{n}') for n in range(31,1,-1) if pathlib.Path(f'/dev/nbd{n}').exists() and not pathlib.Path(f'/sys/class/block/nbd{n}/pid').exists()]
if not devices:raise RuntimeError('no unused NBD device; existing devices will never be disconnected')
device=devices[0]
report={'binary_sha256':hashlib.sha256(BIN.read_bytes()).hexdigest(),'script_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'id':run_id,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'backend':store,'device':str(device),'tests':{},'benchmarks':{},'config':str(config)}
report['checks_only']=args.checks_only
if args.resume_report:
    WORK=args.resume_report.resolve().parent
    if ROOT/'test-output' not in WORK.parents:raise RuntimeError('resume report must be under test-output')
    report=json.loads(args.resume_report.read_text());run_id=report['id']
    config=WORK/'volume.toml';MOUNT=WORK/'mount'
    report['resumed_with_binary_sha256']=hashlib.sha256(BIN.read_bytes()).hexdigest()
manifest={
    'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'binary_path':str(BIN),'binary_sha256':hashlib.sha256(BIN.read_bytes()).hexdigest(),
    'script_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
    'requested_engine_options':ENGINE_OPTIONS,'legacy_cli_options':CLI_OPTIONS,
    'applied_engine_options':APPLIED_OPTIONS,
    'initial_effective_engine_options':public_engine_options(initial_config),
    'initial_config_sha256':hashlib.sha256(initial_config.encode()).hexdigest(),
    'engine_options_sha256':hashlib.sha256(json.dumps(APPLIED_OPTIONS,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
    'contract':'local-fsync; acknowledged data must survive local SIGKILL; complete S3 checkpoints restore independently',
    'transport':'nbd','resume':bool(args.resume_report),'engine_starts':[],
}
manifest_path=WORK/('validation-manifest-'+str(time.time_ns())+'.json')
manifest_path.write_text(json.dumps(manifest,indent=2))
report.setdefault('validation_executions',[]).append(manifest)
report['latest_validation_manifest']=str(manifest_path)
server=None;client=None;mounted=False;postgres_name=f'infinidisk2-pg-{run_id}'

def command(argv,label,timeout=120,check=True):
    argv=list(map(str,argv))
    print(label,flush=True)
    r=subprocess.run(argv,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=timeout)
    (WORK/f'{label}.log').write_text(r.stdout)
    if check and r.returncode:raise RuntimeError(f'{label} failed ({r.returncode}); see {WORK}/{label}.log')
    return r
def cli(*argv,label,timeout=120,check=True):return command([BIN,'-c',config,*argv],label,timeout,check)
def start():
    global server,client
    text=config.read_text()
    patch_engine_options(text,{})  # every resumed/restored stage keeps strict-fsync semantics
    start_id=time.time_ns()
    snapshot=WORK/f'start-{start_id}-config.toml'
    snapshot.write_text(text)
    manifest['engine_starts'].append({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'config_snapshot':str(snapshot),'config_sha256':hashlib.sha256(text.encode()).hexdigest(),
        'effective_engine_options':public_engine_options(text)})
    manifest_path.write_text(json.dumps(manifest,indent=2))
    sf=open(WORK/f'serve-{start_id}.log','w')
    server=subprocess.Popen([str(BIN),'-c',str(config),'serve'],env=env,stdout=sf,stderr=subprocess.STDOUT);sf.close()
    for _ in range(200):
        if server.poll() is not None:raise RuntimeError('server exited during startup')
        try:
            with socket.create_connection(('127.0.0.1',11990),timeout=.1):break
        except OSError:time.sleep(.1)
    else:raise RuntimeError('server startup timed out')
    cf=open(WORK/f'attach-{time.time_ns()}.log','w')
    client=subprocess.Popen([str(BIN),'-c',str(config),'attach','--device',str(device),'--connections','8'],env=env,stdout=cf,stderr=subprocess.STDOUT);cf.close()
    for _ in range(100):
        if client.poll() is not None:raise RuntimeError('native Linux attachment failed')
        if pathlib.Path(f'/sys/class/block/{device.name}/pid').exists():break
        time.sleep(.1)
    else:raise RuntimeError('kernel attachment timed out')
def mount():
    global mounted
    command(['mount','-o','noatime',device,MOUNT],'mount-'+str(time.time_ns()));mounted=True
def unmount():
    global mounted
    if mounted:command(['umount',MOUNT],'unmount-'+str(time.time_ns()));mounted=False
def stop():
    global server,client
    unmount()
    if pathlib.Path(f'/sys/class/block/{device.name}/pid').exists():cli('detach','--device',device,label='detach-'+str(time.time_ns()))
    if client:client.wait(timeout=70);client=None
    if server:
        server.send_signal(signal.SIGTERM);server.wait(timeout=150)
        if server.returncode:raise RuntimeError('server failed orderly shutdown; WAL retained, inspect logs')
        server=None
def fio(label,target,**options):
    out=WORK/f'{label}.json'
    base={'name':label,'filename':str(target),'ioengine':'libaio','direct':1,'group_reporting':1,'output-format':'json','output':str(out)}
    base.update(options)
    command(['fio',*[f'--{k}={v}' for k,v in base.items()]],label,timeout=180)
    result=json.loads(out.read_text());job=result['jobs'][0]
    if job.get('error'):raise RuntimeError(f'fio returned I/O/verification errors: {label}')
    report['benchmarks'][label]={k:job[k] for k in ('read','write','sync') if k in job}
    return job
try:
    if not args.resume_report:
        cli('init','--size','2GiB',label='init')
        start()
        with open(device,'rb',buffering=0) as f:
            if any(f.read(4096)):raise RuntimeError('new export is not blank; refusing mkfs')
        command(['mkfs.ext4','-F','-E','lazy_itable_init=0,lazy_journal_init=0',device],'mkfs',timeout=180)
        mount()
        payload=os.urandom(4*1024*1024)
        with open(MOUNT/'durable.bin','wb') as f:f.write(payload);f.flush();os.fsync(f.fileno())
        report['tests']['durable_file_sha256']=hashlib.sha256(payload).hexdigest()
        # Verified writes exercise real ext4 and compare every fio data block.
        fio('verify-write-read',MOUNT/'fio.bin',rw='write',bs='128k',size='256m',iodepth=16,verify='crc32c',do_verify=1,verify_fatal=1,fsync_on_close=1)
        if not args.checks_only:
            fio('randread-hot',MOUNT/'fio.bin',rw='randread',bs='4k',size='256m',iodepth=32,numjobs=4,runtime=10,time_based=1)
            fio('randwrite-fsync',MOUNT/'fsync.bin',rw='randwrite',bs='4k',size='64m',iodepth=1,numjobs=4,fsync=1,runtime=10,time_based=1)
            # Same physical VM disk, same fio settings; no global page-cache flush.
            fio('baseline-randwrite-fsync',WORK/'baseline.bin',rw='randwrite',bs='4k',size='64m',iodepth=1,numjobs=4,fsync=1,runtime=10,time_based=1)
            fio('seqread-hot',MOUNT/'fio.bin',rw='read',bs='1m',size='256m',iodepth=16,runtime=10,time_based=1)
        # Abrupt server death after application fsync; only this new test mount is affected.
        server.kill();server.wait();server=None
        command(['umount','-l',MOUNT],'crash-unmount');mounted=False
        if client:
            try:client.wait(timeout=15)
            except subprocess.TimeoutExpired:
                cli('detach','--device',device,label='crash-detach',check=False);client.wait(timeout=70)
            client=None
        start()
        r=command(['e2fsck','-f','-p',device],'fsck-local-crash',check=False)
        if r.returncode not in (0,1):raise RuntimeError('ext4 did not recover after local server crash')
        mount()
        assert (MOUNT/'durable.bin').read_bytes()==payload
        report['tests']['local_SIGKILL_ext4_fsync']='passed'
        if args.postgres:
            pgdir=MOUNT/'postgres';pgdir.mkdir()
            command(['docker','run','-d','--name',postgres_name,'--network','none','--memory','512m','--cpus','1','-e','POSTGRES_HOST_AUTH_METHOD=trust','-e','POSTGRES_INITDB_ARGS=--data-checksums','-v',f'{pgdir}:/var/lib/postgresql/data','postgres:16'],'postgres-start')
            for _ in range(180):
                r=command(['docker','exec',postgres_name,'sh','-c','[ "$(cat /proc/1/comm)" = postgres ] && pg_isready -U postgres'],'postgres-ready',check=False)
                if r.returncode==0:break
                time.sleep(.5)
            else:raise RuntimeError('PostgreSQL startup failed')
            command(['docker','exec',postgres_name,'psql','-U','postgres','-c','CREATE DATABASE bench;'],'postgres-createdb')
            command(['docker','exec',postgres_name,'pgbench','-U','postgres','-i','-s','2','bench'],'pgbench-init')
            duration=['-t','16'] if args.checks_only else ['-T','15']
            r=command(['docker','exec',postgres_name,'pgbench','-U','postgres','-c','4','-j','4',*duration,'bench'],'pgbench-durable',timeout=45)
            report['tests' if args.checks_only else 'benchmarks']['pgbench-durable']=r.stdout
            settings=command(['docker','exec',postgres_name,'psql','-U','postgres','-At','-c',"SELECT name||'='||setting FROM pg_settings WHERE name IN ('fsync','full_page_writes','synchronous_commit');"],'postgres-durability-settings').stdout
            assert 'fsync=on' in settings and 'full_page_writes=on' in settings and 'synchronous_commit=on' in settings
            report['tests']['postgres_settings']=settings
            command(['docker','kill','--signal','KILL',postgres_name],'postgres-crash')
            command(['docker','start',postgres_name],'postgres-restart')
            for _ in range(120):
                r=command(['docker','exec',postgres_name,'pg_isready','-U','postgres'],'postgres-recovered-ready',check=False)
                if r.returncode==0:break
                time.sleep(.5)
            totals=command(['docker','exec',postgres_name,'psql','-U','postgres','-d','bench','-At','-c','SELECT (SELECT sum(abalance) FROM pgbench_accounts), (SELECT sum(bbalance) FROM pgbench_branches), (SELECT sum(tbalance) FROM pgbench_tellers);'],'postgres-consistency').stdout.strip().split('|')
            assert len(totals)==3 and totals[0]==totals[1]==totals[2]
            command(['docker','exec',postgres_name,'pg_amcheck','-U','postgres','--database','bench','--install-missing'],'postgres-amcheck',timeout=180)
            report['tests']['postgres_SIGKILL_amcheck']='passed'
            # Crash the actual storage engine while PostgreSQL commits transactions.
            crash_log=open(WORK/'pgbench-engine-crash.log','w')
            crash_load=subprocess.Popen(['docker','exec',postgres_name,'pgbench','-U','postgres','-c','4','-j','4','-T','30','bench'],env=env,stdout=crash_log,stderr=subprocess.STDOUT)
            time.sleep(3)
            server.kill();server.wait();server=None
            command(['docker','kill','--signal','KILL',postgres_name],'postgres-stop-after-storage-crash',check=False)
            crash_load.wait(timeout=40);crash_log.close()
            command(['docker','rm',postgres_name],'postgres-remove-crashed')
            command(['umount','-l',MOUNT],'postgres-storage-crash-unmount');mounted=False
            if client:
                try:client.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    cli('detach','--device',device,label='postgres-storage-crash-detach',check=False);client.wait(timeout=70)
                client=None
            start()
            r=command(['e2fsck','-f','-p',device],'fsck-postgres-storage-crash',check=False)
            if r.returncode not in (0,1):raise RuntimeError('ext4 failed to recover PostgreSQL storage engine crash')
            mount()
            command(['docker','run','-d','--name',postgres_name,'--network','none','--memory','512m','--cpus','1','-v',f'{pgdir}:/var/lib/postgresql/data','postgres:16'],'postgres-after-storage-crash')
            for _ in range(120):
                r=command(['docker','exec',postgres_name,'pg_isready','-U','postgres'],'postgres-storage-recovered-ready',check=False)
                if r.returncode==0:break
                time.sleep(.5)
            totals=command(['docker','exec',postgres_name,'psql','-U','postgres','-d','bench','-At','-c','SELECT (SELECT sum(abalance) FROM pgbench_accounts), (SELECT sum(bbalance) FROM pgbench_branches), (SELECT sum(tbalance) FROM pgbench_tellers);'],'postgres-storage-crash-consistency').stdout.strip().split('|')
            assert len(totals)==3 and totals[0]==totals[1]==totals[2]
            command(['docker','exec',postgres_name,'pg_amcheck','-U','postgres','--database','bench','--install-missing'],'postgres-storage-crash-amcheck',timeout=180)
            report['tests']['storage_engine_SIGKILL_postgres_amcheck']='passed'
            command(['docker','rm','-f',postgres_name],'postgres-remove')
        stop()
    cli('scrub',label='scrub-remote',timeout=300)
    report['tests']['remote_scrub']='passed'
    # Discard ALL local state and restore solely from a remote checkpoint.
    old_config=config.read_text()
    lines=old_config.splitlines()
    config.write_text('\n'.join(f'local_dir = "{WORK}/restored-{uuid.uuid4().hex[:8]}"' if line.lstrip().startswith('local_dir') else 'disk_cache_mib = 128' if line.lstrip().startswith('disk_cache_mib') else line for line in lines)+'\n')
    cli('adopt','--takeover',label='adopt-remote')
    start()
    r=command(['e2fsck','-f','-p',device],'fsck-remote-recovery',check=False,timeout=180)
    if r.returncode not in (0,1):raise RuntimeError('remote checkpoint is not an ext4 crash-consistent volume')
    mount();assert hashlib.sha256((MOUNT/'durable.bin').read_bytes()).hexdigest()==report['tests']['durable_file_sha256']
    report['tests']['remote_only_ext4_recovery']='passed'
    if 'postgres_SIGKILL_amcheck' in report['tests']:
        pgdir=MOUNT/'postgres'
        command(['docker','run','-d','--name',postgres_name,'--network','none','--memory','512m','--cpus','1','-v',f'{pgdir}:/var/lib/postgresql/data','postgres:16'],'postgres-remote-only-start')
        for _ in range(120):
            r=command(['docker','exec',postgres_name,'pg_isready','-U','postgres'],'postgres-remote-ready',check=False)
            if r.returncode==0:break
            time.sleep(.5)
        totals=command(['docker','exec',postgres_name,'psql','-U','postgres','-d','bench','-At','-c','SELECT (SELECT sum(abalance) FROM pgbench_accounts), (SELECT sum(bbalance) FROM pgbench_branches), (SELECT sum(tbalance) FROM pgbench_tellers);'],'postgres-remote-consistency').stdout.strip().split('|')
        assert len(totals)==3 and totals[0]==totals[1]==totals[2]
        command(['docker','exec',postgres_name,'pg_amcheck','-U','postgres','--database','bench','--install-missing'],'postgres-remote-amcheck',timeout=180)
        report['tests']['remote_only_postgres_amcheck']='passed'
        command(['docker','rm','-f',postgres_name],'postgres-remote-remove')
    if args.checks_only:
        fio('verify-remote-restored',MOUNT/'fio.bin',rw='read',bs='128k',size='256m',iodepth=16,verify='crc32c',verify_only=1,verify_fatal=1)
        report['tests']['remote_fio_crc32c']='passed'
    else:
        fio('randread-cold-remote',MOUNT/'fio.bin',rw='randread',bs='4k',size='256m',iodepth=32,numjobs=4,runtime=10,time_based=1)
        fio('randread-undersized-cache-pass2',MOUNT/'fio.bin',rw='randread',bs='4k',size='256m',iodepth=32,numjobs=4,runtime=10,time_based=1)
        stop()
        config.write_text(config.read_text().replace('disk_cache_mib = 128','disk_cache_mib = 512'))
        start();mount()
        fio('prime-512m-cache',MOUNT/'fio.bin',rw='read',bs='1m',size='256m',iodepth=16)
        fio('randread-fully-warm-cache',MOUNT/'fio.bin',rw='randread',bs='4k',size='256m',iodepth=32,numjobs=4,runtime=10,time_based=1)
    stop()
    report['passed']=True
    report.pop('error',None)
except BaseException as exc:
    report['passed']=False;report['error']=str(exc)
    raise
finally:
    (WORK/'report.json').write_text(json.dumps(report,indent=2))
    if args.postgres or 'postgres_SIGKILL_amcheck' in report['tests']:command(['docker','rm','-f',postgres_name],'cleanup-postgres',check=False)
    try:
        if mounted:unmount()
        if client and pathlib.Path(f'/sys/class/block/{device.name}/pid').exists():cli('detach','--device',device,label='cleanup-detach',check=False)
        if client:client.wait(timeout=70)
    except Exception as exc:print('cleanup:',exc,flush=True)
    if server:
        server.send_signal(signal.SIGTERM)
        try:server.wait(timeout=150)
        except subprocess.TimeoutExpired:server.kill();server.wait()
    print('Report:',WORK/'report.json',flush=True)
