#!/usr/bin/env python3
"""Same-host isolated S3 comparison; never uses an existing volume or service.
All destructive operations target a new, blank NBD export in a UUID S3 prefix.
"""
import argparse,datetime,hashlib,json,os,pathlib,re,resource,shlex,signal,socket,subprocess,time,tomllib,uuid

# Only non-secret, scalar performance/format knobs can be supplied by a campaign.
# Volume paths, object-store credentials, listen addresses and endpoints remain
# controlled by this isolated fixture builder.
ENGINE_BOOLEAN_OPTIONS={
    'sync_data_only','wal_preallocate','wal_writev','logical_cache',
    'wal_commit_records','wal_fixed_size','async_cache','fast_local_reads',
    'checkpoint_pipeline','selective_sync','ublk_fast_path','generation_mode',
    'paged_index','compact_checkpoints','aligned_wal',
}
ENGINE_INTEGER_OPTIONS={
    'cache_queue_mib':(1,128),'max_index_mib':(1,65536),
    'generation_max_lag_seconds':(1,3600),'checkpoint_seconds':(1,3600),
    'segment_mib':(1,64),'max_pending_mib':(2,1024*1024),
    'max_inflight':(1,4096),'hot_wal_mib':(0,1024*1024),
    'memory_cache_mib':(0,16384),'disk_cache_mib':(0,1024*1024),
    'read_extent_kib':(16,256),'flush_batch_us':(0,5000),
}
ASTRA_OPTIONS={
    'async_cache','cache_queue_mib','fast_local_reads','checkpoint_pipeline',
    'selective_sync','ublk_fast_path','generation_mode','generation_max_lag_seconds',
    'paged_index','compact_checkpoints','aligned_wal',
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

def patch_engine_options(text,options,legacy=False):
    for key,value in sorted(options.items()):
        text=re.sub(r'^'+re.escape(key)+r'\s*=.*$', '',text,flags=re.M)
        text+='\n'+key+' = '+json.dumps(value)+'\n'
    if legacy:
        for key in ASTRA_OPTIONS:
            text=re.sub(r'^\s*(?:'+key+'|"'+key+'"|\''+key+r"')\s*=.*$", '',text,flags=re.M)
    # Catch malformed/duplicate keys before writing or launching the engine.
    parsed=tomllib.loads(text)
    if legacy and ASTRA_OPTIONS.intersection(parsed):raise ValueError('could not remove all Astra options from legacy config')
    return text

def cgroup_cpu_snapshot(pid,proc_root=pathlib.Path('/proc'),cgroup_root=pathlib.Path('/sys/fs/cgroup')):
    """Read the process's cgroup v2 CPU counters, including its descendants.

    cpu.max is the direct cgroup limit; ancestors can impose additional limits.
    Unavailable/v1 measurements must not invalidate the per-process counters.
    """
    try:
        memberships=(proc_root/str(pid)/'cgroup').read_text().splitlines()
        unified=[line.split(':',2)[2] for line in memberships if line.startswith('0::')]
        if len(unified)!=1:return {'available':False,'reason':'no_unified_cgroup'}
        relative=pathlib.PurePosixPath(unified[0])
        if not relative.is_absolute() or '..' in relative.parts:
            return {'available':False,'reason':'cgroup_outside_visible_root'}
        directory=cgroup_root/pathlib.Path(*relative.parts[1:])
        identity=directory.stat()
        allowed={'usage_usec','user_usec','system_usec','nr_periods','nr_throttled','throttled_usec'}
        counters={key:int(value) for key,value in (line.split() for line in (directory/'cpu.stat').read_text().splitlines()) if key in allowed}
        if not counters or min(counters.values())<0:return {'available':False,'reason':'invalid_cpu_stat'}
        limit=None
        try:
            quota,period=(directory/'cpu.max').read_text().split()
            period=int(period)
            quota=None if quota=='max' else int(quota)
            if period>0 and (quota is None or quota>0):
                limit={'quota_usec':quota,'period_usec':period,'quota_cores':quota/period if quota is not None else None}
        except (OSError,ValueError):pass
        return {'available':True,'version':2,'path':str(relative),
                'identity':[identity.st_dev,identity.st_ino],'counters':counters,'direct_cpu_max':limit}
    except (OSError,ValueError,IndexError) as error:
        return {'available':False,'error_type':type(error).__name__}

def cgroup_cpu_window(before,after,elapsed):
    result={'comparable':False,'note':'cgroup subtree; throttled_usec is not a per-request I/O delay measurement'}
    if not before or not after or not before.get('available') or not after.get('available') or elapsed<=0:return result
    if (before['path'],before['identity'],before['direct_cpu_max'])!=(after['path'],after['identity'],after['direct_cpu_max']):return result
    delta={key:after['counters'][key]-value for key,value in before['counters'].items() if key in after['counters']}
    if not delta or min(delta.values())<0:return result
    result.update({'comparable':True,'path':after['path'],'direct_cpu_max':after['direct_cpu_max'],
                   'counters_delta':delta,'window_seconds':elapsed})
    if 'usage_usec' in delta:result['cpu_percent_of_one_core']=100*delta['usage_usec']/(elapsed*1e6)
    if delta.get('nr_periods',0)>0 and 'nr_throttled' in delta:
        result['throttled_periods_percent']=100*delta['nr_throttled']/delta['nr_periods']
    return result

def process_snapshot(pid):
    if pid is None:return None
    at=time.monotonic_ns()
    try:
        proc=pathlib.Path('/proc')/str(pid)
        # comm is parenthesized and may contain spaces or ')': split at its end.
        stat=(proc/'stat').read_text().rsplit(')',1)[1].split()
        status={k:v.strip() for k,v in (line.split(':',1) for line in (proc/'status').read_text().splitlines() if ':' in line)}
        io={k:int(v.strip()) for k,v in (line.split(':',1) for line in (proc/'io').read_text().splitlines() if ':' in line)}
        return {'pid':pid,'available':True,'monotonic_ns':at,'start_ticks':int(stat[19]),
                'user_ticks':int(stat[11]),'system_ticks':int(stat[12]),'clock_ticks_per_second':os.sysconf('SC_CLK_TCK'),
                'rss_kib':int(status.get('VmRSS','0 kB').split()[0]),
                'peak_rss_lifetime_kib':int(status.get('VmHWM','0 kB').split()[0]),'io':io,
                'cgroup_cpu':cgroup_cpu_snapshot(pid)}
    except (OSError,ValueError,IndexError) as error:
        return {'pid':pid,'available':False,'monotonic_ns':at,'error_type':type(error).__name__}

def process_window(before,after):
    result={'before':before,'after':after,'comparable':False}
    if not before or not after or not before.get('available') or not after.get('available'):return result
    if (before['pid'],before['start_ticks'])!=(after['pid'],after['start_ticks']):return result
    elapsed=(after['monotonic_ns']-before['monotonic_ns'])/1e9
    user=(after['user_ticks']-before['user_ticks'])/before['clock_ticks_per_second']
    system=(after['system_ticks']-before['system_ticks'])/before['clock_ticks_per_second']
    if elapsed<=0 or min(user,system)<0:return result
    result.update({'comparable':True,'window_seconds':elapsed,'user_cpu_seconds':user,
                   'system_cpu_seconds':system,'cpu_percent_of_one_core':100*(user+system)/elapsed,
                   'io_delta':{key:after['io'][key]-value for key,value in before['io'].items() if key in after['io']},
                   'cgroup_cpu':cgroup_cpu_window(before.get('cgroup_cpu'),after.get('cgroup_cpu'),elapsed)})
    return result

def host_cpu_snapshot():
    # guest and guest_nice are already included in user/nice: exclude them.
    return [int(v) for v in pathlib.Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]

def host_cpu_window(before,after):
    delta=[end-start for start,end in zip(before,after)]
    total=sum(delta)
    if total<=0 or min(delta)<0:return {'comparable':False}
    return {'comparable':True,'ticks_delta':delta,'busy_percent':100*(total-delta[3]-delta[4])/total,
            'iowait_percent':100*delta[4]/total,'note':'host aggregate; includes other services'}

def parse_sysbench_sample(output,percentile):
    transactions=re.search(r'transactions:\s+\d+\s+\(([\d.]+) per sec',output)
    average=re.search(r'avg:\s+([\d.]+)',output)
    latency=re.search(r'(\d+(?:\.\d+)?)th percentile:\s+([\d.]+)',output)
    errors=re.search(r'ignored errors:\s+(\d+)',output)
    if not transactions or not average or not latency:raise RuntimeError('sysbench result missing')
    if float(latency[1])!=percentile:raise RuntimeError('sysbench returned a different latency percentile')
    measured=float(latency[2])
    return {'tps':float(transactions[1]),'avg_ms':float(average[1]),
            'latency_percentile':percentile,'percentile_ms':measured,
            'p95_ms':measured if percentile==95 else None,'p99_ms':measured if percentile==99 else None,
            'ignored_errors':int(errors[1]) if errors else None}

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary',type=pathlib.Path,help='InfiniDisk2 executable; defaults to this checkout target/release/infinidisk2')
parser.add_argument('--engine-options',type=pathlib.Path,help='JSON object containing allowlisted non-secret InfiniDisk2 settings')
parser.add_argument('--legacy-config',action='store_true',help='remove Astra-only keys for an old binary; rejects active Astra options and explicit Astra numeric parameters')
parser.add_argument('--postgres-only',action='store_true')
parser.add_argument('--verify-report',type=pathlib.Path,help='verify unchanged read dataset of a completed comparison')
parser.add_argument('--warm-memory-report',type=pathlib.Path,help='repeat reads with a 1 GiB RAM cache and 4 GiB SSD budget')
parser.add_argument('--postgres-roomy-report',type=pathlib.Path,help='repeat existing DBs with roomy caches; ZeroFS fsync ignored, InfiniDisk2 local fsync honored')
parser.add_argument('--engine',choices=['both','zerofs','infinidisk2','native'],default='both')
parser.add_argument('--phase-label',help='preserve a separate optimization result and log set')
parser.add_argument('--sync-mode',choices=['data','all'],default='data',help='InfiniDisk2 fdatasync or original fsync path')
parser.add_argument('--mysql-only',action='store_true')
parser.add_argument('--mysql-recovery-report',type=pathlib.Path,help='crash InfiniDisk2 while an existing disposable MySQL DB is transacting')
parser.add_argument('--mysql-resume-report',type=pathlib.Path,help='finish integrity checks and native comparison of an interrupted isolated MySQL campaign')
parser.add_argument('--mysql-repeat-report',type=pathlib.Path,help='repeat selected workloads on an existing disposable MySQL DB')
parser.add_argument('--mysql-workloads',nargs='+',choices=['read_write','read_only','write_only'],default=['read_write','read_only','write_only'])
parser.add_argument('--mysql-skip-crash-check',action='store_true',help='only for read-only repeats of an already validated disposable dataset')
parser.add_argument('--mysql-rand-type',choices=['special','uniform'],default='special')
parser.add_argument('--mysql-rows',type=int,default=25000)
parser.add_argument('--mysql-volume-gib',type=int,default=2)
parser.add_argument('--mysql-memory-cache-mib',type=int,default=1024)
parser.add_argument('--mysql-disk-cache-mib',type=int,default=4096)
parser.add_argument('--mysql-cpus',type=float,default=1,help='Docker CPU quota for every MySQL engine; default 1 core')
parser.add_argument('--mysql-samples',type=int,default=3)
parser.add_argument('--mysql-seconds',type=int,default=15)
parser.add_argument('--mysql-percentile',type=int,choices=[95,99],default=95,help='latency percentile actually requested from sysbench')
parser.add_argument('--mysql-repeat-memory-cache-mib',type=int)
parser.add_argument('--mysql-repeat-disk-cache-mib',type=int)
parser.add_argument('--mysql-warm-seconds',type=int,default=10)
parser.add_argument('--read-extent-kib',type=int,choices=[16,64,256])
parser.add_argument('--wal-preallocate',choices=['on','off'])
parser.add_argument('--wal-writev',choices=['on','off'])
parser.add_argument('--logical-cache',choices=['on','off'])
parser.add_argument('--wal-fixed-size',choices=['on','off'])
parser.add_argument('--wal-commit-records',choices=['on','off'])
parser.add_argument('--flush-batch-us',type=int)
parser.add_argument('--hot-wal-mib',type=int)
parser.add_argument('--offline-warm',action='store_true')
parser.add_argument('--offline-compact',action='store_true')
parser.add_argument('--transport',choices=['nbd','ublk'],default='nbd')
parser.add_argument('--trace-engine',action='store_true',help='strace aggregate counters; timings are profiling only')
ARGS=parser.parse_args()
try:ENGINE_OPTIONS=load_engine_options(ARGS.engine_options) if ARGS.engine_options else {}
except (OSError,ValueError) as error:parser.error('invalid engine options: '+str(error))
if ARGS.legacy_config:
    invalid=[key for key,value in ENGINE_OPTIONS.items() if key in ASTRA_OPTIONS and (type(value) is not bool or value)]
    if invalid:parser.error('legacy config cannot enable or parameterize Astra options: '+', '.join(sorted(invalid)))
for key,value in [('wal_preallocate',ARGS.wal_preallocate),('wal_writev',ARGS.wal_writev),('logical_cache',ARGS.logical_cache),('wal_fixed_size',ARGS.wal_fixed_size),('wal_commit_records',ARGS.wal_commit_records),('flush_batch_us',ARGS.flush_batch_us),('hot_wal_mib',ARGS.hot_wal_mib),('read_extent_kib',ARGS.read_extent_kib)]:
    if value is not None and key in ENGINE_OPTIONS and ENGINE_OPTIONS[key]!=(value=='on' if isinstance(value,str) else value):parser.error('conflicting CLI and JSON engine option: '+key)
if ARGS.phase_label and not re.fullmatch(r'[A-Za-z0-9_-]+',ARGS.phase_label):parser.error('phase label may only contain letters, digits, underscores and hyphens')
if not 1000<=ARGS.mysql_rows<=2000000 or not 2<=ARGS.mysql_volume_gib<=16 or not 1<=ARGS.mysql_memory_cache_mib<=16384 or not 1<=ARGS.mysql_disk_cache_mib<=65536:parser.error('invalid MySQL dataset/cache limits')
if not 0.25<=ARGS.mysql_cpus<=16:parser.error('MySQL CPU quota must be between 0.25 and 16 cores')
if ARGS.mysql_skip_crash_check and (not ARGS.mysql_repeat_report or ARGS.mysql_workloads!=['read_only']):parser.error('crash checks can only be skipped for read-only repeats')
if ARGS.mysql_repeat_report and not ARGS.phase_label:parser.error('MySQL repeat requires a phase label to preserve the original report')
if not 0<=ARGS.mysql_warm_seconds<=300:parser.error('invalid warmup duration')
if any(v is not None and not 1<=v<=16384 for v in (ARGS.mysql_repeat_memory_cache_mib,ARGS.mysql_repeat_disk_cache_mib)):parser.error('invalid repeat cache budgets')
if ARGS.engine=='native' and not ARGS.mysql_repeat_report:parser.error('native is only supported for MySQL repeat')
if not 1<=ARGS.mysql_samples<=10 or not 5<=ARGS.mysql_seconds<=120:parser.error('invalid MySQL sampling limits')
if sum(bool(x) for x in (ARGS.verify_report,ARGS.warm_memory_report,ARGS.postgres_roomy_report,ARGS.mysql_recovery_report,ARGS.mysql_repeat_report,ARGS.mysql_resume_report))>1:parser.error('select only one existing-report phase')
EXISTING=ARGS.verify_report or ARGS.warm_memory_report or ARGS.postgres_roomy_report or ARGS.mysql_recovery_report or ARGS.mysql_repeat_report or ARGS.mysql_resume_report

ROOT=pathlib.Path(__file__).resolve().parents[1]
BIN=(ARGS.binary or ROOT/'target/release/infinidisk2').resolve()
if not BIN.is_file() or not os.access(BIN,os.X_OK):parser.error('InfiniDisk2 binary is missing or not executable: '+str(BIN))
W=EXISTING.resolve().parent if EXISTING else ROOT/'test-output'/('comparison-'+uuid.uuid4().hex[:12])
if EXISTING:
    if W.parent!=ROOT/'test-output' or not W.name.startswith('comparison-'):raise RuntimeError('verification only accepts an isolated comparison directory')
else:W.mkdir(parents=True,mode=0o700)
ENV=os.environ.copy()
for line in pathlib.Path('/opt/elestio/infinidisk/bench.env').read_text().splitlines():
    if '=' not in line or line.startswith('#'):continue
    k,v=line.split('=',1)
    if k in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN','INFINIDISK_PASSWORD'):
        ENV[k]=shlex.split(v)[0]
ENV['ZEROFS_PASSWORD']=ENV.pop('INFINIDISK_PASSWORD')
DEV=next(pathlib.Path('/dev/nbd'+str(n)) for n in range(31,1,-1) if pathlib.Path('/dev/nbd'+str(n)).exists() and not pathlib.Path('/sys/class/block/nbd'+str(n)+'/pid').exists())
if ARGS.transport=='ublk':
    if ARGS.engine!='infinidisk2' or pathlib.Path('/dev/ublkc31').exists():raise RuntimeError('ublk31 already exists or unsupported engine')
    subprocess.run(['modprobe','ublk_drv'],check=True)
    DEV=pathlib.Path('/dev/ublkb31')
ublk_started=False
R={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'work':str(W),'device':str(DEV),'size_bytes':268435456,'duration_seconds':15,'connections':8,'memory_mib':64,'cold_disk_mib':128,'warm_disk_mib':512,'runs':{},'contracts':{'infinidisk2':'FLUSH/FUA durable on local disk; S3 checkpoint asynchronous every 5 seconds, longer during backlog','zerofs':'ignore_fsync=false: explicit FLUSH/FUA seals extents and flushes metadata to S3','zerofs-async':'ignore_fsync=true: explicit FLUSH/FUA ignored; not a durable database configuration'}}
if EXISTING:
    R=json.loads(EXISTING.read_text())
    if not R.get('complete') and not ARGS.mysql_resume_report:raise RuntimeError('only verify a completed comparison')
if ARGS.mysql_only:
    R['memory_mib']=1024
    R['disk_mib']=ARGS.mysql_disk_cache_mib
    R['memory_mib']=ARGS.mysql_memory_cache_mib
    R['volume_size_bytes']=ARGS.mysql_volume_gib*1024**3
    R['contracts'].update({'zerofs-async':'FLUSH/FUA ignored by ZeroFS, unsafe DB durability mode','zerofs-durable':'ZeroFS FLUSH/FUA publishes S3','native':'native VM ext4 disk; MySQL fsync honored'})
server=client=tracer=None;nfsmounted=False;cfg=None;mounted=False;pgname=None;mysqlname=None;mysql_password=None;active_engine_metadata=None
ORIGINAL_CONFIGS={}
def save(): (W/('optimization-'+ARGS.phase_label+'.json' if ARGS.phase_label else 'report.json')).write_text(json.dumps(R,indent=2))
def existing_text(engine):
    text=(W/(engine+'.toml')).read_text();data=tomllib.loads(text)
    expected='s3://testperf-6czebk/infinidisk2-comparison-'+W.name.removeprefix('comparison-')+'/'+engine
    actual=data['store'] if engine=='infinidisk2' else data['storage']['url']
    if actual!=expected:raise RuntimeError('existing configuration is not this comparison volume')
    return text
def run(a,label,timeout=240,check=True):
    if not label.endswith(('-pg-ready','-mysql-ready')):print(label,flush=True)
    p=subprocess.Popen(list(map(str,a)),env=ENV,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,start_new_session=True)
    try: output,_=p.communicate(timeout=timeout)
    except BaseException:
        os.killpg(p.pid,signal.SIGKILL);p.communicate();raise
    (W/(label+'.log')).write_text(output)
    if check and p.returncode:raise RuntimeError(label+' failed: '+str(p.returncode))
    return subprocess.CompletedProcess(a,p.returncode,output)
def waitport(port):
    for _ in range(600):
        if server.poll() is not None:raise RuntimeError('server startup failed: see log')
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.1):return
        except OSError:time.sleep(.2)
    raise RuntimeError('startup timeout')
def delete_ublk(label):
    run([BIN,'-c',cfg,'ublk-delete','--id','31'],label)
    deadline=time.monotonic()+10
    while pathlib.Path('/dev/ublkc31').exists() or pathlib.Path('/sys/class/block/ublkb31').exists():
        if time.monotonic()>deadline:raise RuntimeError('ublk31 deletion did not finish')
        time.sleep(.02)
def start(engine,label):
    global server,client,tracer,ublk_started,active_engine_metadata
    if engine=='infinidisk2':
        import re
        if EXISTING and cfg.resolve()==(W/'infinidisk2.toml').resolve():
            ORIGINAL_CONFIGS.setdefault(cfg,cfg.read_bytes())
        text=cfg.read_text()
        for key,value in [('logical_cache',ARGS.logical_cache),('wal_commit_records',ARGS.wal_commit_records),('wal_fixed_size',ARGS.wal_fixed_size),('flush_batch_us',ARGS.flush_batch_us),('hot_wal_mib',ARGS.hot_wal_mib)]:
            if value is not None:
                text=re.sub(r'^'+key+r'\s*=.*$', '',text,flags=re.M)
                text+='\n'+key+' = '+(str(value=='on').lower() if isinstance(value,str) else str(value))+'\n'
        text=patch_engine_options(text,ENGINE_OPTIONS,legacy=ARGS.legacy_config)
        cfg.write_text(text)
        (W/(label+'-config.toml')).write_text(text)
        options=tomllib.loads(text)
        contract=('complete-generation recovery: FLUSH/FUA order writes but acknowledged transactions can be lost until a complete S3 generation is published'
                  if options.get('generation_mode') else R['contracts']['infinidisk2'])
        active_engine_metadata={'engine':'infinidisk2','binary_path':str(BIN),'binary_sha256':hashlib.sha256(BIN.read_bytes()).hexdigest(),
                                'config_path':str(cfg),'config_sha256':hashlib.sha256(text.encode()).hexdigest(),
                                'config_snapshot_path':str(W/(label+'-config.toml')),'legacy_config':ARGS.legacy_config,
                                'options':{key:value for key,value in options.items() if key in ENGINE_BOOLEAN_OPTIONS or key in ENGINE_INTEGER_OPTIONS},
                                'contract':contract}
        R.setdefault('engine_starts',{})[label]=active_engine_metadata
        R['contracts'][label]=contract
        for operation,enabled in [('compact',ARGS.offline_compact),('warm',ARGS.offline_warm)]:
            if enabled:
                preparation_at=time.monotonic();run([BIN,'-c',cfg,operation],label+'-offline-'+operation,timeout=3600)
                R.setdefault('preparation',{}).setdefault(label,{})[operation+'_seconds']=time.monotonic()-preparation_at
    else:
        active_engine_metadata={'engine':'zerofs','config_sha256':hashlib.sha256(cfg.read_bytes()).hexdigest(),
                                'ignore_fsync':tomllib.loads(cfg.read_text()).get('filesystem',{}).get('ignore_fsync',False)}
    f=open(W/(label+'-server.log'),'w')
    a=[BIN,'-c',cfg,'serve'] if engine=='infinidisk2' else ['zerofs','run','-c',cfg]
    if ARGS.transport=='ublk':a=[BIN,'-c',cfg,'ublk','--id','31','--queues','4']
    server=subprocess.Popen(list(map(str,a)),env=ENV,stdout=f,stderr=subprocess.STDOUT);f.close()
    if ARGS.transport=='ublk':
        for _ in range(600):
            if server.poll() is not None:raise RuntimeError('ublk server startup failed')
            if DEV.exists():ublk_started=True;return
            time.sleep(.1)
        server.kill();server.wait();server=None
        raise RuntimeError('ublk device startup timeout')
    waitport(11991)
    if ARGS.trace_engine and engine=='infinidisk2':
        tracer=subprocess.Popen(['strace','-f','-c','-w','-e','trace=write,writev,pwrite64,pwritev,fdatasync,fsync,fallocate,futex,clone,openat,close','-p',str(server.pid),'-o',str(W/(label+'-strace.log'))],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        time.sleep(.2)
        if tracer.poll() is not None:raise RuntimeError('strace attach failed')
    if engine!='infinidisk2' and label.endswith('initial'):
        global nfsmounted
        m=W/'nfs';m.mkdir(exist_ok=True)
        run(['mount','-t','nfs','-o','nolock,vers=3,tcp,port=12991,mountport=12991','127.0.0.1:/',m],label+'-nfs');nfsmounted=True
        (m/'.nbd').mkdir(exist_ok=True)
        with open(m/'.nbd/infinidisk2','wb') as f:f.truncate((ARGS.mysql_volume_gib if ARGS.mysql_only else 2)*1024**3);f.flush();os.fsync(f.fileno())
        run(['umount',m],label+'-nfs-umount');nfsmounted=False
    # Native Rust attachment is shared by both servers, with the same export name.
    ac=W/'attach.toml';ac.write_text(f'local_dir = "{W}/unused"\nstore = "file://{W}/unused-objects"\nlisten = "127.0.0.1:11991"\n')
    f=open(W/(label+'-attach.log'),'w')
    client=subprocess.Popen([str(BIN),'-c',str(ac),'attach','--device',str(DEV),'--connections','8'],env=ENV,stdout=f,stderr=subprocess.STDOUT);f.close()
    for _ in range(200):
        if client.poll() is not None:raise RuntimeError('attachment failed')
        if pathlib.Path('/sys/class/block/'+DEV.name+'/pid').exists():return
        time.sleep(.1)
    raise RuntimeError('attachment timeout')
def stop():
    global server,client,tracer,nfsmounted,mounted,pgname,mysqlname,ublk_started
    if mysqlname:
        run(['docker','rm','-f',mysqlname],'cleanup-mysql-'+str(time.time_ns()),check=False);mysqlname=None
    if pgname:
        run(['docker','rm','-f',pgname],'cleanup-pg-'+str(time.time_ns()),check=False);pgname=None
    # mount(8) may time out after the kernel has already mounted the filesystem.
    # Only inspect our isolated fixture's exact mountpoint, never a global unmount.
    if mounted or subprocess.run(['mountpoint','-q',W/'mount']).returncode==0:
        run(['umount',W/'mount'],'cleanup-ext4-'+str(time.time_ns()));mounted=False
    if nfsmounted:run(['umount',W/'nfs'],'cleanup-nfs',check=False);nfsmounted=False
    if client:
        if pathlib.Path('/sys/class/block/'+DEV.name+'/pid').exists():run([BIN,'-c',W/'attach.toml','detach','--device',DEV],'detach-'+str(time.time_ns()))
        client.wait(timeout=70);client=None
    if tracer:
        tracer.send_signal(signal.SIGINT);tracer.wait(timeout=10);tracer=None
    if ublk_started and pathlib.Path('/dev/ublkc31').exists():
        delete_ublk('ublk-delete-'+str(time.time_ns()))
    ublk_started=False
    if server:
        if ARGS.transport!='ublk':server.send_signal(signal.SIGTERM)
        try:server.wait(timeout=240)
        except subprocess.TimeoutExpired:server.kill();server.wait();raise RuntimeError('shutdown timeout; checkpoint not established')
        if server.returncode:raise RuntimeError('server shutdown failure')
        server=None
def configuration(engine,label,disk=128,async_mode=False,memory=64):
    global cfg
    cfg=W/(engine+'.toml');prefix='infinidisk2-comparison-'+W.name.removeprefix('comparison-')+'/'+engine
    if engine=='infinidisk2':
        cfg.write_text(f'''local_dir = "{W}/{label}-local"
store = "s3://testperf-6czebk/{prefix}"
endpoint = "https://storage.elestio.com"
region = "auto"
listen = "127.0.0.1:11991"
checkpoint_seconds = 5
memory_cache_mib = {memory}
disk_cache_mib = {disk}
hot_wal_mib = 0
max_pending_mib = 1024
segment_mib = 8
''')
    else:
        cfg.write_text(f'''[cache]
dir = "{W}/{label}-cache"
disk_size_gb = {disk/1024}
memory_size_gb = {memory/1024}
[storage]
url = "s3://testperf-6czebk/{prefix}"
encryption_password = "${{ZEROFS_PASSWORD}}"
[servers.nbd]
addresses = ["127.0.0.1:11991"]
[servers.nfs]
addresses = ["127.0.0.1:12991"]
[aws]
access_key_id = "${{AWS_ACCESS_KEY_ID}}"
secret_access_key = "${{AWS_SECRET_ACCESS_KEY}}"
endpoint = "https://storage.elestio.com"
default_region = "auto"
[filesystem]
ignore_fsync = {str(async_mode).lower()}
compression = "lz4"
[lsm]
flush_interval_secs = 5
''')
    if engine=='infinidisk2':cfg.write_text(patch_engine_options(cfg.read_text(),ENGINE_OPTIONS,legacy=ARGS.legacy_config))
    R.setdefault('prefixes',{})[engine]='s3://testperf-6czebk/'+prefix
    (W/(label+'-config.toml')).write_text(cfg.read_text())
def fio(engine,label,**kwargs):
    name=engine+'-'+label;out=W/(name+'.json')
    opts=dict(name=name,filename=str(DEV),ioengine='libaio',direct=1,group_reporting=1,size='256m',offset='16m',**{'output-format':'json','output':str(out)})
    opts.update(kwargs)
    if opts.get('rw')=='randwrite':opts['offset']='512m'
    run(['fio']+[f'--{k}={v}' for k,v in opts.items()],name,timeout=300)
    d=json.loads(out.read_text());j=d['jobs'][0]
    if j['error']:raise RuntimeError(name+' I/O error')
    R['runs'][name]={k:j[k] for k in ('read','write','sync') if k in j};save()
def postgres(engine,existing=False):
    global mounted,pgname
    mount=W/'mount';mount.mkdir(exist_ok=True)
    # This device belongs to the freshly created UUID comparison volume only.
    if not existing:run(['mkfs.ext4','-F','-E','lazy_itable_init=0,lazy_journal_init=0',DEV],engine+'-mkfs',timeout=600)
    run(['mount','-o','noatime',DEV,mount],engine+'-mount');mounted=True
    pgname='id2-compare-pg-'+W.name.removeprefix('comparison-')
    pgdir=mount/'postgres'
    if existing:
        if not (pgdir/'PG_VERSION').exists():raise RuntimeError('existing comparison database missing')
    else:pgdir.mkdir()
    run(['docker','run','-d','--name',pgname,'--network','none','--memory','512m','--cpus','1','-e','POSTGRES_HOST_AUTH_METHOD=trust','-e','POSTGRES_INITDB_ARGS=--data-checksums','-v',str(pgdir)+':/var/lib/postgresql/data','postgres:16'],engine+'-pg-start',timeout=600)
    def ready():
        for _ in range(900):
            p=run(['docker','exec',pgname,'sh','-c','[ "$(cat /proc/1/comm)" = postgres ] && pg_isready -U postgres'],engine+'-pg-ready',check=False)
            if p.returncode==0:return
            time.sleep(.5)
        raise RuntimeError('PostgreSQL startup timed out')
    ready()
    settings=run(['docker','exec',pgname,'psql','-U','postgres','-Atc','SHOW fsync; SHOW synchronous_commit; SHOW full_page_writes; SHOW data_checksums;'],engine+'-pg-settings').stdout.strip().splitlines()
    if settings!=['on']*4:raise RuntimeError('database durability/checksums disabled')
    R.setdefault('postgres',{})[engine]={'settings':settings,'samples':[],'scale':2,'clients':4,'cpu_limit':1,'memory_mib':512}
    if not existing:run(['docker','exec',pgname,'pgbench','-U','postgres','-i','-s','2','postgres'],engine+'-pg-init',timeout=600)
    for i in range(3):
        p=run(['docker','exec',pgname,'pgbench','-U','postgres','-c','4','-j','4','-T','15','postgres'],f'{engine}-pgbench-{i}',timeout=300)
        import re
        t=re.search(r'tps = ([\d.]+)',p.stdout);l=re.search(r'latency average = ([\d.]+)',p.stdout);f=re.search(r'number of failed transactions: (\d+)',p.stdout)
        if not t or not f or int(f[1]):raise RuntimeError('pgbench failed transactions or missing result')
        R['postgres'][engine]['samples'].append({'tps':float(t[1]),'latency_ms':float(l[1]),'failed_transactions':int(f[1])});save()
    run(['docker','kill','--signal','KILL',pgname],engine+'-pg-crash')
    run(['docker','start',pgname],engine+'-pg-restart');ready()
    p=run(['docker','exec',pgname,'psql','-U','postgres','-Atc','SELECT (SELECT sum(abalance) FROM pgbench_accounts)=(SELECT sum(bbalance) FROM pgbench_branches) AND (SELECT sum(abalance) FROM pgbench_accounts)=(SELECT sum(tbalance) FROM pgbench_tellers);'],engine+'-pg-sums')
    if p.stdout.strip()!='t':raise RuntimeError('database sums inconsistent after crash')
    run(['docker','exec',pgname,'pg_amcheck','-U','postgres','--database','postgres','--install-missing'],engine+'-pg-amcheck',timeout=600)
    R['postgres'][engine]['database_SIGKILL_recovery']='passed'
    run(['docker','stop','-t','120',pgname],engine+'-pg-stop',timeout=150)
    run(['docker','rm',pgname],engine+'-pg-remove');pgname=None
    run(['umount',mount],engine+'-unmount');mounted=False;save()
def mysql(label,native=False,existing=False,measure=True,storage_crash=False):
    global mounted,mysqlname,mysql_password,server,client,ublk_started
    import re
    mount=W/'mount';mount.mkdir(exist_ok=True)
    if native:datadir=W/'native-mysql';datadir.mkdir(exist_ok=True)
    else:
        if not existing:run(['mkfs.ext4','-F','-E','lazy_itable_init=0,lazy_journal_init=0',DEV],label+'-mkfs',timeout=600)
        run(['mount','-o','noatime',DEV,mount],label+'-mount');mounted=True
        datadir=mount/'mysql';datadir.mkdir(exist_ok=True)
    rows_per_table=R.get('mysql',{}).get(label.split('-')[0],{}).get('rows_per_table',ARGS.mysql_rows) if existing else ARGS.mysql_rows
    password=uuid.uuid4().hex
    mysqlname='id2-compare-mysql-'+W.name.removeprefix('comparison-')
    # Only the disposable test account, never a production database credential.
    secret=W/('mysql-'+label.split('-')[0]+'.secret')
    if not existing:
        mysql_password=password
        fd=os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        with os.fdopen(fd,'w') as f:f.write(mysql_password)
    else:mysql_password=secret.read_text().strip()
    ENV['MYSQL_ROOT_PASSWORD']=mysql_password
    ENV['MYSQL_PWD']=ENV['MYSQL_ROOT_PASSWORD']
    startargs=['docker','run','-d','--name',mysqlname,'--network','none','--memory','1g','--cpus',f'{ARGS.mysql_cpus:g}','-e','MYSQL_ROOT_PASSWORD','-v',str(datadir)+':/var/lib/mysql','mysql:8.0','--socket=/var/lib/mysql/mysql.sock','--innodb-buffer-pool-size=268435456','--innodb-redo-log-capacity=134217728','--innodb-flush-log-at-trx-commit=1','--innodb-doublewrite=ON','--innodb-flush-method=O_DIRECT','--log-bin=mysql-bin','--sync-binlog=1','--binlog-expire-logs-seconds=3600']
    startup_at=time.monotonic()
    run(startargs,label+'-mysql-start',timeout=120)
    def sql(q,tag,check=True):return run(['docker','exec','-e','MYSQL_PWD',mysqlname,'mysql','--socket=/var/lib/mysql/mysql.sock','-uroot','-NBe',q],label+'-'+tag,timeout=1800 if tag=='mysql-check' else 300,check=check)
    def ready():
        for _ in range(3600):
            p=sql('SELECT 1','mysql-ready',check=False)
            if 'is not running' in p.stdout:raise RuntimeError('MySQL exited during initialization')
            if p.returncode==0:
                # Entrypoint starts a temporary server; wait for final PID 1 mysqld.
                p=run(['docker','exec',mysqlname,'sh','-c','[ "$(cat /proc/1/comm)" = mysqld ]'],label+'-mysql-ready',check=False)
                if p.returncode==0:return
            time.sleep(.5)
        raise RuntimeError('MySQL startup timed out')
    ready()
    settings=sql('SELECT @@innodb_flush_log_at_trx_commit,@@sync_binlog,@@innodb_doublewrite,@@innodb_flush_method,@@innodb_buffer_pool_size,@@version,@@log_bin','mysql-settings').stdout.strip().split('\t')
    if settings[:4]!=['1','1','ON','O_DIRECT'] or settings[-1]!='1':raise RuntimeError('InnoDB durability settings unexpected: '+str(settings))
    R.setdefault('mysql',{})[label]={'settings':settings,'tables':4,'rows_per_table':rows_per_table,'threads':8,'cpu_limit':ARGS.mysql_cpus,'memory_mib':1024,'samples':{},'sample_count':ARGS.mysql_samples,'sample_seconds':ARGS.mysql_seconds,'latency_percentile':ARGS.mysql_percentile,'trace_engine':ARGS.trace_engine,'transport':ARGS.transport,'warmup_seconds':ARGS.mysql_warm_seconds,'startup_seconds':time.monotonic()-startup_at,
                                  'engine_metadata':None if native else active_engine_metadata,
                                  'resource_window_note':'/proc snapshots bracket each sysbench process including its setup/exit; process CPU includes all threads; peak RSS is lifetime high water, not the isolated window maximum'}
    if not existing:sql('CREATE DATABASE bench','mysql-create')
    common=['sysbench','--db-driver=mysql','--mysql-socket='+str(datadir/'mysql.sock'),'--mysql-user=root','--mysql-password='+ENV['MYSQL_ROOT_PASSWORD'],'--mysql-db=bench','--tables=4','--table-size='+str(rows_per_table),'--threads=8','--rand-seed=42','--rand-type='+ARGS.mysql_rand_type,'--percentile='+str(ARGS.mysql_percentile)]
    if not existing:run(common+['/usr/share/sysbench/oltp_read_write.lua','prepare'],label+'-mysql-prepare',timeout=900)
    R['mysql'][label]['table_file_bytes']=sum(p.stat().st_size for p in (datadir/'bench').glob('sbtest*.ibd'))
    R['mysql'][label]['random_distribution']=ARGS.mysql_rand_type
    # Warm the DB buffer pool before timing; this is a deliberately cached DB case.
    if measure and ARGS.mysql_warm_seconds:run(common+['--time='+str(ARGS.mysql_warm_seconds),'/usr/share/sysbench/oltp_read_only.lua','run'],label+'-mysql-warm',timeout=300)
    if not native and measure:
        settings_cache=tomllib.loads(cfg.read_text())
        if 'local_dir' in settings_cache:
            cache_dir=pathlib.Path(settings_cache['local_dir'])/'cache'
            cache_bytes=0
            for entry in cache_dir.glob('*.cache'):
                try:cache_bytes+=entry.stat().st_size
                except FileNotFoundError:pass
            if settings_cache.get('logical_cache'):
                page_dir=pathlib.Path(settings_cache['local_dir'])/'logical-cache'
                cache_bytes=sum(f.stat().st_blocks*512 for f in page_dir.iterdir() if f.is_file())
            R['mysql'][label]['ssd_cache_bytes_before_samples']=cache_bytes
            if server:
                status=pathlib.Path('/proc')/str(server.pid)/'status'
                fields={line.split(':')[0]:line.split(':')[1].strip() for line in status.read_text().splitlines() if ':' in line}
                R['mysql'][label]['engine_rss_kib_before_samples']=int(fields['VmRSS'].split()[0])
                R['mysql'][label]['engine_peak_rss_kib_before_samples']=int(fields['VmHWM'].split()[0])
            R['mysql'][label]['engine_memory_cache_mib']=settings_cache.get('memory_cache_mib',128)
            R['mysql'][label]['engine_disk_cache_mib']=settings_cache.get('disk_cache_mib',2048)
    mysql_pid=int(run(['docker','inspect','--format','{{.State.Pid}}',mysqlname],label+'-mysql-host-pid').stdout.strip()) if measure else None
    measured_processes={'engine':server.pid if server and not native else None,'mysql':mysql_pid,'nbd_client':client.pid if client and not native else None}
    for work in (ARGS.mysql_workloads if measure else ()):
        R['mysql'][label]['samples'][work]=[]
        for i in range(ARGS.mysql_samples):
            before={name:process_snapshot(pid) for name,pid in measured_processes.items()}
            host_before=host_cpu_snapshot();child_before=resource.getrusage(resource.RUSAGE_CHILDREN)
            sample_at=time.monotonic()
            p=run(common+['--time='+str(ARGS.mysql_seconds),f'/usr/share/sysbench/oltp_{work}.lua','run'],f'{label}-mysql-{work}-{i}',timeout=300)
            elapsed=time.monotonic()-sample_at
            child_after=resource.getrusage(resource.RUSAGE_CHILDREN);host_after=host_cpu_snapshot()
            after={name:process_snapshot(pid) for name,pid in measured_processes.items()}
            sample=parse_sysbench_sample(p.stdout,ARGS.mysql_percentile)
            user=child_after.ru_utime-child_before.ru_utime;system=child_after.ru_stime-child_before.ru_stime
            sample['resources']={'processes':{name:process_window(before[name],after[name]) for name in measured_processes},
                                 'host_cpu':host_cpu_window(host_before,host_after),
                                 'sysbench':{'window_seconds':elapsed,'user_cpu_seconds':user,'system_cpu_seconds':system,
                                             'cpu_percent_of_one_core':100*(user+system)/elapsed}}
            R['mysql'][label]['samples'][work].append(sample);save()
    if ARGS.mysql_skip_crash_check:
        R['mysql'][label]['database_SIGKILL_recovery']='not_run_read_only_repeat'
    else:
        recovery_at=time.monotonic()
        run(['docker','kill','--signal','KILL',mysqlname],label+'-mysql-crash')
        run(['docker','start',mysqlname],label+'-mysql-restart');ready()
        R['mysql'][label]['database_recovery_seconds']=time.monotonic()-recovery_at
        if storage_crash:
            f=open(W/(label+'-active-crash.log'),'w')
            workload=subprocess.Popen(common+['--time=30','/usr/share/sysbench/oltp_read_write.lua','run'],env=ENV,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);f.close()
            try:
                time.sleep(3)
                if workload.poll() is not None:raise RuntimeError('workload ended before storage crash')
                server.kill();server.wait();server=None
                run(['docker','kill','--signal','KILL',mysqlname],label+'-storage-crash-db-kill',check=False)
                run(['docker','rm','-f',mysqlname],label+'-storage-crash-db-remove')
                run(['umount','-l',mount],label+'-storage-crash-unmount');mounted=False
                try:workload.wait(timeout=10)
                except subprocess.TimeoutExpired:os.killpg(workload.pid,signal.SIGKILL);workload.wait()
                if ARGS.transport=='ublk':
                    delete_ublk(label+'-storage-crash-ublk-delete');ublk_started=False
                else:
                    if pathlib.Path('/sys/class/block/'+DEV.name+'/pid').exists():run([BIN,'-c',W/'attach.toml','detach','--device',DEV],label+'-storage-crash-detach')
                    client.wait(timeout=70);client=None
                start('infinidisk2',label+'-local-recovery')
                p=run(['e2fsck','-f','-p',DEV],label+'-storage-crash-fsck',check=False)
                if p.returncode not in (0,1):raise RuntimeError('ext4 recovery failed after storage crash')
                run(['mount','-o','noatime',DEV,mount],label+'-storage-crash-remount');mounted=True
                run(startargs,label+'-storage-crash-mysql-start');ready()
            finally:
                if workload.poll() is None:os.killpg(workload.pid,signal.SIGKILL);workload.wait()
        rows=sql(' UNION ALL '.join(f"SELECT 'sbtest{i}',COUNT(*) FROM bench.sbtest{i}" for i in range(1,5)),'mysql-counts').stdout.strip().splitlines()
        if len(rows)!=4 or any(int(row.split('\t')[1])!=rows_per_table for row in rows):raise RuntimeError('transactional row counts inconsistent')
        checks=sql('CHECK TABLE '+','.join(f'bench.sbtest{i}' for i in range(1,5))+' EXTENDED','mysql-check').stdout.strip().splitlines()
        if len(checks)!=4 or any(not row.endswith('\tOK') for row in checks):raise RuntimeError('InnoDB check failed')
        R['mysql'][label]['database_SIGKILL_recovery']='passed'
        if storage_crash:R['mysql'][label]['storage_engine_SIGKILL_recovery']='passed'
    run(['docker','stop','-t','120',mysqlname],label+'-mysql-stop',timeout=150)
    run(['docker','rm',mysqlname],label+'-mysql-remove');mysqlname=None
    if mounted:run(['umount',mount],label+'-mysql-unmount');mounted=False
    save()
try:
    R['complete']=False
    R.setdefault('executions',[]).append({'phase':'mysql-resume' if ARGS.mysql_resume_report else 'mysql-repeat' if ARGS.mysql_repeat_report else 'mysql-storage-crash' if ARGS.mysql_recovery_report else 'mysql' if ARGS.mysql_only else 'verify' if ARGS.verify_report else 'memory-warm' if ARGS.warm_memory_report else 'postgres-roomy' if ARGS.postgres_roomy_report else 'postgres' if ARGS.postgres_only else 'raw','utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'script_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'load_start':os.getloadavg()})
    runner_source=pathlib.Path(__file__).read_text()
    (W/('runner-'+hashlib.sha256(runner_source.encode()).hexdigest()+'.json')).write_text(json.dumps({'sha256':hashlib.sha256(runner_source.encode()).hexdigest(),'source':runner_source},indent=2))
    R['binary_sha256']={'infinidisk2':hashlib.sha256(BIN.read_bytes()).hexdigest(),'zerofs':hashlib.sha256(pathlib.Path('/usr/local/bin/zerofs').read_bytes()).hexdigest()}
    R['binary_path']={'infinidisk2':str(BIN),'zerofs':'/usr/local/bin/zerofs'}
    R['requested_engine_options']=ENGINE_OPTIONS
    R['legacy_config']=ARGS.legacy_config
    R['requested_engine_options_sha256']=hashlib.sha256(json.dumps(ENGINE_OPTIONS,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    R['executions'][-1].update({'binary_path':str(BIN),'binary_sha256':R['binary_sha256']['infinidisk2'],
                                'engine_options':ENGINE_OPTIONS,'legacy_config':ARGS.legacy_config,'mysql_percentile':ARGS.mysql_percentile,'mysql_cpu_limit':ARGS.mysql_cpus})
    R['host']=run(['uname','-a'],'host').stdout.strip()
    R['logical_cpus']=os.cpu_count()
    R['load_start']=os.getloadavg()
    R['fio_version']=run(['fio','--version'],'fio-version').stdout.strip()
    R['zerofs_version']=run(['zerofs','--version'],'zerofs-version').stdout.strip()
    R['infinidisk2_version']=run([BIN,'--version'],'id2-version').stdout.strip()
    for engine in (('zerofs','infinidisk2') if ARGS.engine=='both' else (ARGS.engine,)):
        if ARGS.mysql_resume_report:
            if engine!='infinidisk2':continue
            original=W/'before-resume-report.json'
            if not original.exists():original.write_text(EXISTING.read_text())
            text=existing_text(engine)
            import re
            text=re.sub(r'^disk_cache_mib\s*=.*$', 'disk_cache_mib = 2048',text,flags=re.M)
            cfg=W/'infinidisk2-resume-check-config.toml';cfg.write_text(text)
            start(engine,'infinidisk2-resume-check')
            mysql('infinidisk2-resume-check',existing=True,measure=False)
            R['mysql']['infinidisk2']['database_SIGKILL_recovery']='passed_after_resume_larger_cache'
            R['mysql']['infinidisk2']['integrity_resume']={'ssd_cache_mib':2048,'check_timeout_seconds':1800,'original_timeout_seconds':300}
            stop()
            mysql('native',native=True)
            continue
        if ARGS.mysql_repeat_report:
            label=engine+'-'+(ARGS.phase_label or 'repeat')
            if engine=='native':
                mysql(label,native=True,existing=True);stop();continue
            text=existing_text(engine)
            if engine=='infinidisk2':
                import re
                for key,value in [('wal_preallocate',ARGS.wal_preallocate),('wal_writev',ARGS.wal_writev)]:
                    if value is not None:
                        text=re.sub(r'^'+key+r'\s*=.*$', '',text,flags=re.M)
                        text+='\n'+key+' = '+str(value=='on').lower()+'\n'
            if engine=='infinidisk2':
                for key,value in [('memory_cache_mib',ARGS.mysql_repeat_memory_cache_mib),('disk_cache_mib',ARGS.mysql_repeat_disk_cache_mib)]:
                    if value is not None:
                        text=re.sub(r'^'+key+r'\s*=.*$', '',text,flags=re.M)+'\n'+key+' = '+str(value)+'\n'
            if engine=='infinidisk2' and ARGS.read_extent_kib is not None:
                text=re.sub(r'^read_extent_kib\s*=.*$', '',text,flags=re.M)+'\nread_extent_kib = '+str(ARGS.read_extent_kib)+'\n'
            cfg=W/(label+'-config.toml');cfg.write_text(text)
            start(engine,label);mysql(label,existing=True);stop();continue
        if ARGS.mysql_recovery_report:
            if engine!='infinidisk2':continue
            label=engine+'-'+(ARGS.phase_label or 'storage-recovery')
            text=existing_text(engine);cfg=W/(label+'-config.toml');cfg.write_text(text)
            start(engine,label)
            mysql(label,existing=True,measure=False,storage_crash=True);stop();continue
        if ARGS.mysql_only:
            configuration(engine,'initial-'+engine,disk=ARGS.mysql_disk_cache_mib,async_mode=engine=='zerofs',memory=ARGS.mysql_memory_cache_mib)
            if engine=='infinidisk2':
                for key,value in [('wal_preallocate',ARGS.wal_preallocate),('wal_writev',ARGS.wal_writev)]:
                    if value is not None:
                        with cfg.open('a') as f:f.write('\n'+key+' = '+str(value=='on').lower()+'\n')
                if ARGS.read_extent_kib is not None:
                    with cfg.open('a') as f:f.write('\nread_extent_kib = '+str(ARGS.read_extent_kib)+'\n')
                (W/('initial-'+engine+'-config.toml')).write_text(cfg.read_text())
                run([BIN,'-c',cfg,'init','--size',str(ARGS.mysql_volume_gib)+'GiB'],'id2-init')
            start(engine,engine+'-initial')
            with open(DEV,'rb',buffering=0) as f:
                if any(f.read(4096)):raise RuntimeError('MySQL export not blank')
            mysql(engine+'-async' if engine=='zerofs' else engine)
            stop()
            if engine=='zerofs':
                text=cfg.read_text().replace('ignore_fsync = true','ignore_fsync = false');cfg.write_text(text)
                start(engine,'zerofs-durable')
                mysql('zerofs-durable',existing=True);stop()
            continue
        if ARGS.postgres_roomy_report:
            text=existing_text(engine)
            if engine=='infinidisk2':text=text.replace('memory_cache_mib = 64','memory_cache_mib = 1024').replace('disk_cache_mib = 128','disk_cache_mib = 4096')
            else:text=text.replace('memory_size_gb = 0.0625','memory_size_gb = 1.0').replace('disk_size_gb = 0.125','disk_size_gb = 4.0').replace('ignore_fsync = false','ignore_fsync = true')
            cfg=W/(engine+'-pg-roomy.toml');cfg.write_text(text)
            if engine=='infinidisk2':
                text+='\nsync_data_only = '+str(ARGS.sync_mode=='data').lower()+'\n'
                cfg.write_text(text)
            label='infinidisk2-roomy' if engine=='infinidisk2' else 'zerofs-async-roomy'
            if ARGS.phase_label:label=engine+'-'+ARGS.phase_label
            R['contracts'][label]='local fsync honored' if engine=='infinidisk2' else 'ZeroFS ignores FLUSH/FUA despite PostgreSQL fsync=on'
            start(engine,label)
            postgres(label,existing=True);stop();continue
        if ARGS.warm_memory_report:
            text=existing_text(engine)
            if engine=='infinidisk2':text=text.replace('memory_cache_mib = 64','memory_cache_mib = 1024').replace('disk_cache_mib = 512','disk_cache_mib = 4096')
            else:text=text.replace('memory_size_gb = 0.0625','memory_size_gb = 1.0').replace('disk_size_gb = 0.5','disk_size_gb = 4.0').replace('ignore_fsync = true','ignore_fsync = false')
            cfg=W/(engine+'-memory-warm.toml');cfg.write_text(text)
            start(engine,engine+'-memory-warm')
            fio(engine,'memory-prefetch',rw='read',bs='1m',iodepth=16)
            for i in range(3):
                fio(engine,f'memory-warm-read-{i}',rw='randread',bs='4k',iodepth=32,numjobs=4,runtime=15,time_based=1)
                fio(engine,f'memory-warm-seq-{i}',rw='read',bs='1m',iodepth=16,runtime=15,time_based=1)
            R.setdefault('tests',{})[engine+'-memory-warm']='completed';stop();continue
        if ARGS.verify_report:
            existing_text(engine)
            cfg=W/(engine+'.toml')
            start(engine,engine+'-verify-restored')
            fio(engine,'verify-restored',rw='read',bs='128k',iodepth=16,verify='crc32c',verify_fatal=1,verify_only=1)
            R.setdefault('tests',{})[engine+'-restored-256m-crc32c']='passed';stop();continue
        configuration(engine,'initial-'+engine)
        if engine=='infinidisk2':run([BIN,'-c',cfg,'init','--size','2GiB'],'id2-init')
        start(engine,engine+'-initial')
        with open(DEV,'rb',buffering=0) as f:
            if any(f.read(4096)):raise RuntimeError('export was not blank; refusing benchmark write')
        if ARGS.postgres_only:
            postgres(engine);stop();continue
        fio(engine,'verified-write',rw='write',bs='128k',iodepth=16,verify='crc32c',verify_fatal=1,do_verify=1,refill_buffers=1,fsync_on_close=1)
        for i in range(3):
            fio(engine,f'buffered-write-{i}',rw='randwrite',bs='4k',iodepth=32,numjobs=4,runtime=15,time_based=1,refill_buffers=1)
            fio(engine,f'fsync-write-{i}',rw='randwrite',bs='4k',iodepth=1,numjobs=4,fsync=1,runtime=15,time_based=1,refill_buffers=1)
        stop()
        for i in range(3):
            configuration(engine,f'cold-{engine}-{i}')
            if engine=='infinidisk2':run([BIN,'-c',cfg,'adopt','--takeover'],f'id2-adopt-{i}')
            start(engine,f'{engine}-cold-{i}')
            fio(engine,f'cold-read-{i}',rw='randread',bs='4k',iodepth=32,numjobs=4,runtime=15,time_based=1)
            stop()
        configuration(engine,f'warm-{engine}',disk=512)
        if engine=='infinidisk2':run([BIN,'-c',cfg,'adopt','--takeover'],'id2-adopt-warm')
        start(engine,engine+'-warm')
        fio(engine,'prefetch',rw='read',bs='1m',iodepth=16)
        for i in range(3):
            fio(engine,f'warm-read-{i}',rw='randread',bs='4k',iodepth=32,numjobs=4,runtime=15,time_based=1)
            fio(engine,f'seq-read-{i}',rw='read',bs='1m',iodepth=16,runtime=15,time_based=1)
        stop()
        if engine=='zerofs':
            configuration(engine,'warm-zerofs',disk=512,async_mode=True)
            start(engine,'zerofs-async')
            for i in range(3):fio('zerofs-async',f'fsync-write-{i}',rw='randwrite',bs='4k',iodepth=1,numjobs=4,fsync=1,runtime=15,time_based=1,refill_buffers=1)
            stop()
    if ARGS.mysql_only:mysql('native',native=True)
    R['complete']=True;save()
finally:
    try:stop()
    finally:
        # Repeats normally write a dedicated label config. Recovery/verification
        # may reuse the fixture's original path: restore its exact bytes even on
        # failure, while the executed snapshot and hash remain in the report.
        for original,content in ORIGINAL_CONFIGS.items():original.write_bytes(content)
        save()
        print('REPORT '+str(W/('optimization-'+ARGS.phase_label+'.json' if ARGS.phase_label else 'report.json')),flush=True)
