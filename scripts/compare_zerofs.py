#!/usr/bin/env python3
"""Same-host isolated S3 comparison; never uses an existing volume or service.
All destructive operations target a new, blank NBD export in a UUID S3 prefix.
"""
import argparse,datetime,hashlib,json,os,pathlib,shlex,signal,socket,subprocess,time,tomllib,uuid

parser=argparse.ArgumentParser(description=__doc__)
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
parser.add_argument('--mysql-samples',type=int,default=3)
parser.add_argument('--mysql-seconds',type=int,default=15)
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
if not 1000<=ARGS.mysql_rows<=2000000 or not 2<=ARGS.mysql_volume_gib<=16 or not 1<=ARGS.mysql_memory_cache_mib<=16384 or not 1<=ARGS.mysql_disk_cache_mib<=65536:parser.error('invalid MySQL dataset/cache limits')
if ARGS.mysql_skip_crash_check and (not ARGS.mysql_repeat_report or ARGS.mysql_workloads!=['read_only']):parser.error('crash checks can only be skipped for read-only repeats')
if ARGS.mysql_repeat_report and not ARGS.phase_label:parser.error('MySQL repeat requires a phase label to preserve the original report')
if not 0<=ARGS.mysql_warm_seconds<=300:parser.error('invalid warmup duration')
if any(v is not None and not 1<=v<=16384 for v in (ARGS.mysql_repeat_memory_cache_mib,ARGS.mysql_repeat_disk_cache_mib)):parser.error('invalid repeat cache budgets')
if ARGS.engine=='native' and not ARGS.mysql_repeat_report:parser.error('native is only supported for MySQL repeat')
if not 1<=ARGS.mysql_samples<=10 or not 5<=ARGS.mysql_seconds<=120:parser.error('invalid MySQL sampling limits')
if sum(bool(x) for x in (ARGS.verify_report,ARGS.warm_memory_report,ARGS.postgres_roomy_report,ARGS.mysql_recovery_report,ARGS.mysql_repeat_report,ARGS.mysql_resume_report))>1:parser.error('select only one existing-report phase')
EXISTING=ARGS.verify_report or ARGS.warm_memory_report or ARGS.postgres_roomy_report or ARGS.mysql_recovery_report or ARGS.mysql_repeat_report or ARGS.mysql_resume_report

ROOT=pathlib.Path(__file__).resolve().parents[1]
BIN=ROOT/'target/release/infinidisk2'
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
server=client=tracer=None;nfsmounted=False;cfg=None;mounted=False;pgname=None;mysqlname=None;mysql_password=None
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
    global server,client,tracer,ublk_started
    if engine=='infinidisk2':
        import re
        text=cfg.read_text()
        for key,value in [('logical_cache',ARGS.logical_cache),('wal_commit_records',ARGS.wal_commit_records),('wal_fixed_size',ARGS.wal_fixed_size),('flush_batch_us',ARGS.flush_batch_us),('hot_wal_mib',ARGS.hot_wal_mib)]:
            if value is not None:
                text=re.sub(r'^'+key+r'\s*=.*$', '',text,flags=re.M)
                text+='\n'+key+' = '+(str(value=='on').lower() if isinstance(value,str) else str(value))+'\n'
        cfg.write_text(text)
        (W/(label+'-config.toml')).write_text(text)
        for operation,enabled in [('compact',ARGS.offline_compact),('warm',ARGS.offline_warm)]:
            if enabled:
                preparation_at=time.monotonic();run([BIN,'-c',cfg,operation],label+'-offline-'+operation,timeout=3600)
                R.setdefault('preparation',{}).setdefault(label,{})[operation+'_seconds']=time.monotonic()-preparation_at
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
    startargs=['docker','run','-d','--name',mysqlname,'--network','none','--memory','1g','--cpus','1','-e','MYSQL_ROOT_PASSWORD','-v',str(datadir)+':/var/lib/mysql','mysql:8.0','--socket=/var/lib/mysql/mysql.sock','--innodb-buffer-pool-size=268435456','--innodb-redo-log-capacity=134217728','--innodb-flush-log-at-trx-commit=1','--innodb-doublewrite=ON','--innodb-flush-method=O_DIRECT','--log-bin=mysql-bin','--sync-binlog=1','--binlog-expire-logs-seconds=3600']
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
    R.setdefault('mysql',{})[label]={'settings':settings,'tables':4,'rows_per_table':rows_per_table,'threads':8,'cpu_limit':1,'memory_mib':1024,'samples':{},'sample_count':ARGS.mysql_samples,'sample_seconds':ARGS.mysql_seconds,'trace_engine':ARGS.trace_engine,'transport':ARGS.transport,'warmup_seconds':ARGS.mysql_warm_seconds,'startup_seconds':time.monotonic()-startup_at}
    if not existing:sql('CREATE DATABASE bench','mysql-create')
    common=['sysbench','--db-driver=mysql','--mysql-socket='+str(datadir/'mysql.sock'),'--mysql-user=root','--mysql-password='+ENV['MYSQL_ROOT_PASSWORD'],'--mysql-db=bench','--tables=4','--table-size='+str(rows_per_table),'--threads=8','--rand-seed=42','--rand-type='+ARGS.mysql_rand_type]
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
    for work in (ARGS.mysql_workloads if measure else ()):
        R['mysql'][label]['samples'][work]=[]
        for i in range(ARGS.mysql_samples):
            p=run(common+['--time='+str(ARGS.mysql_seconds),f'/usr/share/sysbench/oltp_{work}.lua','run'],f'{label}-mysql-{work}-{i}',timeout=300)
            t=re.search(r'transactions:\s+\d+\s+\(([\d.]+) per sec',p.stdout)
            avg=re.search(r'avg:\s+([\d.]+)',p.stdout);p95=re.search(r'95th percentile:\s+([\d.]+)',p.stdout)
            err=re.search(r'ignored errors:\s+(\d+)',p.stdout)
            if not t:raise RuntimeError('sysbench result missing')
            R['mysql'][label]['samples'][work].append({'tps':float(t[1]),'avg_ms':float(avg[1]),'p95_ms':float(p95[1]),'ignored_errors':int(err[1]) if err else None});save()
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
            existing_text(engine);cfg=W/(engine+'.toml')
            start(engine,'infinidisk2-storage-recovery')
            mysql('infinidisk2-storage-recovery',existing=True,measure=False,storage_crash=True);stop();continue
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
    stop();save()
    print('REPORT '+str(W/('optimization-'+ARGS.phase_label+'.json' if ARGS.phase_label else 'report.json')),flush=True)
