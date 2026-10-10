#!/usr/bin/env python3
"""Non-formatting Linux safety test on an existing isolated validation volume.
Run via with_aws_env.py, passing the test volume config. No existing device is used.
"""
import json,os,pathlib,signal,socket,subprocess,sys,time,uuid
root=pathlib.Path(__file__).resolve().parents[1]
config=pathlib.Path(sys.argv[1]).resolve()
if root/'test-output' not in config.parents:raise SystemExit('test config must be inside test-output')
binary=root/'target/release/infinidisk';device='/dev/nbd31'
if pathlib.Path('/sys/class/block/nbd31/pid').exists():raise SystemExit('nbd31 already in use')
work=root/'validation';work.mkdir(exist_ok=True)
mount=work/'namespace-mount';mount.mkdir(exist_ok=True)
ready=work/f'namespace-ready-{uuid.uuid4().hex}'
args=[str(binary),'-c',str(config)]
server=None;client=None;child=None
result={'namespace_mount_detach_refused':False,'detach_after_unmount':False}
with open(work/'detach-namespace.log','w') as log:
    try:
        server=subprocess.Popen([*args,'serve'],stdout=log,stderr=log)
        for _ in range(200):
            if server.poll() is not None:raise RuntimeError('server exited')
            try:
                with socket.create_connection(('127.0.0.1',11990),.1):break
            except OSError:time.sleep(.1)
        client=subprocess.Popen([*args,'attach','--device',device,'--connections','8'],stdout=log,stderr=log)
        for _ in range(100):
            if pathlib.Path('/sys/class/block/nbd31/pid').exists():break
            if client.poll() is not None:raise RuntimeError('attachment failed')
            time.sleep(.1)
        child=subprocess.Popen(['unshare','--mount','--propagation','private','sh','-c',
            'mount "$1" "$2" && touch "$3" && read stop && umount "$2"',
            'namespace-test',device,str(mount),str(ready)],stdin=subprocess.PIPE,stdout=log,stderr=log,text=True)
        for _ in range(100):
            if ready.exists():break
            if child.poll() is not None:raise RuntimeError('private mount failed')
            time.sleep(.1)
        else:raise RuntimeError('private mount timed out')
        assert str(mount) not in pathlib.Path('/proc/self/mountinfo').read_text()
        r=subprocess.run([*args,'detach','--device',device],capture_output=True,text=True,timeout=15)
        log.write(r.stdout+r.stderr);log.flush()
        assert r.returncode!=0 and 'still claimed' in r.stderr
        assert pathlib.Path('/sys/class/block/nbd31/pid').exists()
        result['namespace_mount_detach_refused']=True
        child.communicate('stop\n',timeout=30);assert child.returncode==0;child=None
        r=subprocess.run([*args,'detach','--device',device],stdout=log,stderr=log,timeout=15)
        assert r.returncode==0;client.wait(timeout=70);client=None
        result['detach_after_unmount']=True
    finally:
        if child:
            try:child.communicate('stop\n',timeout=30)
            except Exception:child.kill();child.wait()
        if client:
            subprocess.run([*args,'detach','--device',device],stdout=log,stderr=log,timeout=20)
            client.wait(timeout=70)
        if server:
            server.send_signal(signal.SIGTERM);server.wait(timeout=150)
        ready.unlink(missing_ok=True)
        (work/'detach-namespace.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
