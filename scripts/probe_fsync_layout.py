#!/usr/bin/env python3
"""Isolated syscall probe, NOT a DB benchmark or a storage recovery test."""
import argparse,json,os,pathlib,statistics,time,uuid
p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=10);a=p.parse_args()
root=pathlib.Path(__file__).resolve().parents[1]
work=root/'test-output'/('fsync-layout-'+uuid.uuid4().hex[:12]);work.mkdir(mode=0o700)
capacity=512*1024*1024;record=b'R'*32+b'P'*4096;results=[]
try:
    for mode in ['growing','initialized','initialized','growing','growing','initialized']:
        f=work/('probe-'+uuid.uuid4().hex);fd=os.open(f,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        try:
            if mode=='initialized':
                zero=bytes(1024*1024)
                for offset in range(0,capacity,len(zero)):os.pwrite(fd,zero,offset)
            os.fdatasync(fd)
            count=0;start=time.monotonic();end=start+a.seconds
            while time.monotonic()<end:
                offset=count*len(record)
                if offset+len(record)>capacity:raise RuntimeError('probe capacity exceeded')
                if os.pwrite(fd,record,offset)!=len(record):raise RuntimeError('short probe write')
                os.fdatasync(fd);count+=1
            elapsed=time.monotonic()-start
            results.append({'mode':mode,'operations':count,'seconds':elapsed,'operations_per_second':count/elapsed,'mean_operation_ms':elapsed/count*1000})
        finally:os.close(fd);f.unlink()
    report={'scope':'single pwrite(4128)+fdatasync; preparation excluded; no DB or recovery guarantee','samples':results,'median_operations_per_second':{m:statistics.median(s['operations_per_second'] for s in results if s['mode']==m) for m in ['growing','initialized']},'host':os.uname().nodename}
    (work/'report.json').write_text(json.dumps(report,indent=2));print(work/'report.json')
finally:
    for f in work.glob('probe-*'):f.unlink()
