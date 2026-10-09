#!/usr/bin/env python3
"""Run a command with only AWS credentials imported from a private env file.
Never prints values, imports unrelated secrets, or evaluates shell expressions.
"""
import os, pathlib, shlex, sys
if len(sys.argv)<3:
    raise SystemExit('usage: with_aws_env.py private.env command [args...]')
env=os.environ.copy()
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    if '=' not in line or line.startswith('#'):continue
    key,value=line.split('=',1)
    if key in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN'):
        fields=shlex.split(value)
        if len(fields)!=1:raise SystemExit('invalid AWS env value')
        env[key]=fields[0]
os.execvpe(sys.argv[2],sys.argv[2:],env)
