#!/usr/bin/env python3
"""A separate SQL-error diagnostic; never a replacement performance sample.

Reuse the existing runner's functions, without editing it or executing its main
campaign. The only workload changes are debug verbosity and InnoDB deadlock
logging. Snapshot queries run outside each sysbench invocation.
"""
import argparse
import ast
import collections
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
import pathlib
import re
import shlex
import socket
import subprocess
import sys
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def load_runner_definitions(path, argv):
    tree = ast.parse(path.read_text(), filename=str(path))
    main = tree.body[-1]
    if not isinstance(main, ast.Try) or not isinstance(main.body[0], ast.Assign):
        raise RuntimeError("runner main layout changed; refuse partial execution")
    if ast.unparse(main.body[0]) != "R['complete'] = False":
        raise RuntimeError("unexpected runner campaign entry")
    namespace = {"__name__": "astra_sql_diagnostic_runner", "__file__": str(path)}
    previous = sys.argv
    try:
        sys.argv = [str(path), *argv]
        exec(compile(ast.Module(body=tree.body[:-1], type_ignores=[]), str(path), "exec"), namespace)
    finally:
        sys.argv = previous
    return namespace


def parse_status(output):
    return {parts[0]: int(parts[1]) for line in output.splitlines()
            if len(parts := line.split("\t")) == 2 and parts[1].isdigit()}


def parse_errors(output):
    result = {}
    for line in output.splitlines():
        fields = line.split("\t")
        if len(fields) != 5:
            raise RuntimeError("unexpected performance_schema error row")
        number, name, state, raised, handled = fields
        result[number] = {"name": name, "sql_state": state,
                          "raised": int(raised), "handled": int(handled)}
    return result


def parse_ignored(output):
    matches = re.findall(r"Ignoring error (\d+) ([^\r\n]*)", output)
    return [{"code": int(code), "message": message.rstrip(", ")} for code, message in matches]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[3])
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    work = root / "validation/astra/mysql"
    campaign = json.loads((work / "report.json").read_text())
    source_stage = next(item for item in reversed(campaign["stages"])
                        if item["label"] == "small-qualified-compact" and item.get("complete"))
    if source_stage["binary_sha256"] != args.expected_binary_sha256:
        raise RuntimeError("source stage has a different binary")
    protocol = {"source_phase": source_stage["phase"], "binary_sha256": args.expected_binary_sha256,
                "fixture": "comparison-323bb1097b47", "profile": "compact", "transport": "nbd",
                "samples": 3, "seconds": 30, "workloads": ["read_write", "write_only"],
                "threads": 8, "mysql_cpus": 1, "warmup_seconds": 10, "random_seed": 42,
                "random_distribution": "special", "sysbench_verbosity": 5,
                "mysql_debug": False, "db_debug": False, "ignored_error_codes": [1213, 1020, 1205],
                "innodb_print_all_deadlocks": True, "eligible_for_comparison": False,
                "note": "Separate fixed diagnostic, no retry loop. Original error-bearing measurements remain excluded. SQL status snapshots bracket each workload, including warmup, before any database restart."}
    if not args.execute:
        print(json.dumps(protocol, indent=2))
        return
    binary = root / "target/release/infinidisk2-astra"
    if digest(binary) != args.expected_binary_sha256:
        raise RuntimeError("unexpected Astra binary")
    if not (work / "pause-before-next-stage").exists():
        raise RuntimeError("main campaign must be explicitly paused")
    fixture = root / "test-output/comparison-323bb1097b47"
    phase = "sql-diagnostic-" + args.expected_binary_sha256[:12] + "-" + str(time.time_ns())
    output = work / phase
    output.mkdir(mode=0o700)
    report = {"protocol": protocol, "started_utc": utc(), "phase": phase, "complete": False,
              "observations": [], "source_sha256": {}}
    credentials = []
    for line in pathlib.Path("/opt/elestio/infinidisk/bench.env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            if key in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "INFINIDISK_PASSWORD"}:
                credentials.extend(item.encode() for item in shlex.split(value) if len(item) > 8)
    credentials.extend(path.read_bytes().strip() for path in fixture.glob("*.secret") if len(path.read_bytes().strip()) > 8)

    def checked_write(path, content):
        if any(secret in content for secret in credentials):
            raise RuntimeError("secret detected; evidence export refused")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    def save_report():
        checked_write(output / "report.json", json.dumps(report, indent=2).encode())

    def preflight():
        if pathlib.Path("/sys/class/block/nbd31/pid").exists() or pathlib.Path("/dev/ublkc31").exists():
            raise RuntimeError("test device busy")
        if os.path.ismount(fixture / "mount"):
            raise RuntimeError("test fixture still mounted")
        for port in (11991, 12991):
            with socket.socket() as sock:
                sock.settimeout(.2)
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    raise RuntimeError("test port busy")
        names = subprocess.check_output(["docker", "ps", "-a", "--format", "{{.Names}}"], text=True).splitlines()
        if "id2-compare-mysql-323bb1097b47" in names:
            raise RuntimeError("test MySQL container still exists")
        for path in pathlib.Path("/proc").glob("[0-9]*/comm"):
            try:
                if path.read_text().strip() in {"cargo", "rustc", "cc1", "ld.lld"}:
                    raise RuntimeError("compiler active")
            except FileNotFoundError:
                pass

    with contextlib.ExitStack() as stack:
        for path in (root / "test-output/astra-recovery.lock", work / "campaign.lock"):
            lock = stack.enter_context(path.open("a+b"))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        preflight()
        runner = root / "scripts/compare_zerofs.py"
        for path in (runner, pathlib.Path(__file__).resolve()):
            report["source_sha256"][path.name] = digest(path)
            checked_write(output / "sources" / path.name, path.read_bytes())
        checked_write(output / "options.json", json.dumps(source_stage["options"], indent=2).encode())
        helper_args = ["--binary", str(binary), "--mysql-repeat-report", str(fixture / "report.json"),
                       "--phase-label", phase, "--engine", "infinidisk2", "--mysql-workloads", "read_write", "write_only",
                       "--mysql-samples", "3", "--mysql-seconds", "30", "--mysql-percentile", "99", "--mysql-cpus", "1",
                       "--mysql-warm-seconds", "10", "--mysql-rand-type", "special", "--engine-options", str(output / "options.json")]
        report["runner_arguments"] = helper_args
        report["sysbench_package"] = subprocess.check_output(["dpkg-query", "-W", "-f=${Version}", "sysbench"], text=True)
        report["sysbench_binary_sha256"] = digest(pathlib.Path("/usr/bin/sysbench"))
        if b"Ignoring error %u %s" not in pathlib.Path("/usr/bin/sysbench").read_bytes():
            raise RuntimeError("installed sysbench lacks expected ignored-error debug logger")
        ns = load_runner_definitions(runner, helper_args)
        if str(ns["DEV"]) != "/dev/nbd31":
            raise RuntimeError("runner did not select reserved test device")
        ns["ENV"]["RUST_LOG"] = "infinidisk2=info,libublk=warn"
        original_run = ns["run"]

        def query(statement, label):
            return original_run(["docker", "exec", "-e", "MYSQL_PWD", ns["mysqlname"], "mysql",
                                 "--socket=/var/lib/mysql/mysql.sock", "-uroot", "-NBe", statement], label, timeout=300)

        def snapshot(label):
            status = query("SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_deadlocks','Innodb_row_lock_time','Innodb_row_lock_waits','Innodb_row_lock_current_waits','Innodb_data_reads','Innodb_data_writes','Innodb_data_fsyncs','Innodb_log_waits','Uptime')", label + "-status")
            metrics = query("SELECT NAME,COUNT FROM information_schema.INNODB_METRICS WHERE NAME IN ('lock_deadlocks','lock_timeouts','lock_row_lock_time','lock_row_lock_waits') ORDER BY NAME", label + "-innodb-metrics")
            errors = query("SELECT ERROR_NUMBER,ERROR_NAME,SQL_STATE,SUM_ERROR_RAISED,SUM_ERROR_HANDLED FROM performance_schema.events_errors_summary_global_by_error WHERE SUM_ERROR_RAISED>0 OR ERROR_NUMBER IN (1213,1020,1205) ORDER BY ERROR_NUMBER", label + "-errors")
            return {"utc": utc(), "status": parse_status(status.stdout), "innodb_metrics": parse_status(metrics.stdout), "errors": parse_errors(errors.stdout)}

        def diagnostic_run(command, label, timeout=240, check=True):
            command = list(map(str, command))
            if command[:2] == ["docker", "run"]:
                command.append("--innodb-print-all-deadlocks=ON")
            if command[0] == "sysbench":
                command[1:1] = ["--verbosity=5", "--mysql-debug=off", "--db-debug=off"]
                before = snapshot(label + "-before")
                started = time.monotonic()
                result = original_run(command, label, timeout=timeout, check=check)
                elapsed = time.monotonic() - started
                after = snapshot(label + "-after")
                events = parse_ignored(result.stdout)
                sample = ns["parse_sysbench_sample"](result.stdout, 99)
                observation = {"label": label, "seconds": elapsed, "before": before, "after": after,
                               "status_delta": {name: value - before["status"].get(name, 0) for name, value in after["status"].items()},
                               "innodb_metrics_delta": {name: value - before["innodb_metrics"].get(name, 0) for name, value in after["innodb_metrics"].items()},
                               "error_deltas": {code: {**row, "raised_delta": row["raised"] - before["errors"].get(code, {}).get("raised", 0),
                                                       "handled_delta": row["handled"] - before["errors"].get(code, {}).get("handled", 0)} for code, row in after["errors"].items()},
                               "logged_ignored_errors": events, "ignored_error_codes": dict(collections.Counter(str(item["code"]) for item in events)),
                               "sysbench_ignored_errors": sample["ignored_errors"],
                               "all_ignored_errors_logged": len(events) == sample["ignored_errors"],
                               "eligible_for_comparison": False}
                report["observations"].append(observation)
                if events:
                    query("SHOW ENGINE INNODB STATUS", label + "-innodb-status")
                save_report()
                print("DIAGNOSTIC " + json.dumps({"label": label, "ignored_codes": observation["ignored_error_codes"], "ignored_count": sample["ignored_errors"], "all_logged": observation["all_ignored_errors_logged"]}), flush=True)
                return result
            if command[:3] == ["docker", "kill", "--signal"]:
                report["before_database_restart"] = snapshot(label + "-final")
                query("SHOW ENGINE INNODB STATUS", label + "-innodb-status")
                original_run(["docker", "logs", "--timestamps", ns["mysqlname"]], label + "-innodb-server", check=False)
                save_report()
            if command[:2] == ["docker", "rm"] and ns["mysqlname"]:
                original_run(["docker", "logs", "--timestamps", ns["mysqlname"]], label + "-innodb-server", check=False)
            result = original_run(command, label, timeout=timeout, check=check)
            if label.endswith("-mysql-settings"):
                effective = query("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('innodb_flush_log_at_trx_commit','sync_binlog','innodb_doublewrite','innodb_flush_method','innodb_buffer_pool_size','innodb_redo_log_capacity','innodb_print_all_deadlocks','innodb_deadlock_detect','innodb_lock_wait_timeout','transaction_isolation','autocommit','binlog_format','log_bin','binlog_expire_logs_seconds','version')", label + "-effective")
                report["effective_mysql_settings"] = dict(line.split("\t", 1) for line in effective.stdout.splitlines())
                save_report()
            return result

        ns["run"] = diagnostic_run
        label = "infinidisk2-" + phase
        ns["cfg"] = fixture / (label + "-config.toml")
        ns["cfg"].write_text(ns["existing_text"]("infinidisk2"))
        ns["R"]["complete"] = False
        ns["R"]["sql_diagnostic"] = protocol
        save_report()
        try:
            ns["start"]("infinidisk2", label)
            ns["mysql"](label, existing=True)
            ns["stop"]()
            ns["R"]["complete"] = True
            report["database_SIGKILL_recovery"] = ns["R"]["mysql"][label]["database_SIGKILL_recovery"]
            report["complete"] = True
        finally:
            try:
                ns["stop"]()
            finally:
                ns["save"]()
                report["ended_utc"] = utc()
                for path in sorted(fixture.glob("*" + phase + "*")):
                    if path.is_file() and path.suffix in {".json", ".log", ".toml"}:
                        checked_write(output / "raw" / path.name, path.read_bytes())
                save_report()
        preflight()
        report["clean_teardown"] = True
        save_report()
        print("REPORT " + str(output / "report.json"), flush=True)


if __name__ == "__main__":
    main()
