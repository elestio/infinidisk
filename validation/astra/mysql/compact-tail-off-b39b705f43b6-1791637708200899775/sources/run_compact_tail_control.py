#!/usr/bin/env python3
"""Run the single predeclared compaction diagnostic after MySQL extensions.

Keeps the main campaign and its selection untouched. The only engine option
changed from the reference is compact_checkpoints=false. A started control is
never automatically retried. Run without --execute to inspect the protocol.
"""
import argparse
import contextlib
import datetime
import fcntl
import hashlib
import importlib.util
import json
import os
import pathlib
import re
import shlex
import socket
import statistics
import subprocess
import sys
import time
import tomllib


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def resource_summary(entry):
    result = {}
    for workload, samples in entry["samples"].items():
        processes = [sample["resources"]["processes"]["engine"] for sample in samples]
        if not all(process.get("comparable") for process in processes):
            raise RuntimeError("non-comparable engine process identity in resource samples")
        fields = {key: [process["io_delta"][key] for process in processes]
                  for key in ("read_bytes", "write_bytes", "rchar", "wchar")}
        fields.update({"rss_before_kib": [process["before"]["rss_kib"] for process in processes],
                       "rss_after_kib": [process["after"]["rss_kib"] for process in processes],
                       "peak_rss_lifetime_kib": [process["after"]["peak_rss_lifetime_kib"] for process in processes]})
        result[workload] = {**fields, **{"median_" + key: statistics.median(values) for key, values in fields.items()},
                            "max_peak_rss_lifetime_kib": max(fields["peak_rss_lifetime_kib"])}
    return result


def checkpoint_context(path):
    samples = []
    for line in path.read_text().splitlines():
        line = re.sub(r"\x1b\[[0-9;]*m", "", line)
        if "volume status status=" in line:
            status = json.loads(line.split("status=", 1)[1])
            samples.append({"utc": line.split()[0], **status})
    transitions = []
    for before, after in zip(samples, samples[1:]):
        if before["remote_generation"] != after["remote_generation"]:
            transitions.append({"observed_between_utc": [before["utc"], after["utc"]],
                                "generation_before": before["remote_generation"], "generation_after": after["remote_generation"]})
    return {"status": "captured", "status_samples": samples,
            "max_pending_bytes": max((sample["pending_bytes"] for sample in samples), default=None),
            "max_wal_pool_bytes": max((sample.get("wal_pool_bytes", 0) for sample in samples), default=None),
            "observed_publish_transitions": transitions,
            "exact_checkpoint_durations_available": False,
            "note": "Periodic status covers the whole phase, not exact sysbench windows. Publication transitions are bracketed by status observations; they are not individual checkpoint durations."}


def cleanup_state(root):
    fixture = root / "test-output/comparison-be4ba51a4a38"
    names = subprocess.check_output(["docker", "ps", "-a", "--format", "{{.Names}}"], text=True).splitlines()
    state = {"checked": True, "utc": utc(),
             "test_nbd31_detached": not pathlib.Path("/sys/class/block/nbd31/pid").exists(),
             "test_ublk31_absent": not pathlib.Path("/dev/ublkc31").exists() and not pathlib.Path("/sys/class/block/ublkb31").exists(),
             "test_mysql_container_absent": "id2-compare-mysql-be4ba51a4a38" not in names,
             "fixture_unmounted": not os.path.ismount(fixture / "mount"), "test_ports_free": {}}
    for port in (11991, 12991):
        with socket.socket() as sock:
            sock.settimeout(.2)
            state["test_ports_free"][str(port)] = sock.connect_ex(("127.0.0.1", port)) != 0
    state["passed"] = all(state[key] for key in ("test_nbd31_detached", "test_ublk31_absent", "test_mysql_container_absent", "fixture_unmounted")) and all(state["test_ports_free"].values())
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[3])
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    work = root / "validation/astra/mysql"
    report_path = work / "compact-tail-control-report.json"
    report = json.loads(report_path.read_text())
    if report["binary_sha256"] != args.expected_binary_sha256:
        raise RuntimeError("requested binary differs from the predeclared reference")
    if report.get("started_utc") or report["control"].get("phase"):
        raise RuntimeError("a control was already started; no automatic retry or overwrite")
    if not args.execute:
        print(json.dumps({key: report[key] for key in ("status", "binary_sha256", "option_delta", "protocol", "scope")}, indent=2))
        return
    binary = root / "target/release/infinidisk2-astra"
    if digest(binary) != args.expected_binary_sha256:
        raise RuntimeError("unexpected binary on disk")
    campaign = json.loads((work / "report.json").read_text())
    if not campaign.get("complete") or not all(any(item.get("complete") and item["binary_sha256"] == args.expected_binary_sha256
                                                for item in campaign.get(key, [])) for key in ("final_confirmations", "cpu_controls")):
        raise RuntimeError("matrix, final confirmation, and CPU controls must finish first")
    reference = report["reference"]
    proof = (work / reference["proof_report"]).resolve()
    if not proof.is_relative_to(work) or digest(proof) != reference["proof_report_sha256"]:
        raise RuntimeError("reference proof missing or modified")
    options = {**reference["options"], "compact_checkpoints": False}
    if not reference["options"].get("compact_checkpoints") or not options.get("paged_index"):
        raise RuntimeError("reference does not match the predeclared compact+paged profile")
    if report["control"]["options"] != options:
        raise RuntimeError("control options changed beyond the single declared flag")
    fixture = root / "test-output/comparison-be4ba51a4a38"
    config = tomllib.loads((fixture / "infinidisk2.toml").read_text())
    local = pathlib.Path(config["local_dir"]).resolve()
    if not local.is_relative_to(fixture) or (local / "logical-cache").is_symlink():
        raise RuntimeError("invalid test cache path")
    if (local / "logical-cache/identity").read_text().split(":")[-1].strip() != "16":
        raise RuntimeError("control requires the preserved sixteen-partition cache already active")
    credentials = []
    for line in pathlib.Path("/opt/elestio/infinidisk/bench.env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            if key in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "INFINIDISK_PASSWORD"}:
                credentials.extend(item.encode() for item in shlex.split(value) if len(item) > 8)
    credentials.extend(path.read_bytes().strip() for path in fixture.glob("*.secret") if len(path.read_bytes().strip()) > 8)

    def checked_write(path, data):
        if any(secret in data for secret in credentials):
            raise RuntimeError("credential detected; refused evidence export")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def save():
        pending = report_path.with_suffix(".json.tmp")
        checked_write(pending, json.dumps(report, indent=2).encode())
        pending.replace(report_path)

    with contextlib.ExitStack() as stack:
        for path in (root / "test-output/astra-recovery.lock", work / "campaign.lock"):
            lock = stack.enter_context(path.open("a+b"))
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current_report = json.loads(report_path.read_text())
        if current_report != report:
            raise RuntimeError("control declaration changed before acquiring the campaign locks")
        if not cleanup_state(root)["passed"]:
            raise RuntimeError("test devices, mount, container, or ports are busy")
        for path in pathlib.Path("/proc").glob("[0-9]*/comm"):
            try:
                if path.read_text().strip() in {"cargo", "rustc", "cc1", "ld.lld"}:
                    raise RuntimeError("compiler active")
            except FileNotFoundError:
                pass
        spec = importlib.util.spec_from_file_location("astra_campaign", root / "scripts/run_astra_mysql.py")
        campaign_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(campaign_module)
        phase = "compact-tail-off-" + args.expected_binary_sha256[:12] + "-" + str(time.time_ns())
        directory = work / phase
        directory.mkdir(mode=0o700)
        option_path = directory / "options.json"
        checked_write(option_path, json.dumps(options, indent=2).encode())
        helper = root / "scripts/compare_zerofs.py"
        source_files = (helper, root / "scripts/run_astra_mysql.py", pathlib.Path(__file__).resolve())
        report["source_sha256"] = {path.name: digest(path) for path in source_files}
        for path in source_files:
            checked_write(directory / "sources" / path.name, path.read_bytes())
        command = [sys.executable, str(helper), "--binary", str(binary), "--mysql-repeat-report", str(fixture / "report.json"),
                   "--phase-label", phase, "--engine", "infinidisk2", "--engine-options", str(option_path),
                   "--mysql-workloads", "read_write", "write_only", "--mysql-samples", "3", "--mysql-seconds", "30",
                   "--mysql-percentile", "99", "--mysql-cpus", "1", "--mysql-warm-seconds", "60", "--mysql-rand-type", "uniform",
                   "--offline-warm", "--warm-concurrency", "128"]
        control = report["control"]
        control.update({"phase": phase, "arguments": command, "started_utc": utc()})
        report.update({"status": "running", "started_utc": control["started_utc"]})
        save()

        def preserve_incomplete_report():
            # An evidence-validation error must remain a visible, non-retryable
            # attempt. The helper owns teardown; this callback only observes it.
            if report["status"] != "running":
                return
            reason = "control_interrupted_or_evidence_processing_failed"
            control.update({"complete": False, "eligible_for_comparison": False})
            control.setdefault("comparison_exclusion_reasons", []).append(reason)
            report.update({"status": "failed", "complete": False, "ended_utc": utc(),
                           "eligible_for_comparison": False, "comparison_exclusion_reasons": [reason]})
            try:
                report["cleanup"] = cleanup_state(root)
            except Exception as error:
                report["cleanup"] = {"checked": False, "passed": False, "check_error_type": type(error).__name__}
            save()

        stack.callback(preserve_incomplete_report)
        print("CONTROL " + phase, flush=True)
        started = time.monotonic()
        environment = {**os.environ, "RUST_LOG": "infinidisk2=info,libublk=warn"}
        try:
            with (directory / "runner.log").open("w") as log:
                completed = subprocess.run(command, cwd=root, env=environment, stdout=log, stderr=subprocess.STDOUT)
            control["returncode"] = completed.returncode
        finally:
            control.update({"ended_utc": utc(), "seconds": time.monotonic() - started})
            save()
        source_proof = fixture / ("optimization-" + phase + ".json")
        if source_proof.exists():
            checked_write(directory / "comparison.json", source_proof.read_bytes())
            control["proof_report"] = str((directory / "comparison.json").relative_to(work))
            control["proof_report_sha256"] = digest(directory / "comparison.json")
            data = json.loads(source_proof.read_text())
            entry = data.get("mysql", {}).get("infinidisk2-" + phase)
            if entry:
                metadata = entry["engine_metadata"]
                if metadata["binary_sha256"] != args.expected_binary_sha256 or metadata["options"] != options:
                    raise RuntimeError("executed engine differs from the declared control")
                executed_config = fixture / ("infinidisk2-" + phase + "-config.toml")
                if digest(executed_config) != metadata["config_sha256"]:
                    raise RuntimeError("executed config snapshot changed")
                expected_parameters = {"sample_count": 3, "sample_seconds": 30, "latency_percentile": 99,
                                       "cpu_limit": 1, "memory_mib": 1024, "transport": "nbd", "warmup_seconds": 60,
                                       "random_distribution": "uniform", "threads": 8, "tables": 4, "rows_per_table": 1000000}
                if any(entry.get(key) != value for key, value in expected_parameters.items()):
                    raise RuntimeError("executed MySQL parameters differ from the declared control")
                populated_entry = {**entry, "samples": {key: values for key, values in entry["samples"].items() if values}}
                control["summary"] = campaign_module.summarize(populated_entry)
                control["resource_summary"] = resource_summary(populated_entry)
                control["database_SIGKILL_recovery"] = entry.get("database_SIGKILL_recovery")
                control["engine_metadata"] = metadata
                control["preparation"] = data.get("preparation", {}).get("infinidisk2-" + phase, {})
                exact_samples = set(entry["samples"]) == {"read_write", "write_only"} and all(len(values) == 3 for values in entry["samples"].values())
                control["complete"] = bool(data.get("complete") and exact_samples and entry.get("database_SIGKILL_recovery") == "passed")
                report["integrity"]["database_SIGKILL_recovery"] = entry.get("database_SIGKILL_recovery")
                if entry.get("database_SIGKILL_recovery") == "passed":
                    report["integrity"].update({"table_counts": "passed", "check_table_extended": "passed"})
            for path in sorted(fixture.glob("*" + phase + "*")):
                if path.is_file() and path.suffix in {".json", ".log", ".toml"}:
                    checked_write(directory / "raw" / path.name, path.read_bytes())
        checked_write(directory / "runner.log", (directory / "runner.log").read_bytes())
        server_log = directory / "raw" / ("infinidisk2-" + phase + "-server.log")
        if server_log.exists():
            control["checkpoint_context"] = checkpoint_context(server_log)
        campaign_module.assess_comparison_eligibility(control)
        report["cleanup"] = cleanup_state(root)
        report["complete"] = bool(control.get("complete") and control["returncode"] == 0 and report["cleanup"]["passed"])
        report["eligible_for_comparison"] = report["complete"] and control["eligible_for_comparison"] and reference["eligible_for_comparison"]
        report["comparison_exclusion_reasons"] = list(control["comparison_exclusion_reasons"])
        if not report["cleanup"]["passed"]:
            report["comparison_exclusion_reasons"].append("cleanup_failed")
        report.update({"status": "complete" if report["complete"] else "failed", "ended_utc": utc()})
        save()
        if not report["complete"]:
            raise RuntimeError("control incomplete or teardown failed; preserve evidence and stop")
        print("REPORT " + str(report_path), flush=True)


if __name__ == "__main__":
    main()
