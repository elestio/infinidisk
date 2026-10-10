#!/usr/bin/env python3
"""Sequential MySQL A/B campaign on the two existing disposable fixtures only.

Never builds a binary, drops host caches, or changes a production service.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import math
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
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = {
    "small": ROOT / "test-output/comparison-323bb1097b47/report.json",
    "large": ROOT / "test-output/comparison-be4ba51a4a38/report.json",
}
ASTRA_DEFAULTS = {
    "async_cache": False, "cache_queue_mib": 16, "fast_local_reads": False,
    "checkpoint_pipeline": False, "selective_sync": False, "ublk_fast_path": False,
    "generation_mode": False, "generation_max_lag_seconds": 30,
    "paged_index": False, "compact_checkpoints": False, "aligned_wal": False,
}
PROFILES = {
    "compat": {},
    "core": {"async_cache": True, "fast_local_reads": True, "selective_sync": True},
    "pipeline": {"async_cache": True, "fast_local_reads": True, "selective_sync": True,
                 "checkpoint_pipeline": True},
    "compact": {"async_cache": True, "fast_local_reads": True, "selective_sync": True,
                "checkpoint_pipeline": True, "compact_checkpoints": True, "paged_index": True},
    "ublk": {"async_cache": True, "fast_local_reads": True, "selective_sync": True,
             "ublk_fast_path": True},
}
WORKLOADS = ("read_only", "read_write", "write_only")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def options(size, profile):
    historical = {
        "logical_cache": True, "wal_fixed_size": True, "wal_commit_records": True,
        "sync_data_only": True, "wal_preallocate": False, "wal_writev": True,
        "flush_batch_us": 0, "checkpoint_seconds": 5, "segment_mib": 8,
        "max_pending_mib": 1024, "hot_wal_mib": 0, "max_inflight": 128,
        "max_index_mib": 1024, "memory_cache_mib": 1024 if size == "small" else 64,
        "disk_cache_mib": 4096, "read_extent_kib": 64,
    }
    if profile == "baseline":
        return historical
    return {**historical, **ASTRA_DEFAULTS, **PROFILES[profile]}


def summarize(entry):
    result = {}
    for workload, samples in entry["samples"].items():
        cpu = [s["resources"]["processes"]["engine"].get("cpu_percent_of_one_core") for s in samples]
        cpu = [value for value in cpu if value is not None]
        mysql_cpu = [s["resources"]["processes"]["mysql"].get("cpu_percent_of_one_core") for s in samples]
        mysql_cpu = [value for value in mysql_cpu if value is not None]
        throttling = [s["resources"]["processes"]["mysql"].get("cgroup_cpu", {}) for s in samples]
        throttle_percent = [value["throttled_periods_percent"] for value in throttling if value.get("comparable") and "throttled_periods_percent" in value]
        result[workload] = {
            "samples": len(samples), "tps": [s["tps"] for s in samples],
            "median_tps": statistics.median(s["tps"] for s in samples),
            "p99_ms": [s["p99_ms"] for s in samples],
            "median_p99_ms": statistics.median(s["p99_ms"] for s in samples),
            "ignored_errors": sum(s.get("ignored_errors") or 0 for s in samples),
            "median_engine_cpu_percent_of_one_core": statistics.median(cpu) if cpu else None,
            "median_mysql_cpu_percent_of_one_core": statistics.median(mysql_cpu) if mysql_cpu else None,
            "mysql_throttled_periods_percent": throttle_percent,
            "median_mysql_throttled_periods_percent": statistics.median(throttle_percent) if throttle_percent else None,
            "mysql_throttled_usec": [value["counters_delta"]["throttled_usec"] for value in throttling if value.get("comparable") and "throttled_usec" in value["counters_delta"]],
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, default=ROOT / "target/release/infinidisk2-astra")
    parser.add_argument("--baseline", type=pathlib.Path, default=ROOT / "target/release/infinidisk2-pre-astra")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--build-revision-reason", help="explicitly resume the unfinished matrix with a new, expected Astra hash while retaining all prior per-stage evidence")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--confirm-final", action="store_true", help="append final-build NBD/ublk qualification and ublk engine crash recovery to a completed campaign")
    parser.add_argument("--cpu-control", action="store_true", help="append a separate 2-CPU baseline/Astra/native diagnostic after final confirmation")
    parser.add_argument("--preserve-caches", action="store_true", help="rename stopped large-fixture caches between one- and sixteen-partition profiles; still run complete warm")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--screen-only", action="store_true")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    if not 5 <= args.seconds <= 120:
        parser.error("sample duration must be between 5 and 120 seconds")
    if (args.confirm_final or args.cpu_control) and (not args.resume or args.screen_only):
        parser.error("final confirmation/CPU control requires --resume and cannot be combined with --screen-only")
    if args.confirm_final and args.cpu_control:
        parser.error("select final confirmation or CPU control, not both")
    if args.build_revision_reason and (not args.resume or not args.expected_binary_sha256 or args.confirm_final or args.cpu_control):
        parser.error("build revision requires an unfinished --resume and --expected-binary-sha256")
    screen = [
        ("small-screen-baseline-before", "small", "baseline", ("write_only", "read_write")),
        ("small-screen-core", "small", "core", ("write_only", "read_write")),
        ("small-screen-pipeline", "small", "pipeline", ("write_only", "read_write")),
        ("small-screen-compact", "small", "compact", ("write_only", "read_write")),
        ("small-screen-baseline-after", "small", "baseline", ("write_only", "read_write")),
        ("large-screen-baseline-before", "large", "baseline", ("read_only",)),
        ("large-screen-core", "large", "core", ("read_only",)),
        ("large-screen-ublk", "large", "ublk", ("read_only",)),
        ("large-screen-baseline-after", "large", "baseline", ("read_only",)),
    ]
    if args.plan_only:
        if args.cpu_control:
            print(json.dumps({"diagnostic": "CPU quota only; original 1-CPU results remain authoritative and separate", "mysql_cpus": 2, "samples": 3, "seconds": args.seconds, "stages": [{"size": size, "workload": workload, "engine": engine} for size, workload in (("small", "read_write"), ("large", "read_only")) for engine in ("baseline", "selected NBD", "native")], "caches": "preserved by rename only while volume lock held; complete warm before every large InfiniDisk2 stage"}, indent=2))
            return
        if args.confirm_final:
            print(json.dumps({"confirmation": "completed campaign selected profile; small fixture NBD then ublk, then separate ublk engine SIGKILL recovery", "samples": 3, "seconds": args.seconds, "workloads": WORKLOADS, "mysql_cpus": 1}, indent=2))
            return
        print(json.dumps({"screen": screen, "qualification": "baseline/chosen/native for both sizes: 3 samples of all 3 workloads; small ZeroFS durable/async: 1 sample of all workloads", "seconds": args.seconds, "profiles": PROFILES}, indent=2))
        return
    work = ROOT / "validation/astra/mysql"
    work.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = (work / "campaign.lock").open("w")
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    report_path = work / "report.json"
    if report_path.exists() and not args.resume:
        raise RuntimeError("existing report: use --resume to preserve prior evidence")
    binaries = {"astra": args.binary.resolve(), "baseline": args.baseline.resolve()}
    hashes = {name: digest(path) for name, path in binaries.items()}
    if args.expected_binary_sha256 and hashes["astra"] != args.expected_binary_sha256:
        raise RuntimeError("Astra binary differs from the qualified build")
    for fixture in FIXTURES.values():
        if not json.loads(fixture.read_text()).get("complete"):
            raise RuntimeError("incomplete fixture: " + str(fixture))
    report = json.loads(report_path.read_text()) if args.resume else {
        "campaign": "astra-" + uuid.uuid4().hex[:10], "created_utc": utc(),
        "complete": False, "stages": [], "binary_sha256": hashes,
        "binary_paths": {name: str(path) for name, path in binaries.items()},
        "fixtures": {name: {"report": str(path), "sha256": digest(path)} for name, path in FIXTURES.items()},
        "protocol": {
            "sample_seconds": args.seconds, "screen_samples": 1, "qualification_samples": 3,
            "latency_percentile": 99, "small_warm_seconds": 10, "large_warm_seconds": 60,
            "small_random_distribution": "special", "large_random_distribution": "uniform",
            "large_ssd_prefill": "offline warm before every InfiniDisk2 large stage, including baseline",
            "small_engine_memory_mib": 1024, "large_engine_memory_mib": 64, "ssd_mib": 4096,
            "mysql_cpu_limit": 1, "mysql_memory_mib": 1024, "mysql_buffer_pool_mib": 256,
            "threads": 8, "durability": "InnoDB fsync=1, sync_binlog=1, doublewrite ON; InfiniDisk2 local WAL fsync; ZeroFS durable S3 and ignored-fsync modes reported separately",
            "recovery": "write and qualification stages include MySQL SIGKILL + CHECK TABLE; read-only screening reuses validated fixture and explicitly skips that repeated check",
            "selection": "highest geometric mean of small read_write/write_only TPS, choosing simpler profile within 3%; reject candidates with ignored SQL errors; use NBD for conservative qualification",
            "limitations": "single VM and evolving fixed-size database; no global cache drop; Linux page cache outside configured engine RAM; screening has one sample; host services remain running",
            "excluded_formats": ["aligned_wal", "generation_mode"],
        },
        "host": {"kernel": " ".join(os.uname()), "logical_cpus": os.cpu_count()},
    }
    extension = args.confirm_final or args.cpu_control
    if report.get("complete") and not extension:
        raise RuntimeError("campaign already complete")
    if extension and not report.get("complete"):
        raise RuntimeError("confirmation/CPU control requires a completed original campaign")
    if args.cpu_control and not any(value.get("complete") and value["binary_sha256"] == hashes["astra"] for value in report.get("final_confirmations", [])):
        raise RuntimeError("CPU control requires successful final confirmation with this binary")
    if args.build_revision_reason and report["binary_sha256"]["baseline"] == hashes["baseline"] and report["binary_sha256"]["astra"] != hashes["astra"]:
        old_hash = report["binary_sha256"]["astra"]
        report.setdefault("build_revisions", []).append({
            "utc": utc(), "old_sha256": old_hash, "new_sha256": hashes["astra"],
            "reason": args.build_revision_reason, "baseline_unchanged": True,
            "retained_stages": [value["phase"] for value in report["stages"]],
            "superseded_stages": [],
            "protocol": "Prior per-stage binary hashes and raw proofs remain unchanged. Completed screening is exploratory evidence for profile selection; failed stages are retried and remaining qualification uses this revision.",
        })
        for value in report["stages"]:
            if "-screen-" in value["label"] and value["profile"] != "baseline":
                value["evidence_scope"] = "exploratory_screening_prior_binary"
        report["binary_sha256"] = hashes
        report["binary_paths"] = {name: str(path) for name, path in binaries.items()}
    binary_mismatch = report["binary_sha256"]["baseline"] != hashes["baseline"] if extension else report["binary_sha256"] != hashes
    if binary_mismatch or report["protocol"]["sample_seconds"] != args.seconds:
        raise RuntimeError("resume requires the same binaries and sampling protocol")
    secrets = []
    for line in pathlib.Path("/opt/elestio/infinidisk/bench.env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            if key in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "INFINIDISK_PASSWORD"}:
                secrets.extend(s.encode() for s in shlex.split(value) if len(s) > 8)
    for fixture in FIXTURES.values():
        secrets.extend(p.read_bytes().strip() for p in fixture.parent.glob("*.secret") if len(p.read_bytes().strip()) > 8)

    def checked_write(path, content):
        if any(secret in content for secret in secrets):
            if path.is_file():
                path.unlink()
            raise RuntimeError("credential detected in evidence; refused to export " + path.name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    def save():
        data = json.dumps(report, indent=2).encode()
        pending = report_path.with_suffix(".json.tmp")
        checked_write(pending, data)
        pending.replace(report_path)

    source_files = [ROOT / "Cargo.toml", ROOT / "Cargo.lock", *sorted((ROOT / "src").glob("*.rs")), *sorted((ROOT / "tests").glob("*.rs")), pathlib.Path(__file__).resolve(), ROOT / "scripts/compare_zerofs.py"]
    source = {str(path.relative_to(ROOT)): {"sha256": digest(path), "content": path.read_text()} for path in source_files}
    suffix = "-cpu-control-" + str(time.time_ns()) if args.cpu_control else "-final-" + str(time.time_ns()) if args.confirm_final else "-resume-" + str(time.time_ns()) if args.resume else ""
    checked_write(work / ("source-snapshot" + suffix + ".json"), json.dumps(source, indent=2).encode())
    checked_write(work / ("manifest" + suffix + ".json"), json.dumps({"binary_sha256": hashes, "source_sha256": {name: value["sha256"] for name, value in source.items()}, "created_utc": utc()}, indent=2).encode())
    save()

    def preflight():
        for name, path in binaries.items():
            if digest(path) != hashes[name]:
                raise RuntimeError("binary changed during campaign: " + name)
        for path in pathlib.Path("/proc").glob("[0-9]*/comm"):
            try:
                if path.read_text().strip() in {"cargo", "rustc", "cc1", "ld.lld"}:
                    raise RuntimeError("compiler active; measurements would be contaminated")
            except FileNotFoundError:
                pass
        if not pathlib.Path("/dev/nbd31").exists() or pathlib.Path("/sys/class/block/nbd31/pid").exists():
            raise RuntimeError("reserved test nbd31 is unavailable")
        if pathlib.Path("/dev/ublkc31").exists() or pathlib.Path("/sys/class/block/ublkb31").exists():
            raise RuntimeError("test ublk31 is still present")
        for port in (11991, 12991):
            with socket.socket() as sock:
                sock.settimeout(0.2)
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    raise RuntimeError("test port still listening: " + str(port))
        for fixture in FIXTURES.values():
            if os.path.ismount(fixture.parent / "mount"):
                raise RuntimeError("test fixture still mounted")

    def select_large_cache(partitions):
        fixture = FIXTURES["large"].parent.resolve()
        config = tomllib.loads((fixture / "infinidisk2.toml").read_text())
        local = pathlib.Path(config["local_dir"]).resolve()
        if not local.is_relative_to(fixture):
            raise RuntimeError("cache preservation may only use the large test fixture")
        active = local / "logical-cache"
        bank = local / "astra-cache-bank"
        if active.is_symlink() or bank.is_symlink():
            raise RuntimeError("cache preservation refuses symlinks")

        def partition_count(directory):
            identity = (directory / "identity").read_text().split(":")
            if len(identity) == 2:
                return 1
            if len(identity) == 3 and identity[-1] in {"1", "16"}:
                return int(identity[-1])
            raise RuntimeError("unexpected disposable-cache identity")

        def cache_details(directory):
            return {"path": str(directory), "identity": (directory / "identity").read_text(), "allocated_bytes": sum(path.stat().st_blocks * 512 for path in directory.iterdir() if path.is_file())}

        with (local / "LOCK").open("a+b") as volume_lock:
            fcntl.flock(volume_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            current = partition_count(active) if active.exists() else None
            if current == partitions:
                return
            bank.mkdir(exist_ok=True)
            actions = []
            if current is not None:
                saved = bank / ("partitions-" + str(current))
                if saved.exists() or saved.is_symlink():
                    raise RuntimeError("refuse to overwrite a preserved cache")
                details = cache_details(active)
                active.rename(saved)
                actions.append({"operation": "preserve", "from": str(active), "to": str(saved), "cache": details})
            saved = bank / ("partitions-" + str(partitions))
            if saved.exists():
                if saved.is_symlink() or partition_count(saved) != partitions:
                    raise RuntimeError("saved cache has unexpected identity")
                details = cache_details(saved)
                saved.rename(active)
                actions.append({"operation": "restore", "from": str(saved), "to": str(active), "cache": details})
            proof = work / "cache-preservations.json"
            history = json.loads(proof.read_text()) if proof.exists() else {"note": "Only disposable caches renamed while the volume LOCK is held and test devices are stopped. Full warm still validates references. Preparation times are not controlled cold-cache comparisons.", "events": []}
            history["events"].append({"utc": utc(), "target_partitions": partitions, "actions": actions, "complete_warm_required": True})
            checked_write(proof, json.dumps(history, indent=2).encode())
            report["cache_preservation_proof"] = str(proof.relative_to(work))

    def stage(label, size, profile, workloads, samples, engine="infinidisk2", zero_async=None, transport=None, storage_crash=False, mysql_cpus=1):
        previous = [item for item in report["stages"] if item["label"] == label and item.get("returncode") == 0 and item.get("complete") and not item.get("superseded")]
        if previous:
            return previous[-1]
        pause = work / "pause-before-next-stage"
        if pause.exists():
            print("PAUSED before " + label, flush=True)
            while pause.exists():
                time.sleep(1)
        preflight()
        fixture = FIXTURES[size]
        attempt = 1 + sum(item["label"] == label for item in report["stages"])
        phase = report["campaign"] + "-" + label + "-" + str(attempt)
        directory = work / phase
        directory.mkdir(exist_ok=True)
        selected_binary = binaries["baseline" if profile == "baseline" else "astra"]
        transport = transport or ("ublk" if profile == "ublk" else "nbd")
        command = [sys.executable, str(ROOT / "scripts/compare_zerofs.py"), "--binary", str(selected_binary), "--mysql-recovery-report" if storage_crash else "--mysql-repeat-report", str(fixture), "--phase-label", phase, "--engine", engine, "--mysql-workloads", *workloads, "--mysql-samples", str(samples), "--mysql-seconds", str(args.seconds), "--mysql-percentile", "99", "--mysql-cpus", str(mysql_cpus), "--mysql-warm-seconds", "10" if size == "small" else "60", "--mysql-rand-type", "special" if size == "small" else "uniform"]
        settings = options(size, profile) if engine == "infinidisk2" else {}
        if engine == "infinidisk2" and transport == "ublk":
            settings["ublk_fast_path"] = True
        if (args.preserve_caches or args.cpu_control) and engine == "infinidisk2" and size == "large":
            select_large_cache(16 if settings.get("fast_local_reads") else 1)
        if engine == "infinidisk2":
            option_path = directory / "options.json"
            checked_write(option_path, json.dumps(settings, indent=2).encode())
            command += ["--engine-options", str(option_path)]
            if profile == "baseline":
                command.append("--legacy-config")
            if transport == "ublk":
                command += ["--transport", "ublk"]
            if size == "large":
                command.append("--offline-warm")
        if tuple(workloads) == ("read_only",) and samples == 1 and not storage_crash:
            command.append("--mysql-skip-crash-check")
        original = None
        zero_config = fixture.parent / "zerofs.toml"
        if zero_async is not None:
            original = zero_config.read_bytes()
            text = original.decode()
            if not re.search(r"^ignore_fsync\s*=", text, re.M):
                raise RuntimeError("ZeroFS fixture has no explicit fsync setting")
            text = re.sub(r"^ignore_fsync\s*=.*$", "ignore_fsync = " + str(zero_async).lower(), text, flags=re.M)
            zero_config.write_text(text)
        item = {"label": label, "size": size, "profile": profile, "engine": engine, "phase": phase, "attempt": attempt, "started_utc": utc(), "arguments": command, "options": settings, "binary_sha256": digest(selected_binary), "samples": 0 if storage_crash else samples, "workloads": [] if storage_crash else list(workloads), "load_before": os.getloadavg(), "returncode": None, "zero_ignore_fsync": zero_async, "transport": transport, "storage_crash": storage_crash, "mysql_cpus": mysql_cpus}
        report["stages"].append(item)
        save()
        print("STAGE " + label, flush=True)
        started = time.monotonic()
        try:
            with (directory / "runner.log").open("w") as log:
                result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            item["returncode"] = result.returncode
        finally:
            if original is not None:
                zero_config.write_bytes(original)
            item["seconds"] = time.monotonic() - started
            item["ended_utc"] = utc()
            item["load_after"] = os.getloadavg()
            save()
        source_report = fixture.parent / ("optimization-" + phase + ".json")
        if source_report.exists():
            checked_write(directory / "comparison.json", source_report.read_bytes())
            item["proof_report"] = str((directory / "comparison.json").relative_to(work))
            data = json.loads(source_report.read_text())
            entry = data.get("mysql", {}).get(engine + "-" + phase)
            if entry:
                item["summary"] = summarize(entry)
                item["database_SIGKILL_recovery"] = entry.get("database_SIGKILL_recovery")
                item["storage_engine_SIGKILL_recovery"] = entry.get("storage_engine_SIGKILL_recovery")
                item["engine_metadata"] = entry.get("engine_metadata")
                item["preparation"] = data.get("preparation", {}).get(engine + "-" + phase, {})
                item["measurement_context"] = {key: entry.get(key) for key in ("table_file_bytes", "ssd_cache_bytes_before_samples", "engine_rss_kib_before_samples", "engine_peak_rss_kib_before_samples", "engine_memory_cache_mib", "engine_disk_cache_mib", "warmup_seconds", "startup_seconds")}
            item["complete"] = data.get("complete", False)
            for path in sorted(fixture.parent.glob("*" + phase + "*")):
                if path.is_file() and path.suffix in {".json", ".log", ".toml"}:
                    checked_write(directory / "raw" / path.name, path.read_bytes())
        checked_write(directory / "runner.log", (directory / "runner.log").read_bytes())
        save()
        proof_ok = item.get("storage_engine_SIGKILL_recovery") == "passed" if storage_crash else bool(item.get("summary"))
        if item["returncode"] != 0 or not item.get("complete") or not proof_ok:
            raise RuntimeError("failed stage " + label + ": " + str(directory / "runner.log"))
        preflight()
        print("RESULT " + json.dumps({"label": label, "summary": item["summary"], "recovery": item.get("database_SIGKILL_recovery")}), flush=True)
        return item

    try:
        if args.cpu_control:
            selected = report["selection"]["profile"]
            controls = report.setdefault("cpu_controls", [])
            control = next((value for value in controls if value["binary_sha256"] == hashes["astra"]), None)
            if control is None:
                control = {"binary_sha256": hashes["astra"], "baseline_sha256": hashes["baseline"], "started_utc": utc(), "complete": False,
                           "profile": selected, "mysql_cpus": 2, "samples": 3, "sample_seconds": args.seconds,
                           "protocol": "Separate CPU-quota diagnostic, not a replacement for the 1-CPU campaign. Baseline/Astra NBD/native, small mixed then large uniform read-only; same seed, configured caches and warmup durations. Cache banks preserve only disposable cache files; every large InfiniDisk2 stage still runs complete warm."}
                controls.append(control)
                save()
            prefix = "cpu2-" + hashes["astra"][:12]
            stages = []
            for size, workload in (("small", "read_write"), ("large", "read_only")):
                stages.append(stage(prefix + "-" + size + "-baseline", size, "baseline", (workload,), 3, mysql_cpus=2))
                stages.append(stage(prefix + "-" + size + "-" + selected, size, selected, (workload,), 3, mysql_cpus=2))
                stages.append(stage(prefix + "-" + size + "-native", size, "compat", (workload,), 3, engine="native", mysql_cpus=2))
            control.update({"stages": [value["phase"] for value in stages], "complete": True, "ended_utc": utc()})
            save()
            print("CPU_CONTROL " + json.dumps(control), flush=True)
            return
        if args.confirm_final:
            selected = report["selection"]["profile"]
            confirmations = report.setdefault("final_confirmations", [])
            confirmation = next((value for value in confirmations if value["binary_sha256"] == hashes["astra"]), None)
            if confirmation is None:
                confirmation = {"binary_sha256": hashes["astra"], "started_utc": utc(), "complete": False,
                                "matrix_binary_sha256": report["binary_sha256"]["astra"], "profile": selected,
                                "protocol": f"small fixture, 3 samples of {args.seconds}s per workload, NBD then ublk; separate engine SIGKILL during mixed writes, ext4 recovery and InnoDB CHECK TABLE; process cgroup v2 CPU counters sampled"}
                confirmations.append(confirmation)
                save()
            prefix = "small-final-" + hashes["astra"][:12]
            already_qualified = next((value for value in reversed(report["stages"]) if value["label"] == "small-qualified-" + selected and value.get("complete") and value.get("returncode") == 0 and value["binary_sha256"] == hashes["astra"]), None)
            if already_qualified:
                confirmation["reused_nbd_qualification"] = already_qualified["phase"]
            stages = [already_qualified or stage(prefix + "-nbd", "small", selected, WORKLOADS, 3),
                      stage(prefix + "-ublk", "small", selected, WORKLOADS, 3, transport="ublk"),
                      stage(prefix + "-ublk-recovery", "small", selected, WORKLOADS, 1, transport="ublk", storage_crash=True)]
            confirmation.update({"stages": [value["phase"] for value in stages], "complete": True, "ended_utc": utc()})
            save()
            print("FINAL_CONFIRMATION " + json.dumps(confirmation), flush=True)
            return
        for label, size, profile, workloads in screen:
            stage(label, size, profile, workloads, 1)
        candidates = []
        for profile in ("core", "pipeline", "compact"):
            result = next(item for item in reversed(report["stages"]) if item["label"] == "small-screen-" + profile and item.get("returncode") == 0 and not item.get("superseded"))
            if any(value["ignored_errors"] for value in result["summary"].values()):
                continue
            if result.get("database_SIGKILL_recovery") != "passed":
                continue
            score = math.sqrt(result["summary"]["write_only"]["median_tps"] * result["summary"]["read_write"]["median_tps"])
            candidates.append((profile, score))
        if not candidates:
            raise RuntimeError("no error-free recovered Astra candidate for qualification")
        best = max(score for _, score in candidates)
        selected = next(profile for profile, score in candidates if score >= best * 0.97)
        report["selection"] = {"profile": selected, "transport": "nbd", "candidate_scores": dict(candidates), "explanation": "geometric mean of small write/mixed throughput, with simpler profile selected within 3%; ublk remains separately measured because screen covered reads only"}
        save()
        print("SELECTED " + json.dumps(report["selection"]), flush=True)
        if args.screen_only:
            report["screen_complete"] = True
            save()
            return
        for size in ("small", "large"):
            stage(size + "-qualified-baseline", size, "baseline", WORKLOADS, 3)
            stage(size + "-qualified-" + selected, size, selected, WORKLOADS, 3)
            stage(size + "-qualified-native", size, "compat", WORKLOADS, 3, engine="native")
        if (FIXTURES["small"].parent / "zerofs.toml").exists():
            stage("small-zerofs-durable", "small", "compat", WORKLOADS, 1, engine="zerofs", zero_async=False)
            stage("small-zerofs-async", "small", "compat", WORKLOADS, 1, engine="zerofs", zero_async=True)
        else:
            report["zerofs_skipped"] = "existing small fixture has no ZeroFS configuration"
        report["complete"] = True
        report["completed_utc"] = utc()
        save()
    finally:
        print("CAMPAIGN " + str(report_path), flush=True)


if __name__ == "__main__":
    main()
