#!/usr/bin/env python3
"""Sequential durable S3/PostgreSQL qualification; export sanitized text evidence only.

Run on the reserved test VM after the final binary is frozen. --plan-only and
--self-test are local, do not read credentials, and never start the validator.
"""
import argparse
import ast
import datetime
import fcntl
import hashlib
import json
import os
import pathlib
import re
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts/validate_vm.py"
OUT = ROOT / "validation/astra"
PROFILES = {name: ROOT / f"scripts/profiles/astra-recovery-{name}.json"
            for name in ("core", "aligned")}
REQUIRED_TESTS = {
    "local_SIGKILL_ext4_fsync", "postgres_SIGKILL_amcheck",
    "storage_engine_SIGKILL_postgres_amcheck", "remote_scrub",
    "remote_only_ext4_recovery", "remote_only_postgres_amcheck",
}
FIO_LABELS = {
    "verify-write-read", "randread-hot", "randwrite-fsync",
    "baseline-randwrite-fsync", "seqread-hot", "randread-cold-remote",
    "randread-undersized-cache-pass2", "prime-512m-cache", "randread-fully-warm-cache",
}
MAX_TEXT_BYTES = 64 * 1024 * 1024


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def option_helpers():
    """Read pure definitions by AST; importing validate_vm would run a VM test."""
    names = {"ENGINE_BOOLEAN_OPTIONS", "ENGINE_INTEGER_OPTIONS", "load_engine_options"}
    nodes = [node for node in ast.parse(VALIDATOR.read_text()).body
             if (isinstance(node, ast.FunctionDef) and node.name in names)
             or (isinstance(node, ast.Assign) and any(
                 isinstance(target, ast.Name) and target.id in names for target in node.targets))]
    scope = {"json": json}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(VALIDATOR), "exec"), scope)
    return scope


def load_profiles():
    helpers = option_helpers()
    allowed = helpers["ENGINE_BOOLEAN_OPTIONS"] | helpers["ENGINE_INTEGER_OPTIONS"].keys()
    profiles = {name: helpers["load_engine_options"](path) for name, path in PROFILES.items()}
    for name, options in profiles.items():
        if set(options) != allowed - {"adaptive_reads", "remote_index_cache_mib", "download_budget_mib", "download_max_requests"}:
            raise ValueError("qualification profile must specify every allowlisted option: " + name)
        if options["generation_mode"] or options["ublk_fast_path"]:
            raise ValueError("durable qualification requires generation_mode=false and NBD")
        if options["max_pending_mib"] != 1024 or options["max_index_mib"] != 64:
            raise ValueError("qualification budgets differ from the agreed profile")
    different = {key for key in profiles["core"] if profiles["core"][key] != profiles["aligned"][key]}
    if different != {"aligned_wal"} or profiles["core"]["aligned_wal"]:
        raise ValueError("qualification profiles must differ only in aligned_wal")
    return profiles


def secret_key(key):
    return re.search(r"(?:^|_)(?:PASSWORD|TOKEN|SECRET|ACCESS_KEY|API_KEY|PRIVATE_KEY|AUTHORIZATION)(?:$|_)", key, re.I)


class Redactor:
    def __init__(self, values=()):
        variants = set()
        for value in values:
            if not value:
                continue
            if len(value) < 8:
                raise ValueError("credential is too short for reliable evidence redaction")
            variants.update((value, json.dumps(value)[1:-1],
                             urllib.parse.quote(value, safe=""), urllib.parse.quote_plus(value)))
        self.values = sorted(variants, key=len, reverse=True)

    @classmethod
    def from_credentials(cls, path):
        values = [value for key, value in os.environ.items() if secret_key(key)]
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if secret_key(key):
                values.extend(shlex.split(value, comments=True))
        return cls(values)

    def text(self, text):
        for value in self.values:
            text = text.replace(value, "[REDACTED]")
        text = re.sub(r"(?i)([?&]X-Amz-(?:Credential|Signature|Security-Token)=)[^&\s\"<>]+",
                      r"\1[REDACTED]", text)
        text = re.sub(r"(?im)(\bAuthorization\s*[:=]\s*)[^\r\n]+", r"\1[REDACTED]", text)
        text = re.sub(r"(?i)(\b(?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|"
                      r"INFINIDISK_PASSWORD)\s*[:=]\s*)[^\r\n,;]+", r"\1[REDACTED]", text)
        if any(value in text for value in self.values):
            raise ValueError("credential remained in evidence after redaction")
        return text

    def object(self, value):
        if isinstance(value, dict):
            return {key: "[REDACTED]" if secret_key(key) else self.object(item)
                    for key, item in value.items()}
        if isinstance(value, list):
            return [self.object(item) for item in value]
        return self.text(value) if isinstance(value, str) else value


def atomic_json(path, value):
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    with temporary.open("x") as output:
        output.write(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def is_evidence(name):
    if name.endswith(".json"):
        return name[:-5] in FIO_LABELS or bool(re.fullmatch(r"validation-manifest-\d+\.json", name))
    if not name.endswith(".log"):
        return False
    label = name[:-4]
    return label in FIO_LABELS or bool(re.fullmatch(
        r"(?:init|mkfs|scrub-remote|adopt-remote|(?:serve|attach|mount|unmount|detach)-\d+|"
        r"(?:fsck|crash|postgres|pgbench|cleanup)-[a-z-]+)", label))


def read_regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_TEXT_BYTES:
        raise ValueError("evidence is not a bounded regular text file: " + path.name)
    return path.read_text(encoding="utf-8", errors="strict")


def report_errors(report, options, binary_sha, validator_sha):
    errors = []
    if report.get("passed") is not True:
        errors.append("validator did not report passed=true")
    if report.get("binary_sha256") != binary_sha or report.get("script_sha256") != validator_sha:
        errors.append("validator binary/script SHA differs from the frozen run")
    if any(report.get("tests", {}).get(name) != "passed" for name in REQUIRED_TESTS):
        errors.append("a required crash/recovery test is missing or failed")
    if not (FIO_LABELS | {"pgbench-durable"}) <= report.get("benchmarks", {}).keys():
        errors.append("a required fio/PostgreSQL measurement is missing")
    executions = report.get("validation_executions", [])
    manifest = executions[-1] if executions else {}
    if manifest.get("binary_sha256") != binary_sha or manifest.get("script_sha256") != validator_sha:
        errors.append("execution manifest binary/script SHA differs from the frozen run")
    if manifest.get("transport") != "nbd" or manifest.get("resume") is not False:
        errors.append("qualification must run the complete fresh NBD validator")
    if manifest.get("applied_engine_options") != options:
        errors.append("validator options differ from the qualification profile")
    if any(manifest.get("initial_effective_engine_options", {}).get(k) != v for k, v in options.items()):
        errors.append("initial effective options differ from the qualification profile")
    starts = manifest.get("engine_starts", [])
    if not starts or starts[-1].get("effective_engine_options", {}).get("disk_cache_mib") != 512:
        errors.append("final 512 MiB cache phase was not recorded")
    for start in starts:
        effective = start.get("effective_engine_options", {})
        if effective.get("disk_cache_mib") not in (128, 512) or any(
                effective.get(k) != v for k, v in options.items() if k != "disk_cache_mib"):
            errors.append("engine options changed unexpectedly between recovery stages")
            break
    return errors


def archive_attempt(destination, work, controller_log, report, options, provenance, redactor):
    """No recursion into the fixture: databases, WAL, caches and configs stay private."""
    destination.mkdir(parents=True, exist_ok=True)
    attempt = provenance["attempt"]
    attempts = destination / "attempts"
    attempts.mkdir(exist_ok=True)
    pending = attempts / (".pending-" + attempt)
    pending.mkdir(mode=0o700)
    files = []

    def write(name, text):
        data = text.encode("utf-8")
        (pending / name).write_bytes(data)
        files.append({"name": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})

    write("report.json", json.dumps(redactor.object(report), indent=2) + "\n")
    write("engine-options.json", json.dumps(options, indent=2) + "\n")
    write("runner.log", redactor.text(read_regular(controller_log)))
    if work:
        for source in sorted(work.iterdir()):
            if not is_evidence(source.name):
                continue
            text = read_regular(source)
            write(source.name, json.dumps(redactor.object(json.loads(text)), indent=2) + "\n"
                  if source.suffix == ".json" else redactor.text(text))
    manifest = {"schema": "infinidisk2.astra.durable-evidence.v1", **provenance,
                "passed": report["passed"], "files": files,
                "export_policy": "allowlisted flat UTF-8 logs/JSON; known credentials and auth signatures redacted; no fixture configs, WAL, database, cache, secret file or binary"}
    atomic_json(pending / "manifest.json", redactor.object(manifest))
    final = attempts / attempt
    pending.rename(final)
    atomic_json(destination / "manifest.json", {
        "schema": "infinidisk2.astra.durable-evidence-pointer.v1",
        "manifest": f"attempts/{attempt}/manifest.json",
        "sha256": digest(final / "manifest.json"),
        "report_sha256": digest(final / "report.json"),
    })
    # Publish last: render_astra only sees completed, archived attempts here.
    atomic_json(destination / "report.json", redactor.object(report))
    return final


def verify_existing(destination, options, binary_sha, validator_sha):
    report = json.loads(read_regular(destination / "report.json"))
    pointer = json.loads(read_regular(destination / "manifest.json"))
    manifest_path = destination / pointer["manifest"]
    if manifest_path.resolve().parent.parent != (destination / "attempts").resolve():
        raise ValueError("archived manifest escapes its attempt directory")
    if digest(manifest_path) != pointer["sha256"]:
        raise ValueError("archived manifest checksum mismatch")
    manifest = json.loads(read_regular(manifest_path))
    for entry in manifest["files"]:
        if pathlib.Path(entry["name"]).name != entry["name"]:
            raise ValueError("archived evidence path is not flat")
        file = manifest_path.parent / entry["name"]
        read_regular(file)
        if digest(file) != entry["sha256"]:
            raise ValueError("archived evidence checksum mismatch: " + entry["name"])
    archived = json.loads(read_regular(manifest_path.parent / "report.json"))
    if report != archived or digest(manifest_path.parent / "report.json") != pointer["report_sha256"]:
        raise ValueError("published report differs from its archived attempt")
    if report.get("passed"):
        errors = report_errors(report, options, binary_sha, validator_sha)
        if errors:
            raise ValueError("completed qualification cannot be resumed with different inputs: " + "; ".join(errors))
        return True
    return False


def run_child(argv, log, timeout, label):
    started = time.monotonic()
    interrupted = None
    with log.open("x") as output:
        process = subprocess.Popen(argv, cwd=ROOT, stdin=subprocess.DEVNULL,
                                   stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        next_notice = started + 30
        try:
            while process.poll() is None:
                now = time.monotonic()
                if now - started >= timeout:
                    raise TimeoutError("validator exceeded its stage time limit")
                if now >= next_notice:
                    print(f"RUNNING {label}: {int(now-started)} s", flush=True)
                    next_notice = now + 30
                time.sleep(.5)
        except (KeyboardInterrupt, TimeoutError) as error:
            interrupted = type(error).__name__
            process.send_signal(signal.SIGINT)  # validator's finally owns only its fresh fixture
            for _ in range(360):
                if process.poll() is not None:
                    break
                time.sleep(1)
            else:
                process.kill()
                process.wait()
                interrupted += ": cleanup exceeded 360 s; inspect private fixture before another run"
    return process.returncode, time.monotonic() - started, interrupted


def locate_work(before, controller_log):
    candidates = set((ROOT / "test-output").glob("run-*")) - before
    matches = re.findall(r"^Report:\s+(.+/report\.json)\s*$", controller_log.read_text(), re.M)
    if matches:
        candidates = {pathlib.Path(matches[-1]).resolve().parent} & candidates
    candidates = {path for path in candidates if path.parent == ROOT / "test-output"
                  and re.fullmatch(r"run-[0-9a-f]{12}", path.name)
                  and path.is_dir() and not path.is_symlink()}
    if len(candidates) > 1:
        raise RuntimeError("more than one new validator fixture; refusing ambiguous evidence")
    return next(iter(candidates), None)


def source_hashes():
    files = [ROOT / "Cargo.toml", ROOT / "Cargo.lock", VALIDATOR,
             pathlib.Path(__file__).resolve(), *PROFILES.values(),
             *sorted((ROOT / "src").glob("*.rs")), *sorted((ROOT / "tests").glob("*.rs"))]
    return {str(path.relative_to(ROOT)): digest(path) for path in files}


def self_test():
    profiles = load_profiles()
    secret = "example-secret-1234/+=xyz"
    redactor = Redactor([secret])
    assert secret not in redactor.text("Authorization: Bearer " + secret)
    assert urllib.parse.quote(secret, safe="") not in redactor.text(urllib.parse.quote(secret, safe=""))
    assert "signature-content" not in redactor.text("https://example/?X-Amz-Signature=signature-content&x=1")
    assert redactor.object({"AWS_SECRET_ACCESS_KEY": secret})["AWS_SECRET_ACCESS_KEY"] == "[REDACTED]"
    assert all(not is_evidence(name) for name in ("wal.wal", "data.bin", "database.log", "volume.toml", "password.secret"))
    assert all(is_evidence(name) for name in ("serve-123.log", "postgres-amcheck.log", "validation-manifest-123.json", "randread-hot.json"))
    options = profiles["core"]
    execution = {"transport": "nbd", "resume": False, "applied_engine_options": options,
                 "binary_sha256": "a" * 64, "script_sha256": "b" * 64,
                 "initial_effective_engine_options": options,
                 "engine_starts": [{"effective_engine_options": {**options, "disk_cache_mib": 512}}]}
    report = {"passed": True, "binary_sha256": "a" * 64, "script_sha256": "b" * 64,
              "tests": dict.fromkeys(REQUIRED_TESTS, "passed"),
              "benchmarks": dict.fromkeys(FIO_LABELS | {"pgbench-durable"}, {}),
              "validation_executions": [execution], "diagnostic": secret}
    assert not report_errors(report, options, "a" * 64, "b" * 64)
    assert report_errors(report, profiles["aligned"], "a" * 64, "b" * 64)
    assert report_errors(report, options, "c" * 64, "b" * 64)
    parent = ROOT / "test-output/astra-tmp"
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=parent) as temporary:
        base = pathlib.Path(temporary)
        work = base / "private"
        work.mkdir()
        (work / "serve-123.log").write_text("Authorization: Bearer " + secret + "\nstatus ok\n")
        (work / "password.secret").write_text(secret)
        (work / "wal").mkdir()
        (work / "wal/a.wal").write_bytes(b"PRIVATE-WAL")
        (work / "volume.toml").write_text("private fixture config")
        controller = base / "controller.log"
        controller.write_text("stage diagnostic " + secret)
        destination = base / "archive"
        archive_attempt(destination, work, controller, report, options,
                        {"attempt": "self-test", "binary_sha256": "a" * 64}, redactor)
        assert verify_existing(destination, options, "a" * 64, "b" * 64)
        assert not any(secret in path.read_text() for path in destination.rglob("*") if path.is_file())
        assert not list(destination.rglob("*.secret")) and not list(destination.rglob("*.wal"))
        (destination / "attempts/self-test/runner.log").write_text("tampered")
        try:
            verify_existing(destination, options, "a" * 64, "b" * 64)
        except ValueError:
            pass
        else:
            raise AssertionError("altered evidence was accepted")
        (work / "serve-456.log").symlink_to(work / "password.secret")
        try:
            read_regular(work / "serve-456.log")
        except ValueError:
            pass
        else:
            raise AssertionError("symlink evidence was accepted")
    print("Recovery harness self-test passed: profiles, redaction, evidence allowlist, manifest integrity, option/SHA mismatch and symlink refusal; no VM touched.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, default=ROOT / "target/release/infinidisk2-astra")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--credentials", type=pathlib.Path, default=pathlib.Path("/opt/elestio/infinidisk/bench.env"))
    parser.add_argument("--bucket", default="testperf-6czebk")
    parser.add_argument("--endpoint", default="https://storage.elestio.com")
    parser.add_argument("--stage-timeout", type=int, default=5400)
    parser.add_argument("--resume", action="store_true", help="verify and skip passing stages; retry failed stages in new fixtures, preserving every attempt")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    profiles = load_profiles()
    if args.plan_only:
        print(json.dumps({"order": ["recovery-core", "recovery-aligned"], "profiles": profiles,
                          "output": {name: str(OUT / ("recovery-" + name)) for name in profiles},
                          "validator": str(VALIDATOR), "flags": ["--s3", "--postgres"],
                          "expected_binary_sha256_required_at_execution": True,
                          "transport": "nbd", "contract": "local-fsync", "starts_no_process": True}, indent=2))
        return 0
    if not args.expected_binary_sha256 or not re.fullmatch(r"[0-9a-f]{64}", args.expected_binary_sha256):
        parser.error("execution requires --expected-binary-sha256 from the frozen final build")
    if not 60 <= args.stage_timeout <= 10800:
        parser.error("stage timeout must be 60..10800 seconds")
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK) or digest(binary) != args.expected_binary_sha256:
        parser.error("binary is not executable or differs from the required SHA256")
    if os.geteuid() != 0:
        parser.error("execution requires root on the reserved test VM")
    redactor = Redactor.from_credentials(args.credentials)
    for tool in ("docker", "fio", "mount", "umount", "mkfs.ext4", "e2fsck"):
        if not shutil.which(tool):
            parser.error("missing validator dependency: " + tool)
    (ROOT / "test-output").mkdir(exist_ok=True)
    locks = []
    for path in (ROOT / "test-output/astra-recovery.lock", OUT / "mysql/campaign.lock"):
        if path.parent.exists():
            lock = path.open("a")
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locks.append(lock)
    controller = ROOT / "test-output" / ("astra-recovery-" + uuid.uuid4().hex[:12])
    controller.mkdir(mode=0o700)
    validator_sha = digest(VALIDATOR)
    sources = source_hashes()
    for name, options in profiles.items():
        label = "recovery-" + name
        destination = OUT / label
        if (destination / "report.json").exists():
            if not args.resume:
                raise RuntimeError("archived stage exists; --resume preserves and verifies previous evidence")
            if verify_existing(destination, options, args.expected_binary_sha256, validator_sha):
                print("SKIP verified passing stage " + label, flush=True)
                continue
        if digest(binary) != args.expected_binary_sha256 or digest(VALIDATOR) != validator_sha:
            raise RuntimeError("binary or validator changed before stage start")
        if option_helpers()["load_engine_options"](PROFILES[name]) != options:
            raise RuntimeError("qualification profile changed before stage start")
        # Production nbd0/1 and databases remain active. The validator selects
        # the highest available NBD; refuse rather than falling back off slot31.
        if not pathlib.Path("/dev/nbd31").exists() or pathlib.Path("/sys/class/block/nbd31/pid").exists():
            raise RuntimeError("reserved test nbd31 is unavailable")
        if pathlib.Path("/dev/ublkc31").exists() or pathlib.Path("/sys/class/block/ublkb31").exists():
            raise RuntimeError("reserved test ublk31 is present")
        with socket.socket() as check:
            check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            check.bind(("127.0.0.1", 11990))
        attempt = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
        provenance = {"attempt": attempt, "profile": label, "started_utc": utc(),
                      "binary_path": str(binary), "binary_sha256": args.expected_binary_sha256,
                      "validator_sha256": validator_sha, "launcher_sha256": digest(pathlib.Path(__file__)),
                      "profile_sha256": digest(PROFILES[name]), "source_files_sha256": sources,
                      "transport": "nbd", "contract": "local-fsync", "source_snapshot_is_build_attestation": False}
        before = set((ROOT / "test-output").glob("run-*"))
        log = controller / (label + ".log")
        command = [sys.executable, str(VALIDATOR), "--s3", "--postgres", "--binary", str(binary),
                   "--credentials", str(args.credentials.resolve()), "--bucket", args.bucket,
                   "--endpoint", args.endpoint, "--engine-options", str(PROFILES[name])]
        print("STAGE " + label + " binary_sha256=" + args.expected_binary_sha256, flush=True)
        code, seconds, interruption = run_child(command, log, args.stage_timeout, label)
        provenance.update(finished_utc=utc(), returncode=code, seconds=seconds, interruption=interruption)
        work = locate_work(before, log)
        report_path = work / "report.json" if work else None
        report = json.loads(read_regular(report_path)) if report_path and report_path.exists() else {
            "passed": False, "tests": {}, "benchmarks": {}, "binary_sha256": args.expected_binary_sha256,
            "script_sha256": validator_sha, "error": "validator exited without a report"}
        errors = report_errors(report, options, args.expected_binary_sha256, validator_sha)
        if code != 0 or interruption:
            errors.append("validator process failed or was interrupted")
        if digest(binary) != args.expected_binary_sha256 or digest(VALIDATOR) != validator_sha:
            errors.append("binary or validator changed during stage")
        if digest(PROFILES[name]) != provenance["profile_sha256"]:
            errors.append("qualification profile changed during stage")
        report["passed"] = not errors
        report["qualification"] = {**{key: provenance[key] for key in ("profile", "attempt", "started_utc", "finished_utc", "returncode", "seconds", "profile_sha256")},
                                   "schema": "infinidisk2.astra.durable-report.v1", "errors": errors,
                                   "manifest": f"attempts/{attempt}/manifest.json"}
        archive_attempt(destination, work, log, report, options, provenance, redactor)
        print("ARCHIVED " + str(destination / "report.json") + " passed=" + str(report["passed"]), flush=True)
        if errors:
            print("STOP: " + "; ".join(errors) + "; next profile was not started", flush=True)
            return 1
    print("Both durable qualifications passed and their evidence manifests are archived.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
