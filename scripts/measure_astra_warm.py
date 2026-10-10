#!/usr/bin/env python3
"""Offline warm measurement on one frozen HEAD; never mount, serve, adopt or compact.

The original volume LOCK remains held throughout. Child processes use a private
metadata replica; existing caches are preserved by rename and restored in finally.
"""
import argparse
import contextlib
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
import struct
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
import zlib

from run_astra_recovery import Redactor, atomic_json, digest, utc

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "test-output/comparison-be4ba51a4a38"
OLD_SHA = "839abdf0cec29812ba280cf24c26ed8f122970fb76195d5241f38c98f736d5e9"
CACHES = ("cache", "logical-cache", "astra-cache-bank", "index-scratch")
PAGE = 4096
META = 40
AWS_KEYS = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def regular(path):
    require(stat.S_ISREG(path.lstat().st_mode), "expected a regular file: " + str(path))
    return path


def checked_binary(path, expected):
    path = path.absolute()
    require(re.fullmatch(r"[a-f0-9]{64}", expected or ""), "an exact SHA256 is required")
    regular(path)
    require(os.access(path, os.X_OK) and digest(path) == expected, "binary SHA/executable mismatch")
    return path


def is_test_process(name, argv, cwd, fixture):
    roots = (str(ROOT / "test-output") + "/", str(fixture) + "/")
    references_test = any(value.partition("=")[2].startswith(roots) if "=" in value
                          else value.startswith(roots) for value in argv)
    references_test |= bool(cwd and (str(cwd) + "/").startswith(roots))
    storage = name in {"mysqld", "mariadbd", "postgres", "sysbench", "fio", "zerofs"} or name.startswith("infinidisk2")
    compiler = name in {"cargo", "rustc", "cc1", "ld.lld"} and cwd is not None and pathlib.Path(cwd).is_relative_to(ROOT)
    return (storage and references_test) or compiler


def preflight(fixture, port):
    for process in pathlib.Path("/proc").glob("[0-9]*/comm"):
        try:
            name = process.read_text().strip()
            argv = (process.parent / "cmdline").read_bytes().decode(errors="replace").split("\0")
            try:
                cwd = (process.parent / "cwd").readlink()
            except (FileNotFoundError, PermissionError):
                cwd = None
            if is_test_process(name, argv, cwd, fixture):
                raise RuntimeError("an InfiniDisk2 test workload or checkout compiler is active: " + name)
        except (FileNotFoundError, ProcessLookupError):
            pass
    # nbd0/1 and production databases coexist on this VM. Only our reserved
    # slots and explicitly named disposable containers may block this harness.
    require(not pathlib.Path("/sys/class/block/nbd31/pid").exists(), "test nbd31 is attached")
    require(not pathlib.Path("/sys/class/block/ublkb31").exists()
            and not pathlib.Path("/dev/ublkc31").exists(), "test ublk31 is present")
    if shutil.which("docker"):
        containers = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=15)
        require(containers.returncode == 0, "could not inspect disposable test containers")
        require(not any(re.fullmatch(r"(?:id2-compare-(?:mysql|pg)|infinidisk2-pg)-[0-9a-f]+", name)
                        for name in containers.stdout.splitlines()), "a disposable database test container is active")
    require(not os.path.ismount(fixture / "mount"), "the fixture is mounted")
    for line in pathlib.Path("/proc/self/mountinfo").read_text().splitlines():
        require(str(fixture) not in line, "a fixture path remains mounted")
    for candidate in {11990, 11991, 12991, port}:
        with socket.socket() as sock:
            sock.settimeout(0.15)
            require(sock.connect_ex(("127.0.0.1", candidate)) != 0, "a test port is still listening")


@contextlib.contextmanager
def exclusive(path):
    require(not path.is_symlink(), "lock file is a symlink")
    with path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def credential_env(path):
    env = os.environ.copy()
    for line in regular(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in AWS_KEYS:
            values = shlex.split(value, comments=True)
            require(len(values) == 1, "invalid private AWS credential format")
            env[key] = values[0]
    env.update(RUST_LOG="infinidisk2=info,libublk=warn", NO_COLOR="1", TERM="dumb")
    return env


def write_config(path, source, local):
    # A flat scalar allowlist prevents exporting or interpreting arbitrary TOML.
    profile = json.loads((ROOT / "scripts/profiles/astra-recovery-core.json").read_text())
    profile.update(async_cache=False, checkpoint_pipeline=False, disk_cache_mib=4096,
                   hot_wal_mib=0, memory_cache_mib=64, generation_mode=False)
    allowed = set(profile) | {"local_dir", "store", "endpoint", "region", "listen"}
    require(not (set(source) - allowed), "fixture config contains unsupported keys")
    config = {**source, **profile, "local_dir": str(local)}
    require(config.get("store", "").startswith("s3://"), "measurement requires the existing S3 fixture")
    require(all(type(value) in (str, int, bool) for value in config.values()), "config must contain only scalar values")
    text = "\n".join(f"{key} = {json.dumps(value)}" for key, value in sorted(config.items())) + "\n"
    require(tomllib.loads(text) == config, "could not serialize private config exactly")
    path.write_text(text)
    path.chmod(0o600)
    return profile


def status(binary, config, env):
    result = subprocess.run([str(binary), "-c", str(config), "status"], env=env,
                            capture_output=True, timeout=60)
    require(result.returncode == 0, "remote HEAD inspection failed; raw output was not exported")
    try:
        head = json.loads(result.stdout)
    except (ValueError, UnicodeError):
        raise RuntimeError("remote HEAD inspection did not return JSON") from None
    require(head.get("format") == 1 and isinstance(head.get("shards"), dict),
            "this measurement requires a durable format-1 HEAD")
    return head


def published_metadata(local, head):
    """Refuse unpublished local work instead of silently warming an older image."""
    identity_path = regular(local / "identity.json")
    identity = json.loads(identity_path.read_text())
    require(all(identity.get(key) == head.get(key) for key in ("volume", "writer", "size")),
            "local identity differs from the committed HEAD")
    marker = regular(local / "durable").read_bytes()
    require(len(marker) == 32, "invalid local durability marker size")
    slots = [struct.unpack_from("<Q", marker, offset + 4)[0] for offset in (0, 16)
             if marker[offset:offset + 4] == b"IDWM"
             and zlib.crc32(marker[offset:offset + 12]) == struct.unpack_from("<I", marker, offset + 12)[0]]
    require(slots and max(slots) <= head["seq"], "fixture has an invalid marker or unpublished durable writes")
    checked = 0
    for path in sorted((local / "wal").iterdir()):
        require(path.suffix == ".wal", "unexpected local WAL entry")
        regular(path)
        with path.open("rb") as source:
            h = source.read(64)
            require(len(h) == 64 and h[:8] in (b"IDWAL001", b"IDWAL002")
                    and zlib.crc32(h[:60]) == struct.unpack_from("<I", h, 60)[0], "invalid local WAL header")
            require(str(uuid.UUID(bytes=h[24:40])) == head["volume"], "WAL belongs to another volume")
            first, capacity = struct.unpack_from("<QQ", h, 40)
            require(0 < first <= head["seq"] + 1, "local WAL starts after the published prefix")
            require(path.name == f"{first:020}-{uuid.UUID(bytes=h[8:24])}.wal", "local WAL filename identity mismatch")
            aligned = h[:8] == b"IDWAL002"
            header, overhead = (PAGE, PAGE) if aligned else (64, 32)
            padding = source.read(header - 64)
            require(len(padding) == header - 64 and not any(padding), "invalid WAL header padding")
            size = os.fstat(source.fileno()).st_size
            require(not capacity or (size == capacity and capacity >= header), "incomplete initialized WAL")
            previous = first - 1
            while source.tell() < size:
                record = source.read(overhead)
                require(len(record) == overhead, "truncated local WAL record")
                if capacity and not any(record):
                    while tail := source.read(1024 * 1024):
                        require(not any(tail), "nonzero data after WAL padding")
                    break
                magic, length, seq, page, count, checksum = struct.unpack_from("<4sIQQII", record)
                require(not any(record[32:]) and magic in (b"WRT1", b"ZER1", b"CMT1"), "invalid local WAL record")
                require(seq <= head["seq"], "fixture contains unpublished WAL; checkpoint it before measuring")
                if magic == b"CMT1":
                    require(length == count == page == 0 and seq == previous, "invalid WAL commit")
                else:
                    require(seq > previous and count > 0 and page + count <= head["size"] // PAGE, "invalid WAL record range/sequence")
                    require((magic == b"ZER1" and length == 0) or
                            (magic == b"WRT1" and length == count * PAGE and length <= 8 * 1024 * 1024 + PAGE),
                            "invalid WAL payload length")
                    previous = seq
                payload = source.read(length)
                require(len(payload) == length and zlib.crc32(payload, zlib.crc32(record[:28])) == checksum,
                        "invalid local WAL payload/checksum")
        checked += 1
    return identity_path.read_bytes(), marker, checked


class PreservedCaches:
    def __init__(self, local, work):
        self.local, self.work = local, work
        self.saved = work / "preserved"
        self.saved.mkdir()
        self.journal = work / "restore.json"
        self.restored = False
        for name in CACHES:
            path = local / name
            require(not path.is_symlink() and (not path.exists() or path.is_dir()), "cache preservation refuses non-directories/symlinks")
        atomic_json(self.journal, {"schema": 1, "local": str(local), "names": list(CACHES), "restored": False})

    def move(self):
        for name in CACHES:
            active = self.local / name
            if active.exists():
                active.rename(self.saved / name)
                fsync_dir(self.local)
                fsync_dir(self.saved)

    def restore(self):
        for name in CACHES:
            saved, active = self.saved / name, self.local / name
            if saved.exists():
                require(not active.exists() and not active.is_symlink(), "restore collision; preserved cache kept at " + str(saved))
                saved.rename(active)
                fsync_dir(self.saved)
                fsync_dir(self.local)
        atomic_json(self.journal, {"schema": 1, "local": str(self.local), "names": list(CACHES), "restored": True})
        self.restored = True


def run_warm(binary, config, env, log, timeout, label, concurrency=None):
    begin = time.monotonic()
    command = [str(binary), "-c", str(config), "warm"]
    if concurrency is not None:
        command += ["--concurrency", str(concurrency)]
    with log.open("xb") as output:
        child = subprocess.Popen(command, env=env,
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            while child.poll() is None:
                remaining = timeout - (time.monotonic() - begin)
                if remaining <= 0:
                    raise TimeoutError(label + " exceeded its allotted time")
                try:
                    child.wait(timeout=min(30, remaining))
                except subprocess.TimeoutExpired:
                    print(json.dumps({"phase": label, "elapsed_seconds": round(time.monotonic() - begin)}), flush=True)
        except BaseException:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=10)
            raise
    duration = time.monotonic() - begin
    text = re.sub(r"\x1b\[[0-9;]*m", "", log.read_text(errors="replace"))
    require(child.returncode == 0, label + " exited unsuccessfully; inspect sanitized evidence")
    counts = re.findall(r"Warmed (\d+) allocated pages", text)
    require(len(counts) == 1, "warm completion count missing or ambiguous")
    metrics = [line for line in text.splitlines() if "offline cache warm completed" in line]
    result = {"seconds": duration, "pages": int(counts[0]), "remote_gets": None, "remote_bytes": None}
    if metrics:
        require(len(metrics) == 1, "warm metric summary is ambiguous")
        for key in ("pages", "remote_gets", "remote_bytes"):
            match = re.search(r"\b" + key + r"=(\d+)\b", metrics[0])
            require(match is not None, "warm metric missing: " + key)
            require(key != "pages" or int(match[1]) == result["pages"], "warm page count differs between summaries")
            result[key] = int(match[1])
        for key in ("groups", "concurrency", "max_range_inflight"):
            match = re.search(r"\b" + key + r"=(\d+)\b", metrics[0])
            result[key] = int(match[1]) if match else None
    result["useful_bytes"] = result["pages"] * PAGE
    result["useful_mib_per_second"] = result["useful_bytes"] / duration / 1024 ** 2
    result["network_mib_per_second"] = (result["remote_bytes"] / duration / 1024 ** 2
                                           if result["remote_bytes"] is not None else None)
    return result


def cache_summary(local, volume):
    cache = local / "logical-cache"
    parts = regular(cache / "identity").read_text().split(":")
    require(len(parts) in (2, 3) and parts[0] == volume, "unexpected warmed-cache identity")
    capacity, partitions = int(parts[1]), int(parts[2]) if len(parts) == 3 else 1
    require(partitions in (1, 16), "unexpected cache partition count")
    versions, pages = regular(cache / "versions"), regular(cache / "pages")
    require(versions.stat().st_size == capacity * META and pages.stat().st_size == capacity * PAGE,
            "warmed cache physical layout mismatch")
    valid, seen, slot = 0, set(), 0
    with versions.open("rb") as source:
        while block := source.read(META * 16384):
            for offset in range(0, len(block), META):
                record = block[offset:offset + META]
                if zlib.crc32(record[:36]) == struct.unpack_from("<I", record, 36)[0]:
                    page = struct.unpack_from("<Q", record)[0]
                    require(page not in seen and page % partitions == slot % partitions,
                            "duplicate page or wrong partition in warmed cache")
                    seen.add(page)
                    valid += 1
                slot += 1
    return {"capacity_pages": capacity, "partitions": partitions, "valid_metadata_pages": valid,
            "allocated_bytes": sum(path.stat().st_blocks * 512 for path in (versions, pages)),
            "logical_bytes": versions.stat().st_size + pages.stat().st_size}


def measure(args, report, work, local, head, identity, marker, env):
    binary = checked_binary(args.binary, args.expected_binary_sha256)
    choices = [(f"final-c{n}", binary, args.expected_binary_sha256, args.timeout_seconds, n, False)
               for n in args.concurrencies]
    if args.include_single:
        choices.append(("final-c1", binary, args.expected_binary_sha256, args.single_timeout_seconds, 1, True))
    if args.old_binary:
        choices.append(("old-839", checked_binary(args.old_binary, OLD_SHA), OLD_SHA, args.old_timeout_seconds, None, True))
    source = tomllib.loads(regular(args.config).read_text())
    deadline = time.monotonic() + args.campaign_timeout_seconds
    def allotted(maximum):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("warm campaign time budget exhausted")
        return min(maximum, remaining)
    for label, selected, expected, timeout, concurrency, optional in choices:
        shadow = work / label
        shadow.mkdir()
        (shadow / "wal").mkdir()
        (shadow / "identity.json").write_bytes(identity)
        (shadow / "durable").write_bytes(marker)
        config = work / (label + ".toml")
        options = write_config(config, source, shadow)
        item = {"label": label, "binary_sha256": expected, "engine_options": options, "complete": False,
                "concurrency": concurrency, "optional": optional, "samples": 1}
        report["runs"].append(item)
        require(status(binary, config, env) == head, "remote HEAD changed before warm")
        begin = time.monotonic()
        try:
            item["cold"] = run_warm(selected, config, env, work / (label + "-cold.log"), allotted(timeout), label + "-cold", concurrency)
            require(status(binary, config, env) == head, "remote HEAD changed during cold warm")
            checked_binary(selected, expected)
            print(json.dumps({"phase": label + "-cold", **item["cold"]}), flush=True)
            if selected == binary:
                require(item["cold"]["remote_gets"] is not None, "final binary lacks the required warm metrics")
                require(item["cold"]["pages"] > 0 and item["cold"]["remote_gets"] > 0,
                        "large-fixture cold warm did not fetch a nonempty remote population")
                require(item["cold"].get("concurrency") == concurrency
                        and 1 <= (item["cold"].get("max_range_inflight") or 0) <= concurrency,
                        "final binary did not report bounded physical-range concurrency")
            item["cache_after_cold"] = cache_summary(shadow, head["volume"])
            require(item["cache_after_cold"]["valid_metadata_pages"] == item["cold"]["pages"],
                    "cold warm did not retain its full page count")
            # A fresh final process verifies every expected version and payload CRC.
            # Metadata/index requests occur but no segment payload GET is permitted.
            item["retention"] = run_warm(binary, config, env, work / (label + "-retention.log"),
                                         allotted(args.timeout_seconds), label + "-retention", concurrency or 32)
            require(item["retention"]["pages"] == item["cold"]["pages"]
                    and item["retention"]["remote_gets"] == 0 and item["retention"]["remote_bytes"] == 0
                    and item["retention"].get("max_range_inflight") == 0,
                    "cache retention verification required remote payloads or lost pages")
            item["complete"] = True
            print(json.dumps({"phase": label + "-retention", **item["retention"]}), flush=True)
        except TimeoutError:
            item["timeout_seconds"] = timeout
            item["elapsed_seconds_until_timeout"] = time.monotonic() - begin
            item["limitation"] = "incomplete warm/retention; no complete timing or speedup ratio is reported"
            if not optional:
                raise
        finally:
            try:
                require(status(binary, config, env) == head, "remote HEAD changed; measurement is invalid")
                checked_binary(binary, args.expected_binary_sha256)
            finally:
                # Keep one measured SSD cache at a time; the originals remain
                # preserved separately and are never removed by this branch.
                shutil.rmtree(shadow)
    completed = [run for run in report["runs"] if run["complete"]]
    require(len({run["cold"]["pages"] for run in completed}) == 1, "warm page populations differ")
    final = next((run for run in completed if run["label"] == "final-c32"), None)
    old = next((run for run in completed if run["label"] == "old-839"), None)
    if final and old:
        report["same_head_cold_seconds_ratio_old_over_final"] = old["cold"]["seconds"] / final["cold"]["seconds"]


def self_test():
    scratch = ROOT / "test-output/astra-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="warm-harness-", dir=scratch) as name:
        root = pathlib.Path(name)
        local, work = root / "volume", root / "work"
        local.mkdir()
        work.mkdir()
        for cache in CACHES:
            (local / cache).mkdir()
            (local / cache / "sentinel").write_text(cache)
        with exclusive(local / "LOCK"):
            preserved = PreservedCaches(local, work)
            try:
                preserved.move()
                require(all(not (local / cache).exists() for cache in CACHES), "test preservation failed")
                raise ValueError("simulated failure")
            except ValueError:
                pass
            finally:
                preserved.restore()
        require(all((local / cache / "sentinel").read_text() == cache for cache in CACHES), "test restoration failed")
        volume, writer = str(uuid.uuid4()), str(uuid.uuid4())
        head = {"volume": volume, "writer": writer, "size": 16 * PAGE, "seq": 0}
        (local / "identity.json").write_text(json.dumps({key: head[key] for key in ("volume", "writer", "size")}))
        marker = b"IDWM" + struct.pack("<Q", 0)
        marker += struct.pack("<I", zlib.crc32(marker))
        (local / "durable").write_bytes(marker * 2)
        (local / "wal").mkdir()
        h = bytearray(64)
        h[:8], h[8:24], h[24:40] = b"IDWAL001", uuid.uuid4().bytes, uuid.UUID(volume).bytes
        struct.pack_into("<Q", h, 40, 1)
        struct.pack_into("<I", h, 60, zlib.crc32(h[:60]))
        wal = local / "wal" / f"{1:020}-{uuid.UUID(bytes=bytes(h[8:24]))}.wal"
        wal.write_bytes(h)
        require(published_metadata(local, head)[2] == 1, "test metadata replica guard failed")
        record = b"ZER1" + struct.pack("<IQQI", 0, 1, 0, 1)
        wal.write_bytes(h + record + struct.pack("<I", zlib.crc32(record)))
        try:
            published_metadata(local, head)
            raise AssertionError("unpublished source work was not rejected")
        except RuntimeError as error:
            require("unpublished WAL" in str(error), "unexpected unpublished-work rejection")
        fake = root / "fake-warm"
        fake.write_text("#!/usr/bin/env python3\nimport sys\nassert sys.argv[-2:] == ['--concurrency', '8']\nprint('offline cache warm completed pages=7 remote_gets=3 remote_bytes=12288 groups=3 concurrency=8 max_range_inflight=3')\nprint('Warmed 7 allocated pages')\n")
        fake.chmod(0o700)
        result = run_warm(fake, root / "private.toml", os.environ.copy(), root / "fake.log", 5, "self-test", 8)
        require(result["pages"] == 7 and result["remote_gets"] == 3, "test metric parsing failed")
        require(result["max_range_inflight"] == 3 and result["concurrency"] == 8
                and result["useful_bytes"] == 7 * PAGE and result["network_mib_per_second"] > 0, "parallel metric parsing failed")
        require(not is_test_process("mysqld", ["mysqld", "--datadir=/var/lib/mysql"], pathlib.Path("/var/lib/mysql"), FIXTURE), "production database rejected")
        require(not is_test_process("infinidisk2", ["infinidisk2", "-c", "/etc/infinidisk/data.toml"], pathlib.Path("/"), FIXTURE), "production storage rejected")
        require(is_test_process("infinidisk2", ["infinidisk2", "-c", str(FIXTURE / "infinidisk2.toml")], pathlib.Path("/"), FIXTURE), "fixture storage was not rejected")
        require(is_test_process("fio", ["fio", "--filename=" + str(FIXTURE / "native.bin")], pathlib.Path("/"), FIXTURE), "fixture fio was not rejected")
        fake.write_text("#!/usr/bin/env python3\nimport time\ntime.sleep(60)\n")
        begin = time.monotonic()
        try:
            run_warm(fake, root / "private.toml", os.environ.copy(), root / "timeout.log", 0.05, "timeout-self-test")
            raise AssertionError("timeout did not terminate the child")
        except TimeoutError:
            require(time.monotonic() - begin < 5, "timeout cleanup did not finish promptly")
        isolated = root / "cache-test"
        cache = isolated / "logical-cache"
        cache.mkdir(parents=True)
        (cache / "identity").write_text(volume + ":2:1")
        payload = bytes([23]) * PAGE
        entry = struct.pack("<Q", 7) + uuid.uuid4().bytes + struct.pack("<QI", 64, zlib.crc32(payload))
        entry += struct.pack("<I", zlib.crc32(entry))
        (cache / "versions").write_bytes(entry + bytes(META))
        (cache / "pages").write_bytes(payload + bytes(PAGE))
        require(cache_summary(isolated, volume)["valid_metadata_pages"] == 1, "cache metadata count failed")
        (cache / "versions").write_bytes(entry + entry)
        try:
            cache_summary(isolated, volume)
            raise AssertionError("duplicate retained cache entry was not rejected")
        except RuntimeError as error:
            require("duplicate page" in str(error), "unexpected cache duplicate rejection")
        require(Redactor(["private-test-secret"]).text("value=private-test-secret") == "value=[REDACTED]", "test redaction failed")
    print("local self-test passed; no fixture, credentials or network accessed")


def restore_interrupted(work):
    """Explicit recovery after SIGKILL/power loss; never guess an active directory."""
    work = work.absolute()
    require(work.parent.resolve() == FIXTURE.resolve() and work.name.startswith(".warm-measure-"),
            "restore path must be a private warm attempt of the large fixture")
    require(not work.is_symlink(), "restore attempt is a symlink")
    journal = json.loads(regular(work / "restore.json").read_text())
    require(journal.get("schema") == 1 and journal.get("names") == list(CACHES), "invalid preservation journal")
    local = pathlib.Path(journal["local"])
    require(local.is_absolute() and local.resolve().is_relative_to(FIXTURE.resolve()) and not local.is_symlink(),
            "invalid preservation source")
    with contextlib.ExitStack() as stack:
        for lock in (ROOT / "test-output/astra-recovery.lock", ROOT / "validation/astra/mysql/campaign.lock", local / "LOCK"):
            lock.parent.mkdir(parents=True, exist_ok=True)
            stack.enter_context(exclusive(lock))
        preflight(FIXTURE, 11991)
        preserved = PreservedCaches.__new__(PreservedCaches)
        preserved.local, preserved.work, preserved.saved = local, work, work / "preserved"
        preserved.journal, preserved.restored = work / "restore.json", False
        require(preserved.saved.is_dir() and not preserved.saved.is_symlink(), "invalid preservation directory")
        require(all(path.name in CACHES and path.is_dir() and not path.is_symlink() for path in preserved.saved.iterdir()),
                "unexpected preserved cache entry")
        preserved.restore()
        # Retain the private logs for inspection, but retire the active journal.
        preserved.journal.rename(work / "restored.json")
        fsync_dir(work)
    print(json.dumps({"original_caches_restored": True, "private_attempt": str(work)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=pathlib.Path, default=FIXTURE)
    parser.add_argument("--config", type=pathlib.Path)
    parser.add_argument("--binary", type=pathlib.Path, default=ROOT / "target/release/infinidisk2-astra")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--credentials", type=pathlib.Path, default=pathlib.Path("/opt/elestio/infinidisk/bench.env"))
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "validation/astra/warm")
    parser.add_argument("--old-binary", type=pathlib.Path, help="explicitly opt into 839 on the identical frozen HEAD")
    parser.add_argument("--old-timeout-seconds", type=int, default=180)
    parser.add_argument("--concurrencies", type=int, nargs="+", default=[32, 8, 64, 16], help="explicit mandatory sweep order; default 32 8 64 16")
    parser.add_argument("--include-single", action="store_true", help="optional final concurrency=1 pass, limited to 180 seconds by default")
    parser.add_argument("--single-timeout-seconds", type=int, default=180)
    parser.add_argument("--campaign-timeout-seconds", type=int, default=480, help="shared budget for cold/retention passes; cleanup and HEAD safety checks always run")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--restore-from", type=pathlib.Path, help="restore a private preservation journal after an uncatchable interruption")
    args = parser.parse_args()
    require(1 <= args.old_timeout_seconds <= 180 and 30 <= args.timeout_seconds <= 3600, "invalid measurement timeout")
    require(1 <= args.single_timeout_seconds <= 180 and 30 <= args.campaign_timeout_seconds <= 7200, "invalid campaign/single timeout")
    require(args.concurrencies and len(set(args.concurrencies)) == len(args.concurrencies)
            and all(2 <= n <= 128 for n in args.concurrencies), "mandatory concurrency levels must be unique values in 2..128")
    if args.self_test:
        self_test()
        return
    if args.restore_from:
        require(os.geteuid() == 0, "restore requires root on the reserved VM")
        restore_interrupted(args.restore_from)
        return
    if args.plan_only:
        print(json.dumps({"fixture": str(args.fixture), "concurrency_order": args.concurrencies,
                         "phases": ["cold warm + retention for each mandatory concurrency"]
                         + (["optional concurrency1, maximum180s"] if args.include_single else [])
                         + (["optional 839 cold warm, maximum180s", "final verifies 839 cache"] if args.old_binary else []),
                         "campaign_budget_seconds": args.campaign_timeout_seconds, "samples_per_level": 1,
                         "preflight_scope": "test fixture argv/paths, named disposable test containers, nbd31/ublk31 and test ports; production services untouched",
                         "source_volume_lock": "held for the entire measurement; warm uses private local metadata",
                         "preserve_and_restore": list(CACHES), "HEAD": "identical before/after every pass",
                         "no_global_drop_caches": True, "old_1322_seconds_is_not_a_comparator": True}, indent=2))
        return
    require(os.geteuid() == 0, "run only on the reserved test VM as root")
    args.fixture = args.fixture.absolute()
    require(args.fixture.resolve() == FIXTURE.resolve(), "only the existing large disposable fixture is allowed")
    args.config = args.config or args.fixture / "infinidisk2.toml"
    require(args.config.resolve().is_relative_to(args.fixture.resolve()), "config must belong to the large fixture")
    checked_binary(args.binary, args.expected_binary_sha256)
    source = tomllib.loads(regular(args.config).read_text())
    local = pathlib.Path(source["local_dir"]).absolute()
    require(local.resolve().is_relative_to(args.fixture.resolve()) and not local.is_symlink(), "unsafe local fixture path")
    fixture_report = json.loads(regular(args.fixture / "report.json").read_text())
    require(fixture_report.get("complete") is True, "fixture qualification is incomplete")
    env, redactor = credential_env(args.credentials), Redactor.from_credentials(args.credentials)
    port = int(source.get("listen", "127.0.0.1:11991").rsplit(":", 1)[1])
    attempt = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
    report = {"schema": 1, "utc": utc(), "attempt": attempt, "passed": False, "runs": [],
              "concurrency_order": args.concurrencies, "samples_per_level": 1,
              "campaign_budget_seconds": args.campaign_timeout_seconds,
              "fixture_report_sha256": digest(args.fixture / "report.json"), "fixture_config_sha256": digest(args.config),
              "limitations": ["application caches start empty; Linux/host/S3 caches are not globally flushed",
                              "GET/byte counters count segment payload reads, excluding HEAD/index metadata",
                              "cold duration includes CLI startup/index loading; retention is timed separately",
                              "original caches are restored; warmed test caches are private and discarded",
                              "no comparison with the historical 1322-second run, whose HEAD differed"]}
    report["limitations"].append("shared VM production services remain running; one sample per level is exploratory")
    failure, work, preserved = None, None, None
    # These cooperative campaign locks complement the original volume's LOCK.
    with contextlib.ExitStack() as stack:
        for lock in (ROOT / "test-output/astra-recovery.lock", ROOT / "validation/astra/mysql/campaign.lock", local / "LOCK"):
            lock.parent.mkdir(parents=True, exist_ok=True)
            stack.enter_context(exclusive(lock))
        preflight(args.fixture, port)
        require(not list(args.fixture.glob(".warm-measure-*/restore.json")), "a previous preservation journal remains; inspect/restore it first")
        work = args.fixture / (".warm-measure-" + attempt)
        work.mkdir(mode=0o700)
        try:
            probe_config = work / "probe.toml"
            report["engine_options"] = write_config(probe_config, source, work / "probe")
            needed = (report["engine_options"]["disk_cache_mib"] + 1024) * 1024 ** 2
            require(shutil.disk_usage(work).free >= needed, "insufficient free disk for one private warm cache plus 1 GiB headroom")
            report["preflight_private_free_bytes"] = shutil.disk_usage(work).free
            head = status(args.binary, probe_config, env)
            report["HEAD"] = {"canonical_sha256": canonical_hash(head), "generation": head["generation"],
                              "sequence": head["seq"], "volume": head["volume"], "shards": len(head["shards"])}
            identity, marker, checked = published_metadata(local, head)
            report["published_local_WALs_checked"] = checked
            preserved = PreservedCaches(local, work)
            preserved.move()
            measure(args, report, work, local, head, identity, marker, env)
            required = [run for run in report["runs"] if not run["optional"]]
            report["passed"] = len(required) == len(args.concurrencies) and all(run["complete"] for run in required)
        except BaseException as error:
            failure = error
            report["error"] = redactor.text(str(error))
        finally:
            if preserved:
                try:
                    preserved.restore()
                    report["original_caches_restored"] = True
                except BaseException as error:
                    report["original_caches_restored"] = False
                    report["restore_error"] = redactor.text(str(error))
                    failure = error
                    report["passed"] = False
            destination = args.output / attempt
            destination.mkdir(parents=True, exist_ok=False)
            files = {}
            for log in sorted(work.glob("*.log")):
                require(log.stat().st_size <= 32 * 1024 * 1024, "log exceeds evidence size limit")
                (destination / log.name).write_text(redactor.text(log.read_text(errors="replace")))
                files[log.name] = digest(destination / log.name)
            atomic_json(destination / "report.json", redactor.object(report))
            files["report.json"] = digest(destination / "report.json")
            atomic_json(destination / "manifest.json", {"schema": 1, "utc": utc(), "files_sha256": files,
                        "script_sha256": digest(pathlib.Path(__file__)), "redactor_sha256": digest(ROOT / "scripts/run_astra_recovery.py"),
                        "final_binary_sha256": args.expected_binary_sha256,
                        "old_binary_sha256": OLD_SHA if args.old_binary else None,
                        "profile_sha256": digest(ROOT / "scripts/profiles/astra-recovery-core.json")})
            if preserved is None or preserved.restored:
                shutil.rmtree(work)
            print(json.dumps({"passed": report["passed"], "report": str(destination / "report.json"),
                              "private_restore_journal": str(work / "restore.json") if work.exists() else None}), flush=True)
    if failure:
        raise SystemExit("warm measurement failed; see sanitized report")


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt("warm measurement interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit("warm measurement interrupted") from None
