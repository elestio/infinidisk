#!/usr/bin/env python3
"""Combine verified warm evidence; retain interrupted attempts and their logs."""
import argparse
import copy
import json
import pathlib
import shutil
import tempfile
import time
import uuid

from run_astra_recovery import ROOT, atomic_json, digest, utc


def require(value, message):
    if not value:
        raise ValueError(message)


def load_source(path, binary_sha):
    require(path.name == "report.json" and path.is_file() and not path.is_symlink(), "invalid source report")
    manifest_path = path.parent / "manifest.json"
    require(manifest_path.is_file() and not manifest_path.is_symlink(), "source manifest missing")
    manifest = json.loads(manifest_path.read_text())
    require(manifest.get("final_binary_sha256") == binary_sha, "source binary differs")
    files = manifest["files_sha256"]
    require("report.json" in files, "source report lacks an integrity hash")
    for name, expected in files.items():
        item = path.parent / name
        require(pathlib.Path(name).name == name and pathlib.Path(name).suffix in {".json", ".log"}, "unexpected source evidence path")
        require(item.is_file() and not item.is_symlink() and item.stat().st_size <= 32 * 1024 ** 2,
                "source evidence is not a bounded regular file")
        require(digest(item) == expected, "source evidence hash mismatch: " + name)
    report = json.loads(path.read_text())
    require(report.get("schema") == 1 and report.get("original_caches_restored") is True,
            "source did not restore its original caches")
    return {"path": path, "report": report, "manifest": manifest,
            "manifest_sha256": digest(manifest_path)}


def consolidate(paths, binary_sha, levels, destination):
    sources = sorted((load_source(path, binary_sha) for path in paths), key=lambda source: source["report"]["utc"])
    require(sources and len({source["path"].parent.name for source in sources}) == len(sources), "duplicate source attempts")
    first = sources[0]["report"]
    selected, interrupted = {}, []
    for source in sources:
        report = source["report"]
        require(report["HEAD"] == first["HEAD"] and report["engine_options"] == first["engine_options"],
                "source HEAD or engine options differ")
        evidence_path = "sources/" + source["path"].parent.name + "/report.json"
        for original in report["runs"]:
            run = copy.deepcopy(original)
            run["source_report"] = evidence_path
            if not run.get("complete"):
                interrupted.append(run)
                continue
            n = run.get("concurrency")
            if n not in levels or n in selected:
                continue
            cold, retained = run["cold"], run["retention"]
            require(run["binary_sha256"] == binary_sha and run["engine_options"] == first["engine_options"],
                    "run binary/options differ")
            require(cold["pages"] > 0 and cold["seconds"] > 0 and cold["remote_gets"] > 0
                    and cold["remote_bytes"] > 0 and cold["concurrency"] == n
                    and 1 <= cold["max_range_inflight"] <= n, "invalid cold range measurement")
            require(retained["pages"] == cold["pages"] and retained["remote_gets"] == 0
                    and retained["remote_bytes"] == 0 and retained["max_range_inflight"] == 0
                    and run["cache_after_cold"]["valid_metadata_pages"] == cold["pages"], "retention was not complete")
            selected[n] = run
    require(set(selected) == set(levels), "one or more concurrency levels lack a complete cold/retention pair")
    require(len({tuple(run["cold"][key] for key in ("pages", "remote_gets", "remote_bytes"))
                 for run in selected.values()}) == 1, "page populations or transferred data differ")
    report = {"schema": 1, "utc": utc(), "attempt": destination.name, "passed": True,
              "original_caches_restored": True, "HEAD": first["HEAD"], "engine_options": first["engine_options"],
              "concurrency_order": levels, "samples_per_level": 1, "runs": [selected[n] for n in levels],
              "interrupted_runs": interrupted, "limitations": first.get("limitations", []),
              "consolidation": {"selection": "first complete cold+retention pair per concurrency, in source timestamp order",
                                "required_equal": ["HEAD", "engine_options", "binary_sha256", "pages", "remote_gets", "remote_bytes"],
                                "source_reports": []}}
    require(not destination.exists(), "consolidation destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / (".consolidating-" + uuid.uuid4().hex)
    temporary.mkdir(mode=0o700)
    try:
        files = {}
        for source in sources:
            name = source["path"].parent.name
            target = temporary / "sources" / name
            target.mkdir(parents=True)
            for filename in [*source["manifest"]["files_sha256"], "manifest.json"]:
                source_file = source["path"].parent / filename
                target_file = target / filename
                shutil.copyfile(source_file, target_file)
                expected = source["manifest_sha256"] if filename == "manifest.json" else source["manifest"]["files_sha256"][filename]
                require(digest(target_file) == expected, "source changed during consolidation")
                files[str(target_file.relative_to(temporary))] = expected
            report["consolidation"]["source_reports"].append({"report": f"sources/{name}/report.json",
                "report_sha256": source["manifest"]["files_sha256"]["report.json"],
                "manifest_sha256": source["manifest_sha256"], "passed": source["report"].get("passed"),
                "original_caches_restored": True})
        atomic_json(temporary / "report.json", report)
        files["report.json"] = digest(temporary / "report.json")
        atomic_json(temporary / "manifest.json", {"schema": 1, "utc": utc(), "files_sha256": files,
            "final_binary_sha256": binary_sha, "script_sha256": digest(pathlib.Path(__file__)),
            "export": "verified copies of sanitized source evidence only; no configs, credentials, data, WAL or cache"})
        temporary.rename(destination)
    except BaseException:
        shutil.rmtree(temporary)
        raise
    return report


def self_test():
    root = ROOT / "test-output/astra-tmp"
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=root, prefix="warm-consolidate-") as temporary:
        base = pathlib.Path(temporary)
        paths = []
        for number, levels in enumerate(([32, 8], [16, 128])):
            directory = base / str(number)
            directory.mkdir()
            runs = [{"label": "final-c" + str(n), "concurrency": n, "binary_sha256": "a" * 64,
                     "engine_options": {"memory_cache_mib": 64}, "complete": True,
                     "cold": {"pages": 7, "seconds": 1, "remote_gets": 3, "remote_bytes": 12288,
                              "concurrency": n, "max_range_inflight": min(n, 3)},
                     "retention": {"pages": 7, "remote_gets": 0, "remote_bytes": 0, "max_range_inflight": 0},
                     "cache_after_cold": {"valid_metadata_pages": 7}} for n in levels]
            if number == 0:
                runs.append({"label": "final-c16", "concurrency": 16, "complete": False, "limitation": "timeout"})
            atomic_json(directory / "report.json", {"schema": 1, "utc": str(number), "original_caches_restored": True,
                "HEAD": {"canonical_sha256": "b" * 64}, "engine_options": {"memory_cache_mib": 64}, "runs": runs})
            atomic_json(directory / "manifest.json", {"final_binary_sha256": "a" * 64,
                "files_sha256": {"report.json": digest(directory / "report.json")}})
            paths.append(directory / "report.json")
        report = consolidate(paths, "a" * 64, [32, 8, 16, 128], base / "complete")
        require(report["passed"] and len(report["interrupted_runs"]) == 1, "incomplete evidence was lost")
        try:
            consolidate(paths, "a" * 64, [32, 8, 16, 64, 128], base / "missing")
            raise AssertionError("missing level accepted")
        except ValueError:
            pass
        paths[0].write_text("{}")
        try:
            consolidate(paths, "a" * 64, [32, 8, 16, 128], base / "tampered")
            raise AssertionError("modified source accepted")
        except ValueError:
            pass
    print("warm consolidation self-test passed; no VM or credentials accessed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=pathlib.Path, nargs="+")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--levels", type=int, nargs="+", default=[32, 8, 64, 16, 128])
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    require(args.reports and args.expected_binary_sha256, "reports and exact binary SHA required")
    destination = args.output or ROOT / "validation/astra/warm" / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-consolidated")
    consolidate(args.reports, args.expected_binary_sha256, args.levels, destination)
    print("REPORT " + str(destination / "report.json"))


if __name__ == "__main__":
    main()
