#!/usr/bin/env python3
"""No VM access: verify resume provenance and retained error evidence."""
import copy
import importlib.util
import json
import pathlib
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("campaign", ROOT / "scripts/run_astra_mysql.py")
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def check():
    with tempfile.TemporaryDirectory() as directory:
        work = pathlib.Path(directory)
        phase = "test-small-qualified-compact-1"
        raw = work / phase / "raw"
        raw.mkdir(parents=True)
        config = raw / "config.toml"
        config.write_text("test = true\n")
        options = campaign.options("small", "compact")
        sample = {"tps": 100.0, "p99_ms": 10.0, "ignored_errors": 0,
                  "resources": {"processes": {"engine": {}, "mysql": {}}}}
        entry = {"sample_count": 3, "sample_seconds": 30, "warmup_seconds": 10,
                 "random_distribution": "special", "latency_percentile": 99,
                 "cpu_limit": 1, "memory_mib": 1024, "tables": 4, "rows_per_table": 25000,
                 "threads": 8, "transport": "nbd", "database_SIGKILL_recovery": "passed",
                 "samples": {"read_only": [copy.deepcopy(sample) for _ in range(3)]},
                 "engine_metadata": {"binary_sha256": "a" * 64, "options": options,
                                     "legacy_config": False, "config_snapshot_path": "/remote/config.toml",
                                     "config_sha256": campaign.digest(config)}}
        item = {"label": "small-qualified-compact", "phase": phase, "size": "small", "profile": "compact",
                "engine": "infinidisk2", "samples": 3, "workloads": ["read_only"], "options": options,
                "zero_ignore_fsync": None, "transport": "nbd", "storage_crash": False, "mysql_cpus": 1,
                "binary_sha256": "a" * 64, "complete": True, "returncode": 0,
                "proof_report": phase + "/comparison.json", "summary": campaign.summarize(entry)}
        expected = {key: copy.deepcopy(item[key]) for key in ("label", "size", "profile", "engine", "samples", "workloads", "options", "zero_ignore_fsync", "transport", "storage_crash", "mysql_cpus")}
        proof = work / item["proof_report"]

        def publish():
            proof.write_text(json.dumps({"complete": True, "mysql": {"infinidisk2-" + phase: entry}}))

        def reuse(value=None, settings=None, sha="a" * 64, exploratory=False):
            return campaign.reusable_stage(value if value is not None else copy.deepcopy(item), work,
                                           settings or expected, sha, 30, exploratory=exploratory)

        publish()
        accepted = copy.deepcopy(item)
        assert reuse(accepted) and accepted["eligible_for_comparison"]
        assert not reuse(sha="b" * 64)
        changed = copy.deepcopy(expected)
        changed["samples"] = 2
        assert not reuse(settings=changed)
        changed = copy.deepcopy(expected)
        changed["options"]["async_cache"] = False
        assert not reuse(settings=changed)
        config.write_text("test = false\n")
        assert not reuse()
        config.write_text("test = true\n")
        proof.write_text(proof.read_text() + "\n")
        try:
            reuse(accepted)
        except RuntimeError:
            pass
        else:
            raise AssertionError("changed archived proof was accepted")
        publish()
        changed = copy.deepcopy(item)
        changed["summary"]["read_only"]["median_tps"] = 999.0
        try:
            reuse(changed)
        except RuntimeError:
            pass
        else:
            raise AssertionError("altered summary was accepted")
        entry["samples"]["read_only"][0]["ignored_errors"] = 1
        item["summary"] = campaign.summarize(entry)
        publish()
        errored = copy.deepcopy(item)
        assert reuse(errored) and not errored["eligible_for_comparison"]
        assert errored["ignored_sql_errors_total"] == 1
        assert reuse(errored), "an error-bearing completed run must not trigger a beauty-score retry"
        item["label"] = expected["label"] = "small-screen-compact"
        item["samples"] = expected["samples"] = entry["sample_count"] = 1
        entry["samples"]["read_only"] = [copy.deepcopy(sample)]
        item["summary"] = campaign.summarize(entry)
        publish()
        historical = copy.deepcopy(item)
        assert not reuse(sha="b" * 64)
        assert reuse(historical, sha="b" * 64, exploratory=True)
        assert historical["retained_for_selection_only"] and not historical["eligible_for_comparison"]
        assert historical["evidence_scope"] == "exploratory_screening_prior_binary"
    print("PASS: matching proof, SHA/options/count/config rejection, immutable proof, summary agreement, SQL-error exclusion/no retry, explicit historical screening")


if __name__ == "__main__":
    check()
