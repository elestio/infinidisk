# Benchmark evidence

The README charts summarize archived measurements. They do not launch benchmarks, combine different builds into a fictional common run, or substitute a projected price for an observed request count. [Return to the README](../README.md).

## Reference comparison

The four-panel figure uses the 10 October 2026 reference campaign, with InfiniDisk build `b39b705f43b626563d76e1065b48f6891227ad2016f1df120977b4eda4b23151`. The executable was then named `infinidisk2`; chart labels use the current product name. **These are historical profiles**, including 8 MiB segments and compact checkpoints, rather than the current recommended 32 MiB / noncompact profile.

| Workload | InfiniDisk NBD | InfiniDisk ublk | ZeroFS, S3 `fsync` | Native VM storage |
| :--- | ---: | ---: | ---: | ---: |
| PostgreSQL, TPS | 2,937.572 | — | 0.823 | 4,833.823 |
| fio cached 4 KiB random read, IOPS | 54,429.143 | — | 16,330.024 | 15,097.875 |
| MySQL large read-only, 2 CPU, TPS | 1,528.420 | 2,549.800 | Not measured | 1,058.890 |
| fio 4 KiB write + fsync, IOPS | 9,462.036 | — | 1.492 | 13,092.527 |

All values are medians of three samples; chart axes are linear and independently scaled per panel. Small ZeroFS write bars remain small: their numerical labels preserve the measured values. The MySQL chart does not invent a missing ZeroFS bar. Every plotted sample, source path and source-file SHA-256 is recorded in [benchmark-data.json](assets/benchmark-data.json).

### Contracts and comparability

InfiniDisk's durability barrier persists to its **local WAL**, followed by asynchronous S3 publication. ZeroFS with durable fsync waits for S3. Native storage uses the VM's disk. A ratio across those write modes is not a speedup at equal remote durability; the README deliberately shows absolute results rather than a giant cross-contract multiplier.

The machine was shared, stages ran sequentially, and Linux page caches were not globally dropped. InfiniDisk's local reads can benefit from the host page cache even when fio uses direct I/O on the exported block device. Native fio uses direct I/O on a regular file. Configured application caches do not equalize all physical memory usage. ZeroFS retained compression and encryption, while the Rust engine does not implement application-level encryption.

CPU quotas apply to database containers, not the separate storage processes. These are short workload-specific observations on one VM, not results for every database, object provider, cache size or dataset.

### PostgreSQL

- PostgreSQL 16, pgbench scale 2, four clients and four workers.
- Three 15-second samples per variant; one CPU and 512 MiB per database container.
- `fsync`, `synchronous_commit`, `full_page_writes` and page checksums enabled.
- Fresh fixtures; InfiniDisk RAM/SSD caches 64/128 MiB, hot WAL 64 MiB, index 64 MiB.
- Zero failed transactions across the four series, including the older InfiniDisk baseline. SQL consistency and the recorded recovery checks passed.
- The reference InfiniDisk profile improved median throughput from **1,778.975 to 2,937.572 TPS** versus its earlier local-durable build (+65.1%). This is a same-contract before/after comparison.

Sources: [canonical report](../validation/astra/postgres/report.json), [full protocol, in French](astra-postgres-comparison.md).

### fio

Three 15-second samples; `libaio`, `direct=1`, shared 256 MiB regions. Random reads use 4 KiB, depth 32 and four jobs. Synchronized writes use 4 KiB, depth 1 and four jobs. Native storage is a file on the VM filesystem; engines expose block devices. Dataset CRC checks are recorded separately from throughput measurements.

The selected read panel is **warm cached data**, not cold S3 throughput. Do not derive a physical-disk speedup from it. Sources: [canonical fio report](../validation/astra/fio/report.json), [chart-series summary](../validation/astra/summary.json).

### MySQL

The plotted large, uniform read-only fixture uses the separate **two-CPU** diagnostic, three 30-second samples, with a full InfiniDisk warmup. NBD and ublk have the same selected profile and configured caches, with the transport and its fast path identified. This chart measures a cached read workload, not database commit durability. Native storage is a separate reference on the same VM, not an identical cache hierarchy.

Only qualified series are plotted. Earlier small-fixture results with ignored SQL errors are explicitly excluded from throughput claims; the archive retains their error records. ZeroFS has no large-fixture measurement in this panel. Source reports:

- [InfiniDisk NBD](../validation/astra/mysql/astra-c3588919f4-cpu2-b39b705f43b6-large-compact-1/comparison.json)
- [InfiniDisk ublk](../validation/astra/mysql/astra-c3588919f4-large-ublk-cpu2-b39b705f43b6-1/comparison.json)
- [Native](../validation/astra/mysql/astra-c3588919f4-cpu2-b39b705f43b6-large-native-1/comparison.json)

## Current profile improvements

These experiments qualified individual changes now enabled in new configurations. They ran on different dated builds; their gains must not be multiplied together. The binary rename to `infinidisk` does not constitute a new performance campaign.

### Adaptive reads

Same-binary fixed-64-KiB versus adaptive-16/256-KiB reads, direct HTTPS, fresh local caches, ABBA order and two samples per variant:

| Metric | Fixed 64 KiB | Adaptive | Change |
| :--- | ---: | ---: | ---: |
| Sequential data GETs | 4,096 | 1,024 | −75.0% |
| Sequential MiB/s | 80.187 | 105.813 | +32.0% |
| Sequential p99, ms | 352.322 | 419.430 | +19.0% |
| Random downloaded MiB | 215.767 | 71.192 | −67.0% |
| Random data GETs | 3,250 | 3,646 | +12.2% |

A later PostgreSQL comparison of the earlier reference and selected adaptive/32-MiB/noncompact profiles yielded **2,904.572 → 3,000.144 TPS** (+3.3%), 3 × 15 seconds with zero failed transactions. It did not rerun ZeroFS and native, so its results are not spliced into the four-way reference chart.

Sources: [summary](../validation/adaptive/summary.json), [design and selection](adaptive-reads.md).

### Warm-open index cache

With the same HEAD and binary, metadata-only opens in ABBA order used **15 / 1 / 1 / 15 Class-B reads** with the immutable-index cache off/on/on/off. There were no Class-A operations or data GETs in this phase. Fourteen index GETs were avoided; response bytes fell from 1,266,451 to 2,011.

HEAD was still fetched and checked. All recorded durations were approximately 0.303 seconds at the harness polling floor, so this demonstrates fewer requests, **not a measured startup-time speedup**. Full database-startup accounting is a separate fixture and phase.

Sources: [summary](../validation/index-cache/summary.json), [restart accounting](../validation/index-cache/restart/report.json), [design](index-cache.md).

### Download admission

Same-binary direct-HTTPS comparison, fresh local caches, ABBA order, two samples per variant. The selected limit is **8 MiB** of admitted online data-range payloads and at most **64** admitted requests, including a small-read reserve.

| Median metric | Budget off | 8 MiB budget | Change |
| :--- | ---: | ---: | ---: |
| Sequential-only MiB/s | 85.994 | 103.455 | +20.3% |
| Sequential-only p99, ms | 408.945 | 413.139 | +1.0% |
| Mixed sequential p99, ms | 392.167 | 362.807 | −7.5% |
| Mixed random p99, ms | 152.830 | 130.286 | −14.8% |
| Mixed random IOPS | 344.864 | 346.004 | +0.3% |

The initial acceptance gate required a 5% sequential-p99 improvement and **failed**. The default was subsequently selected for bounded transfers and the overall mixed-workload compromise. The original failed gate remains recorded. Two samples and network variability do not support a universal latency promise. This admission budget is not a cap on SDK allocations, TLS buffers, cache copies or total RSS.

Sources: [summary](../validation/downloads/summary.json), [default selection manifest](../validation/downloads/default-profile/manifest.json), [recovery checks](../validation/downloads/recovery/report.json), [design](download-admission.md).

### S3 operation cost

The repository records Class-A/Class-B operations, transferred bytes, warm/cold restart phases and projected costs per 1,000 / 10,000 SQL statements. **SQL statements, transactions, S3 requests and bytes are different denominators.** Cost depends on working set, checkpoints, object layout and provider pricing.

The historical pricing calculations apply a Tigris tariff to observed Elestio requests before allowances; they are projections, not Tigris invoices or a claim about today's prices. An 8/16/32/64-MiB segment sweep and direct-HTTPS confirmation informed the 32-MiB default. Bigger ranges can reduce GET counts while worsening random-read latency.

See the [accounting and sizing study, in French](astra-s3-operations.md) and [SQL cost summary](../validation/index-cache/summary.json). Reprice the observed operations using your provider's current rates.

## Regenerate the figures

The figures are rendered from the archived JSON data with Python and Matplotlib. No benchmark, network access or cloud request is required:

```sh
python3 scripts/render_readme.py
```

This writes PNG/SVG figures and the source-hash manifest under `docs/assets/`. Source references are relative to the repository; the figures travel with a checkout of `elestio/infinidisk`.

## Reproduce workloads

The original VM harnesses are development tools with environment-specific paths, Docker images, spare NBD/ublk devices and reserved ports. Review their arguments and prerequisites before running them on another host. `--help` is available on the entry points below; a command is not a portable one-click benchmark merely because its results are archived.

- [`validate_vm.py`](../scripts/validate_vm.py): filesystem, WAL, remote recovery and optional PostgreSQL checks. `--checks-only` skips repeated throughput tests.
- [`compare_zerofs.py`](../scripts/compare_zerofs.py): fio, PostgreSQL and MySQL comparisons; accepts an explicit `--binary` path.
- [`run_astra_postgres_compare.py`](../scripts/run_astra_postgres_compare.py): frozen-binary PostgreSQL campaign with required source/binary identities.
- [`run_download_reads.py`](../scripts/run_download_reads.py): focused download-admission ABBA measurements.

Always reserve isolated devices and unique disposable S3 prefixes. The archived campaigns did not drop global VM caches or stop production services. Exported reports omit credentials and volume contents. Earlier reports preserve their original binary names and hashes; rebuilding the renamed CLI does not reproduce those hashes.
