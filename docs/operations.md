# Operating InfiniDisk

This guide covers the experimental Rust engine, whose executable is `infinidisk`. Examples use `/etc/infinidisk/volume.toml`, `/dev/nbd31` and `/mnt/infinidisk`; substitute the paths and device reserved for your volume. [Return to the README](../README.md).

## Before mounting

Use a unique S3 prefix, local directory and listening port per volume. The local directory contains authoritative, possibly uncheckpointed WAL data as well as disposable caches. Keep it on reliable local storage with sufficient free space. The cache and WAL limits are separate; allow additional space for prepared segments, index scratch and the host.

The storage process reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optional `AWS_SESSION_TOKEN` from its environment. Use a provider-appropriate region and endpoint. An S3 provider must support the atomic writes and conditional updates used by the engine. Elestio object storage was used for the archived S3 measurements; S3 API compatibility alone is not a qualification of every provider.

Keep NBD bound to loopback. The supplied NBD client attaches over multiple local sockets and remains in the foreground until detached. For databases, preserve normal filesystem and database durability settings. Size the SSD cache for the working set and measure cold reads separately. A successful local `fsync` does not mean the same transaction is already in S3.

## Orderly shutdown

1. Stop the database and other applications using the mount.
2. Unmount the filesystem.
3. Detach the NBD device.
4. Send SIGTERM or SIGINT to the `serve` process and wait for its exit status.

```sh
umount /mnt/infinidisk
infinidisk -c /etc/infinidisk/volume.toml detach --device /dev/nbd31
```

The NBD server attempts a final checkpoint after draining its serving loop. Shutdown waits are bounded; a failed or timed-out final publication returns an error. Preserve `local_dir` and its WAL if publication fails. Do not treat disappearance of the process as proof that S3 is current.

For ublk, stop the applications and unmount first, then run:

```sh
infinidisk -c /etc/infinidisk/volume.toml ublk-delete --id 31
```

The command checks that the device belongs to InfiniDisk and is not in use. The ublk server performs its final checkpoint when its device loop exits; inspect its exit status and retain local state if it fails.

## Recovery

### Restart with the local disk intact

Preserve the entire local directory and use the matching configuration and compatible binary. Restart `serve`, attach the assigned device and perform normal filesystem/database recovery. Do not format the device again.

After an engine crash, clear any stale attachment safely before reattaching. Unmount first; do not attach the same volume to a second active filesystem instance. Run `e2fsck` only while the filesystem is unmounted. Do not use `norecovery` to bypass required journal replay.

A warm open still reads the current remote HEAD and replays the local WAL. The verified remote-index cache may satisfy matching immutable index objects from SSD. Mutable paged-index scratch is not an authoritative copy of remote state.

### Loss of the local disk / transfer to another host

First stop or externally fence the previous writer. Configure the **same remote prefix** with a **new, empty local directory**, then run:

```sh
infinidisk -c /etc/infinidisk/recovery.toml adopt --takeover
infinidisk -c /etc/infinidisk/recovery.toml serve
```

Attach and mount without running `mkfs`. Apply normal filesystem and database recovery. This restores the last complete remote generation, not necessarily the latest locally acknowledged commit.

`--takeover` is an assertion that fencing has already happened. It does not contact or terminate the old host. Never run two copies of the same local writer identity. There is no distributed automatic failover lease.

### S3 outage or a full backlog

The engine retains pending WAL data when publication fails. At the pending-data limit, writes wait up to approximately 50 seconds for room and then fail if progress is still impossible. Monitor server logs and free space. Restore remote access or stop the workload; do not delete pending journal files to make room.

### Detected corruption

A bad cache copy can be rebuilt from a verified source. A damaged authoritative record causes an error; it is not converted to a successful zero-filled read. A sealed WAL is validated before upload so a corrupt local segment cannot replace the previous valid remote checkpoint.

CRCs detect accidental data damage. SHA-256 verifies index-object contents. These checks do not establish authenticity against a malicious endpoint. Hardware that lies about completed persistence barriers is outside the durability contract.

## Inspection and offline maintenance

```sh
infinidisk -c /etc/infinidisk/volume.toml status
infinidisk -c /etc/infinidisk/volume.toml scrub
```

`status` inspects committed remote HEAD; it is not a live daemon-health query. Periodic server logs expose local/remote sequences, pending bytes, cache activity, synchronization work and download-admission counters. `scrub` reads the remote indexes and all referenced pages, grouping data GETs by extent. Scrubbing incurs S3 requests.

**Stop the server before `warm`, `compact` or `gc`.** These operations take the exclusive local volume lock. An offline operation is not a substitute for fencing another host that has a copy of the writer identity.

### Preload the working data

```sh
infinidisk -c /etc/infinidisk/volume.toml warm --concurrency 128
```

`warm` requires `logical_cache=true` and an SSD data-cache budget large enough for all allocated logical pages. It verifies fetched pages before filling the cache. Concurrency accepts 1–128 physical range groups and defaults to 128; these are concurrent requests, not 128 OS threads. This offline path has its own concurrency limit and is outside the online download byte budget.

### Compact remote data

```sh
infinidisk -c /etc/infinidisk/volume.toml compact
```

Compaction rewrites live pages in logical order into new objects. Old objects remain until a later garbage collection. This operation is separate from `compact_checkpoints`, which only controls what a new checkpoint uploads and is disabled in the recommended profile.

### Collect unreferenced objects

```sh
# Preview only; objects must be at least 24 hours old by default.
infinidisk -c /etc/infinidisk/volume.toml gc

# After reviewing the preview:
infinidisk -c /etc/infinidisk/volume.toml gc --apply
```

Collection is limited to the volume's prefix and protects objects referenced by HEAD. During deletion, HEAD carries a reserved nil writer ID; opening or adopting the volume is refused.

If collection is interrupted, repeat `gc --apply` with the **same configuration and local directory**. Preserve `gc-token.json`: it records the owner and generation needed to resume. Do not delete the token or manually remove the fencing state. `--min-age-seconds 0` is for isolated tests only.

## Service supervision

The repository includes [server](../scripts/infinidisk-server.service.example) and [NBD client](../scripts/infinidisk-client.service.example) systemd examples. They use `/usr/local/bin/infinidisk`, `/etc/infinidisk/volume.toml` and `/etc/infinidisk/credentials.env`. Adapt these paths and reserve the selected NBD device before installation. Install the examples as `infinidisk-server.service` and `infinidisk-client.service` so their dependency names match.

Use a root-owned credential file with mode `0600`. Keep the mount and database service ordering explicit: start storage, attach, mount, then start the application; reverse that order for shutdown. The examples do not automatically format, mount or enable any services. They are not an automatic HA deployment.

## Configuration upgrades and experimental formats

`infinidisk config` generates all current recommended settings and refuses to replace an existing file. Omitted fields in an old file retain their original defaults. Compare the [complete profile](../configs/recommended.toml) with the existing file instead of assuming an upgrade enables new options.

The default configuration filename is now `infinidisk.toml`. An existing configuration with another name remains usable through `-c /path/to/file.toml`; there is no automatic file renaming. The internal Rust package, volume formats and historical benchmark identifiers retain `infinidisk2` where applicable.

The recommended profile uses local durable mode. Experimental generation mode has a different format and weaker per-commit guarantees: `FLUSH` / `FUA` establish ordering but do not promise individual local persistence, and recovery returns to a complete S3 generation. It requires a fresh volume and remains disabled. See the [original specification, in French](generation-mode.md).

Retain a compatible binary and its matching local state when using experimental WAL formats. The earlier ZeroFS wrapper uses a different volume format and CLI; migrate files or database backups to a separate new InfiniDisk volume. This engine does not resize or convert those volumes in place.
