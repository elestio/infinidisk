# elestio-infinidisk

S3-backed block volumes for Elestio.

Each volume is an **ext4 filesystem on an NBD block device** served by a per-volume
ZeroFS daemon running in **write-back mode (`ignore_fsync`)**.

## Why these choices (validated 2026-10-09)

- **`ignore_fsync = true`** → fast write-back (~2600 tps pgbench, vs 16 tps durable).
  The accepted trade-off is a few-seconds durability window on *total VM loss*.
  ZeroFS **never corrupts** the filesystem (survived freeze/network-cut/crash tests,
  backed by their Jepsen campaign), so the worst case is losing the last un-flushed
  writes — never a broken volume.
- **`nbd-client` without `-persist`** → on an abrupt daemon death, `-persist` wedges
  the kernel NBD device (uninterruptible D-state, reboot required). Without it the
  device errors out cleanly.
- **`-nbd-request-timeout` left at the ZeroFS default** → shortening it turns slow
  requests into I/O errors and corrupts ext4.

## Install

One line (downloads the CLI, then fetches the runtime):

```bash
curl -fsSL https://raw.githubusercontent.com/elestio/infinidisk/main/install.sh | sudo bash
```

Pin a version with `INFINIDISK_REF=v0.4.0`. Or install manually:

```
infinidisk install                 # auto-downloads the latest ZeroFS release, verifies
                                   # sha256, installs nbd tooling + a systemd template
infinidisk install --binary ./zerofs   # or use a local binary
```
Pin the ZeroFS version with `INFINIDISK_ZEROFS_VERSION=v2.3.5 infinidisk install`.

## Usage

```
infinidisk create <name> --size 20G --bucket <b> [--prefix p] [--endpoint u] \
                         [--region r] [--cache-gb 4] [--mem-gb 1] \
                         [--access-key .. --secret-key ..]      # or AWS_* env
infinidisk mount    <name>
infinidisk umount   <name>
infinidisk enable   <name>                  # auto-mount at boot
infinidisk disable  <name>
infinidisk resize   <name> <newsize>        # grow only (e.g. 40G)
infinidisk adopt    <name> --bucket <b> [--prefix p] [--key k] ...  # re-attach existing (DR)
infinidisk snapshot create <name> [snap]    # point-in-time (ZeroFS checkpoint)
infinidisk snapshot list   <name>
infinidisk snapshot info   <name> <snap>
infinidisk snapshot delete <name> <snap>
infinidisk snapshot mount  <name> <snap>    # read-only clone at <mnt>/<name>@<snap>
infinidisk snapshot umount <name> <snap>
infinidisk snapshot schedule   <name> [--every 1h] [--keep 24]  # auto snapshots + retention
infinidisk snapshot unschedule <name>
infinidisk clone    <srcvol> <snap> <newvol> # promote a snapshot to a new writable volume (full copy)
infinidisk rollback <name> <snap> [--yes]    # DESTRUCTIVE: revert content to a snapshot
infinidisk key      <export|import|derive> <name> [keyvalue]
infinidisk status   [name] [--json]
infinidisk list     [--json]
infinidisk metrics  <name>                  # raw Prometheus metrics
infinidisk doctor                           # preflight checks
infinidisk destroy  <name> [--yes]          # removes the volume locally; S3 data is kept
```

Site-wide defaults go in `/etc/infinidisk/infinidisk.conf` (sourced at startup), e.g.
`INFINIDISK_DEFAULT_ENDPOINT`, `INFINIDISK_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `INFINIDISK_DEFAULT_CACHE_GB` — so `create` can be shorter.

**Snapshots** use ZeroFS checkpoints (point-in-time, copy-on-write in the object store).
`snapshot mount` opens a checkpoint read-only on a parallel daemon and mounts it at
`<mnt>/<name>@<snap>` — handy for backups or spinning up a prod clone for testing.

**Single-mount guard:** a best-effort lease marker in the bucket refuses mounting a
volume that another host already holds (double RW mount = corruption). Fail-open on
errors; override with `INFINIDISK_FORCE=1`. Each volume exposes Prometheus metrics on
`127.0.0.1:16000+index`; `status` surfaces the bytes still pending to S3 (the
`ignore_fsync` loss window).

### Encryption key

Each volume is encrypted at rest. The password is, in order of preference:
`--key` → **derived from a stable per-VM secret** (`sha256(secret|infinidisk|<name>)`,
reproducible, nothing random to lose). The secret is `/opt/server_token.secret`; if it
does not exist it is bootstrapped from the VM token in `/opt/renew-vm-cert.sh` (present
on every Elestio VM), or generated once as a last resort. `key export <vol>` prints the
key so the backend can escrow it in a vault. **Losing the key = losing the volume.**

### Disaster recovery (`adopt`)

If the VM dies, re-attach the volume on a fresh VM:
`infinidisk adopt <name> --bucket <b> --prefix <p> --key <escrowed-key>`. It rebuilds
the local config and mounts the existing filesystem **without formatting**. On the
same VM the key is re-derived automatically; on a new VM pass the escrowed `--key`.

### Backup policy & clones

- `snapshot schedule <vol> --every 1h --keep 24` installs a systemd timer that takes a
  checkpoint every interval and prunes `auto-*` snapshots to the newest N.
- `clone <src> <snap> <new>` builds a new **independent writable** volume from a
  snapshot (currently a full rsync copy).
- `rollback <vol> <snap> --yes` reverts a volume's content to a snapshot (destructive,
  file-level).

The volume lifecycle is managed by systemd (`infinidisk@<name>.service`): `ExecStart`
runs the ZeroFS daemon, `ExecStartPost` (`_up`) declares the NBD export, attaches it,
formats it on first use and mounts it; `ExecStop` (`_down`) unmounts and detaches before
the daemon drains. So `mount`/`umount` are thin wrappers over `systemctl start/stop`, and
`enable` gives boot-time auto-mount for free.

`destroy` removes the volume locally (daemon, mount, config, keys) but **keeps the S3 data**.
Deleting the S3 data is out of scope for the CLI: per-object deletion does not scale, so the
clean path is **one bucket per volume** + a server-side bucket delete (Tigris
`DELETE ?force=true`), handled by the provisioning layer, not here.

## Layout on disk

| Path | Content |
|------|---------|
| `/usr/local/bin/infinidisk` | this CLI |
| `/usr/local/bin/zerofs` | ZeroFS binary (auto-installed) |
| `/etc/infinidisk/<name>.conf` | per-volume metadata |
| `/etc/infinidisk/<name>.toml` | generated ZeroFS config |
| `/opt/elestio/infinidisk/<name>.key` | encryption password (0600) — **lose it, lose the volume** |
| `/opt/elestio/infinidisk/<name>.env` | AWS creds + encryption password (0600) |
| `/var/lib/infinidisk/<name>/` | ZeroFS cache + runtime state (device file) |
| `/mnt/infinidisk/<name>` | mountpoint |
| `infinidisk@<name>.service` | systemd unit running the ZeroFS daemon |

Per volume index `N`: NBD port `10900+N`, NFS `12000+N`, RPC `14000+N`.

## Status

v0.4 — tested end-to-end on a real VM:
- `install` auto-downloads + sha256-verifies the latest ZeroFS release
- systemd-managed lifecycle (create / mount / umount / enable), survives `systemctl restart`
- `resize` (online grow), `status --json` / `list --json`, `metrics`, `doctor`, lease guard
- **encryption key derived from the per-VM secret** + `key export/import/derive` (escrow)
- **`adopt`** — DR re-attach of an existing bucket, no format, data intact (verified)
- **`snapshot schedule` + retention** (systemd timer, prunes `auto-*` to newest N)
- **`clone`** (independent writable copy, verified) and **`rollback`** (destructive, verified)
- `destroy` (local teardown; S3 data kept)

TODO: multi-FS (xfs/btrfs), lease refresh for long-lived mounts, native/instant clone
(vs full copy), snapshot-list consistency settle, packaging (.deb), the GitHub repo.
