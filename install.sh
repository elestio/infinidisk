#!/usr/bin/env bash
#
# InfiniDisk installer — downloads the prebuilt engine and installs it.
#
#   curl -fsSL https://raw.githubusercontent.com/elestio/infinidisk/main/install.sh | sudo bash
#
# Pin a version with INFINIDISK_VERSION=v0.1.0
#
set -euo pipefail
REPO="${INFINIDISK_REPO:-elestio/infinidisk}"
VERSION="${INFINIDISK_VERSION:-latest}"
DEST="${INFINIDISK_BIN:-/usr/local/bin/infinidisk}"
[ "$(id -u)" = 0 ] || { echo "infinidisk: run as root (sudo)"; exit 1; }
command -v curl >/dev/null || { echo "infinidisk: curl is required"; exit 1; }

OS=$(uname -s | tr "[:upper:]" "[:lower:]")
case "$(uname -m)" in
  x86_64|amd64) ARCH=amd64;;
  aarch64|arm64) ARCH=arm64;;
  *) echo "infinidisk: unsupported CPU arch $(uname -m)"; exit 1;;
esac
asset="infinidisk-${OS}-${ARCH}"
if [ "$VERSION" = latest ]; then url="https://github.com/$REPO/releases/latest/download/$asset"
else url="https://github.com/$REPO/releases/download/$VERSION/$asset"; fi

echo "infinidisk: ensuring nbd kernel module + nbd-client"
if ! command -v nbd-client >/dev/null; then
  if command -v apt-get >/dev/null; then DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y nbd-client >/dev/null 2>&1 || true
  elif command -v dnf >/dev/null; then dnf install -y nbd >/dev/null 2>&1 || true; fi
fi
modprobe nbd nbds_max=64 2>/dev/null || modprobe nbd 2>/dev/null || true
install -d /etc/modules-load.d /etc/modprobe.d 2>/dev/null || true
echo nbd > /etc/modules-load.d/infinidisk-nbd.conf 2>/dev/null || true
echo "options nbd nbds_max=64" > /etc/modprobe.d/infinidisk-nbd.conf 2>/dev/null || true

echo "infinidisk: downloading $asset ($VERSION)"
tmp=$(mktemp); trap "rm -f \"$tmp\"" EXIT
curl -fSL --retry 3 -o "$tmp" "$url" || { echo "infinidisk: download failed ($url)"; exit 1; }
install -m 0755 "$tmp" "$DEST"
echo "infinidisk: installed -> $DEST"
"$DEST" --version 2>/dev/null || true
echo "next: infinidisk -c /etc/infinidisk/volume.toml config   (see the README Quick start)"
