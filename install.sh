#!/usr/bin/env bash
#
# infinidisk bootstrap installer.
#
#   curl -fsSL https://raw.githubusercontent.com/elestio/infinidisk/main/install.sh | sudo bash
#
# Pin a version:
#   INFINIDISK_REF=v0.4.0 bash -c "$(curl -fsSL https://raw.githubusercontent.com/elestio/infinidisk/main/install.sh)"
#
# It downloads the infinidisk CLI, then runs `infinidisk install` (which fetches
# the ZeroFS binary, the NBD tooling and the systemd template).
#
set -euo pipefail

REPO="${INFINIDISK_REPO:-elestio/infinidisk}"
REF="${INFINIDISK_REF:-main}"
DEST="${INFINIDISK_BIN:-/usr/local/bin/infinidisk}"

[ "$(id -u)" = 0 ] || { echo "infinidisk: please run as root (sudo)"; exit 1; }
command -v curl >/dev/null || { echo "infinidisk: curl is required"; exit 1; }

url="https://raw.githubusercontent.com/$REPO/$REF/bin/infinidisk"
echo "infinidisk: downloading CLI ($REF) from $url"
tmp=$(mktemp)
curl -fsSL "$url" -o "$tmp"
install -m 0755 "$tmp" "$DEST"
rm -f "$tmp"

echo "infinidisk: installing runtime (ZeroFS + nbd tooling + systemd template)"
"$DEST" install

echo "infinidisk: ready — $("$DEST" version)"
echo "next: infinidisk create <name> --size 20G --bucket <bucket>   (see: infinidisk help)"
