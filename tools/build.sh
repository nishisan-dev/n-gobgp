#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

output=${1:-dist/local}
go_bin=${GO_BIN:-go}
commit=$(git rev-parse HEAD)
dirty=false
if [ -n "$(git status --porcelain)" ]; then dirty=true; fi
pkg=github.com/osrg/gobgp/v4/internal/pkg/version
flags="-s -w -buildid= -X $pkg.COMMIT=$commit -X $pkg.DIRTY=$dirty"
if [ -n "${RELEASE_TAG:-}" ]; then
    test "$dirty" = false || { echo 'release requires a clean checkout' >&2; exit 1; }
    flags="$flags -X $pkg.IDENTIFIER="
fi
mkdir -p "$output"
for binary in gobgp gobgpd; do
    CGO_ENABLED=0 "$go_bin" build -trimpath -buildvcs=false -ldflags "$flags" -o "$output/$binary" "./cmd/$binary"
done
if [ -n "${RELEASE_TAG:-}" ]; then
    built=$("$output/gobgp" --version | awk '{print $4}' | cut -d+ -f1)
    test "$RELEASE_TAG" = "v$built" || { echo "tag $RELEASE_TAG differs from fork version v$built" >&2; exit 1; }
fi
