#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
go_bin=${GO_BIN:-go}
tools/build.sh dist/first
tools/build.sh dist/second
for binary in gobgp gobgpd; do
    cmp "dist/first/$binary" "dist/second/$binary"
    "dist/first/$binary" --version | grep -F "commit=$(git rev-parse HEAD)"
    "dist/first/$binary" --version | grep -E 'upstream=osrg/gobgp@[0-9a-f]{40}'
done
upstream=$(dist/first/gobgp --version | sed -n 's/.*upstream=osrg\/gobgp@\([0-9a-f]*\).*/\1/p')
git merge-base --is-ancestor "$upstream" HEAD
# Run the actual release linker configuration without publishing a release.
pkg=github.com/osrg/gobgp/v4/internal/pkg/version
CGO_ENABLED=0 "$go_bin" build -trimpath -buildvcs=false -ldflags "-X $pkg.COMMIT=$(git rev-parse HEAD) -X $pkg.IDENTIFIER= -X $pkg.DIRTY=false" -o dist/release-check ./cmd/gobgp
dist/release-check --version | grep -E 'n-gobgp [0-9]+\.[0-9]+\.[0-9]+\+commit\.[0-9a-f]{40} commit=[0-9a-f]{40} dirty=false'
if [ -z "$(git status --porcelain)" ]; then
    release_version=$(dist/release-check --version | awk '{print $4}' | cut -d+ -f1)
    RELEASE_TAG="v$release_version" tools/build.sh dist/tag-check
    if RELEASE_TAG=v999.999.999 tools/build.sh dist/bad-tag; then
        echo 'mismatched release tag was accepted' >&2
        exit 1
    fi
fi
