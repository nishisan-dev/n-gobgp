#!/usr/bin/env bash
# Fetch the same pinned tools used by CI, only inside an isolated job.
set -euo pipefail
case "${1:-}" in
  golangci-lint)
    version=2.15.0
    archive="golangci-lint-$version-linux-amd64.tar.gz"
    url="https://github.com/golangci/golangci-lint/releases/download/v$version/$archive"
    checksum=9b7fe6a6bf6334b98decf8d86b11415ea7889afdabdd1959fdce23af2a9ebcea
    member="golangci-lint-$version-linux-amd64/golangci-lint"
    ;;
  goreleaser)
    version=2.19.0
    archive=goreleaser_Linux_x86_64.tar.gz
    url="https://github.com/goreleaser/goreleaser/releases/download/v$version/$archive"
    checksum=ebc3194ad1d1b1787614405285a0039665cd4c5bd3305215a1b201600f90630b
    member=goreleaser
    ;;
  *) echo 'Expected golangci-lint or goreleaser' >&2; exit 2 ;;
esac
tool=$1
shift
cache="/ci/cache/tools/$tool-$version-$checksum"
if [[ ! -x "$cache/$tool" ]]; then
  mkdir -p /ci/cache/tools
  work=$(mktemp -d /ci/cache/tools/.download.XXXXXX)
  trap 'rm -rf -- "$work"' EXIT
  curl --fail --location --retry 3 "$url" -o "$work/$archive"
  printf '%s  %s\n' "$checksum" "$work/$archive" | sha256sum -c -
  tar -xzf "$work/$archive" -C "$work" "$member"
  mkdir -p "$cache"
  install -m 0755 "$work/$member" "$cache/.installing"
  mv "$cache/.installing" "$cache/$tool"
  rm -rf -- "$work"
  trap - EXIT
fi
exec "$cache/$tool" "$@"
