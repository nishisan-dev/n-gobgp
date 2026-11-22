#!/usr/bin/env bash
# Each serial job starts with a default daemon configuration and no containers.
set -euo pipefail
mapfile -t containers < <(docker ps -aq)
if ((${#containers[@]})); then docker rm -f "${containers[@]}"; fi
docker network prune -f
printf '{}\n' > /etc/docker/daemon.json
ngobgp-ci-docker restart
unshare -rnm true
