#!/usr/bin/env bash
# Control only the Docker daemon inside the dedicated CI runner container.
set -euo pipefail
pidfile=/run/ngobgp-dockerd.pid
start() {
    if docker info >/dev/null 2>&1; then return; fi
    # The daemon belongs to the runner, not to a job orphan-cleanup scope.
    env -u RUNNER_TRACKING_ID nohup dockerd --pidfile="$pidfile" \
        </dev/null > /var/log/ngobgp-dockerd.log 2>&1 &
    for ((attempt=0; attempt<60; attempt++)); do
        if docker info >/dev/null 2>&1; then return; fi
        sleep 1
    done
    tail -80 /var/log/ngobgp-dockerd.log >&2
    return 1
}
stop() {
    if [[ -f "$pidfile" ]]; then
        pid=$(cat "$pidfile")
        kill -TERM "$pid" 2>/dev/null || true
        for ((attempt=0; attempt<30; attempt++)); do
            if ! kill -0 "$pid" 2>/dev/null || ps -o stat= -p "$pid" | grep -q '^Z'; then return; fi
            sleep 1
        done
        kill -KILL "$pid" 2>/dev/null || true
    fi
}
case "${1:-}" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    *) echo "Usage: ngobgp-ci-docker {start|stop|restart}" >&2; exit 2 ;;
esac
