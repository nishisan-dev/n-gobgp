#!/usr/bin/env bash
set -euo pipefail
if [[ ${NGOBGP_DIND_READY:-} != 1 ]]; then
    exec /usr/local/bin/dind env NGOBGP_DIND_READY=1 "$0" "$@"
fi
# This image is a disposable tool environment, not an Actions runner.
# Refuse an unexpected host socket before starting the private daemon.
if [[ -S /var/run/docker.sock ]]; then
    echo 'The host Docker socket must not be mounted into a CI job.' >&2
    exit 1
fi
mkdir -p /run/n-gobgp-ci /work/tmp /work/steps /work/source
printf 'isolated-job\n' > /run/n-gobgp-ci/owner
ngobgp-ci-docker start
# Legacy scenario helpers intentionally discard environment variables. Their
# default endpoint must still reach this job's daemon, never the host daemon.
ln -s /run/n-gobgp-ci/docker.sock /var/run/docker.sock
touch /run/n-gobgp-ci/ready
# Bound the lifetime even if the native runner crashes before post-job cleanup.
exec timeout --signal=TERM --kill-after=10s "${NGOBGP_CI_TTL:-3000}s" tail -f /dev/null
