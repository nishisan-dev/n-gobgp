#!/usr/bin/env bash
set -euo pipefail
if [[ ${NGOBGP_DIND_READY:-} != 1 ]]; then
    # Use Docker's nesting setup before starting any daemon or runner process.
    # In cgroup v2 this moves init into a leaf and enables domain controllers.
    exec /usr/local/bin/dind env NGOBGP_DIND_READY=1 "$0" "$@"
fi
ngobgp-ci-docker start
if [[ ! -f .runner ]]; then
    token=$(cat /run/secrets/runner-registration-token)
    ./config.sh --unattended --url https://github.com/nishisan-dev/n-gobgp \
        --token "$token" --name "${RUNNER_NAME:-n-gobgp-vm192-1}" \
        --labels n-gobgp,isolated-docker --work /work
    unset token
fi
exec ./run.sh
