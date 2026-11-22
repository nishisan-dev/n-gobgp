#!/usr/bin/env bash
set -euo pipefail
ngobgp-ci-docker start
if [[ ! -f .runner ]]; then
    token=$(cat /run/secrets/runner-registration-token)
    ./config.sh --unattended --url https://github.com/nishisan-dev/n-gobgp \
        --token "$token" --name "${RUNNER_NAME:-n-gobgp-vm192-1}" \
        --labels n-gobgp,isolated-docker --work /work
    unset token
fi
exec ./run.sh
