#!/usr/bin/env bash
# New runner kernels may already provide VRF without a modules-extra package.
# Require the feature itself; install the matching package only when needed.
set -euo pipefail

if modprobe vrf; then
    exit 0
fi
apt-get update
apt-get install -y "linux-modules-extra-$(uname -r)"
modprobe vrf
