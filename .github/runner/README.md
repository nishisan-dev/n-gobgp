# Dedicated n-gobgp runner

CI and release jobs require the `self-hosted`, `Linux`, `X64`, `n-gobgp` and
`isolated-docker` labels. The repository runner `n-gobgp-vm192-1` runs on the
validation VM (`192.168.7.2`), independently of the organization's shared runners.

One runner executes jobs serially because the scenario suite uses fixed container
names. Its privileged container has a private Docker daemon and storage volume;
it does not mount the host Docker socket or host credentials. Restarting Docker
from a scenario affects only this daemon. A job-start hook removes containers and
unused networks and restores the default daemon configuration between jobs.
The daemon runs without the job tracking identifier so Actions orphan cleanup
does not terminate it between jobs. Go caches remain on the dedicated runner;
setup-go artifact caching is disabled to avoid transferring them for every job.
The outer bridge uses `10.231.90.0/24` to avoid the scenario subnets.

The container is limited to three CPUs, 8 GiB of memory and 4096 processes.
This isolates normal CI resource use and Docker operations. Privileged containers
are not a security boundary for untrusted code. Keep fork workflow approval set
to **all external contributors** and review such changes before approving runs.
Do not assign the dedicated labels to shared runners.

## Provisioning and maintenance

Run the following commands on the validation VM, from this directory. Keep the
registration token out of Git and command history. Obtain a short-lived token
through the repository's Actions runner settings or the GitHub API, and write
only its value to the private file below (mode `0600`).

```sh
install -d -m 0700 /var/tmp/ngobgp-ci-runner/private
# Write the short-lived registration token to:
# /var/tmp/ngobgp-ci-runner/private/registration-token
docker compose build
docker compose up -d
docker compose logs --tail=40
```

Registration is repository-scoped. The runner configuration remains in the
container across restarts. After registration, truncate the host token file to zero bytes, retaining the
bind mount path for container restarts. The token expires after one hour.
Before recreating the container, remove its old runner
registration in GitHub and supply a new token. Removing the Docker data volume
also removes the nested daemon's cached images.

The image pins the Actions runner download and verifies its SHA-256 checksum.
It includes the modules matching the VM kernel for VRF tests. After a VM kernel
upgrade, rebuild with `--build-arg KERNEL_VERSION=$(uname -r)` and repeat the
namespace, VRF and FIB smoke checks before accepting jobs. Runner updates inside
the container are supported, but image rebuilds should update the pinned version
and checksum together.

Useful checks and diagnostics:

```sh
docker exec n-gobgp-ci-runner unshare -rnm true
docker exec n-gobgp-ci-runner modprobe vrf
docker exec n-gobgp-ci-runner docker info
docker logs --tail=80 n-gobgp-ci-runner
```

Never mount the host Docker socket or restart the host Docker service for these
jobs. To pause scheduling, stop only `n-gobgp-ci-runner`; the other runners and
applications remain independent.
