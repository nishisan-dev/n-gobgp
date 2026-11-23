# Organization runners with isolated jobs

CI and release use the existing **nishisan** organization runner group, with
`self-hosted`, `Linux` and `X64` labels. The current matching runners are
`nishisan-vm157-1` and `nishisan-vm157-2` on the validation VM (`192.168.7.2`).
No repository runner, registration token, runner service or organization hook
is created or modified.

Checkout and artifact actions run under the organization's existing runner
account. Project shell commands run through `tools/ci-job.py` inside one disposable
container per job. This includes Go/Python builds, lint, broker integration,
network namespace tests, routing scenarios and release commands. Go 1.27.3 and
Python 3.13 come from the tool image. Lint and GoReleaser downloads retain the
previous versions and verify the official archive SHA-256 hashes.

Each environment has its own network/PID/mount namespaces, Docker daemon and
anonymous Docker storage volume. It receives a **copy** of the checkout rather
than a writable bind mount. The host Docker socket, runner home, SSH keys and
other projects' directories are not mounted. Legacy Docker restarts, fixed
container names and fixture cleanup affect only that job's private daemon.
The controller refuses cleanup when the resource ownership labels do not match.
It never calls a host-wide prune or restarts the host Docker service.

The job container is limited to **2 CPUs, 6 GiB and 4096 processes**. With the
two existing runners, n-gobgp can consume at most four CPUs and 12 GiB in job
containers. A tool-image build is serialized and limited separately to two CPUs
and 4 GiB through its temporary Buildx builder; it does not select a different
builder for other projects. The `prepare-tools` dependency completes image preparation before the shorter
validation job timeouts start. The image is cached by its source and the VM
kernel version. Rebuilds install matching VRF modules inside the tool image,
without installing packages on the VM.

## Lifetime and cleanup

The local `isolated-job` action registers post-job cleanup before setup starts.
It removes only that job's container, anonymous Docker volume and network on
success, failure or cancellation. The private environment also has a 50-minute
lifetime bound if the native runner disappears. An ensuing project job reclaims
expired, stopped project containers and expired project Buildx servers. Persistent
Go/tool cache volumes belong to this repository and native runner; they contain
rebuildable dependencies, not a runner registration or a service.

The containers are privileged because the existing kernel/network tests need
it. This provides operational separation for trusted CI; it is not a security
boundary against malicious code. Keep approval required for **all external
contributors** before executing fork pull requests on these organization runners.

## Workflow conventions

Keep `actions/checkout` before `./.github/actions/isolated-job`. The workflow's
custom default shell copies the workspace on the first project command, after
additional YANG checkouts and artifact downloads have completed. The copy stays
private and persists across commands, so generated-file checks compare the actual
modified files. Use `cd` inside a command rather than a host working-directory
override. Native `shell: bash` steps are reserved for artifact transfers.

Artifacts are explicitly exported with `tools/ci-job.py export` or `export-temp`
before a native upload action. Routing image archives retain the verified local
cache keyed by repository/run/attempt/commit, with original GitHub artifacts as
fallback. The build exports its image archives before seeding that native cache.
All validations remain dependencies of the `required` gate.

Do not switch these jobs to GitHub's `jobs.container` without reviewing its
mounts: the [runner implementation](https://github.com/actions/runner/blob/main/src/Runner.Worker/Container/ContainerInfo.cs)
mounts the host Docker socket into that form of job container. Do not add host sockets or host credentials to this controller.
