FROM docker:29.1.3-dind AS docker
FROM node:24-bookworm-slim AS node
FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
ARG RUNNER_VERSION=2.338.2
ARG RUNNER_SHA256=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40
# Match the VM kernel so privileged network tests can load VRF when needed.
ARG KERNEL_VERSION=6.8.0-147-generic
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git jq sudo unzip zip tar gzip xz-utils \
    iproute2 iputils-ping iptables kmod procps net-tools util-linux \
    gcc g++ make gcc-multilib g++-multilib libc6-dev pkg-config \
    python3 python-is-python3 python3-pip python3-setuptools python3-venv \
    nodejs npm gh libicu74 libssl3t64 libkrb5-3 zlib1g liblttng-ust1t64 \
    linux-modules-extra-${KERNEL_VERSION} \
    && rm -rf /var/lib/apt/lists/*
# Legacy scenario jobs install their Python dependencies inside this image.
RUN printf '[global]\nbreak-system-packages = true\n' > /etc/pip.conf
COPY --from=node /usr/local/ /usr/local/
COPY --from=docker /usr/local/bin/ /usr/local/bin/
COPY --from=docker /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
RUN mkdir -p /opt/actions-runner /etc/docker /work && \
    curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz" -o /tmp/runner.tar.gz && \
    echo "${RUNNER_SHA256}  /tmp/runner.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/runner.tar.gz -C /opt/actions-runner && rm /tmp/runner.tar.gz
COPY docker-control.sh /usr/local/bin/ngobgp-ci-docker
COPY job-start.sh entrypoint.sh /opt/ci/
ENV RUNNER_ALLOW_RUNASROOT=1 \
    ACTIONS_RUNNER_HOOK_JOB_STARTED=/opt/ci/job-start.sh \
    AGENT_TOOLSDIRECTORY=/opt/hostedtoolcache
WORKDIR /opt/actions-runner
ENTRYPOINT ["/opt/ci/entrypoint.sh"]
