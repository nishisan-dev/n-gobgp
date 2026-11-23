FROM docker:29.1.3-dind AS docker
FROM node:24-bookworm-slim AS node
FROM python:3.13-slim-bookworm AS python
FROM golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS go
FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
ARG KERNEL_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git jq sudo unzip zip tar gzip xz-utils \
    iproute2 iputils-ping iptables kmod procps net-tools util-linux \
    gcc g++ make gcc-multilib g++-multilib libc6-dev pkg-config \
    python3 python3-pip python3-setuptools python3-venv \
    libexpat1 libbz2-1.0 liblzma5 libsqlite3-0 libffi8 libreadline8t64 \
    gh "linux-modules-extra-${KERNEL_VERSION}" \
    && rm -rf /var/lib/apt/lists/*
COPY --from=python /usr/local/ /usr/local/
COPY --from=node /usr/local/ /usr/local/
COPY --from=go /usr/local/go/ /usr/local/go/
COPY --from=docker /usr/local/bin/ /usr/local/bin/
COPY --from=docker /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
RUN ldconfig && python3 -m pip install --no-cache-dir 'setuptools<82' && printf '[global]\nbreak-system-packages = true\n' > /etc/pip.conf
COPY docker-control.sh /usr/local/bin/ngobgp-ci-docker
COPY job-entrypoint.sh /opt/ci/job-entrypoint.sh
ENV PATH=/usr/local/go/bin:/ci/cache/go/bin:$PATH \
    GOPATH=/ci/cache/go GOCACHE=/ci/cache/build GOTOOLCHAIN=local \
    DOCKER_HOST=unix:///run/n-gobgp-ci/docker.sock
WORKDIR /work/source
ENTRYPOINT ["/opt/ci/job-entrypoint.sh"]
