#!/usr/bin/env python3
"""Manual cloud-only acceptance for two already-started isolated job environments.

Run as the existing runner account on the authorized VM. This uses only the
provided controller states and an explicitly named, disposable host sentinel.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid


def docker(*args, check=True):
    result = subprocess.run(['docker', *args], text=True, capture_output=True)
    if check and result.returncode:
        raise RuntimeError(result.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('workspace', type=Path)
    parser.add_argument('states', nargs=2, type=Path)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    controller = workspace / 'tools/ci-job.py'
    states = [json.loads(path.read_text()) for path in args.states]
    sentinel = 'n-gobgp-isolation-sentinel-' + uuid.uuid4().hex[:16]
    envs = [{**os.environ, 'NGOBGP_CI_STATE': str(path),
             'GITHUB_WORKSPACE': str(workspace), 'GITHUB_REPOSITORY_ID': '1403785031'}
            for path in args.states]
    def command(index, verb, *values, check=True):
        result = subprocess.run(['python3', str(controller), verb, *map(str, values)],
                                env=envs[index], text=True, capture_output=True)
        if check and result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        return result
    def run(index, text):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh') as script:
            script.write(text)
            script.flush()
            return command(index, 'run', script.name)
    def inspected(name):
        return json.loads(docker('inspect', name).stdout)[0]
    try:
        docker('run', '--detach', '--name', sentinel, '--network', 'none',
               '--cpus=0.1', '--memory=16m', '--pids-limit=32',
               '--label', 'n-gobgp.ci.acceptance=sentinel', 'busybox:stable', 'sleep', '1800')
        assert states[0]['network'] != states[1]['network']
        for index, state in enumerate(states):
            resource = inspected(state['container'])
            assert resource['HostConfig']['NanoCpus'] == 2000000000
            assert resource['HostConfig']['Memory'] == 6 * 1024 ** 3
            assert all(m['Type'] == 'volume' for m in resource['Mounts'])
            assert not resource['HostConfig']['PidMode']
            run(index, '''
                test -S /var/run/docker.sock
                test "$(readlink -f /var/run/docker.sock)" = /run/n-gobgp-ci/docker.sock
                test "$DOCKER_HOST" = unix:///run/n-gobgp-ci/docker.sock
                private_id=$(docker info --format '{{.ID}}')
                default_id=$(env -u DOCKER_HOST docker info --format '{{.ID}}')
                test -n "$private_id"
                test "$default_id" = "$private_id"
                go version | grep 'go1.27.3'
                python3 --version | grep 'Python 3.13'
                docker run -d --name g1 --network none busybox:stable sleep 600
                printf 'private-change\n' > ci-isolation-private.txt
            ''')
            print('PASS private default socket, daemon, checkout, versions and resource limits', index, flush=True)
        assert not (workspace / 'ci-isolation-private.txt').exists()
        run(0, '''
            test "$(cat ci-isolation-private.txt)" = private-change
            docker rm -f $(docker ps -aq)
            printf '{"ipv6":true,"fixed-cidr-v6":"2001:db8:1::/64"}\\n' > daemon.json
            install -D -m 644 daemon.json /etc/docker/daemon.json
            ngobgp-ci-docker restart
            docker info >/dev/null
            docker network inspect bridge --format '{{.EnableIPv6}}' | grep true
            env -u DOCKER_HOST docker inspect g1 >/dev/null 2>&1 && exit 1
            private_id=$(docker info --format '{{.ID}}')
            default_id=$(env -u DOCKER_HOST docker info --format '{{.ID}}')
            test -n "$private_id"
            test "$default_id" = "$private_id"
        ''')
        run(1, "docker inspect g1 --format '{{.State.Running}}' | grep true")
        run(1, "docker network inspect bridge --format '{{.EnableIPv6}}' | grep false")
        assert inspected(sentinel)['State']['Running']
        print('PASS same fixture name, IPv6 configuration and daemon restart stay within each job', flush=True)
        run(0, "printf 'artifact-data\\n' > ci-isolation-artifact.txt")
        command(0, 'export', 'ci-isolation-artifact.txt')
        artifact = workspace / 'ci-isolation-artifact.txt'
        assert artifact.read_text() == 'artifact-data\n' and artifact.stat().st_uid == os.getuid()
        artifact.unlink()
        print('PASS exported artifacts are owned by the native runner account', flush=True)
        # A failed command must not hide behind a successful pipeline tail.
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh') as script:
            script.write('false | true\n')
            script.flush()
            assert command(0, 'run', script.name, check=False).returncode != 0
        command(0, 'cleanup')
        assert docker('inspect', states[0]['container'], check=False).returncode != 0
        print('PASS failure status and post-job cleanup', flush=True)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh') as script:
            script.write('sleep 600\n')
            script.flush()
            process = subprocess.Popen(['python3', str(controller), 'run', script.name], env=envs[1])
            try:
                time.sleep(2)
                process.send_signal(signal.SIGTERM)
                assert process.wait(timeout=15) != 0
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        command(1, 'cleanup')
        assert docker('inspect', states[1]['container'], check=False).returncode != 0
        assert inspected(sentinel)['State']['Running']
        print('PASS cancellation and independent post-job cleanup', flush=True)
    finally:
        for index in range(2):
            command(index, 'cleanup', check=False)
        docker('rm', '--force', sentinel, check=False)


if __name__ == '__main__':
    main()
