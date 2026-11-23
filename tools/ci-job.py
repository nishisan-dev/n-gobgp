#!/usr/bin/env python3
"""Run project commands in an owned, disposable container on an existing runner.

Only this controller uses the host Docker API. Test commands receive a private
Docker socket, a copied checkout and a repository/runner-specific build cache.
Cleanup never prunes the host daemon or selects resources belonging to others.
"""
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import signal
import tempfile
import time
import uuid

REPO_LABEL = 'n-gobgp.ci.repository'
INSTANCE_LABEL = 'n-gobgp.ci.instance'
ROOT = Path(__file__).resolve().parent.parent


def host_environment():
    environment = dict(os.environ)
    environment.pop('DOCKER_CONTEXT', None)
    environment['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
    return environment


def docker(*args, check=True, capture=False):
    return subprocess.run(['docker', *map(str, args)], check=check, text=True, env=host_environment(),
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None)


def state_path():
    return Path(os.environ['NGOBGP_CI_STATE'])


def load():
    return json.loads(state_path().read_text())


def save(state):
    path = state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.n-gobgp-state-')
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(state, stream)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def inspect(kind, name):
    result = docker(kind, 'inspect', name, check=False, capture=True)
    return json.loads(result.stdout)[0] if result.returncode == 0 else None


def owned(resource, state, container=False):
    labels = (resource.get('Config', {}) if container else resource).get('Labels') or {}
    return labels.get(REPO_LABEL) == state['repository'] and labels.get(INSTANCE_LABEL) == state['instance']


def tools_image(state):
    kernel = os.uname().release
    context = ROOT / '.github/ci'
    checksum = hashlib.sha256(kernel.encode())
    for path in sorted(context.iterdir()):
        if path.is_file() and path.name != 'README.md':
            checksum.update(path.name.encode())
            checksum.update(path.read_bytes())
    image = 'n-gobgp-ci-tools:' + checksum.hexdigest()[:20]
    # Both organization runners use the same VM and Unix account. Serialize only
    # our tool-image build; never change their selected builder or Docker config.
    lock_path = Path(tempfile.gettempdir()) / 'n-gobgp-ci-tools-build.lock'
    with lock_path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if inspect('image', image):
            return image
        builder = 'n-gobgp-build-' + state['instance']
        state['builder'] = builder
        save(state)
        try:
            docker('buildx', 'create', '--name', builder, '--driver', 'docker-container',
                   '--driver-opt', 'memory=4g,cpu-period=100000,cpu-quota=200000,restart-policy=no',
                   '--driver-opt', f'env.NGOBGP_CI_REPOSITORY={state["repository"]}',
                   '--driver-opt', f'env.NGOBGP_CI_EXPIRES={int(time.time()) + 3000}',
                   'unix:///var/run/docker.sock')
            docker('buildx', 'build', '--builder', builder, '--load', '--tag', image,
                   '--build-arg', f'KERNEL_VERSION={kernel}', context)
        finally:
            docker('buildx', 'rm', '--force', builder, check=False)
            state.pop('builder', None)
            save(state)
    return image


def reap(repository):
    """Reclaim only expired project resources left after a native runner crash."""
    result = docker('ps', '--all', '--quiet', '--filter', f'label={REPO_LABEL}={repository}',
                    '--filter', 'status=exited', capture=True)
    for identifier in result.stdout.split():
        resource = inspect('container', identifier)
        if not resource:
            continue
        labels = resource.get('Config', {}).get('Labels') or {}
        instance = labels.get(INSTANCE_LABEL, '')
        expires = labels.get('n-gobgp.ci.expires', '')
        if (re.fullmatch(r'[0-9a-f]{20}', instance) and expires.isdigit()
                and int(expires) < time.time()
                and resource['Name'] == '/n-gobgp-job-' + instance):
            state = {'repository': repository, 'instance': instance,
                     'container': resource['Name'].lstrip('/'), 'network': 'n-gobgp-net-' + instance}
            if owned(resource, state, container=True):
                docker('rm', '--force', '--volumes', state['container'])
                network = inspect('network', state['network'])
                if network and owned(network, state):
                    docker('network', 'rm', state['network'])
    # Buildx servers are also temporary. Their environment marks ownership and
    # expiry without changing the builder selected by another project.
    result = docker('ps', '--all', '--quiet', '--filter',
                    'name=buildx_buildkit_n-gobgp-build-', capture=True)
    for identifier in result.stdout.split():
        resource = inspect('container', identifier)
        if not resource:
            continue
        env = dict(item.split('=', 1) for item in (resource['Config'].get('Env') or []) if '=' in item)
        name = resource['Name'].lstrip('/')
        expires = env.get('NGOBGP_CI_EXPIRES', '')
        if (env.get('NGOBGP_CI_REPOSITORY') == repository and expires.isdigit()
                and int(expires) < time.time()
                and re.fullmatch(r'buildx_buildkit_n-gobgp-build-[0-9a-f]{20}0', name)):
            docker('buildx', 'rm', '--force', name[len('buildx_buildkit_'):-1], check=False)


def start():
    repository = os.environ['GITHUB_REPOSITORY_ID']
    if not re.fullmatch(r'[0-9]+', repository):
        raise ValueError('invalid repository ID')
    instance = uuid.uuid4().hex[:20]
    state = {'repository': repository, 'instance': instance,
             'container': 'n-gobgp-job-' + instance, 'network': 'n-gobgp-net-' + instance,
             'workspace_ready': False}
    save(state)  # Names exist before creation so cancellation can always clean up.
    reap(repository)
    image = tools_image(state)
    runner = hashlib.sha256(os.environ['RUNNER_NAME'].encode()).hexdigest()[:12]
    cache = f'n-gobgp-cache-{repository}-{runner}'
    existing = inspect('volume', cache)
    if existing and (existing.get('Labels') or {}).get(REPO_LABEL) != repository:
        raise ValueError('refusing an unrelated cache volume')
    if not existing:
        docker('volume', 'create', '--label', f'{REPO_LABEL}={repository}', cache)
    routes = json.loads(subprocess.check_output(['ip', '-j', '-4', 'route', 'show', 'table', 'all']))
    occupied = [ipaddress.ip_network(route['dst'], strict=False) for route in routes
                if route.get('dst') not in (None, 'default')]
    networks = [subnet for subnet in ipaddress.ip_network('10.231.96.0/22').subnets(new_prefix=28)
                if not any(subnet.overlaps(route) for route in occupied)]
    if not networks:
        raise RuntimeError('CI pool overlaps existing VM routes; refusing to shadow them')
    offset = int(instance, 16) % len(networks)
    for index in range(len(networks)):
        subnet = networks[(offset + index) % len(networks)]
        result = docker('network', 'create', '--subnet', subnet,
                        '--label', f'{REPO_LABEL}={repository}',
                        '--label', f'{INSTANCE_LABEL}={instance}', state['network'],
                        check=False, capture=True)
        if result.returncode == 0:
            break
        if 'overlap' not in result.stderr.lower():
            raise RuntimeError(result.stderr)
    else:
        raise RuntimeError('no non-overlapping CI subnet available')
    docker('run', '--detach', '--init', '--privileged', '--restart=no',
           '--cpus=2', '--memory=6g', '--pids-limit=4096',
           '--name', state['container'], '--network', state['network'],
           '--label', f'{REPO_LABEL}={repository}', '--label', f'{INSTANCE_LABEL}={instance}',
           '--label', f'n-gobgp.ci.expires={int(time.time()) + 3000}',
           '--volume', '/var/lib/docker', '--volume', f'{cache}:/ci/cache', image)
    for _ in range(90):
        result = docker('exec', state['container'], 'test', '-f', '/run/n-gobgp-ci/ready',
                        check=False, capture=True)
        if result.returncode == 0:
            print('Isolated CI job ready; no host socket or host workspace mounted')
            return
        resource = inspect('container', state['container'])
        if not resource or not resource['State']['Running']:
            break
        time.sleep(1)
    docker('logs', '--tail', '80', state['container'], check=False)
    raise RuntimeError('isolated job did not become ready')


def copy_workspace(state):
    if not state['workspace_ready']:
        docker('cp', str(Path(os.environ['GITHUB_WORKSPACE']).resolve()) + '/.',
               state['container'] + ':/work/source')
        docker('exec', state['container'], 'git', 'config', '--global', '--add',
               'safe.directory', '/work/source')
        event = os.environ.get('GITHUB_EVENT_PATH')
        if event and Path(event).is_file():
            docker('cp', event, state['container'] + ':/work/event.json')
        state['workspace_ready'] = True
        save(state)


def run(script):
    state = load()
    resource = inspect('container', state['container'])
    if not resource or not owned(resource, state, container=True) or not resource['State']['Running']:
        raise RuntimeError('owned isolated job is not running')
    copy_workspace(state)
    target = '/work/steps/' + uuid.uuid4().hex + '.sh'
    docker('cp', script, state['container'] + ':' + target)
    # Forward values by environment name, not as secret-bearing command arguments.
    environment = host_environment()
    forwarded = []
    for key in environment:
        if ((key.startswith(('GITHUB_', 'GOBGP_', 'FIB_', 'R04_'))
             and key not in {'GITHUB_ENV', 'GITHUB_OUTPUT', 'GITHUB_PATH', 'GITHUB_STATE',
                             'GITHUB_STEP_SUMMARY', 'GITHUB_WORKSPACE', 'GITHUB_EVENT_PATH'})
                or key in {'GOOS', 'GOARCH', 'CGO_ENABLED', 'GORACE', 'CI', 'NEEDS_JSON', 'GH_TOKEN',
                           'COMMIT', 'IDENTIFIER', 'METADATA', 'DIRTY'}):
            forwarded.extend(['--env', key])
    command = ['docker', 'exec', *forwarded, '--env', 'GITHUB_WORKSPACE=/work/source',
               '--env', 'RUNNER_TEMP=/work/tmp', '--env', 'PYTHONDONTWRITEBYTECODE=1',
               '--env', 'GITHUB_EVENT_PATH=/work/event.json', '--workdir', '/work/source', state['container'],
               'bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', target]
    process = subprocess.Popen(command, env=environment)
    previous = {}
    interrupted = 0
    def forward(signum, _frame):
        nonlocal interrupted
        interrupted = signum
        if process.poll() is None:
            process.send_signal(signum)
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        previous[signum] = signal.signal(signum, forward)
    try:
        result = process.wait()
        return 128 + interrupted if interrupted else result
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def export(paths, temporary=False):
    state = load()
    resource = inspect('container', state['container'])
    if not resource or not owned(resource, state, container=True):
        raise RuntimeError('owned isolated job is unavailable for artifact export')
    root = Path(os.environ['RUNNER_TEMP'] if temporary else os.environ['GITHUB_WORKSPACE'])
    source = '/work/tmp' if temporary else '/work/source'
    for name in paths:
        path = Path(name)
        if path.is_absolute() or '..' in path.parts:
            raise ValueError('artifact paths must stay within the job directory')
        destination = root / path
        if not destination.resolve().is_relative_to(root.resolve()):
            raise ValueError('artifact destination escapes the workspace through a symlink')
        destination.parent.mkdir(parents=True, exist_ok=True)
        docker('cp', f"{state['container']}:{source}/{path}", destination)


def cleanup():
    if not state_path().exists():
        return
    state = load()
    container = inspect('container', state['container'])
    if container:
        if not owned(container, state, container=True):
            raise ValueError('refusing to remove an unrelated container')
        docker('rm', '--force', '--volumes', state['container'])
    network = inspect('network', state['network'])
    if network:
        if not owned(network, state):
            raise ValueError('refusing to remove an unrelated network')
        docker('network', 'rm', state['network'])
    if state.get('builder'):
        expected = 'n-gobgp-build-' + state['instance']
        if state['builder'] != expected:
            raise ValueError('refusing an unrelated builder')
        docker('buildx', 'rm', '--force', expected, check=False)
    state_path().unlink()
    print('Removed only this job container, anonymous Docker storage and network')


def main():
    try:
        command, *args = sys.argv[1:]
        if command == 'start':
            start()
        elif command == 'run' and len(args) == 1:
            return run(args[0])
        elif command in ('export', 'export-temp'):
            export(args, temporary=command == 'export-temp')
        elif command == 'cleanup':
            cleanup()
        else:
            raise ValueError('expected start, run SCRIPT, export PATH..., export-temp PATH..., or cleanup')
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.CalledProcessError) as err:
        print(f'Isolated CI job: {err}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
