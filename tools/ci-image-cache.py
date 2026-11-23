#!/usr/bin/env python3
"""Save and restore verified test images on one dedicated, serial CI runner."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

NAMES = ('gobgp.tar', 'gobgp-oq.tar')


def identity():
    result = {key: os.environ[key] for key in
              ('GITHUB_REPOSITORY_ID', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_SHA')}
    for key, value in result.items():
        pattern = r'[0-9a-f]{40}' if key == 'GITHUB_SHA' else r'[0-9]+'
        if not re.fullmatch(pattern, value):
            raise ValueError(f'invalid {key}')
    return result


def digest(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f'not a regular image archive: {path}')
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(directory, base, entry, context):
    base.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix='.pending-', dir=base))
    try:
        for name in NAMES:
            digest(directory / name)
            shutil.copyfile(directory / name, staging / name)
        manifest = {'identity': context,
                    'sha256': {name: digest(staging / name) for name in NAMES}}
        (staging / 'manifest.json').write_text(json.dumps(manifest))
        if entry.exists():
            shutil.rmtree(entry)
        staging.rename(entry)
        entries = sorted((p for p in base.iterdir()
                          if p.is_dir() and not p.is_symlink()
                          and re.fullmatch(r'[0-9]+-[0-9]+', p.name)),
                         key=lambda p: p.stat().st_mtime_ns, reverse=True)
        for old in entries[2:]:
            shutil.rmtree(old)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    print('Saved verified CI image archives')


def restore(directory, entry, context):
    manifest_path = entry / 'manifest.json'
    if entry.is_symlink() or manifest_path.is_symlink():
        return False
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict) or manifest.get('identity') != context:
        return False
    hashes = manifest.get('sha256')
    if not isinstance(hashes, dict) or set(hashes) != set(NAMES):
        return False
    for name in NAMES:
        if not isinstance(hashes[name], str) or not re.fullmatch(r'[0-9a-f]{64}', hashes[name]):
            return False
        if digest(entry / name) != hashes[name]:
            return False
    # Check every image before exposing any archive to a scenario.
    directory.mkdir(parents=True, exist_ok=True)
    for name in NAMES:
        shutil.copyfile(entry / name, directory / name)
    os.utime(entry, None)
    print('Restored verified CI image archives')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('save', 'restore'))
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    try:
        context = identity()
        base = Path(os.environ['RUNNER_TOOL_CACHE']) / 'n-gobgp-test-images' / context['GITHUB_REPOSITORY_ID']
        entry = base / f"{context['GITHUB_RUN_ID']}-{context['GITHUB_RUN_ATTEMPT']}"
        if args.command == 'save':
            save(args.directory, base, entry, context)
        elif not restore(args.directory, entry, context):
            print('CI image cache miss')
            return 1
    except (OSError, ValueError, KeyError, TypeError) as err:
        print(f'CI image cache: {err}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
