#!/usr/bin/env python3
"""Check that local CI artifacts cannot be reused across contexts or corruption."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('ci-image-cache.py').resolve()


class ImageCacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        for name in ('gobgp.tar', 'gobgp-oq.tar'):
            (self.source / name).write_bytes(name.encode() * 20)
        self.env = {**os.environ, 'RUNNER_TOOL_CACHE': str(self.root / 'cache'),
                    'GITHUB_REPOSITORY_ID': '123', 'GITHUB_RUN_ID': '456',
                    'GITHUB_RUN_ATTEMPT': '1', 'GITHUB_SHA': 'a' * 40}

    def command(self, command, directory, env=None):
        return subprocess.run(['python3', str(SCRIPT), command, str(directory)],
                              env=env or self.env, capture_output=True, text=True)

    def save(self, env=None):
        result = self.command('save', self.source, env)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_valid_cache_restores_identical_independent_files(self):
        self.save()
        target = self.root / 'restored'
        result = self.command('restore', target)
        self.assertEqual(0, result.returncode, result.stderr)
        for source in self.source.iterdir():
            self.assertEqual(source.read_bytes(), (target / source.name).read_bytes())
        (target / 'gobgp.tar').write_bytes(b'changed consumer copy')
        self.assertEqual(0, self.command('restore', self.root / 'again').returncode)

    def test_mismatched_context_is_a_miss(self):
        self.save()
        for key, value in [('GITHUB_SHA', 'b' * 40), ('GITHUB_RUN_ID', '457'),
                           ('GITHUB_RUN_ATTEMPT', '2'), ('GITHUB_REPOSITORY_ID', '124')]:
            with self.subTest(key=key):
                target = self.root / key
                self.assertNotEqual(0, self.command('restore', target,
                                                   {**self.env, key: value}).returncode)
                self.assertFalse(target.exists())

    def test_corrupt_or_incomplete_cache_is_a_miss(self):
        for name in ('gobgp.tar', 'gobgp-oq.tar', 'manifest.json'):
            with self.subTest(name=name):
                self.save()
                entry = self.root / 'cache/n-gobgp-test-images/123/456-1'
                (entry / name).write_bytes(b'corrupt')
                target = self.root / name
                self.assertNotEqual(0, self.command('restore', target).returncode)
                self.assertFalse(target.exists())

    def test_retains_only_two_execution_entries(self):
        for run in ('456', '457', '458'):
            self.save({**self.env, 'GITHUB_RUN_ID': run})
        entries = self.root / 'cache/n-gobgp-test-images/123'
        self.assertEqual({'457-1', '458-1'}, {p.name for p in entries.iterdir()})
        self.assertNotEqual(0, self.command('restore', self.root / 'old').returncode)
        self.assertEqual(0, self.command('restore', self.root / 'new',
                                       {**self.env, 'GITHUB_RUN_ID': '458'}).returncode)


if __name__ == '__main__':
    unittest.main()
