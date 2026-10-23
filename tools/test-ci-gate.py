#!/usr/bin/env python3
"""Exercise CI gate failures without publishing synthetic status checks."""
import json
import os
from pathlib import Path
import re
import subprocess
import unittest


class GateTest(unittest.TestCase):
    def setUp(self):
        workflow = Path('.github/workflows/ci.yml').read_text().split('jobs:', 1)[1]
        jobs = set(re.findall(r'^  ([a-zA-Z0-9_-]+):$', workflow, re.M)) - {'required'}
        self.results = {job: {'result': 'success'} for job in jobs}

    def run_gate(self):
        return subprocess.run(['python3', 'tools/check-ci.py'], capture_output=True,
                              env={**os.environ, 'NEEDS_JSON': json.dumps(self.results)},
                              check=False).returncode

    def test_all_succeeded(self):
        self.assertEqual(0, self.run_gate())

    def test_failed_skipped_cancelled(self):
        for result in ('failure', 'skipped', 'cancelled', None):
            self.results['unit'] = {'result': result}
            self.assertNotEqual(0, self.run_gate())

    def test_missing_dependency(self):
        del self.results['unit']
        self.assertNotEqual(0, self.run_gate())

    def test_empty_results(self):
        self.results = {}
        self.assertNotEqual(0, self.run_gate())


if __name__ == '__main__':
    unittest.main()
