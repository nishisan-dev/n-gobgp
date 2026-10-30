#!/usr/bin/env python3
"""Verify VRF runner preparation without loading modules or installing packages."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


MOCK = '''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
name = Path(sys.argv[0]).name
with open(os.environ['MOCK_LOG'], 'a') as log:
    log.write(json.dumps([name, *sys.argv[1:]]) + '\\n')
if name == 'uname':
    print('fixture-kernel')
    sys.exit(0)
if name == 'modprobe':
    path = Path(os.environ['MOCK_COUNT'])
    count = int(path.read_text()) if path.exists() else 0
    path.write_text(str(count + 1))
    codes = json.loads(os.environ['MOCK_MODULE_CODES'])
    sys.exit(codes[min(count, len(codes) - 1)])
sys.exit(int(os.environ.get('MOCK_INSTALL_CODE', '0')) if sys.argv[1] == 'install' else 0)
'''


class VRFPreparationTest(unittest.TestCase):
    def run_preparation(self, module_codes, install_code=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('modprobe', 'apt-get', 'uname'):
                path = root / name
                path.write_text(MOCK)
                path.chmod(0o755)
            log = root / 'calls.jsonl'
            result = subprocess.run(
                ['bash', 'tools/ci-ensure-vrf.sh'], capture_output=True,
                env={**os.environ, 'PATH': f"{root}:{os.environ['PATH']}",
                     'MOCK_LOG': str(log), 'MOCK_COUNT': str(root / 'count'),
                     'MOCK_MODULE_CODES': json.dumps(module_codes),
                     'MOCK_INSTALL_CODE': str(install_code)}, check=False)
            return result.returncode, [json.loads(line) for line in log.read_text().splitlines()]

    def test_existing_module_needs_no_package(self):
        code, calls = self.run_preparation([0])
        self.assertEqual(0, code)
        self.assertEqual([['modprobe', 'vrf']], calls)

    def test_missing_module_installs_then_requires_load(self):
        code, calls = self.run_preparation([1, 0])
        self.assertEqual(0, code)
        self.assertEqual([['modprobe', 'vrf'], ['apt-get', 'update'], ['uname', '-r'],
                          ['apt-get', 'install', '-y', 'linux-modules-extra-fixture-kernel'],
                          ['modprobe', 'vrf']], calls)

    def test_missing_package_fails(self):
        code, calls = self.run_preparation([1, 0], install_code=100)
        self.assertEqual(100, code)
        self.assertEqual(1, sum(call[0] == 'modprobe' for call in calls))

    def test_module_still_unavailable_fails(self):
        code, calls = self.run_preparation([1, 1])
        self.assertNotEqual(0, code)
        self.assertEqual(['modprobe', 'vrf'], calls[-1])


if __name__ == '__main__':
    unittest.main()
