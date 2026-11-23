#!/usr/bin/env python3
"""Exercise the lab's real namespace launcher with disposable daemon fixtures.

Run on an authorized Linux test host with user namespaces enabled. The fixtures
replace Go builds and network programming, while retaining the lab's launcher,
PID tracking and exit traps. The full forwarding lab tests real BGP separately.
"""
import ctypes
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest


DAEMON = '''#!/usr/bin/env python3
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root = Path(os.environ['FIB_CLEANUP_TEST_DIR'])
role = Path(sys.argv[sys.argv.index('-f') + 1]).stem
if os.environ.get('FIB_CLEANUP_STUBBORN') == '1':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
else:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(600)'])
(root / (role + '.ready')).touch()
while True:
    time.sleep(1)
'''

GO = '''#!/usr/bin/env python3
import os
from pathlib import Path
import shutil
import sys

output = Path(sys.argv[sys.argv.index('-o') + 1])
if output.name == 'gobgpd':
    shutil.copyfile(Path(os.environ['FIB_CLEANUP_TEST_DIR']) / 'daemon.py', output)
else:
    output.write_text('#!/bin/sh\\nexit 0\\n')
output.chmod(0o755)
'''

IP = '''#!/usr/bin/env python3
import os
import sys

if sys.argv[1:4] == ['netns', 'exec', 'cpe']:
    os.execvp(sys.argv[4], sys.argv[4:])
'''


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux namespaces required')
class LabCleanupTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reap deliberately orphaned fixtures when characterizing the old lab.
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), 'enable child subreaper')
        cls.source = Path(os.environ.get('FIB_LAB_SCRIPT',
                                        Path(__file__).with_name('lab.sh')))
        cls.prefix, separator, _ = cls.source.read_text().partition(
            'echo "== startup"')
        if not separator:
            raise AssertionError('lab startup boundary missing')
        if shutil.which('unshare') is None:
            raise AssertionError('unshare is required')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='fib-cleanup-test.')
        self.root = Path(self.temp.name)
        self.process = None
        self.log = (self.root / 'lab.log').open('w')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.write_executable(self.root / 'daemon.py', DAEMON)
        self.write_executable(self.bin / 'go', GO)
        self.write_executable(self.bin / 'ip', IP)
        for command in ('mount', 'sysctl', 'mkdir'):
            self.write_executable(self.bin / command, '#!/bin/sh\nexit 0\n')
        self.script = self.root / 'test' / 'fib' / 'lab.sh'
        self.script.parent.mkdir(parents=True)

    @staticmethod
    def write_executable(path, content):
        path.write_text(content)
        path.chmod(0o755)

    def living_processes(self):
        found = []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdecimal():
                continue
            try:
                cwd = Path(os.readlink(proc / 'cwd'))
                if not cwd.is_relative_to(self.root):
                    continue
                state = (proc / 'stat').read_text().rsplit(')', 1)[1].split()[0]
                if state not in ('Z', 'X'):
                    found.append(int(proc.name))
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
        return found

    def tearDown(self):
        if self.process is not None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=10)
        for pid in self.living_processes():
            try:
                fd = os.pidfd_open(pid)
                try:
                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                finally:
                    os.close(fd)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    time.sleep(0.02)
                    continue
            except ChildProcessError:
                break
        self.log.close()
        self.temp.cleanup()

    def start(self, action='sleep 600', stubborn=False):
        fixture = '''
start_server
for attempt in {1..200}; do
    if [[ -f "$FIB_CLEANUP_TEST_DIR/cpe.ready" && -f "$FIB_CLEANUP_TEST_DIR/server.ready" ]]; then
        break
    fi
    sleep 0.02
done
[[ -f "$FIB_CLEANUP_TEST_DIR/cpe.ready" && -f "$FIB_CLEANUP_TEST_DIR/server.ready" ]]
sleep 600 &
WATCH=$!
touch "$FIB_CLEANUP_TEST_DIR/lab.ready"
'''
        self.write_executable(self.script, self.prefix + fixture + action + '\n')
        env = {**os.environ, 'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
               'TMPDIR': str(self.root), 'FIB_CLEANUP_TEST_DIR': str(self.root),
               'FIB_CLEANUP_STUBBORN': str(int(stubborn))}
        env.pop('FIB_LAB_INNER', None)
        self.process = subprocess.Popen(['bash', str(self.script)], env=env,
                                        stdout=self.log, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        deadline = time.monotonic() + 10
        while not (self.root / 'lab.ready').exists():
            if self.process.poll() is not None or time.monotonic() >= deadline:
                self.fail('lab setup failed: ' + (self.root / 'lab.log').read_text())
            time.sleep(0.02)

    def assert_stopped(self, expected):
        result = self.process.wait(timeout=15)
        self.assertIn(result, expected, (self.root / 'lab.log').read_text())
        deadline = time.monotonic() + 3
        while self.living_processes() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual([], self.living_processes(), 'lab descendants survived exit')

    def test_normal_exit(self):
        self.start('exit 0')
        self.assert_stopped([0])

    def test_command_failure(self):
        self.start('false')
        self.assert_stopped([1])

    def test_hangup(self):
        self.start()
        os.killpg(self.process.pid, signal.SIGHUP)
        self.assert_stopped([-signal.SIGHUP, 129])

    def test_interrupt(self):
        self.start()
        os.killpg(self.process.pid, signal.SIGINT)
        self.assert_stopped([-signal.SIGINT, 130])

    def test_termination(self):
        self.start()
        os.killpg(self.process.pid, signal.SIGTERM)
        self.assert_stopped([-signal.SIGTERM, 143])

    def test_killed_launcher(self):
        self.start()
        self.process.kill()
        self.assert_stopped([-signal.SIGKILL])

    def test_killed_namespace_init(self):
        self.start()
        children = Path(f'/proc/{self.process.pid}/task/{self.process.pid}/children')
        pids = [int(pid) for pid in children.read_text().split()]
        self.assertEqual(1, len(pids), 'launcher must supervise one namespace init')
        fd = os.pidfd_open(pids[0])
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        finally:
            os.close(fd)
        # util-linux can return its own error status after init is killed.
        # Setup already completed, and every descendant must still be gone.
        self.assert_stopped([1, -signal.SIGKILL, 128 + signal.SIGKILL])

    def test_stubborn_daemons(self):
        self.start('exit 0', stubborn=True)
        self.assert_stopped([0])


if __name__ == '__main__':
    unittest.main()
