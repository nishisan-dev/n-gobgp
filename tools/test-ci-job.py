#!/usr/bin/env python3
"""Protect resource ownership and artifact boundaries of the CI controller."""
import importlib.util
import json
import os
import signal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('ci_job', Path(__file__).with_name('ci-job.py'))
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


class JobCleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = {'repository': '123', 'instance': 'a' * 20,
                      'container': 'n-gobgp-job-' + 'a' * 20,
                      'network': 'n-gobgp-net-' + 'a' * 20}
        self.path = Path(self.temp.name) / 'state.json'
        self.path.write_text(json.dumps(self.state))
        self.env = patch.dict(os.environ, {'NGOBGP_CI_STATE': str(self.path),
                                           'GITHUB_WORKSPACE': self.temp.name,
                                           'RUNNER_TEMP': self.temp.name})
        self.env.start()
        self.addCleanup(self.env.stop)

    def resource(self, container=False, repository='123', instance=None):
        labels = {controller.REPO_LABEL: repository,
                  controller.INSTANCE_LABEL: instance or self.state['instance']}
        return {'Config': {'Labels': labels}} if container else {'Labels': labels}

    def test_refuses_unrelated_container(self):
        with patch.object(controller, 'inspect', return_value=self.resource(True, 'other')):
            with patch.object(controller, 'docker') as docker:
                with self.assertRaises(ValueError):
                    controller.cleanup()
                docker.assert_not_called()
        self.assertTrue(self.path.exists())

    def test_refuses_other_job_network(self):
        with patch.object(controller, 'inspect', side_effect=[None, self.resource(instance='b' * 20)]):
            with patch.object(controller, 'docker') as docker:
                with self.assertRaises(ValueError):
                    controller.cleanup()
                docker.assert_not_called()

    def test_removes_only_owned_resources_and_is_idempotent(self):
        with patch.object(controller, 'inspect', side_effect=[self.resource(True), self.resource()]):
            with patch.object(controller, 'docker') as docker:
                controller.cleanup()
                self.assertEqual([('rm', '--force', '--volumes', self.state['container']),
                                  ('network', 'rm', self.state['network'])],
                                 [call.args for call in docker.call_args_list])
                controller.cleanup()
                self.assertEqual(2, docker.call_count)
        self.assertFalse(self.path.exists())

    def test_export_rejects_paths_outside_workspace(self):
        with patch.object(controller, 'inspect', return_value=self.resource(True)):
            with patch.object(controller, 'docker') as docker:
                for name in ('../secret', '/etc/passwd'):
                    with self.subTest(name=name), self.assertRaises(ValueError):
                        controller.export([name])
                docker.assert_not_called()


    def test_refuses_to_shadow_an_existing_vm_route(self):
        with patch.dict(os.environ, {'GITHUB_REPOSITORY_ID': '123', 'RUNNER_NAME': 'existing'}):
            with patch.object(controller, 'reap'), patch.object(controller, 'tools_image', return_value='tools'):
                with patch.object(controller, 'inspect', return_value=None):
                    with patch.object(controller.subprocess, 'check_output',
                                      return_value=b'[{"dst":"10.231.96.0/22"}]'):
                        with patch.object(controller, 'docker') as docker:
                            with self.assertRaisesRegex(RuntimeError, 'overlaps existing VM routes'):
                                controller.start()
                            self.assertFalse(any(call.args[:2] == ('network', 'create')
                                                 for call in docker.call_args_list))


    def test_cancellation_stays_nonzero_if_docker_client_returns_success(self):
        resource = self.resource(True)
        resource['State'] = {'Running': True}
        handlers = {}
        def register(signum, handler):
            previous = handlers.get(signum)
            handlers[signum] = handler
            return previous
        with patch.object(controller, 'inspect', return_value=resource):
            with patch.object(controller, 'copy_workspace'), patch.object(controller, 'docker'):
                with patch.object(controller.subprocess, 'Popen') as popen:
                    process = popen.return_value
                    process.poll.return_value = None
                    def wait():
                        handlers[signal.SIGTERM](signal.SIGTERM, None)
                        process.poll.return_value = 0
                        return 0
                    process.wait.side_effect = wait
                    with patch.object(controller.signal, 'signal', side_effect=register):
                        self.assertEqual(143, controller.run('/tmp/step.sh'))
                    process.send_signal.assert_called_once_with(signal.SIGTERM)


    def test_export_refuses_a_symlink_to_another_directory(self):
        with tempfile.TemporaryDirectory() as outside:
            (Path(self.temp.name) / 'link').symlink_to(outside, target_is_directory=True)
            with patch.object(controller, 'inspect', return_value=self.resource(True)):
                with patch.object(controller, 'docker') as docker:
                    with self.assertRaisesRegex(ValueError, 'symlink'):
                        controller.export(['link/artifact'])
                    docker.assert_not_called()


    def test_forwards_step_settings_without_host_credentials_or_secret_arguments(self):
        resource = self.resource(True)
        resource['State'] = {'Running': True}
        with patch.dict(os.environ, {'GORACE': 'halt_on_error=1', 'GITHUB_TOKEN': 'secret-value',
                                     'SSH_AUTH_SOCK': '/host/agent.sock', 'HOME': '/host/home'}):
            with patch.object(controller, 'inspect', return_value=resource):
                with patch.object(controller, 'copy_workspace'), patch.object(controller, 'docker'):
                    with patch.object(controller.subprocess, 'Popen') as popen:
                        process = popen.return_value
                        process.wait.return_value = 0
                        process.poll.return_value = 0
                        self.assertEqual(0, controller.run('/tmp/step.sh'))
                        args = popen.call_args.args[0]
                        self.assertIn('GORACE', args)
                        self.assertIn('GITHUB_TOKEN', args)
                        self.assertNotIn('secret-value', ' '.join(args))
                        self.assertNotIn('SSH_AUTH_SOCK', args)
                        self.assertNotIn('HOME', args)


if __name__ == '__main__':
    unittest.main()
