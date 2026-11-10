"""Unit tests without Redis, Slurm, downloads, or job submission.

Run: python -m unittest discover -s tests -p test_redis_slurm_launch.py
"""
import os
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch


class RedisSlurmLaunchTests(unittest.TestCase):
    def setUp(self):
        with patch.dict(sys.modules, redis=types.ModuleType('redis'),
                        psutil=types.ModuleType('psutil')):
            self.module = runpy.run_path(str(Path(__file__).resolve().parents[1] /
                                            'lytools_HPC' / 'redis_service.py'))
        self.context = self.module['redis_launch_context']
        self.command = self.module['redis_command']

    def test_local(self):
        with patch.dict(os.environ, {}, clear=True), patch('shutil.which', return_value=None):
            self.assertIsNone(self.context())
        self.assertEqual(self.command('/bin/redis', '/tmp/redis.conf', None),
                         ['/bin/redis', '/tmp/redis.conf'])

    def test_explicit_allocation(self):
        with patch.dict(os.environ, {'LYTOOLS_HPC_REDIS_JOB_ID': '123',
                                     'SLURMD_NODENAME': 'cn1'}, clear=True), \
                patch('shutil.which', return_value='/bin/srun'):
            context = self.context()
        command = self.command('/bin/redis', '/tmp/redis.conf', context)
        for argument in ('--jobid=123', '--nodelist=cn1', '--overlap', '--exact'):
            self.assertIn(argument, command)
        self.assertEqual(command[-2:], ['/bin/redis', '/tmp/redis.conf'])

    def test_ssh_discovery(self):
        with patch.dict(os.environ, {}, clear=True), \
                patch('shutil.which', side_effect=lambda name: '/bin/' + name), \
                patch('socket.gethostname', return_value='cn1.example'), \
                patch('subprocess.run', return_value=types.SimpleNamespace(stdout='123\n123\n')):
            self.assertEqual(self.context()['job_id'], '123')

    def test_ambiguous_allocations_fail(self):
        with patch.dict(os.environ, {}, clear=True), \
                patch('shutil.which', side_effect=lambda name: '/bin/' + name), \
                patch('subprocess.run', return_value=types.SimpleNamespace(stdout='123\n456\n')):
            with self.assertRaisesRegex(RuntimeError, 'LYTOOLS_HPC_REDIS_JOB_ID'):
                self.context()

    def test_missing_srun_with_allocation_fails(self):
        with patch.dict(os.environ, {'SLURM_JOB_ID': '123'}, clear=True), \
                patch('shutil.which', return_value=None):
            with self.assertRaises(RuntimeError):
                self.context()


if __name__ == '__main__':
    unittest.main()
