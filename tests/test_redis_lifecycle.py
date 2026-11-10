"""Run: python -m unittest discover -s tests -p test_redis_lifecycle.py"""
import json
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        redis = types.ModuleType('redis')
        redis.ConnectionError = type('ConnectionError', (Exception,), {})
        with patch.dict(sys.modules, redis=redis, psutil=types.ModuleType('psutil')):
            self.ns = runpy.run_path(str(Path(__file__).resolve().parents[1] /
                                        'lytools_HPC/redis_service.py'))
        self.g = self.ns['_set_lifetime'].__globals__
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'redis.json'
        self.g['config_path'] = lambda: self.path

    def test_deadline_and_detached_watcher(self):
        info = {}
        with patch('time.time', return_value=100), patch('subprocess.Popen') as launch:
            self.ns['_set_lifetime'](info, 2)
        self.assertEqual(info['expires_at'], 220)
        self.assertEqual(json.loads(self.path.read_text())['lifecycle_token'], info['lifecycle_token'])
        self.assertTrue(launch.call_args.kwargs['start_new_session'])
        with patch('time.time', return_value=110), patch('subprocess.Popen'):
            self.ns['_set_lifetime'](info, 1)
        self.assertEqual(info['expires_at'], 220)

    def test_manual_stop_saves(self):
        self.path.write_text('{}')
        client = Mock()
        self.g['connect'] = lambda info: client
        self.assertTrue(self.ns['stop_redis']())
        client.shutdown.assert_called_once_with(save=True)
        client.close.assert_called_once()
        self.assertFalse(self.path.exists())
        self.assertFalse(self.ns['stop_redis']())

    def test_expiry(self):
        self.path.write_text(json.dumps({'lifecycle_token': 'a', 'expires_at': 10}))
        shutdown = Mock(return_value=True)
        self.g['_shutdown'] = shutdown
        with patch('time.time', return_value=11):
            self.ns['_watch_lifetime']('a')
        shutdown.assert_called_once()

    def test_superseded_watcher_cannot_stop_service(self):
        self.path.write_text(json.dumps({'lifecycle_token': 'new', 'expires_at': 0}))
        shutdown = Mock()
        self.g['_shutdown'] = shutdown
        self.ns['_watch_lifetime']('old')
        shutdown.assert_not_called()

    def test_invalid_timeout(self):
        for value in (0, -1, float('inf'), float('nan'), True):
            with self.assertRaises(ValueError):
                self.ns['ensure_redis'](value)


if __name__ == '__main__':
    unittest.main()
