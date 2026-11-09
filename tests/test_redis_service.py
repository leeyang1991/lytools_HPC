"""Run on the allocated node: python tests/test_redis_service.py.

Requires project dependencies. May download/start Redis and enable persistence.
Leaves the service running for subsequent jobs; never prints credentials.
"""
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lytools_HPC.redis_service import (  # noqa: E402
    config_path, connect, ensure_redis,
)


def main():
    info = ensure_redis()
    print('PASS: Redis service available')
    saved = json.loads(config_path().read_text())
    assert 'hostname' not in saved, 'Configuration still contains hostname'
    assert saved['ip'] in saved['ips'], 'Selected IP missing from successful IP list'
    assert saved['port'] > 20000, 'Port must exceed 20000'
    assert config_path().stat().st_mode & 0o077 == 0, 'Configuration permissions are too broad'
    print('PASS: IP configuration and credential file permissions')

    for address in saved['ips']:
        client = connect(dict(saved, ip=address))
        try:
            assert client.ping(), 'PING failed for %s' % address
            print('PASS: authenticated PING to %s:%s' % (address, saved['port']))
        finally:
            client.close()

    client = connect(info)
    key = 'lytools_HPC:test:' + uuid.uuid4().hex
    try:
        assert client.set(key, 'test-value', ex=120)
        assert client.get(key) == b'test-value', 'Read value differs from written value'
        print('PASS: write/read with automatic expiry')

        settings = client.config_get('*')
        expected_directory = str(config_path().parent / 'data')
        assert settings['dir'] == expected_directory, 'Unexpected persistence directory'
        assert settings['appendonly'] == 'yes', 'AOF is disabled'
        assert settings['appendfsync'] == 'everysec', 'Unexpected AOF fsync policy'
        assert settings['save'], 'RDB snapshots are disabled'
        assert settings['dbfilename'] == 'dump.rdb', 'Unexpected RDB filename'

        deadline = time.monotonic() + 30
        while True:
            state = client.info('persistence')
            assert state['aof_enabled'] == 1, 'AOF is not active'
            assert state['aof_last_write_status'] == 'ok', 'AOF write failed'
            if not state.get('aof_rewrite_in_progress') and not state.get('aof_rewrite_scheduled'):
                assert state.get('aof_last_bgrewrite_status') == 'ok', 'AOF rewrite failed'
                break
            if time.monotonic() >= deadline:
                raise AssertionError('AOF initialization did not finish within 30 seconds')
            time.sleep(0.2)
        data = Path(expected_directory)
        assert any(p.is_file() for p in data.rglob('*.aof*')), 'No AOF files found'
        print('PASS: persistence configuration, AOF status and files')

        reused = ensure_redis()
        for field in ('ip', 'port', 'password', 'pid'):
            assert reused[field] == info[field], 'Existing service was not reused (%s)' % field
        print('PASS: second startup reuses the existing service')
    finally:
        try:
            client.delete(key)
        finally:
            client.close()
    print('All checks passed. Redis remains running. Restart recovery was not tested.')


if __name__ == '__main__':
    main()
