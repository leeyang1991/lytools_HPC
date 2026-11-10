"""Start and reuse a Redis service on the current allocated compute node."""
import json
import ipaddress
import psutil
import os
import platform
import shutil
import tempfile
from urllib.request import urlopen
from pathlib import Path
import secrets
import socket
import subprocess
import time
import math
import sys

import redis


def config_path():
    return Path.home() / '.config' / 'lytools_HPC' / 'redis.json'


def load_connection():
    with config_path().open() as stream:
        return json.load(stream)


def node_ips():
    """Return non-loopback IPv4 interface addresses (Redis binds IPv4)."""
    addresses = set()
    for entries in psutil.net_if_addrs().values():
        for entry in entries:
            if entry.family == socket.AF_INET:
                address = ipaddress.ip_address(entry.address)
                if not address.is_loopback and not address.is_unspecified:
                    addresses.add(str(address))
    if not addresses:
        raise RuntimeError('No non-loopback IPv4 address found on this node')
    return sorted(addresses)


def connect(info):
    return redis.Redis(host=info['ip'], port=info['port'],
                       password=info['password'], socket_connect_timeout=3,
                       socket_timeout=3)


def verify_ips(info, addresses):
    """Test every candidate using authenticated PING and save all successes."""
    successful = []
    for address in addresses:
        candidate_info = dict(info, ip=address)
        client = connect(candidate_info)
        try:
            if client.ping():
                successful.append(address)
        except (OSError, redis.RedisError):
            pass
        finally:
            client.close()
    if not successful:
        raise redis.ConnectionError('No reachable Redis IP address')
    info.pop('hostname', None)
    info.update(ip=successful[0], ips=successful)
    return info


def save_connection(info):
    directory = config_path().parent
    temporary = directory / ('redis-%s.json.tmp' % secrets.token_hex(8))
    try:
        descriptor = os.open(str(temporary), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(info, stream)
        os.replace(str(temporary), str(config_path()))
    finally:
        if temporary.exists():
            temporary.unlink()


REDIS_DOWNLOADS = {
    ('Darwin', 'arm64'): 'https://github.com/leeyang1991/lytools_HPC/releases/download/v0.1.0/redis-server_AppleSilicon',
    ('Linux', 'x86_64'): 'https://github.com/leeyang1991/lytools_HPC/releases/download/v0.1.0/redis-server_Linux_x86',
}


def download_redis():
    """Install the platform-specific binary atomically; caller holds startup lock."""
    system, machine = platform.system(), platform.machine().lower()
    machine = {'aarch64': 'arm64', 'amd64': 'x86_64'}.get(machine, machine)
    url = REDIS_DOWNLOADS.get((system, machine))
    if url is None:
        raise RuntimeError('Unsupported Redis platform: %s %s' % (system, machine))
    directory = config_path().parent / 'bin'
    directory.mkdir(parents=True, exist_ok=True)
    executable = directory / url.rsplit('/', 1)[-1]
    if executable.is_file() and executable.stat().st_size > 0:
        executable.chmod(0o700)
        return executable
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=str(directory), delete=False) as stream:
            temporary = Path(stream.name)
            with urlopen(url, timeout=60) as response:
                shutil.copyfileobj(response, stream)
        if temporary.stat().st_size == 0:
            raise RuntimeError('Downloaded Redis executable is empty')
        temporary.chmod(0o700)
        os.replace(str(temporary), str(executable))
    except Exception as exc:
        raise RuntimeError('Could not download Redis from %s: %s' % (url, exc)) from exc
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return executable


def enable_persistence(client, data_directory):
    """Configure an existing server; propagate failures instead of starting a duplicate."""
    settings = {
        'dir': str(data_directory),
        'dbfilename': 'dump.rdb',
        'save': '900 1 300 10 60 10000',
        'appendfsync': 'everysec',
        'appendonly': 'yes',
    }
    current = client.config_get('*')
    for key, value in settings.items():
        actual = current.get(key, current.get(key.encode()))
        if isinstance(actual, bytes):
            actual = actual.decode()
        if actual != value:
            try:
                client.config_set(key, value)
            except redis.RedisError as exc:
                raise RuntimeError(
                    'Cannot enable Redis persistence (%s). Stop the existing Redis '
                    'service and run again to start it with the new configuration.' % key
                ) from exc


def stop_redis():
    """Gracefully stop the configured service, preserving persisted data.

    Returns False when no configuration exists or the service is unreachable.
    """
    import fcntl
    directory = config_path().parent
    if not directory.exists():
        return False
    fd = os.open(str(directory / 'redis.lock'), os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            info = load_connection()
        except FileNotFoundError:
            return False
        return _shutdown(info)


def _shutdown(info):
    client = connect(info)
    try:
        try:
            client.ping()
        except redis.ConnectionError:
            return False
        client.shutdown(save=True)
        config_path().unlink(missing_ok=True)
        return True
    finally:
        client.close()


def _set_lifetime(info, timeout_min):
    """Called under startup lock; never shorten an existing shared lifetime."""
    info['expires_at'] = max(info.get('expires_at', 0), time.time() + timeout_min * 60)
    info['lifecycle_token'] = secrets.token_hex(16)
    save_connection(info)
    log_path = config_path().parent / 'redis-lifecycle.log'
    with log_path.open('ab') as output:
        subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                          '--watch', info['lifecycle_token']],
                         stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                         start_new_session=True)


def _watch_lifetime(token):
    """Detached watcher survives the submitting Python process exiting."""
    import fcntl
    while True:
        fd = os.open(str(config_path().parent / 'redis.lock'), os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(fd, 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                info = load_connection()
            except FileNotFoundError:
                return
            if info.get('lifecycle_token') != token:
                return
            if time.time() >= info['expires_at']:
                if not _shutdown(info):
                    config_path().unlink(missing_ok=True)
                return
        time.sleep(1)


def ensure_redis(timeout_min=600):
    """Return verified connection details, starting a server if necessary.

    Call on the allocated node before submitting jobs. The home directory must
    be shared with workers. An existing reachable service is reused.
    """
    if isinstance(timeout_min, bool) or not math.isfinite(timeout_min) or timeout_min <= 0:
        raise ValueError('timeout_min must be a finite positive number')
    # Serialize startup across processes sharing this configuration directory.
    import fcntl
    directory = config_path().parent
    directory.mkdir(parents=True, exist_ok=True)
    data_directory = directory / 'data'
    data_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = directory / 'redis.lock'
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        client = None
        try:
            info = load_connection()
            addresses = info.get('ips') or ([info['ip']] if 'ip' in info else [])
            if not addresses and info.get('hostname'):
                # Migrate old configuration by resolving it once, then use IPs only.
                addresses = sorted({item[4][0] for item in socket.getaddrinfo(
                    info['hostname'], info['port'], socket.AF_INET, socket.SOCK_STREAM)})
            verify_ips(info, addresses)
            client = connect(info)
        except (OSError, ValueError, KeyError, redis.RedisError):
            pass
        if client is not None:
            try:
                enable_persistence(client, data_directory)
                _set_lifetime(info, timeout_min)
                return info
            finally:
                client.close()
        executable = download_redis()
        addresses = node_ips()
        # Binding a probe alone cannot reserve a port for another process;
        # retry startup if another process wins the race.
        for attempt in range(10):
            port = 20001 + secrets.randbelow(65535 - 20000)
            with socket.socket() as probe:
                try:
                    probe.bind(('0.0.0.0', port))
                except OSError:
                    continue
            password = secrets.token_hex(32)
            info = dict(port=port, password=password)
            log_path = directory / ('redis-%s.log' % port)
            config = ('bind 0.0.0.0\nport %s\nrequirepass %s\n'
                      'protected-mode yes\ndaemonize no\n'
                      'dir %s\ndbfilename dump.rdb\n'
                      'save 900 1\nsave 300 10\nsave 60 10000\n'
                      'appendonly yes\nappendfsync everysec\n'
                      'logfile %s\n' % (port, password, json.dumps(str(data_directory)),
                                          json.dumps(str(log_path))))
            process = subprocess.Popen([str(executable), '-'], stdin=subprocess.PIPE,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                       start_new_session=True)
            process.stdin.write(config.encode())
            process.stdin.close()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    verify_ips(info, addresses)
                except (OSError, redis.RedisError):
                    time.sleep(1)
                    continue
                info.update(pid=process.pid, executable=str(executable))
                _set_lifetime(info, timeout_min)
                return info
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        raise RuntimeError('Could not start Redis; inspect redis-*.log in %s' % directory)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--watch':
        _watch_lifetime(sys.argv[2])
    else:
        raise SystemExit('Use ensure_redis() or stop_redis() from Python.')
