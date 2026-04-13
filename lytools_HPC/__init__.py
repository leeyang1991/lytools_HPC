# coding='utf-8'
__version__ = '0.0.2'
__package_name__ = 'lytools_HPC'
import warnings
import requests
from packaging import version

def check_latest_version(package_name, current_version, cache_hours=0.1):
    cache_file = os.path.expanduser(f"~/.{package_name}_version_check")
    latest_version = None
    now = time.time()

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                data = json.load(f)
            if now - data["timestamp"] < cache_hours * 3600:
                latest_version = data["latest_version"]
        except Exception:
            pass

    if latest_version is None:
        try:
            url = f"https://pypi.org/pypi/{package_name}/json"
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                data = resp.json()
                latest_version = data["info"]["version"]
                with open(cache_file, "w") as f:
                    json.dump({"latest_version": latest_version, "timestamp": now}, f)
        except Exception:
            return

    try:
        if version.parse(current_version) < version.parse(latest_version):
            warnings.warn(
                f"{package_name} {current_version} is outdated. "
                f"Latest version is {latest_version}. "
                f"Upgrade via: pip install -U {package_name}"
            )
    except Exception:
        pass

try:
    current = __version__
    check_latest_version(__package_name__, current)
except Exception:
    pass

from ._lytools_HPC import *
