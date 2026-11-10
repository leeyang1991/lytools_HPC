# lytools_HPC

`lytools_HPC` is a Python batch-processing utility built on Submitit. It runs parameter lists locally or on Slurm HPC clusters and monitors execution through Redis and a terminal progress bar.

## Features

- **Batch submission:** Split a parameter list into batches and submit them through `submitit.AutoExecutor`.
- **Two levels of parallelism:** Run multiple batches concurrently, with a thread pool or process pool inside each batch.
- **Progress monitoring:** Use Redis counters, heartbeats, and job logs to display progress, processing speed, estimated remaining time, and batch status with Rich.
- **Log inspection:** Inspect output, errors, Submitit results, and submission records, and extract parameters from failed batches.

## Installation

```bash
pip install lytools_HPC
```

To install the current source checkout, run from the project root:

```bash
pip install -e .
```

Dependencies include `submitit`, `redis`, `pathos`, `rich`, `requests`, `packaging`, `tqdm`, and `psutil`.

Redis server executable files support these platforms:

| Operating system | Architecture | Executable |
| --- | --- | --- |
| macOS | Apple Silicon / arm64 | [redis-server_AppleSilicon](https://github.com/leeyang1991/lytools_HPC/releases/download/v0.1.0/redis-server_AppleSilicon) |
| Linux | x86_64 | [redis-server_Linux_x86](https://github.com/leeyang1991/lytools_HPC/releases/download/v0.1.0/redis-server_Linux_x86) |


### Running on HPC

1. Request a compute node in advance.
2. Connect to that node through SSH or PyCharm Remote SSH and run your submission script. If a new Redis service is needed, it starts on that node.


### Running locally

Use the same Python interface; Submitit selects the execution backend automatically. 

## Quick Start

The current public submission function is named **`sumbit_jobs_array`**. 

```python
from pathlib import Path
import uuid

from lytools_HPC import sumbit_jobs_array


def multiply(params):
    x, y = params
    return x * y


if __name__ == '__main__':
    job_name = 'multiply-' + uuid.uuid4().hex[:8]
    log_folder = Path.home() / 'lytools_jobs' / job_name
    params_list = [(i, i + 1) for i in range(1, 11)]

    sumbit_jobs_array(
        multiply,
        params_list,
        log_folder=str(log_folder),
        job_name=job_name,
        job_number_limit=2,
        parallel_process_per_task=2,
        slurm_array_parallelism=2,
        parallel_process_p_or_t='t',
        cpus_per_task=2,
        mem_gb=1,
        timeout_min=10,
        slurm_partition='general',  # Set an available partition on your cluster.
        pbar_update_freq=1,
        skip_confirmation=False,
        is_skip_unavailable_nodes=False,
    )
```


## Submission Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `func` | Required | Worker function with the signature `func(params)`. |
| `params_list` | Required | Parameter groups, one per function call. |
| `log_folder` | `None` | Defaults to `~/.config/lytools_HPC/log/<job_name>`. |
| `job_name` | `None` | Generated randomly if omitted; also identifies Redis progress state. |
| `job_number_limit` | `10` | Number of batches; reduced automatically if it exceeds the parameter count. |
| `parallel_process_per_task` | `10` | Number of worker threads or processes within each batch. |
| `slurm_array_parallelism` | `-1` | Maximum concurrent array batches; `-1` uses the batch count. |
| `parallel_process_p_or_t` | `'p'` | `'p'` for processes or `'t'` for threads. |
| `cpus_per_task` | `1` | CPUs requested per job; generally match this to the worker count. |
| `mem_gb` | `1` | Memory requested per job, in GB. |
| `timeout_min` | `600` | Time limit per job, in minutes. |
| `slurm_partition` | `'general'` | Slurm partition. |
| `exclude_nodes` | `None` | Node exclusion string, such as `cn[1-3,8]`. |
| `specific_nodes` | `None` | Explicit node list passed to Slurm. |
| `pbar_update_freq` | `1` | Number of completed parameter groups accumulated per execution process before reporting to Redis. |
| `skip_confirmation` | `False` | Skip the submission confirmation prompt. |
| `watch_dog_timeout_seconds` | `10` | Heartbeat interval and timeout setting, in seconds. |
| `error_skip` | `False` | Catch individual parameter errors and continue when enabled. |
| `is_skip_unavailable_nodes` | `True` | Query `sinfo` and automatically exclude selected nodes. |

The approximate maximum number of concurrent parameter calls is `slurm_array_parallelism × parallel_process_per_task`, subject to scheduling and the remaining work in each batch. Parameters are split evenly by count; uneven task durations can leave a few batches running longer than others.

## Automatic Redis Management

Each submission first reads `~/.config/lytools_HPC/redis.json` and tests its saved IP addresses with authenticated `PING` requests. A reachable service is reused and its persistence settings are checked. Otherwise, a new service is started on the current node.

New services use a randomly selected available port between 20001 and 65535 and a randomly generated password. The program tests every non-loopback IPv4 interface address on the current node and saves successful addresses. Redis listens on IPv4 interfaces; IPv6 is not used.

The connection configuration contains:

| Field | Description |
| --- | --- |
| `ip` | Selected connection IP address. |
| `ips` | All IP addresses that passed the connection test. |
| `port` | Redis port. |
| `password` | Generated authentication password. |
| `pid` | Redis process ID recorded at startup. |
| `executable` | Executable path. |

The configuration file has mode `0600`. Connectivity tests run on the node executing the submission script; access from other workers still depends on cluster routing and firewall rules. Workers connect using the selected `ip` field.

To start or connect to Redis separately:

```python
from lytools_HPC.redis_service import ensure_redis, connect

info = ensure_redis()
client = connect(info)
try:
    assert client.ping()
finally:
    client.close()
```

### Local File Layout

```text
~/.config/lytools_HPC/
├── bin/                       # Downloaded Redis executables
├── data/
│   ├── dump.rdb               # Created when snapshot conditions are met
│   └── appendonlydir/         # Multipart AOF files on Redis 7+
├── redis.json                 # IP addresses, port, password, and metadata
├── redis.lock                 # Startup lock
└── redis-<port>.log            # Redis server log
```

### Persistence

- The data directory is `~/.config/lytools_HPC/data`. Redis loads existing persisted data when it restarts. Reusing a service also checks and attempts to enable these persistence settings.

If an existing server blocks runtime changes to its data directory, the program reports an error. Stop that service and run again to start Redis with the new configuration. Persistence preserves Redis task state; it does not automatically resume or resubmit interrupted computations.

## Progress and Logs

The terminal monitor combines Redis counters, heartbeats, and log markers to display completion, processing speed, estimated remaining time, and batch status. A heartbeat indicates that the reporting process is alive; it does not guarantee that the worker function is making progress.

```python
from lytools_HPC import Check_logs, progress_bar_monitoring

logs = Check_logs('/absolute/path/to/job_logs')
logs.read_err_files()       # Print nonempty error logs.
logs.read_out_files()       # Skip logs with completion markers by default.
logs.read_result_files()    # Print results saved by Submitit.
logs.read_submit_files()    # Print submission records.
failed_batches = logs.get_err_params()  # Extract failed batch parameters.

# Monitor an existing run with its Redis state and log directory.
progress_bar_monitoring('existing-job-name', '/absolute/path/to/job_logs')
```

`Check_logs.watch()` repeatedly displays logs and task counts; press Ctrl+C to exit. Result and submission records use pickle, so only read files from trusted sources.

## Test Scripts

Run from the project root:

```bash
python tests/test_redis_service.py
python tests/test_submit_jobs_array.py
```

- `test_redis_service.py` checks startup or reuse, authenticated connections to saved IPs, configuration permissions, reads and writes, and AOF settings, status, and files. It deletes only its own test key and leaves Redis running. Restart recovery is not tested.
- `test_submit_jobs_array.py` executes computations through the main submission function and checks result files, Redis completion counts, and error logs. Edit constants at the top of the script to configure the partition, parallelism mode, batch count, worker count, memory, timeout, and output directory. No command-line arguments are required. Test artifacts are retained in a unique directory.

Tests may download and start Redis. On HPC, the submission test submits real jobs and consumes compute resources.

## Current Behavior and Usage Notes

- **Use a dedicated log directory.** Submission deletes existing non-hidden files and directories in the selected log folder before displaying the confirmation prompt. Do not use a directory containing unrelated data.
- **Use distinct job names.** Submitting with the same name resets its Redis progress state. Avoid sharing a name between concurrent runs.
- **Single-batch execution:** When `job_number_limit=1`, the current implementation forces thread mode.
- **Progress accuracy:** Keep `pbar_update_freq=1` for accurate counting. Larger values batch updates, and the current implementation does not flush a process's remaining partial batch on exit.
- **Skipped errors:** With `error_skip=True`, failed parameter calls also increase the completion counter. Reaching 100% does not imply that every call succeeded; inspect logs and results.
- **Node filtering:** Automatic node filtering and helper parsers target node names with the `cn` prefix. Set `is_skip_unavailable_nodes=False` for local runs or clusters using other naming conventions.
- **Monitor completion:** If `scancel` exists, the current monitor invokes it for the recorded job when monitoring ends. Otherwise, cancellation is skipped.

## License

[MIT](LICENSE)

## Redis Lifetime and Manual Shutdown

Submission passes `timeout_min` to `ensure_redis(timeout_min=timeout_min)`.
The lifetime begins when the service becomes available, before submission and
queue waiting. A detached watchdog gracefully shuts Redis down with `SHUTDOWN
SAVE` at expiry, even if the submission script has exited. Reusing Redis renews
the lifetime to at least `timeout_min` minutes from that call; an existing later
deadline is retained so a shorter submission cannot shorten it.

The configuration records `expires_at` (Unix timestamp) and a watcher token.
Only the latest watcher can shut down the current service. Watcher output is
recorded in `redis-lifecycle.log`. Shutdown preserves the data directory and
removes the connection configuration after success. The default lifetime for
standalone `ensure_redis()` calls is 600 minutes.

To stop the shared service manually:

```python
from lytools_HPC import stop_redis

stopped = stop_redis()
```

`stop_redis()` returns `True` after shutdown, or `False` if no configuration
exists or the service cannot be reached. Authentication and other server errors
are propagated. Stopping Redis affects every run using it. Queue waiting counts
against the lifetime; this timer does not extend automatically while jobs run.
The watchdog requires its node and Python environment to remain available.
