"""Run locally or on HPC: python tests/test_submit_jobs_array.py.

Edit the configuration constants below. Submitit selects the execution backend.
On HPC, the output directory must be shared with all workers.
Logs and result artifacts are retained in a unique directory for inspection.
"""
import json
import os
from pathlib import Path
import socket
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lytools_HPC import sumbit_jobs_array  # noqa: E402
from lytools_HPC.redis_service import connect, load_connection  # noqa: E402


# Test configuration
PARTITION = 'general'
MODE = 't'  # 't': threads; 'p': processes
NUMBER_OF_JOBS = 5
WORKERS_PER_JOB = 5
MEM_GB = 1
TIMEOUT_MIN = 5
OUTPUT_ROOT = Path.home() / '.config' / 'lytools_HPC' / 'integration_tests'


def test_task(params):
    """Write a verifiable result before reporting task completion."""
    index, value, output_directory = params
    result = dict(index=index, value=value, square=value * value,
                  hostname=socket.gethostname(), pid=os.getpid())
    directory = Path(output_directory)
    temporary = directory / ('%s.tmp' % index)
    temporary.write_text(json.dumps(result))
    temporary.replace(directory / ('%s.json' % index))
    time.sleep(2)
    return result


# This is a manual integration script, not an automatically collected pytest test.
test_task.__test__ = False


def main():
    run_id = 'lytools-test'
    directory = OUTPUT_ROOT.expanduser().resolve() / run_id
    results = directory / 'results'
    results.mkdir(parents=True)
    logs = directory / 'logs'
    values = list(range(1, 90))
    params = [(i, value, str(results)) for i, value in enumerate(values)]
    print('Submitting %s jobs, %s workers per job, %s parameters.' %
          (NUMBER_OF_JOBS, WORKERS_PER_JOB, len(values)))
    print('Artifacts: %s' % directory, flush=True)

    sumbit_jobs_array(test_task, params,
                      log_folder=None,
                      job_name=run_id,
                      job_number_limit=NUMBER_OF_JOBS,
                      parallel_process_per_task=WORKERS_PER_JOB,
                      slurm_array_parallelism=-1,
                      parallel_process_p_or_t='p',
                      cpus_per_task=1,
                      mem_gb=1,
                      timeout_min=TIMEOUT_MIN,
                      slurm_partition="general",
                      # exclude_nodes='cn498',
                      exclude_nodes=None,
                      specific_nodes=None,
                      pbar_update_freq=1,
                      skip_confirmation=False,
                      watch_dog_timeout_seconds=10,
                      error_skip=False,
                      is_skip_unavailable_nodes=True,
                      )


    for index, value in enumerate(values):
        path = results / ('%s.json' % index)
        assert path.is_file(), 'Missing task result: %s; inspect %s' % (index, logs)
        result = json.loads(path.read_text())
        assert result['index'] == index and result['value'] == value
        assert result['square'] == value * value, 'Incorrect result for task %s' % index
    assert len(list(results.glob('*.json'))) == len(values), 'Unexpected result count'
    print('PASS: all task results match expected values')

    client = connect(load_connection())
    try:
        state = client.hgetall(run_id)
        assert int(state[b'total']) == len(values), 'Incorrect Redis total'
        assert int(state[b'step']) == len(values), 'Incorrect Redis completion count'
    finally:
        client.close()
    print('PASS: Redis progress reached')
    errors = []
    for path in logs.glob('*.err'):
        content = path.read_text(errors='replace')
        if 'Traceback' in content or 'submitit ERROR' in content:
            errors.append(path.name)
    assert not errors, 'Errors in job logs: %s' % errors
    print('PASS: no Python traceback or Submitit errors in stderr logs')
    print('Integration test passed. Artifacts retained at %s' % directory)


if __name__ == '__main__':
    main()
