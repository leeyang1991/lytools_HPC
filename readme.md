#  HPC Functions

## Monitoring you jobs with a progress bar. 

This requires a Redis server to store the progress information. The following steps will guide you through setting up the Redis server and using submitit with a progress bar.
### Installation:
```bash
pip install lytools_HPC 
```
### Step 1: Deploy a Redis service
**DO NOT** deploy it on a HPC nodes. You can use your local machine or a server with a public IP address.
#### Deploy redis via Docker
Compose file for Redis server:
```yaml
services:
  redis-progress:
    image: redis:7
    container_name: redis-progress
    restart: unless-stopped

    ports:
      - "6379:6379"

    volumes:
      - ./redis_data:/data

    command: >
      redis-server
      --appendonly yes
      --requirepass yourpassword
      --maxmemory 2gb
      --maxmemory-policy allkeys-lru
      
    deploy:
      resources:
        limits:
          memory: 3g

```
```docker compose up -d``` # Start the Redis server

#### Test Redis connection
```python
import redis
r = redis.Redis(host='your_redis_host', port=6379, password='yourpassword')
r.set('test_key', 'test_value')
print(r.get('test_key').decode())
```

#### Create redis connection configuration file
Location: ```~/.config/redis/redis.conf```
```txt
your_redis_host
6379
yourpassword
```

### Step 2: Submit your jobs
```python
from lytools_HPC import *

def my_function(params):
    x,y = params
    return x * y

job_name = 'Your_job_name'
x_list = [1,2,3,4,5,6,7,8,9,10]
y_list = [2,3,4,5,6,7,8,9,10,11]
params_list = list(zip(x_list,y_list))
log_folder = 'Your_log_folder'
sumbit_jobs_array(my_function, params_list, log_folder,
                  job_name=job_name,
                  job_number_limit=1,
                  parallel_process_per_task=2,
                  slurm_array_parallelism=1,
                  parallel_process_p_or_t='p',
                  cpus_per_task=10,
                  mem_gb=2,
                  timeout_min=10,
                  slurm_partition="general",
                  exclude_nodes=None,
                  pbar_update_freq=1,
                  )
```

### Step 3: Check the log files
```python
from lytools_HPC import *
log_folder = 'Your_log_folder'
Check_logs(log_folder).read_err_files() # Read error files
Check_logs(log_folder).read_out_files() # Read output files
```
