from os.path import isdir
import submitit
import subprocess
from concurrent.futures import ThreadPoolExecutor
import redis
from threading import Lock
import threading
import multiprocessing
from pathos.multiprocessing import ProcessPool
import time
from rich.text import Text
import datetime
import hashlib
import random
from pprint import pprint
import shutil

import os
from pathlib import Path
import pickle
from tqdm import tqdm
from functools import partialmethod

import psutil

from operator import itemgetter
from itertools import groupby

import warnings
from collections import OrderedDict
warnings.filterwarnings('ignore')

from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    ProgressColumn
)



def listdir(fdir):
    '''
    Mac OS
    list the names of the files in the directory
    return sorted files list without '.DS_store'
    '''
    list_dir = []
    for f in sorted(os.listdir(fdir)):
        if f.startswith('.'):
            continue
        list_dir.append(f)
    return list_dir


def mkdir(dir, force=False):
    if not os.path.isdir(dir):
        if force == True:
            os.makedirs(dir)
        else:
            os.mkdir(dir)

def split_into_n_jobs(lst, n_jobs):
    """
    Split the list into n_jobs parts.
    """
    total = len(lst)
    lst = list(lst)
    chunk_size = total // n_jobs
    remainder = total % n_jobs

    # chunks = []
    chunks_with_idx = []
    start = 0

    for i in range(n_jobs):
        extra = 1 if i < remainder else 0
        end = start + chunk_size + extra
        chunks_i = lst[start:end]
        chunks_i_with_idx = []
        for params in chunks_i:
            params = list(params)
            params.append(i)
            chunks_i_with_idx.append(params)
        # chunks.append(chunks_i)
        chunks_with_idx.append(chunks_i_with_idx)
        start = end
    return chunks_with_idx

_REDIS = None

def get_redis():
    global _REDIS
    if _REDIS is None:
        _REDIS = HPC_redis().conn_redis()
    return _REDIS

_LOCK = Lock()
_LOCAL_COUNTER = 0

def update_i_global(job_name, batch_size=1):
    '''
    :param job_name:
    '''
    global _LOCAL_COUNTER

    with _LOCK:
        _LOCAL_COUNTER += 1

        if _LOCAL_COUNTER >= batch_size:
            r = get_redis()
            r.hincrby(job_name, "step", _LOCAL_COUNTER)
            _LOCAL_COUNTER = 0

def update_i(job_name):
    r = get_redis()
    r.hincrby(job_name, "step", 1)

def heartbeat(job_name,watch_dog_timeout_seconds):
    now = time.time()
    r = get_redis()
    r.hset(f"{job_name}", "last_seen", now)
    r.hset(f"{job_name}", "watch_dog_timeout_seconds", watch_dog_timeout_seconds)
    # print('set',now)


def report_start_time(job_name):
    job_name = f"watchdog:{job_name}_start_time"
    now = time.time()
    r = get_redis()
    r.hset(f"{job_name}", "start_time", now)

def report_req_tasks(job_name,req_tasks:int):
    job_name = f"watchdog:{job_name}_req_tasks"
    r = get_redis()
    r.hset(f"{job_name}", "req_tasks", str(req_tasks))

def report_submit_info(job_name,submit_info):
    job_name = f"watchdog:{job_name}_submit_info"
    r = get_redis()
    for k,v in submit_info.items():
        r.hset(f"{job_name}", k, str(v))
    pass

def get_submit_info(job_name):
    job_name = f"watchdog:{job_name}_submit_info"
    r = get_redis()
    submit_info = r.hgetall(job_name)
    submit_info_dict = {}
    # watchdog_info.get(b'watch_dog_timeout_seconds')
    for key in submit_info:
        val = submit_info.get(key)
        val = val.decode('utf-8')
        key = key.decode('utf-8')
        submit_info_dict[key] = val
    if not len(submit_info_dict) == 0:
        pretty_table_print(submit_info_dict)
    return submit_info_dict


def watchdog_loop(job_name, interval, stop_event, watch_dog_timeout_seconds):
    while not stop_event.is_set():
        try:
            heartbeat(job_name,watch_dog_timeout_seconds)
        except Exception:
            pass
        stop_event.wait(interval)

def watch_dog_p(job_name, job_id, watch_dog_timeout_seconds):
    job_key = f"watchdog:{job_name}_{job_id}"

    ctx = multiprocessing.get_context("spawn")
    stop_event = ctx.Event()

    p = ctx.Process(
        target=watchdog_loop,
        args=(job_key, watch_dog_timeout_seconds, stop_event, watch_dog_timeout_seconds),
        daemon=False
    )
    p.start()
    return p, stop_event

def watch_dog(job_name, job_id, watch_dog_timeout_seconds):
    job_name = f"watchdog:{job_name}_{job_id}"
    stop_event = threading.Event()

    def loop():
        while not stop_event.is_set():
            try:
                heartbeat(job_name,watch_dog_timeout_seconds)
            except Exception:
                pass
            time.sleep(watch_dog_timeout_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()

    return stop_event

def check_watchdog(job_name_watch_dog, job_id):
    job_name_watch_dog = f'{job_name_watch_dog}_{job_id}'
    hpc_redis = HPC_redis()
    watchdog_info = hpc_redis.r.hgetall(job_name_watch_dog)
    watchdog_last_seen = watchdog_info.get(b'last_seen')
    watch_dog_timeout_seconds = watchdog_info.get(b'watch_dog_timeout_seconds')
    if watch_dog_timeout_seconds:
        watch_dog_timeout_seconds = int(watch_dog_timeout_seconds)
    else:
        watch_dog_timeout_seconds = 60
    now = time.time()
    if watchdog_last_seen is None:
        watchdog_status = 'UNKNOWN'
    else:
        watchdog_last_seen = float(watchdog_last_seen)
        if now - watchdog_last_seen - 2 >= watch_dog_timeout_seconds: # 2 seconds buffering
            watchdog_status = "DEAD"
        else:
            watchdog_status = "ALIVE"
    return watchdog_status,watch_dog_timeout_seconds,watchdog_last_seen

def check_start_time(job_name_watch_dog):
    job_name_watch_dog = f"{job_name_watch_dog}_start_time"
    hpc_redis = HPC_redis()
    watchdog_info = hpc_redis.r.hgetall(job_name_watch_dog)
    watchdog_start_time = watchdog_info.get(b'start_time')
    if watchdog_start_time:
        watchdog_start_time = float(watchdog_start_time)
        now = time.time()
        time_delta = now - watchdog_start_time
        time_delta = int(time_delta)
        time_delta_obj = datetime.timedelta(seconds=time_delta)
    else:
         time_delta_obj = '-:--:--'
    return time_delta_obj


class IterSpeedColumn(ProgressColumn):

    def render(self, task):
        # if task.finished:
        #     return Text(f"{task.speed:.2f} it/s", style="green")

        speed = task.speed

        if speed is None:
            return Text("- it/s", style="dim")

        return Text(f"{speed:.2f} it/s", style="yellow")


def sumbit_jobs_array(func,params_list,
                        log_folder=None,
                        job_name=None,
                        job_number_limit=10,
                        parallel_process_per_task=10,
                        slurm_array_parallelism=-1,
                        parallel_process_p_or_t='p',
                        cpus_per_task=1,
                        mem_gb=1,
                        timeout_min=600,
                        slurm_partition="general",
                        exclude_nodes=None,
                        specific_nodes=None,
                        pbar_update_freq=1,
                        skip_confirmation=False,
                        watch_dog_timeout_seconds=10,
                        error_skip=False,
                        is_skip_unavailable_nodes=True,
                      ):
    '''
    :param func: the kernel function to run, should take one argument, e.g. func(params)
    :param params_list: list of tuples [params1, params2, ...]
    :param log_folder: log_folder, if not specified, a folder of ~/config/lytools_HPC_log/[RANDOM STRING] will be created
    :param job_name: job_name, if not specified, a [RANDOM STRING] will be generated as job_name
    :param job_number_limit: number of total jobs you want to submit to slurm
    :param parallel_process_per_task: number of parallel processes per task, Recommend equal to :param cpus_per_task
    :param slurm_array_parallelism: slurm array parallelism
    :param parallel_process_p_or_t: 'p' for multiprocessing, 't' for multi-threading
    :param cpus_per_task: number of cpus per task
    :param mem_gb: memory per task
    :param timeout_min: timeout in minutes
    :param slurm_partition: slurm partition
    :param exclude_nodes: list of nodes to exclude
    :param pbar_update_freq: frequency of updating progress bar
    :param skip_confirmation: skip confirmation
    :param watch_dog_timeout_seconds: watchdog timeout seconds
    '''
    if not is_iterable(params_list):
        raise TypeError("params_list must be iterable")
    if job_name is None:
        job_name = generate_random_string()
    if log_folder is None:
        log_folder = Path.home() / '.config' / 'lytools_HPC_log' / job_name

    init_job(job_name, params_list)
    # init_job(job_name_failed, params_list)
    # print(params_list)
    # exit()
    if len(params_list) == 0:
        raise ValueError("params_list is empty")
    if job_number_limit == 1:
        parallel_process_p_or_t = 't' # Using Threading can support MULTIPROCESS in child process, still need to be tested
    if len(params_list) < job_number_limit:
        job_number_limit = len(params_list)
    super_params_list = split_into_n_jobs(params_list,job_number_limit)
    if len(super_params_list[0]) < parallel_process_per_task:
        parallel_process_per_task = len(super_params_list[0])
    if parallel_process_p_or_t == 't':
        def super_func(chunk):
            job_id = chunk[0][-1]
            chunk_new = []
            for params in chunk:
                chunk_new.append(params[:-1])
            stop_watch_dog = watch_dog(job_name, job_id,watch_dog_timeout_seconds=watch_dog_timeout_seconds)
            print('RUNNING_MARKER_1')
            def wrapper(p):
                tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
                if error_skip:
                    try:
                        result_i = func(p)
                    except Exception as e:
                        print('ERROR_MARKER_1')
                        print('Exception:',e)
                        print('Params:',p)
                        result_i = 'error with param:',p
                else:
                    result_i = func(p)
                update_i_global(job_name, pbar_update_freq)
                return result_i

            with ThreadPoolExecutor(max_workers=parallel_process_per_task) as Thread_:
                results = list(Thread_.map(wrapper, chunk_new))
            print('DONE_MARKER_1')
            stop_watch_dog.set()
            return results

    elif parallel_process_p_or_t == 'p':
        def super_func(chunk):
            job_id = chunk[0][-1]
            chunk_new = []
            for params in chunk:
                chunk_new.append(params[:-1])
            # stop_watch_dog = watch_dog(job_name, job_id, watch_dog_timeout_seconds=watch_dog_timeout_seconds)
            p_wd, stop_watch_dog = watch_dog_p(job_name, job_id, watch_dog_timeout_seconds=watch_dog_timeout_seconds)
            print('RUNNING_MARKER_1')
            def wrapper(p):
                # try:
                tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
                if error_skip:
                    try:
                        result_i = func(p)
                    except Exception as e:
                        print('ERROR_MARKER_1')
                        print('Exception:', e)
                        print('Params:', p)
                        result_i = 'error with param:', p
                else:
                    result_i = func(p)
                update_i_global(job_name, pbar_update_freq)
                return result_i

            pool = ProcessPool(nodes=parallel_process_per_task)
            results = pool.map(wrapper, chunk_new)
            pool.close()
            pool.join()
            pool.clear()
            pool.terminate()
            print('DONE_MARKER_1')
            stop_watch_dog.set()
            p_wd.join()
            return results
        pass
    else:
        raise ValueError("parallel_process_p_or_t must be 'p' for multiprocessing or 't' for threading")

    final_params_list = super_params_list
    final_func = super_func

    if os.path.exists(log_folder):
        for f in os.listdir(log_folder):
            if f.startswith('.'):
                continue
            fpath = os.path.join(log_folder, f)
            if os.path.isfile(fpath):
                os.remove(fpath)
            else:
                shutil.rmtree(fpath)

    mkdir(log_folder, force=True)
    # log_folder_obj = Path(log_folder)
    # fail_log_folder = log_folder_obj / 'failed_tasks'
    # mkdir(fail_log_folder, force=True)
    if slurm_array_parallelism == -1:
        slurm_array_parallelism = job_number_limit
    if slurm_array_parallelism > job_number_limit:
        slurm_array_parallelism = job_number_limit
    Concurrent_Processes = parallel_process_per_task * slurm_array_parallelism
    if Concurrent_Processes > len(params_list):
        Concurrent_Processes = len(params_list)
    Total_Cores_Used = cpus_per_task * slurm_array_parallelism
    # if Total_Cores_Used > len(params_list):
    #     Total_Cores_Used = len(params_list)
    timeout_obj = datetime.timedelta(minutes=timeout_min)
    # print(timeout_obj)
    # exit()
    if is_skip_unavailable_nodes:
        exclude_nodes_str = get_unavailable_nodes()
    else:
        exclude_nodes_str = None
    final_exclude_nodes = add_node_list(exclude_nodes_str, exclude_nodes)
    final_exclude_nodes_list, final_exclude_nodes_list_str = parse_node_list(final_exclude_nodes)
    final_exclude_nodes = simplify_nodes_list(final_exclude_nodes_list)
    info = OrderedDict({
        "Total Cores Used": Total_Cores_Used,
        "Concurrent Processes": Concurrent_Processes,
        "Parallel Process Per Task": parallel_process_per_task,
        "Total Loop Length": len(params_list),
        "Number of Jobs": len(final_params_list),
        "Memory for Each Job(GB)": mem_gb,
        "Partition": slurm_partition,
        "Time Out": timeout_obj,
        "Job Name": job_name,
        "Log Folder": log_folder,
        "Watch Dog Time": watch_dog_timeout_seconds,
    })
    info['Exclude Nodes'] = final_exclude_nodes
    pretty_table_print(info)
    if not skip_confirmation:
        input('\33[7m' + "PRESS ENTER TO SUBMIT..." + '\33[0m')
    print('submiting...')


    executor = submitit.AutoExecutor(folder=log_folder)
    executor.update_parameters(
        slurm_job_name=job_name,
        cpus_per_task=cpus_per_task,
        mem_gb=mem_gb,
        timeout_min=timeout_min,
        slurm_array_parallelism=slurm_array_parallelism,
        slurm_partition=slurm_partition,
        slurm_exclude=final_exclude_nodes,
        slurm_nodelist=specific_nodes,
    )

    jobs = executor.map_array(final_func, final_params_list)
    print('jobs submitted, job ids:', jobs[0].job_id)
    report_start_time(job_name)
    report_req_tasks(job_name,job_number_limit)
    info['Job id'] = str(jobs[0].job_id).replace('_0','')
    info['Exclude Nodes'] = final_exclude_nodes
    report_submit_info(job_name, info)

    progress_bar_monitoring(job_name,log_folder)



def shasum_string(input_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_string.encode('utf-8'))
    hex_dig = sha256_hash.hexdigest()
    return hex_dig

def log_error_info(log_folder,err_info,fail_param_list):
    all_id_str = ''
    for param in fail_param_list:
        id_val = id(param)
        id_val_str = str(id_val) + ' '
        all_id_str += id_val_str
    all_id_str_shasum = shasum_string(all_id_str)
    all_id_str_shasum_short = all_id_str_shasum[:10]
    log_folder_obj = Path(log_folder)
    fail_log_folder = log_folder_obj / 'failed_tasks'
    fail_log_file = fail_log_folder / f"{all_id_str_shasum_short}.pkl"
    now = datetime.datetime.now()
    err_info_dict = {
        'time': now.strftime("%Y-%m-%d %H:%M:%S"),
        'params': fail_param_list,
        'err_info': err_info
    }
    with open(fail_log_file, 'wb') as fw:
        pickle.dump(err_info_dict, fw)
    pass



def pretty_table_print(info):
    max_key_len = max(len(k) for k in info)

    print("\n=== Job Summary ===")
    for k, v in info.items():
        print(f"{k + ':':<{max_key_len + 2}} {v}")
    print("=" * (max_key_len + 15))
    pass


class HPC_redis:

    def __init__(self):
        self.r = self.conn_redis()
        pass

    def conn_redis(self):
        redis_conf = Path.home() / '.config' / 'redis' / 'redis.conf'
        with open(redis_conf) as f:
            redis_conf = f.readlines()
            host = redis_conf[0].strip()
            port = int(redis_conf[1].strip())
            passwd = redis_conf[2].strip()

        r = redis.Redis(
            host=host,
            port=port,
            password=passwd,
        )
        # print(f"Connected to Redis at {host}:{port}")
        return r

    def hit_redis(self,job_name,amount=1):
        self.r.hincrby(name=job_name,key=job_name,amount=amount)

    def set_total_num(self,job_name,total_job:int):
        # r.delete(job_name+'_total')
        # r.hincrby(name=job_name, key=task_name+'_total', amount=total_job)
        self.r.hset(job_name, 'total', str(total_job))

    def delete_job(self,job_name):
        self.r.delete(job_name)
        pass

    def query_redis(self,job_name):
        while True:
            print(self.r.hgetall(job_name))
            time.sleep(1)

class Check_logs:
    def __init__(self,log_folder):
        self.log_folder = log_folder
        if not os.path.isdir(log_folder):
            home = Path.home()
            self.log_folder = home / '.config' / 'lytools_HPC_log' / log_folder
        pass

    def read_err_files(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        err_count = 0
        for f in listdir(log_folder):
            if not f.endswith(".err"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                err_content = fr.read()
                if len(err_content) != 0:
                    print('==============')
                    print(f"Error in file: {f}")
                    print(err_content)
                    err_count += 1
        print('#################')
        print(f'Total error logs: {err_count}')
        print('#################')
        return err_count

    def read_err_files_inside_progress_bar(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        for f in listdir(log_folder):
            if not f.endswith(".err"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                err_content = fr.read()
                if len(err_content) != 0:
                    print('==============')
                    print(f"Error in file: {f}")
                    print(err_content)
                    time.sleep(5)
        pass

    def read_err_files_single_job(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        err_count = 0
        for f in listdir(log_folder):
            if not f.endswith(".err"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                err_content = fr.read()
                if len(err_content) != 0:
                    print('==============')
                    print(f"Error in file: {f}")
                    print(err_content)
                    err_count += 1
        print(f'Total error logs: {err_count}')
        pass

    def read_out_files(self,skip_success_file=True):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        success_count = 0
        for f in listdir(log_folder):
            if not f.endswith(".out"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                log_content = fr.read()
                # if not skip_success_file:
                #     print(log_content)
                #     print('---out log---')
                if skip_success_file:
                    if 'DONE_MARKER_1' in log_content:
                        success_count += 1
                        count += 1
                        continue
                print(log_content)
                print('---out log---')
                count += 1
        print('#################')
        print(f'Total out files: {count}, success_count: {success_count}')
        print('#################')
        return count, success_count

    def yield_out_files(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        success_count = 0
        for f in listdir(log_folder):
            if not f.endswith(".out"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                log_content = fr.read()
                yield log_content

    def read_out_files_single_job(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        for f in listdir(log_folder):
            if not f.endswith(".out"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                log_content = fr.read()
                print(log_content)
                print('------------')
                return log_content
        return ' '


    def read_result_files(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        for f in listdir(log_folder):
            if not f.endswith("_result.pkl"):
                continue
            fpath = log_folder / f
            fr = open(fpath, 'rb')
            dic = pickle.load(fr, encoding="latin1")
            # print(fpath)
            # content = pickle.load(fpath)
            print(dic)
            print('------------')
            count += 1
        print(f'Total files: {count}')
        pass

    def read_submit_files(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        for f in listdir(log_folder):
            if not f.endswith("_submitted.pkl"):
                continue
            fpath = log_folder / f
            # print(fpath)
            content = pickle.load(open(fpath, 'rb'))
            print(content)
            print('------------')
            count += 1
        print(f'Total files: {count}')
        pass

    def get_err_params(self):
        fdir = Path(self.log_folder)

        err_id_list = []
        for f in listdir(fdir):
            if not f.endswith('_log.out'):
                continue
            fpath = fdir / f
            with open(fpath, 'r') as fr:
                log_content = fr.read()
                # print(log_content)
                if 'submitit ERROR' in log_content:
                    err_id = f.replace('_0_log.out','')
                    err_id_list.append(err_id)
                    print('========')
                if not 'Job completed successfully' in log_content:
                    err_id = f.replace('_0_log.out', '')
                    err_id_list.append(err_id)
                    print('========')

        for f in listdir(fdir):
            if not f.endswith('_log.err'):
                continue
            fpath = fdir / f
            with open(fpath, 'r') as fr:
                log_content = fr.read()
                print(log_content)
                if 'submitit ERROR' in log_content:
                    err_id = f.replace('_0_log.err', '')
                    err_id_list.append(err_id)
        err_id_list = list(set(err_id_list))
        args_list = []
        for err_id in err_id_list:
            params_f = fdir / f'{err_id}_submitted.pkl'
            with open(params_f, 'rb') as fr:
                submit_info_obj = pickle.load(fr)
                args = submit_info_obj.args[0]
                args_list.append(args)
        return args_list

    def watch(self,timesleep=5,skip_success_file=True):
        while True:
            os.system('clear')
            self.read_out_files(skip_success_file=skip_success_file)
            self.read_err_files()
            submited_count, success_count, err_count, log_count = self.get_alive_jobs()
            print(f"Success tasks: {success_count}/{log_count}")
            print(f'Running tasks: {submited_count-success_count-err_count}')
            time.sleep(timesleep)

    def get_alive_jobs(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        submited_count = 0
        success_count = 0
        log_count = 0
        for f in listdir(log_folder):
            if not f.endswith(".out"):
                continue
            fpath = log_folder / f
            log_count += 1
            with open(fpath) as fr:
                log_content = fr.read()
                # if 'Exiting after successful completion' in log_content:
                #     success_count += 1
                if 'RUNNING_MARKER_1' in log_content:
                    submited_count += 1
                if 'DONE_MARKER_1' in log_content:
                    success_count += 1

        err_count = 0
        for f in listdir(log_folder):
            if not f.endswith(".err"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                err_content = fr.read()
                if 'submitit ERROR' in err_content:
                    err_count += 1
                    continue
                if 'CANCELLED' in err_content:
                    err_count += 1
                    continue
                if 'Terminated' in err_content:
                    err_count += 1
                    continue
                if 'SIGTERM' in err_content:
                    err_count += 1
                    continue
                if 'Traceback' in err_content:
                    err_count += 1
                    continue
        # alive_count = log_count - success_count - err_count
        return submited_count, success_count, err_count, log_count


def init_job(job_name,param_list):
    total_job = len(param_list)
    r = HPC_redis().conn_redis()
    job_name_watchdog = f"watchdog:{job_name}"
    job_name_submit_info = f"watchdog:{job_name}_submit_info"
    job_name_watch_dog_req_tasks = f"watchdog:{job_name}_req_tasks"
    job_name_watch_dog_start_time = f"watchdog:{job_name}_start_time"

    r.delete(job_name)
    r.delete(job_name_watchdog)
    r.delete(job_name_submit_info)
    r.delete(job_name_watch_dog_req_tasks)
    r.delete(job_name_watch_dog_start_time)

    r.hset(job_name, 'total', str(total_job))
    r.hset(job_name, 'step', str(0))


def progress_bar_monitoring(job_name,log_folder=None):
    if log_folder is None:
        log_folder = job_name
    job_name_watch_dog = f"watchdog:{job_name}"
    job_name_str = job_name
    if len(job_name) > 10:
        job_name_str = '+'+job_name[-10:]
    hpc_redis = HPC_redis()
    info_overall=hpc_redis.r.hgetall(job_name)
    total = info_overall.get(b'total')
    total_int = int(total)
    watchdog_status,watch_dog_timeout_seconds,watchdog_last_seen = check_watchdog(job_name_watch_dog, job_id=0)

    job_name_watch_dog_start_time = f"{job_name_watch_dog}_start_time"
    job_name_watch_dog_start_time_info = hpc_redis.r.hgetall(job_name_watch_dog_start_time)
    watchdog_start_time = job_name_watch_dog_start_time_info.get(b'start_time')

    job_name_watch_dog_req_tasks = f"watchdog:{job_name}_req_tasks"
    job_name_watch_dog_req_tasks_info = hpc_redis.r.hgetall(job_name_watch_dog_req_tasks)
    req_count = job_name_watch_dog_req_tasks_info.get(b'req_tasks')
    if req_count:
        req_count = int(req_count)
    else:
        req_count = 0
    if watchdog_start_time:
        watchdog_start_time = float(watchdog_start_time)
    else:
        watchdog_start_time = time.time()
    submit_info_dict = get_submit_info(job_name)
    watch_dog_timeout_seconds = submit_info_dict['Watch Dog Time']
    with Progress(
        TextColumn("{task.fields[watchdog_status]}[black]|{task.fields[alive]}[black]|{task.fields[error]}[black]|{task.fields[requested]}[black]|{task.fields[started]}[black]|{task.fields[success]}[black]|{task.fields[concurrency]}"),
        TextColumn("{task.description}"),
        BarColumn(bar_width=8),
        TextColumn("{task.completed}/{task.total} {task.percentage:>3.0f}%"),
        TextColumn("{task.fields[TimeElapsed]}"),
        IterSpeedColumn(),
        # TransferSpeedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_success = progress.add_task(f"{job_name_str}", total=total_int,
                                         requested=req_count,
                                         alive=0,
                                         success=0,
                                         error=0,
                                         started=0,
                                         watchdog_status=0,
                                         concurrency=0,
                                         TimeElapsed='-:--:--',
                                         )
        progress.console.print(f"[bold green]HeartBeats:{watch_dog_timeout_seconds}s[black]|[bold yellow]Running[black]|[bold red]Error[black]|[bold black]Requested|[bold blue]Started[black]|[bold green]Success[black]|[bold yellow]Concurrency")
        progress.update(task_success, alive=f"[bold yellow]{0}")
        progress.update(task_success, error=f"[bold red]{0}")
        progress.update(task_success, requested=f"[bold black]{req_count}")
        progress.update(task_success, started=f"[bold blue]{0}")
        progress.update(task_success, success=f"[bold green]{0}")
        progress.update(task_success, watchdog_status=f"[bold yellow]Init")
        progress.update(task_success, TimeElapsed='[red]-:--:--')
        progress.update(task_success, concurrency=f'[bold yellow]{0}')

        task_is_done = False
        TimeElapsed = '-:--:--'
        while True:
            info_success = hpc_redis.r.hgetall(job_name)
            step_success = info_success.get(b'step')
            step_int_success = int(step_success)

            # watch dog
            watchdog_status_alive = 0
            watchdog_last_seen_list = []
            for j_id in range(req_count):
                watchdog_status,_,watchdog_last_seen = check_watchdog(job_name_watch_dog, job_id=j_id)
                if watchdog_status == 'ALIVE':
                    watchdog_status_alive += 1
                if watchdog_last_seen is None:
                    continue
                watchdog_last_seen_list.append(watchdog_last_seen)
            if len(watchdog_last_seen_list) == 0:
                watchdog_last_seen_most_recent = time.time()
            else:
                watchdog_last_seen_most_recent = max(watchdog_last_seen_list)

            progress.update(task_success, completed=step_int_success)
            if not task_is_done:
                progress.update(task_success, TimeElapsed=f"[red]{TimeElapsed}")
            else:
                final_TimeElapsed = datetime.timedelta(seconds=int(watchdog_last_seen_most_recent - watchdog_start_time + 2))
                progress.update(task_success, TimeElapsed=f"[red]{final_TimeElapsed}")
                break
            now = time.time()
            time_delta = now - watchdog_start_time
            time_delta = int(time_delta)
            TimeElapsed = datetime.timedelta(seconds=time_delta)
            submited_count, success_count, err_count, log_count = Check_logs(log_folder).get_alive_jobs()

            if not log_count == 0:
                alive_jobs = submited_count - success_count - err_count
                concurrency = min(alive_jobs, watchdog_status_alive) * int(
                    submit_info_dict['Parallel Process Per Task'])
                if concurrency < 0:
                    concurrency = 0
                if concurrency > total_int:
                    concurrency = total_int
                progress.update(task_success, alive=f"[bold yellow]{0 if alive_jobs<0 else alive_jobs}")
                progress.update(task_success, error=f"[bold red]{err_count}")
                progress.update(task_success, requested=f"[bold black]{req_count}")
                progress.update(task_success, started=f"[bold blue]{submited_count}")
                progress.update(task_success, success=f"[bold green]{success_count}")
                progress.update(task_success, watchdog_status=f"[bold green]{watchdog_status_alive}")
                progress.update(task_success, concurrency=f"[bold yellow]{concurrency}")

                if alive_jobs <= 0 and submited_count==req_count:
                    time.sleep(1)
                    info_success = hpc_redis.r.hgetall(job_name)
                    step_success = info_success.get(b'step')
                    step_int_success = int(step_success)
                    progress.update(task_success, completed=step_int_success)
                    if err_count > 0:
                        progress.update(task_success, watchdog_status=f"[bold red]Error")
                    else:
                        progress.update(task_success, watchdog_status=f"[bold green]Done")
                    task_is_done = True
                    # break
                if err_count+success_count==req_count:
                    time.sleep(1)
                    info_success = hpc_redis.r.hgetall(job_name)
                    step_success = info_success.get(b'step')
                    step_int_success = int(step_success)
                    progress.update(task_success, completed=step_int_success)
                    if err_count > 0:
                        progress.update(task_success, watchdog_status=f"[bold red]Error")
                    else:
                        progress.update(task_success, watchdog_status=f"[bold green]Done")
                    # time.sleep(1)
                    task_is_done = True
                    # break
            if step_int_success >= total_int:
                time.sleep(1)
                info_success = hpc_redis.r.hgetall(job_name)
                step_success = info_success.get(b'step')
                step_int_success = int(step_success)
                progress.update(task_success, completed=step_int_success)
                progress.update(task_success, watchdog_status=f"[bold green]Done")
                task_is_done = True
                # break

            time.sleep(2)

def is_iterable(obj):
    try:
        iter(obj)
        return True
    except TypeError:
        return False


def memory_estimate():
    process = psutil.Process(os.getpid())
    mem = process.memory_info()
    print("RSS (MB):", mem.rss / 1024 ** 2)
    print("VMS (MB):", mem.vms / 1024 ** 2)
    # pause
    input('\33[7m' + "PRESS ENTER TO CONTINUE." + '\33[0m')

def generate_random_string():
    now = time.time()
    random_int = random.randrange(0,100)
    hex_dig = shasum_string(str(now)+str(random_int))
    hex_dig = hex_dig[:8]
    return hex_dig


def get_unavailable_nodes():
    command = ["sinfo"]
    try:
        result = subprocess.run(command, capture_output=True, text=True)
    except:
        return None
    output = result.stdout
    result_lines = output.splitlines()
    NODELIST_unvailable = []
    for line in result_lines:
        PARTITION, AVAIL, TIMELIMIT, NODES, STATE, NODELIST = line.split()
        if not 'cn' in NODELIST:
            continue
        if 'comp' in STATE or 'alloc' in STATE or 'drain' in STATE:
            NODELIST_list, NODELIST_list_str = parse_node_list(NODELIST)
            NODELIST_unvailable.extend(NODELIST_list)
        if 'preempt' in PARTITION:
            NODELIST_list, NODELIST_list_str = parse_node_list(NODELIST)
            NODELIST_unvailable.extend(NODELIST_list)
    NODELIST_unvailable = list(set(NODELIST_unvailable))
    NODELIST_unvailable.sort()
    NODELIST_unvailable_str = 'cn' + str(NODELIST_unvailable)
    NODELIST_unvailable_str = NODELIST_unvailable_str.replace(' ', '')
    # print(NODELIST_unvailable_str)
    # exit()
    return NODELIST_unvailable_str

def parse_node_list(node_list_str):
    if node_list_str is None:
        return [], ''
    NODELIST = node_list_str.replace('cn', '')
    NODELIST_list = []
    if '[' in NODELIST:
        NODELIST = NODELIST.replace('[', '')
        NODELIST = NODELIST.replace(']', '')
        NODELIST_split = NODELIST.split(',')
        for nodes in NODELIST_split:
            if '-' in nodes:
                start_nodes, end_nodes = nodes.split('-')
                start_nodes = int(start_nodes)
                end_nodes = int(end_nodes)
                for node_i in range(start_nodes, end_nodes+1):
                    NODELIST_list.append(node_i)
            else:
                node_i = int(nodes)
                NODELIST_list.append(node_i)
    else:
        NODE = int(NODELIST)
        NODELIST_list.append(NODE)
    # print(NODELIST_unvailable)
    NODELIST_list = list(set(NODELIST_list))
    NODELIST_list.sort()
    NODELIST_list_str = 'cn' + str(NODELIST_list)
    NODELIST_list_str = NODELIST_list_str.replace(' ', '')
    return NODELIST_list, NODELIST_list_str

def add_node_list(*args):
    all_NODELIST_list = []
    for arg in args:
        NODELIST_list, _ = parse_node_list(arg)
        for node in NODELIST_list:
            all_NODELIST_list.append(node)
    all_NODELIST_list = list(set(all_NODELIST_list))
    all_NODELIST_list.sort()
    if len(all_NODELIST_list) == 0:
        return None
    all_NODELIST_list_str = 'cn' + str(all_NODELIST_list)
    all_NODELIST_list_str = all_NODELIST_list_str.replace(' ', '')
    return all_NODELIST_list_str

def check_nodes_list(nodes_list_str:str):
    cn_list = nodes_list_str.split('cn')
    if len(cn_list) != 2:
        raise ValueError('incorrect format of nodes_list_str, for example: "cn100" or cn[100,]')

def simplify_nodes_list(nodes_list:list):
    ranges = group_consecutive_vals(nodes_list)
    node_str_all = ''
    for r in ranges:
        if len(r) > 2:
            node_str = f'{r[0]}-{r[-1]},'
            node_str_all += node_str
        else:
            for node in r:
                node_str_all += str(node) + ','
    node_str_all = node_str_all[:-1]
    node_str_all = 'cn[' + node_str_all + ']'
    return node_str_all


def group_consecutive_vals(in_list):
    ranges = []
    for _, group in groupby(enumerate(in_list), lambda index_item: index_item[0] - index_item[1]):
        group = list(map(itemgetter(1), group))
        if len(group) > 1:
            ranges.append(list(range(group[0], group[-1] + 1)))
        else:
            ranges.append([group[0]])
    return ranges


def main():
    raise UserWarning('Do not run this script')

if __name__ == '__main__':

    main()
