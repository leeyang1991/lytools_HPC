from os.path import isdir

import submitit
from concurrent.futures import ThreadPoolExecutor
import redis
from threading import Lock

from pathos.multiprocessing import ProcessPool
import time
from rich.text import Text
import datetime
import hashlib
from pprint import pprint
import shutil

import os
from pathlib import Path
import pickle
from tqdm import tqdm
from functools import partialmethod

import psutil


from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
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
    chunk_size = total // n_jobs
    remainder = total % n_jobs

    chunks = []
    start = 0

    for i in range(n_jobs):
        extra = 1 if i < remainder else 0
        end = start + chunk_size + extra
        chunks.append(lst[start:end])
        start = end
    return chunks

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

class IterSpeedColumn(ProgressColumn):

    def render(self, task):
        # if task.finished:
        #     return Text(f"{task.speed:.2f} it/s", style="green")

        speed = task.speed

        if speed is None:
            return Text("-- it/s", style="dim")

        return Text(f"{speed:.2f} it/s", style="cyan")


def sumbit_jobs_array(func,params_list,log_folder,job_name,
                        job_number_limit=10,
                        parallel_process_per_task=10,
                        slurm_array_parallelism=20,
                        parallel_process_p_or_t='p',
                        cpus_per_task=1,
                        mem_gb=1,
                        timeout_min=5,
                        slurm_partition="general",
                        exclude_nodes=None,
                        specific_nodes=None,
                        pbar_update_freq=1,
                        skip_confirmation=False,
                        **kwargs
                      ):
    '''
    :param func: the kernel function to run, should take one argument, e.g. func(params)
    :param params_list: list of tuples [params1, params2, ...]
    :param log_folder: slurm log_folder
    :param job_name: slurm job_name
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
    :param kwargs: other parameters for submitit.AutoExecutor
    '''
    if not is_iterable(params_list):
        raise TypeError("params_list must be iterable")
    # job_name_failed = job_name + '__FAILED__'
    init_job(job_name, params_list)
    # init_job(job_name_failed, params_list)
    # print(params_list)
    # exit()
    if len(params_list) == 0:
        raise ValueError("params_list is empty")
    if len(params_list) > job_number_limit:
        super_params_list = split_into_n_jobs(params_list,job_number_limit)
        if parallel_process_p_or_t == 't':
            def super_func(chunk):
                def wrapper(p):
                    tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
                    func(p)
                    update_i_global(job_name, pbar_update_freq)

                with ThreadPoolExecutor(max_workers=parallel_process_per_task) as Thread_:
                    list(Thread_.map(wrapper, chunk))

        elif parallel_process_p_or_t == 'p':
            def super_func(chunk):
                # for p in chunk:
                #     func(p)

                def wrapper(p):
                    # try:
                    tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
                    func(p)
                    update_i_global(job_name, pbar_update_freq)

                pool = ProcessPool(nodes=parallel_process_per_task)
                pool.map(wrapper, chunk)
                pool.close()
                pool.join()
            pass
        else:
            raise ValueError("parallel_process_p_or_t must be 'p' for multiprocessing or 't' for threading")

        final_params_list = super_params_list
        final_func = super_func
    else:
        final_params_list = params_list
        def func_wapper(p):
            # try:
            tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
            func(p)
            update_i_global(job_name, pbar_update_freq)
            # except Exception as e:
                # log_error_info(log_folder, e, p)
                # r = get_redis()
                # r.hincrby(job_name_failed, "step", pbar_update_freq)

        final_func = func_wapper

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

    Concurrent_Processes = parallel_process_per_task * slurm_array_parallelism
    if Concurrent_Processes > len(final_params_list):
        Concurrent_Processes = len(final_params_list)
    Total_Cores_Used = cpus_per_task * slurm_array_parallelism
    if Total_Cores_Used > len(final_params_list):
        Total_Cores_Used = len(final_params_list)
    info = {
        "Total Cores Used": Total_Cores_Used,
        "Concurrent Processes": Concurrent_Processes,
        "Total Loop Length": len(params_list),
        "Number of Jobs": len(final_params_list),
        "Memory for Each Job(GB)": mem_gb,
        "Time out Minutes For Each Job": timeout_min,
        "Partition": slurm_partition,
    }
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
        slurm_exclude=exclude_nodes,
        slurm_nodelist=specific_nodes,
        **kwargs
    )
    # print(executor.parameters)
    # exit()

    jobs = executor.map_array(final_func, final_params_list)
    print('jobs submitted, job ids:', jobs[0].job_id)
    progress_bar_monitoring(job_name,log_folder)

def resumbit_failed_jobs_array(func,params_list,log_folder,job_name,mode,
                        job_number_limit=10,
                        parallel_process_per_task=10,
                        slurm_array_parallelism=20,
                        parallel_process_p_or_t='p',
                        cpus_per_task=1,
                        mem_gb=1,
                        timeout_min=5,
                        slurm_partition="general",
                        exclude_nodes=None,
                        specific_nodes=None,
                        pbar_update_freq=1,
                        **kwargs
                      ):
    '''
    :param func: the kernel function to run, should take one argument, e.g. func(params)
    :param params_list: list of tuples [params1, params2, ...]
    :param log_folder: slurm log_folder
    :param job_name: slurm job_name
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
    :param kwargs: other parameters for submitit.AutoExecutor
    '''
    if not is_iterable(params_list):
        raise TypeError("params_list must be iterable")
    log_folder_RESUBMIT = log_folder + '_RESUBMIT'
    if isdir(log_folder_RESUBMIT):
        failed_param_list = Check_logs(log_folder_RESUBMIT).get_err_params()
    else:
        failed_param_list = Check_logs(log_folder).get_err_params()
    if mode == 'all':
        new_params_list = failed_param_list + params_list
    elif mode == 'err':
        new_params_list = failed_param_list
        pass
    else:
        raise ValueError("mode must be one of 'err' or 'all'")
    new_params_list = list(set(new_params_list))

    # print(new_params_list)
    # exit()
    # job_name_failed = job_name + '__FAILED__'
    params_list = new_params_list
    pprint(params_list)
    print('failed params len:',len(params_list))
    init_job(job_name, params_list)
    # init_job(job_name_failed, params_list)
    # print(params_list)
    # exit()
    if len(params_list) == 0:
        raise ValueError("params_list is empty")
    if len(params_list) > job_number_limit:
        super_params_list = split_into_n_jobs(params_list,job_number_limit)
        if parallel_process_p_or_t == 't':
            def super_func(chunk):
                def wrapper(p):
                    tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
                    func(p)
                    update_i_global(job_name, pbar_update_freq)

                with ThreadPoolExecutor(max_workers=parallel_process_per_task) as Thread_:
                    list(Thread_.map(wrapper, chunk))

        elif parallel_process_p_or_t == 'p':
            def super_func(chunk):
                # for p in chunk:
                #     func(p)

                def wrapper(p):
                    # try:
                    tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
                    func(p)
                    update_i_global(job_name, pbar_update_freq)

                pool = ProcessPool(nodes=parallel_process_per_task)
                pool.map(wrapper, chunk)
                pool.close()
                pool.join()
            pass
        else:
            raise ValueError("parallel_process_p_or_t must be 'p' for multiprocessing or 't' for threading")

        final_params_list = super_params_list
        final_func = super_func
    else:
        final_params_list = params_list
        def func_wapper(p):
            # try:
            tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
            func(p)
            update_i_global(job_name, pbar_update_freq)
            # except Exception as e:
                # log_error_info(log_folder, e, p)
                # r = get_redis()
                # r.hincrby(job_name_failed, "step", pbar_update_freq)

        final_func = func_wapper


    # log_folder_obj = Path(log_folder)
    # fail_log_folder = log_folder_obj / 'failed_tasks'
    # mkdir(fail_log_folder, force=True)
    Concurrent_Processes = parallel_process_per_task * slurm_array_parallelism
    if Concurrent_Processes > len(final_params_list):
        Concurrent_Processes = len(final_params_list)
    Total_Cores_Used = cpus_per_task * slurm_array_parallelism
    if Total_Cores_Used > len(final_params_list):
        Total_Cores_Used = len(final_params_list)
    info = {
        "Total Cores Used": Total_Cores_Used,
        "Concurrent Processes": Concurrent_Processes,
        "Total Loop Length": len(params_list),
        "Number of Jobs": len(final_params_list),
        "Memory for Each Job(GB)": mem_gb,
        "Time out Minutes For Each Job": timeout_min,
        "Partition": slurm_partition,
    }
    print("\n=== re-submit===")
    pretty_table_print(info)
    input('\33[7m' + "PRESS ENTER TO SUBMIT..." + '\33[0m')
    if os.path.exists(log_folder_RESUBMIT):
        for f in os.listdir(log_folder_RESUBMIT):
            if f.startswith('.'):
                continue
            fpath = os.path.join(log_folder_RESUBMIT, f)
            if os.path.isfile(fpath):
                os.remove(fpath)
            else:
                shutil.rmtree(fpath)

    mkdir(log_folder_RESUBMIT, force=True)
    print('submiting...')
    executor = submitit.AutoExecutor(folder=log_folder_RESUBMIT)
    executor.update_parameters(
        slurm_job_name=job_name,
        cpus_per_task=cpus_per_task,
        mem_gb=mem_gb,
        timeout_min=timeout_min,
        slurm_array_parallelism=slurm_array_parallelism,
        slurm_partition=slurm_partition,
        slurm_exclude=exclude_nodes,
        slurm_nodelist=specific_nodes,
        **kwargs
    )
    # print(executor.parameters)
    # exit()

    jobs = executor.map_array(final_func, final_params_list)
    print('jobs submitted, job ids:', jobs[0].job_id)
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


def submit_single_job(func,params,log_folder,job_name,
                      cpus_per_task=1,
                      mem_gb=1,
                      timeout_min=5,
                      slurm_partition="general",
                      exclude_nodes=None,
                      specific_nodes=None,
                      ):
    if os.path.exists(log_folder):
        for f in os.listdir(log_folder):
            os.remove(os.path.join(log_folder, f))
    mkdir(log_folder, force=True)
    executor = submitit.AutoExecutor(folder=log_folder)
    executor.update_parameters(
        slurm_job_name=job_name,
        timeout_min=timeout_min,
        cpus_per_task=cpus_per_task,
        mem_gb=mem_gb,
        slurm_partition=slurm_partition,
        slurm_nodelist=specific_nodes,
        slurm_exclude=exclude_nodes,
    )
    def func_wrapper(*args, **kwargs):
        tqdm.__init__ = partialmethod(tqdm.__init__, disable=True)
        func(*args, **kwargs)
    job = executor.submit(func_wrapper, params)
    job_id = job.job_id
    print("job id:", job_id)

    start_time = datetime.datetime.now()
    while 1:
        Check_logs(log_folder).read_out_files_single_job()
        print('='*20)
        Check_logs(log_folder).read_err_files_single_job()
        now = datetime.datetime.now()
        delta = now - start_time
        print('Params:')
        pprint(params)
        print('Job name:',job_name)
        print('Job ID:',job_id)
        print('time elapsed:', delta)

        time.sleep(5)
        os.system('clear')


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
        pass

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

    def read_out_files(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        for f in listdir(log_folder):
            if not f.endswith(".out"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                log_content = fr.read()
                print(log_content)
                print('------------')
                count += 1
        print('#################')
        print(f'Total out files: {count}')
        print('#################')
        pass

    def read_out_files_single_job(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        for f in listdir(log_folder):
            if not f.endswith(".out"):
                continue
            fpath = log_folder / f
            # print(fpath)
            with open(fpath) as fr:
                log_content = fr.read()
                print(log_content)
                print('------------')
                count += 1
        pass


    def read_result_files(self):
        log_folder = self.log_folder
        log_folder = Path(log_folder)
        count = 0
        for f in listdir(log_folder):
            if not f.endswith("_result.pkl"):
                continue
            fpath = log_folder / f
            # print(fpath)
            content = pickle.load(open(fpath, 'rb'))
            print(content)
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

def init_job(job_name,param_list):
    total_job = len(param_list)
    r = HPC_redis().conn_redis()
    r.delete(job_name)
    r.hset(job_name, 'total', str(total_job))
    r.hset(job_name, 'step', str(0))


def progress_bar_monitoring(job_name,log_folder):
    # failed_job_name = job_name + '__FAILED__'
    job_name_str = job_name
    if len(job_name) > 20:
        job_name_str = '...'+job_name[-20:]
    hpc_redis = HPC_redis()
    info_overall=hpc_redis.r.hgetall(job_name)
    total = info_overall.get(b'total')
    total_int = int(total)
    # exit()
    with Progress(
        TextColumn("[bold yellow]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total} {task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        IterSpeedColumn(),
        TimeRemainingColumn(),
        # show_speed=True,
    ) as progress:
        task_success = progress.add_task(f"[bold green]{job_name_str}", total=total_int)
        # task_failed = progress.add_task(f"[bold red]{job_name}_FAILED", total=total_int)
        while True:
            info_success = hpc_redis.r.hgetall(job_name)
            step_success = info_success.get(b'step')
            step_int_success = int(step_success)

            # info_failed = hpc_redis.r.hgetall(failed_job_name)
            # step_failed = info_failed.get(b'step')
            # step_int_failed = int(step_failed)

            progress.update(task_success, completed=step_int_success)
            # progress.update(task_failed, completed=step_int_failed)

            if step_int_success >= total_int:
                break

            # if step_int_failed >= total_int:
            #     break
            Check_logs(log_folder).read_err_files_inside_progress_bar()

            time.sleep(1)

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

