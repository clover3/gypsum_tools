import subprocess
import os
import time
import logging
import time

logging.root.addHandler(logging.StreamHandler())
h = logging.FileHandler("util.log")
logger = logging.getLogger('gypsum_util')
logger.addHandler(h)


max_monitor = 3600 * 100


begin_wait_max = 60

titan1080 = "1080ti-short"
m40 = "m40-short"


class JobSubmitException(Exception):
    def __init__(self, value):
        self.value = value
    
    def __str__(self):
        return repr(self.value)

class JobNotFoundException(Exception):
    pass

def squeue():
    stream = os.popen('squeue | grep youngwoo')
    output = stream.read()
    lines = output.split("\n")
    return [l for l in lines if l.strip()]



def get_all_job_status():
    lines = squeue()
    columns = ['JOBID', 'PARTITION', 'NAME', 'USER', 'ST', 'TIME', 'NODES', 'NODELIST(REASON)']

    def parse_line(line):
        tokens = line.split()
        d = {}
        for i, col in enumerate(columns):
            d[col] = tokens[i]
        return d 
    
    return list([parse_line(l) for l in lines])


# job_id is string
def get_job_status(job_id):
    all_jobs = get_all_job_status()
    
    for job in all_jobs:
        if job["JOBID"] == job_id:
            return job
            
    raise JobNotFoundException(job_id)



def submit_job_raw(partition, sh_path):
    sh_cmd = "sbatch -p {} --gres=gpu:1 {}".format(partition, sh_path)
    p = subprocess.Popen(sh_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    return p.stdout.read()

def submit_job(partition, sh_path):
    success_heading = "Submitted batch job "
    output = submit_job_raw(partition, sh_path)
    #output = "Submitted batch job 5848546\n"
    if output.startswith(success_heading):
        job_id = output[len(success_heading):].strip()
    else:
        raise JobSubmitException(output)
    return job_id

MINUTE = 30

def sleep_minutes(n_minutes):
    real_sleep_time = n_minutes * MINUTE
    msg = "Sleeping {} mins ".format(n_minutes) 
    if MINUTE is not 60:
        msg += "(real sleep : {}sec)".format(real_sleep_time)

    logger.debug(msg)
    time.sleep(real_sleep_time)
    

def monitor_job_until_die(job_id):
    begin_time = None
    monitor_begin = time.time() 
    end_time = None
    result = ""
    logger.info("Monitoring job")
    while time.time() - monitor_begin < max_monitor:
        try:
            stat = get_job_status(job_id)
            if stat['ST'] == "R":
                if begin_time is None:
                    logger.info("Job started!")
                    begin_time = time.time()
                else:
                    logger.info("Job running...")
            
                # Job is running
                sleep_minutes(5)
            elif stat['ST'] == "PD":
                logger.info("Job pending!")
                sleep_minutes(2)
        except JobNotFoundException as e:
            if begin_time is not None:
                end_time = time.time()
                break
            elif time.time() - monitor_begin < begin_wait_max:
                logger.info("Job not visible")
                sleep_minutes(1)
            else:
                # Job is not appearing, maybe died so fast
                result = "job not seen"
                break 
        
    if begin_time is not None and end_time is not None: 
        run_time = end_time - begin_time
        result = "success"
    else:
        run_time = 0

    return run_time, result  

class Task:
    def __init__(partition, sh_path, max_run_time, completion_mark):
        self.partition = partition
        self.sh_path = sh_path
        self.max_run_time = max_run_time
        self.completion_mark = completion_mark

def run_long_job(partition, sh_path, max_run_time, prev_run_time=0):
    acc_run_time = prev_run_time
        
    while acc_run_time < max_run_time:
        job_id = submit_job(partition, sh_path)
        logger.info("Submitted job : {}".format(job_id))
        run_time, result = monitor_job_until_die(job_id)
        acc_run_time += run_time
        logger.info("Accumulated run time : {}".format(acc_run_time))

        if task_completion(completion_mark_path):

def continue_long_job(init_job_id, partition, sh_path, max_run_time):
    acc_run_time = 0
    run_time, result = monitor_job_until_die(init_job_id)
    acc_run_time += run_time
    run_long_job(partition, sh_path, max_run_time, acc_run_time)


def run_nli_w_dict():
    logger.setLevel(logging.DEBUG)
    sh_path = "/home/youngwookim/code/Chair/src/nli_w_dict.sh"
    max_run_time = 3600 * 40
    
    completion_mark = "/home/youngwookim/code/Chair/output/completion/nli_from_dict5"
    nli_w_dict = Task(m40, sh_path, max_run_time, completion_mark)
    run_long_job(nli_w_dict)

if __name__ == "__main__":
    run_nli_w_dict()


