from multiprocessing import Process, Queue
import traceback
from . import validation
import sys
import time
import numpy as np


class Job_wrapper:
    def __init__(self, job_details, work_stopper=False):
        self.job_details = job_details
        self.had_error = False
        self.work_stopper = work_stopper
        self.total_time=0
        self.job_results=None
        
    def start_clock(self):
        self.start_time = time.time()
    
    def stop_clock(self):
        self.total_time = round(time.time() - self.start_time,2)
        
    def flag_error(self):
        self.had_error = True
        
    def load_result(self, job_result):
        self.job_result = job_result

class worker_wrapper_pythonmp:
    def __init__(self, Worker_class, job_q, results_q):
        self.worker_class = Worker_class()
        self.worker_class.setup()
        self.process = Process(target=self.run_job, args=(job_q, results_q))
        self.running=True
        self.process.start()
        
    def run_job(self, job_q, results_q):
        while True:
            job = job_q.get()
            
            if job.work_stopper:
                results_q.put(job)
                break
            
            print('Running job')
            try:
                job.start_clock()
                job.load_result(self.worker_class.run_job(job.job_details))
                job.stop_clock()
                results_q.put(job)
            except Exception as e:
                full_traceback = traceback.format_exc()
                print('Failed job on \n' + str(full_traceback))
                job.load_result(self.worker_class.get_failed_job_result(job.job_details))
                job.flag_error()
                print('Failed job caught, moving on.')
                results_q.put(job)

            sys.stdout.flush()
            
    
def boss_wrapper_pythonmp(Boss_class, Worker_class, n_procs):
    job_queue = Queue()
    results_queue = Queue()

    workers = [worker_wrapper_pythonmp(Worker_class, job_queue, results_queue) for i in range(n_procs)]
    
    boss_class = Boss_class()
    boss_class.setup()
    boss_class.set_total_jobs()
    
    job_timings=[]
    # Dole out first round of jobs
    for i in range(n_procs):
        if boss_class.jobs_available:
            job = Job_wrapper(job_details = boss_class.get_next_job())
            job_queue.put(job)
        else:
            break
        
    total_jobs = boss_class.total_jobs
    jobs_completed = 0
    
    # Process and dole out jobs until none are left
    while boss_class.jobs_available():
        job = results_queue.get()
        if job.had_error:
            boss_class.process_failed_job(job.job_result)
            job_timings.append(1)
        else:
            boss_class.process_job_result(job.job_result)
            job_timings.append(job.total_time)
        
        
        job_queue.put(Job_wrapper(job_details=boss_class.get_next_job()))
        jobs_completed += 1
        print('Completed job {n} of {n_total} in {s} seconds'.format(n=jobs_completed,
                                                                     n_total=total_jobs,
                                                                     s=job_timings[-1]))
        time_remaining = round(np.mean(job_timings)/60/60, 2) * (total_jobs - jobs_completed) / n_procs
        print('Estimated time remaining: {h} hours'.format(h=time_remaining))
        sys.stdout.flush()
        
    # Shutdown signal for all workers
    stop_job = Job_wrapper(job_details=None, work_stopper=True)
    for i in range(n_procs):
        job_queue.put(stop_job)

    # Collect last jobs and track finished workers
    stopped_workers = 0
    while stopped_workers < n_procs:
        job = results_queue.get()
        if job.work_stopper:
            stopped_workers+=1
        elif job.had_error:
            boss_class.process_failed_job(job.job_result)
        else:
            boss_class.process_job_result(job.job_result)
    
    boss_class.process_all_results()

def run_pythonMP(boss_class, worker_class, n_procs):
    validation.validate_boss_class(boss_class)
    validation.validate_worker_class(worker_class)
    
    print('Starting jobs with {n} processors'.format(n=n_procs))
    boss_wrapper_pythonmp(boss_class, worker_class, n_procs)
