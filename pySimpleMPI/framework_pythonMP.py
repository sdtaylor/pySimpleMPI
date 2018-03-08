from multiprocessing import Process, Queue
import traceback
from . import validation
import sys

stop_tag = 1

def had_error(result_object):
    return isinstance(result_object, dict) and 'pySimpleMPI_error' in result_object

def unpack_error(result_object):
    return result_object['pySimpleMPI_error']

def pack_error(result_object):
    return {'pySimpleMPI_error':result_object}

class worker_wrapper_pythonmp:
    def __init__(self, Worker_class, job_q, results_q):
        self.worker_class = Worker_class()
        self.worker_class.setup()
        self.process = Process(target=self.run_job, args=(job_q, results_q))
        self.running=True
        self.process.start()
        
    def run_job(self, job_q, results_q):
        while True:
            job_details = job_q.get()
            
            if job_details == stop_tag:
                break
            
            print('Running job')
            try:
                job_results = self.worker_class.run_job(job_details)
                results_q.put(job_results)
            except Exception as e:
                full_traceback = traceback.format_exc()
                print('Failed job on \n' + str(full_traceback))
                job_results = self.worker_class.get_failed_job_result(job_details)
                print('Failed job caught, moving on.')
                results_q.put(pack_error(job_results))

            sys.stdout.flush()
            
    def join(self):
        self.process.join()

    
def boss_wrapper_pythonmp(Boss_class, Worker_class, n_procs):
    job_queue = Queue()
    results_queue = Queue()

    workers = [worker_wrapper_pythonmp(Worker_class, job_queue, results_queue) for i in range(n_procs)]
    
    boss_class = Boss_class()
    boss_class.setup()
    boss_class.set_total_jobs()
    
    # Dole out first round of jobs
    for i in range(n_procs):
        if boss_class.jobs_available:
            job_queue.put(boss_class.get_next_job())
        else:
            break
        
    total_jobs = boss_class.total_jobs
    jobs_completed = 0
    
    # Process and dole out jobs until none are left
    while boss_class.jobs_available():
        job_result = results_queue.get()
        if had_error(job_result):
            boss_class.process_failed_job(unpack_error(job_result))
        else:
            boss_class.process_job_result(job_result)
        
        job_queue.put(boss_class.get_next_job())
        jobs_completed += 1
        print('Completed job '+str(jobs_completed)+' of '+str(total_jobs))
        sys.stdout.flush()
        
    # Shutdown signal for all workers
    for i in range(n_procs):
        job_queue.put(stop_tag)
    
    # Wait for them all to finish
    for w in workers:
        w.join()
    
    # Collect last jobs
    while not results_queue.empty():
        job_result = results_queue.get()
        if had_error(job_result):
            boss_class.process_failed_job(unpack_error(job_result))
        else:
            boss_class.process_job_result(job_result)
    
    boss_class.process_all_results()

def run_pythonMP(boss_class, worker_class, n_procs):
    validation.validate_boss_class(boss_class)
    validation.validate_worker_class(worker_class)
    
    print('Starting jobs with {n} processors'.format(n=n_procs))
    boss_wrapper_pythonmp(boss_class, worker_class, n_procs)
