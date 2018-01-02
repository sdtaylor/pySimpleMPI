import time
from pySimpleMPI.framework import run_MPI

class ExampleWorker:
    def __init__(self):
        pass

    def get_failed_job_result(self):
        pass

    def setup(self):
        print('seting things up')
    
    def run_job(self, job_details):
        time.sleep(2)
        print('running job')
        return job_details
        
class ExampleBoss:
    def __init__(self):
        pass
    
    def setup(self):
        self.job_list=list(range(10))
        self.total_jobs = len(self.job_list)
    
    def set_total_jobs(self):
        self.total_jobs = len(self.job_list)

    def jobs_available(self):
        return len(self.job_list)>0
        
    def get_next_job(self):
        return self.job_list.pop()
    
    def process_job_result(self, result):
        pass
    
    def process_failed_job(self, result):
        pass

    def process_all_results(self):
        pass
        
if __name__ == "__main__":
    run_MPI(ExampleBoss(), ExampleWorker())
