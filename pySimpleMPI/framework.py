from . import validation

from mpi4py import MPI

work_tag=0
stop_tag=1

def worker(worker_class):
    comm = MPI.COMM_WORLD
    status = MPI.Status()
        
    worker_class.setup()
    
    while True:
        job_details = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag() == stop_tag: break

        job_results = worker_class.run_job(job_details)
        
        comm.send(obj=job_results, dest=0)

def boss(boss_class):
    comm = MPI.COMM_WORLD
    status = MPI.Status()
    num_workers = MPI.COMM_WORLD.Get_size()

    boss_class.setup()
    boss_class.set_total_jobs()
    
    #Dole out the first round of jobs to all workers
    for i in range(1, num_workers):
        if boss_class.jobs_available():
            next_job = boss_class.get_next_job()
        else:
            break
        comm.send(obj=next_job, dest=i, tag=work_tag)
    
    results=[]
    total_jobs = boss_class.total_jobs
    jobs_completed = 0
    #While there are new jobs to assign.
    #Collect results and assign new jobs as others are finished.
    while boss_class.jobs_available():
        job_result = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        results.append(job_result)
        next_job = boss_class.get_next_job()
        comm.send(obj=next_job, dest=status.Get_source(), tag=work_tag)
    
        jobs_completed+=1
        print('Completed job '+str(jobs_completed)+' of '+str(total_jobs))
        
    #Collect last jobs
    for i in range(1, num_workers):
        job_result = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
        results.append(job_result)
        
    #Shut down all workers
    for i in range(1, num_workers):
        comm.send(obj=None, dest=i, tag=stop_tag)
        
    boss_class.process_all_results(results)

def run_MPI(boss_class, worker_class):
    validation.validate_boss_class(boss_class)
    validation.validate_worker_class(worker_class)
    
    rank = MPI.COMM_WORLD.Get_rank()
    name = MPI.Get_processor_name()

    if rank == 0:
        print('boss '+str(rank)+' on '+str(name))
        boss(boss_class)
    else:
        print('worker '+str(rank)+' on '+str(name))
        worker(worker_class)