
def validate_worker_class(worker_class):
    required_methods=['setup','run_job','get_failed_job_result']
    available_methods = dir(worker_class)
    
    missing_methods = [m for m in required_methods if m not in available_methods]
    
    assert len(missing_methods)==0, 'Missing required worker methods: ' + str(missing_methods)

def validate_boss_class(boss_class):
    required_methods=['setup','jobs_available','get_next_job','set_total_jobs',
                      'process_job_result','process_failed_job', 'process_all_results']
    available_methods = dir(boss_class)
    
    missing_methods = [m for m in required_methods if m not in available_methods]
    
    assert len(missing_methods)==0, 'Missing required boss methods: ' + str(missing_methods)
    