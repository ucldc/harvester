from rq import Queue, requeue_job
from rq.queue import FailedQueue
from redis import Redis
import os

conn_redis = Redis(host=os.environ['REDIS_HOST'],
        password=os.environ['REDIS_PASSWORD'])
qfailed=FailedQueue(connection=conn_redis)

fail_registry_api = []
fail_no_xtf_results = []
fail_timeout = []
fail_other = []

###for job in Queue(connection=conn_redis).jobs:
###    job.timeout = 2*job.timeout
###    job.save()
###    print job, job.timeout


for job in qfailed.jobs:
    "ConnectionError: HTTPSConnectionPool(host='registry.cdlib.org'"
    if "HTTPSConnectionPool(host='registry.cdlib.org'" in job.exc_info:
        fail_registry_api.append(job)
    elif "ValueError: http://dsc.cdlib.org/search" in job.exc_info:
        fail_no_xtf_results.append(job)
    elif "Job exceeded maximum timeout value" in job.exc_info:
        fail_timeout.append(job)
    else:
        fail_other.append(job)

print(80*'=')
print('Registry connection fails:{0}, XTF No results:{1}, Timeout:{2} Other:{3}'.format(
                len(fail_registry_api),
                len(fail_no_xtf_results),
                len(fail_timeout),
                len(fail_other)
                )
     )
print(80*'=')
print('\n\n')

#for job in fail_other:
#    print job.exc_info

for job in fail_no_xtf_results:
    print('ValueError job: {}\n\n'.format(job))#, job.exc_info))
    #job.cancel()

for job in fail_other:
    try:
        if "26094" in job.args[1]:
            print('LAPL MARC exc_info:{}'.format(job.exc_info))
    except IndexError:
        pass

for job in fail_registry_api:
    print('Job to requeue:{}'.format(job))
    #requeue_job(job.get_id(), connection=conn_redis)

for job in fail_timeout:
   print('TIMEOUT Before:{} {}'.format(job.timeout, job))
   job.timeout = 2*job.timeout
   job.save()
   print('TIMEOUT after:{} {}'.format(job.timeout, job))
   #requeue_job(job.get_id(), connection=conn_redis)


n_run_ingest = n_img_harv = n_no_shown_by = 0
for job in fail_other:
    if '__getitem__' in job.exc_info:
        n_no_shown_by += 1
        print('{} {}'.format(job, job.exc_info))
        job.cancel()
    if 'Image_harvest' in job.get_call_string():
        n_img_harv += 1
        #print('{} {}'.format(job, job.exc_info))
    if 'run_ingest' in job.get_call_string():
        n_run_ingest += 1
        job.timeout = 2*job.timeout
        job.save()
        #print('REQUEUE: {}\nTIMEOUT: {}'.format(job, job.timeout))
        #requeue_job(job.get_id(), connection=conn_redis)
    else:
        print(job.get_call_string())
print('RIngest:{} IMG:{} NOSHOWNBY:{}'.format(n_run_ingest, n_img_harv, n_no_shown_by))

#print('id {} get_call_string {}, args {}'.format(job.get_id(), job.get_call_string(), job.args))
dir_job_listing= '''
['__class__', '__delattr__', '__dict__', '__doc__', '__eq__', '__format__', '__getattribute__', '__hash__', '__init__', '__module__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_args', '_data', '_dependency_id', '_func_name', '_get_status', '_id', '_instance', '_kwargs', '_result', '_set_status', '_status', '_unpickle_data', 'args', 'cancel', 'cleanup', 'connection', 'create', 'created_at', 'data', 'delete', 'dependency', 'dependents_key', 'dependents_key_for', 'description', 'dump', 'ended_at', 'enqueued_at', 'exc_info', 'exists', 'fetch', 'func', 'func_name', 'get_call_string', 'get_id', 'get_status', 'get_ttl', 'id', 'instance', 'is_failed', 'is_finished', 'is_queued', 'is_started', 'key', 'key_for', 'kwargs', 'meta', 'origin', 'perform', 'refresh', 'register_dependency', 'result', 'result_ttl', 'return_value', 'save', 'set_id', 'set_status', 'status', 'timeout']
'''
