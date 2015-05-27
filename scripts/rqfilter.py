import sys, os
from redis import Redis
from rq import Connection, Queue, Worker
from rq.queue import FailedQueue

REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost' )
redis_conn = Redis(host=REDIS_HOST, password=REDIS_PASSWORD)
qfailed = FailedQueue(connection=redis_conn)

#print(dir(qfailed))

#action : can be one of requeue o, connection=redis_conn)
err_search = 'timeout'
action = 'requeue'
#action = 'cancel'

jobs_filtered = []
for job in qfailed.jobs:
    print(job.dump())
#    print(job.dump().keys())
    if err_search in job.dump()['exc_info']:
        jobs_filtered.append(job)
        job.timeout = 604800 #1week
        job.save()
        if action == 'requeue':
            result = qfailed.requeue(job.id)
            #q = Queue(job.dump()['origin'], connection=redis_conn)
            ##result = q.enqueue(job)
            print result
print('{} jobs matched {}'.format(len(jobs_filtered), err_search))
