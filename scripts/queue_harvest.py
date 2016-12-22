#! /bin/env python
# queue a job on the rq queue
import sys
import datetime
import time
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from rq import Queue

from harvester.config import config
from harvester.config import RQ_Q_LIST

ID_EC2_INGEST = ''
ID_EC2_SOLR_BUILD = ''
TIMEOUT = 600  # for machine start
JOB_TIMEOUT = 600  # for jobs


def get_redis_connection(redis_host, redis_port, redis_pswd, redis_timeout=10):
    return Redis(host=redis_host, port=redis_port, password=redis_pswd,
                 socket_connect_timeout=redis_timeout)

def check_redis_queue(redis_host, redis_port, redis_pswd):
    '''Check if the redis host db is up and running'''
    r = get_redis_connection(redis_host, redis_port, redis_pswd)
    try:
        return r.ping()
    except RedisConnectionError:
        return False

def def_args():
    '''For now only the required email for the user and url for collection api
    object are parsed'''
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('rq_queue', type=str, help='RQ queue to put job in')
    parser.add_argument('url_api_collection', type=str,
                        help='URL for the collection Django tastypie api resource')
    parser.add_argument('--job_timeout', type=int, default=JOB_TIMEOUT,
                        help='Timeout for the RQ job')
    parser.add_argument('--run_image_harvest', type=bool, default=True,
            help='Run image harvest set: --run_image_harvest=False to skip')
    return parser


def main(user_email, url_api_collection,
        redis_host=None, redis_port=None, redis_pswd=None,
        timeout=None, poll_interval=20,
        job_timeout=86400, # 24 hrs
        rq_queue=None,
        run_image_harvest=False):
    timeout_dt = datetime.timedelta(seconds=timeout) if timeout else \
                 datetime.timedelta(seconds=TIMEOUT)
    start_time = datetime.datetime.now()
    while not check_redis_queue(redis_host, redis_port, redis_pswd):
        time.sleep(poll_interval)
        if datetime.datetime.now() - start_time > timeout_dt:
            # TODO: email user
            raise Exception('TIMEOUT ({0}s) WAITING FOR QUEUE.'.format(timeout))
    if rq_queue not in RQ_Q_LIST:
        raise ValueError('{0} is not a valid RQ worker queue'.format(rq_queue))
    rQ = Queue(rq_queue, connection=get_redis_connection(redis_host, redis_port,
                                               redis_pswd))
    url_api_collection = [u.strip() for u in url_api_collection.split(';')]
    results = []
    for url in url_api_collection:
        result = rQ.enqueue_call(func='harvester.run_ingest.main',
                                 args=(user_email, url),
                                 kwargs={'run_image_harvest':run_image_harvest,
                                         'rq_queue': rq_queue},
                                 timeout=job_timeout,
                                 )
        results.append(result)
    return results

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection or not args.rq_queue:
        parser.print_help()
        raise Exception('Missing required parameters')
    env = config()
    main(args.user_email, args.url_api_collection.strip(),
         redis_host=env['redis_host'],
         redis_port=env['redis_port'],
         redis_pswd=env['redis_password'],
         rq_queue=args.rq_queue,
         job_timeout=args.job_timeout,
         run_image_harvest=args.run_image_harvest
         )
