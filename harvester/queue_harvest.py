# queue a job on the rq queue
# may need to start ec2 instances
# and then dump job to queue
import sys
import datetime
import time
from  redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from rq import Queue
import boto.ec2

import harvester.run_ingest
from harvester.parse_env import parse_env

ID_EC2_INGEST = ''
ID_EC2_SOLR_BUILD = ''
TIMEOUT = 600  # for machine start
JOB_TIMEOUT = 600 #for jobs

def get_redis_connection(redis_host, redis_port, redis_pswd, redis_timeout=10):
    return Redis(host=redis_host, port=redis_port, password=redis_pswd, socket_connect_timeout=redis_timeout)

def check_redis_queue(redis_host, redis_port, redis_pswd):
    '''Check if the redis host db is up and running'''
    print "HOST {0} PORT: {1}".format( redis_host, redis_port)
    r = get_redis_connection(redis_host, redis_port, redis_pswd)
    try:
        return r.ping()
    except RedisConnectionError:
        return False

def start_ec2_instances(id_ec2_ingest, id_ec2_solr):
    '''Use boto to start instances
    '''
    conn = boto.ec2.connect_to_region('us-east-1')
    instances = conn.start_instances((id_ec2_ingest, id_ec2_solr))

def def_args():
    '''For now only the required email for the user and url for collection api 
    object are parsed'''
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('url_api_collection', type=str,
            help='URL for the collection Django tastypie api resource')
    parser.add_argument('--job_timeout', type=int, default=JOB_TIMEOUT,
            help='Timeout for the RQ job')
    return parser

def main(user_email, url_api_collection, redis_host=None, redis_port=None,
    redis_pswd=None, id_ec2_ingest=ID_EC2_INGEST, id_ec2_solr=ID_EC2_SOLR_BUILD,
    timeout=None, poll_interval=20, job_timeout=600):
    timeout_dt = datetime.timedelta(seconds=timeout) if timeout else datetime.timedelta(seconds=TIMEOUT)
    if not check_redis_queue(redis_host, redis_port, redis_pswd):
        start_ec2_instances(id_ec2_ingest=id_ec2_ingest, id_ec2_solr=id_ec2_solr)
    start_time = datetime.datetime.now()
    while not check_redis_queue(redis_host, redis_port, redis_pswd):
        time.sleep(poll_interval)
        if datetime.datetime.now() - start_time > timeout_dt:
            raise Exception('TIMEOUT ({0}s) WAITING FOR QUEUE. TODO: EMAIL USER'.format(timeout))
    rQ = Queue(connection=get_redis_connection(redis_host, redis_port, redis_pswd))
    url_api_collection = [ u.strip() for u in url_api_collection.split(';')]
    results = []
    for url in url_api_collection:
        result = rQ.enqueue_call(func=harvester.run_ingest.main,
            args=(user_email, url),
            timeout=job_timeout)
        results.append(result)
    return results

if __name__=='__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection:
        parser.print_help()
        raise Exception('Need to pass in user email and collection api URL')
    redis_host, redis_port, redis_pswd, redis_connect_timeout, id_ec2_ingest, id_ec2_solr_build = parse_env()
    main(args.user_email, args.url_api_collection.strip(), 
            redis_host=redis_host,
            redis_port=redis_port,
            redis_pswd=redis_pswd,
            id_ec2_ingest=id_ec2_ingest,
            id_ec2_solr=id_ec2_solr_build,
            job_timeout = args.job_timeout
            )
