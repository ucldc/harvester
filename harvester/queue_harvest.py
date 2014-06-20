# queue a job on the rq queue
# may need to start ec2 instances
# and then dump job to queue
import sys
import os
import datetime
import time
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from rq import Queue
import boto.ec2

import run_ingest

REDIS_HOST = 'http://127.0.0.1'
REDIS_PORT = '6379'
REDIS_CONNECT_TIMEOUT = 10
ID_EC2_INGEST = ''
ID_EC2_SOLR_BUILD = ''
TIMEOUT = datetime.timedelta(seconds=600)

def get_redis_connection(redis_host, redis_port, redis_pswd):
    return Redis(host=redis_host, port=redis_port, password=redis_pswd, socket_connect_timeout=REDIS_CONNECT_TIMEOUT)

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
    return parser

def parse_env():
    '''Get any overrides from the runtime environment for the server variables
    '''
    redis_host = os.environ.get('REDIS_HOST', REDIS_HOST)
    redis_port = os.environ.get('REDIS_PORT', REDIS_PORT)
    try:
        redis_pswd = os.environ['REDIS_PSWD']
    except KeyError, e:
        raise KeyError('Please set environment variable REDIS_PSWD to redis password!')
    try:
        id_ec2_ingest = os.environ['ID_EC2_INGEST']
    except KeyError, e:
        raise KeyError('Please set environment variable ID_EC2_INGEST to main ingest ec2 instance id.')
    try:
        id_ec2_solr_build = os.environ['ID_EC2_SOLR_BUILD']
    except KeyError, e:
        raise KeyError('Please set environment variable ID_EC2_SOLR_BUILD to ingest solr instance id.')
    return redis_host, redis_port, redis_pswd, id_ec2_ingest, id_ec2_solr_build

def main(user_email, url_api_collection, redis_host=REDIS_HOST, redis_port=REDIS_PORT, redis_pswd=None, id_ec2_ingest=ID_EC2_INGEST, id_ec2_solr=ID_EC2_SOLR_BUILD):
    if not check_redis_queue(redis_host, redis_port, redis_pswd):
        start_ec2_instances(id_ec2_ingest=id_ec2_ingest, id_ec2_solr=id_ec2_solr)
    start_time = datetime.datetime.now()
    while not check_redis_queue(redis_host, redis_port, redis_pswd):
        time.sleep(20)
        if datetime.datetime.now() - start_time > TIMEOUT:
            raise Exception('TIMEOUT WAITING FOR QUEUE. EMAIL USER')
    rQ = Queue(connection=get_redis_connection(redis_host, redis_port, redis_pswd))
    result = rQ.enqueue(run_ingest.main, user_email, url_api_collection)
    print result

if __name__=='__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection:
        parser.print_help()
        raise Exception('Need to pass in user email and collection api URL')
    redis_host, redis_port, redis_pswd, id_ec2_ingest, id_ec2_solr = parse_env()
    main(args.user_email, args.url_api_collection.strip(), 
            redis_host=redis_host,
            redis_port=redis_port,
            redis_pswd=redis_pswd,
            id_ec2_ingest=id_ec2_ingest,
            id_ec2_solr=id_ec2_solr
            )
