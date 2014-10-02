import os

REDIS_HOST = '127.0.0.1'
REDIS_PORT = '6379'


def parse_env():
    '''Get any overrides from the runtime environment for the server variables
    '''
    redis_host = os.environ.get('REDIS_HOST', REDIS_HOST)
    redis_port = os.environ.get('REDIS_PORT', REDIS_PORT)
    redis_connect_timeout = os.environ.get('REDIS_CONNECT_TIMEOUT', 10)
    try:
        redis_pswd = os.environ['REDIS_PASSWORD']
    except KeyError, e:
        raise KeyError('Please set environment variable REDIS_PASSWORD to redis password!')
    try:
        id_ec2_ingest = os.environ['ID_EC2_INGEST']
    except KeyError, e:
        raise KeyError('Please set environment variable ID_EC2_INGEST to main ingest ec2 instance id.')
    try:
        id_ec2_solr_build = os.environ['ID_EC2_SOLR_BUILD']
    except KeyError, e:
        raise KeyError('Please set environment variable ID_EC2_SOLR_BUILD to ingest solr instance id.')
    return redis_host, redis_port, redis_pswd, redis_connect_timeout, id_ec2_ingest, id_ec2_solr_build
