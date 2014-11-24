'''Configuration for the harvester and associated code.
UCLDC specfic information comes from environment variables and the
values are filled in directly in returned namedtuple.

DPLA values are contained in a ConfigParser object that is returned in the
namedtuple.
'''
import os
from collections import namedtuple
import ConfigParser

REDIS_HOST = '127.0.0.1'
REDIS_PORT = '6379'
DPLA_CONFIG_FILE = 'akara.ini'

HarvestConfig = namedtuple('HarvestConfig', 'redis_host, redis_port, \
                                     redis_pswd, redis_timeout, \
                                     id_ec2_ingest, id_ec2_solr_build, \
                                     DPLA')


def config(config_file=None):
    '''Return the HarvestConfig namedtuple for the harvester'''
    if not config_file:
        config_file = os.environ.get('DPLA_CONFIG_FILE', DPLA_CONFIG_FILE)
    rhost, rport, rpswd, r_timeout, ec2_ingest, ec2_solr = parse_env()
    DPLA = ConfigParser.ConfigParser()
    DPLA.readfp(open(config_file))
    return HarvestConfig(rhost, rport, rpswd, r_timeout, ec2_ingest,
                         ec2_solr, DPLA)


def parse_env():
    '''Get any overrides from the runtime environment for the server variables
    '''
    redis_host = os.environ.get('REDIS_HOST', REDIS_HOST)
    redis_port = os.environ.get('REDIS_PORT', REDIS_PORT)
    redis_connect_timeout = os.environ.get('REDIS_CONNECT_TIMEOUT', 10)
    try:
        redis_pswd = os.environ['REDIS_PASSWORD']
    except KeyError, e:
        raise KeyError(''.join(('Please set environment variable ',
                                'REDIS_PASSWORD to redis password!')))
    couchdb_url = os.environ.get('COUCHDB_URL', 'http://127.0.0.1:5984')
    couchdb_db = os.environ.get('COUCHDB_DB', 'ucldc')
    couchdb_view = 'all_provider_docs/by_provider_name'
    try:
        id_ec2_ingest = os.environ['ID_EC2_INGEST']
    except KeyError, e:
        raise KeyError(''.join(('Please set environment variable ',
                                'ID_EC2_INGEST to main ingest ',
                                'ec2 instance id.')))
    try:
        id_ec2_solr_build = os.environ['ID_EC2_SOLR_BUILD']
    except KeyError, e:
        raise KeyError(''.join(('Please set environment variable ',
                                'ID_EC2_SOLR_BUILD to ingest ',
                                'solr instance id.')))
    return redis_host, redis_port, redis_pswd, redis_connect_timeout, \
           id_ec2_ingest, id_ec2_solr_build
