'''Return a couchdb Server object in a consistent way from the environment or 
config file.
Preference the environment, fallback to ingest code akara.ini
'''
import couchdb
import os
from harvester.config import config

def get_couch_server(url=None, username=None, password=None):
    '''Returns a couchdb library Server object'''
    env = config()
    if not url:
        url = env['couchdb_url']
    if not username:
        username = env.get('couchdb_username', None)
    if not password:
        password = env.get('couchdb_password', None)
    if username:
        schema, uri = url.split("//")
        url = "{0}//{1}:{2}@{3}".format(schema, username, password, uri)
    return couchdb.Server(url)

def get_couchdb(url=None, dbname=None, username=None, password=None):
    '''Get a couchdb library Server object
    returns a 
    '''
    if not dbname:
        dbname = os.environ.get('COUCHDB_DB', None)
    if not dbname:
        _config = config()
        dbname = _config['DPLA'].get("CouchDb", "ItemDatabase")
    if not dbname:
        dbname = 'ucldc'
    couchdb_server = get_couch_server(url=url, username=username,
                                      password=password)
    return couchdb_server[dbname]
