'''Return a couchdb Server object in a consistent way from the environment or 
config file.
Preference the environment, fallback to ingest code akara.ini
'''
import couchdb
import os
import sys
from harvester.config import config

def parse_couchdb_url(url):
    '''Return url, username , password for couchdb url'''

def get_couch_server(url=None, username=None, password=None):
    '''Returns a couchdb library Server object'''
    env = config()
    if not url:
        url = env['couchdb_url']
    if username is None:
        username = env.get('couchdb_username', None)
    if password is None:
        password = env.get('couchdb_password', None)
    if username:
        schema, uri = url.split("//")
        url = "{0}//{1}:{2}@{3}".format(schema, username, password, uri)
    py_version = sys.version_info
    if py_version.major == 2 and py_version.minor == 7 and py_version.micro > 8:
        #disable ssl verification
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context
    print "URL:{}".format(url)
    return couchdb.Server(url)

def get_couchdb(url=None, dbname=None, username=None, password=None):
    '''Get a couchdb library Server object
    returns a 
    '''
    env = config()
    if not dbname:
        dbname = env.get('couchdb_dbname', None)
        if not dbname:
            dbname = 'ucldc'
    couchdb_server = get_couch_server(url, username, password)
    return couchdb_server[dbname]
