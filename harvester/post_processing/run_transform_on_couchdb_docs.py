'''This allows running a bit of code on couchdb docs.
code should take a json python object, modify it and hand back to the code
Not quite that slick yet, need way to pass in code or make this a decorator

'''

import os
import datetime
import time
import couchdb
from harvester.collection_registry_client import Collection

COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://127.0.0.1:5984')
COUCHDB_DB = os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'

print("URL: {} DB: {}".format(COUCHDB_URL, COUCHDB_DB))
username = os.environ.get('COUCHDB_USER', None)
password = os.environ.get('COUCHDB_PASSWORD', None)
url = COUCHDB_URL.split("//")
url_server = "{0}//{1}:{2}@{3}".format(url[0], username, password, url[1])
_couchdb = couchdb.Server(COUCHDB_URL)[COUCHDB_DB]


def run_on_couchdb_by_collection(func, collection_key=None):
    '''If collection_key is none, trying to grab all of docs and modify
    func is a function that takes a couchdb doc in and returns it modified.
    (can take long time - not recommended)
    '''
    v = _couchdb.view(COUCHDB_VIEW, include_docs='true',
                           key=collection_key) if collection_key else \
                           _couchdb.view(COUCHDB_VIEW, include_docs='true')
    doc_ids = []
    n = 0
    for r in v:
        n += 1
        #dt_start = dt_end = datetime.datetime.now()

        doc_new = func(r.doc)
        _couchdb.save(doc_new)
        doc_ids.append(r.doc['_id'])
        #dt_end = datetime.datetime.now()
        #time.sleep((dt_end-dt_start).total_seconds())
        if n % 1000  == 0:
            print '{} docs ran. Last doc:{}\n'.format(n,r.doc['_id'])
    return doc_ids


C_CACHE = {}
def update_collection_description(doc):
    cjson = doc['originalRecord']['collection'][0]
    #get collection description
    if not 'description' in cjson:
        if cjson['@id'] in C_CACHE:
            c = C_CACHE[cjson['@id']]
        else:
            c = Collection(url_api=cjson['@id'])
            C_CACHE[cjson['@id']] = c
        description = c['description'] if c['description'] else c['name']
        print('DOC: {} DESCRIP: {}'.format(doc['_id'], c['description'].encode('utf8')))
        doc['originalRecord']['collection'][0]['description']=description
        doc['sourceResource']['collection'][0]['description']=description
    return doc

doc_ids = run_on_couchdb_by_collection(update_collection_description)#, collection_key="172")
print doc_ids
