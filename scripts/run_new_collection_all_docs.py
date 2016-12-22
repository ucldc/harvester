#! /bin/env python
'''run grabbing of new collection and updating in couch docs locally.

'''
import sys
import argparse
import Queue
import threading
import requests
import json

url_api_base = 'https://registry.cdlib.org/api/v1/collection/'
from harvester.post_processing.couchdb_runner import CouchDBWorker
from harvester.fetcher import HarvestController
from harvester.collection_registry_client import Collection

def fix_registry_data(doc, c_couch, _couchdb):
    '''get the registry data for the collection,
    use the HarvestControllers's _add_registry_data to update with @ids &
    name.
    Then for each doc in the collection, remove originalRecord campus &
    repository. Replace originalRecord & sourceResource collection with
    new collection object.
    Save doc.
    '''
    #print "COLLECTION: {}".format(c_couch)
    #print "REPO: {}".format(c_couch[0]['repository'])
    #print "C@ID: {}".format(c_couch[0]['@id'])
    #print "------COLL:{}\n----------------".format(c_couch[0])
    #print "KEYS: {}".format(c_couch[0].keys())
    #print 'KEYS COUCHDB ORIGREC lEN: {}'.format(len(doc['originalRecord'].keys()))
    doc['originalRecord']['collection'] = c_couch
    doc['sourceResource']['collection'] = c_couch
    if 'repository' in doc['originalRecord']:
        del(doc['originalRecord']['repository'])
    if 'campus' in doc['originalRecord']:
        del(doc['originalRecord']['campus'])

    #print 'KEYS COUCHDB ORIGREC lEN: {}'.format(len(doc['originalRecord'].keys()))
    #print 'Collection keys:{}'.format(doc['originalRecord']['collection'][0].keys())
    print "UPDATING {}".format(doc.id)
    # now save & get next one....
    _couchdb[doc.id] = doc

def get_id_on_queue_and_run(queue):
    cdbworker = CouchDBWorker()
    cid = queue.get_nowait()
    while cid:
        c_reg = Collection(url_api_base+cid)
        h = HarvestController('mark.redar@ucop.edu', c_reg)
        c_couch = h._add_registry_data({})['collection']
        del(h)
        print "STARTING COLLECTION: {}".format(cid)
        cdbworker.run_by_collection(cid, fix_registry_data, c_couch, cdbworker._couchdb)
        print "FINISHED COLLECTION: {}".format(cid)
        cid = queue.get_nowait()

if __name__=='__main__':
    queue = Queue.Queue()
    num_threads = 2
    for l in  open('collections-new-fix-stg.list'):
        cid = l.strip()
        #fill q, not parallel yet
        queue.put(cid)
    threads = [threading.Thread(target=get_id_on_queue_and_run,
        args=(queue,)) for i in range(num_threads)]
    print "THREADS:{} starting next".format(threads)
    for t in threads:
        t.start()
