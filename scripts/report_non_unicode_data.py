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

def non_unicode_data(data, doc_id, key):
    #if "kt4m3nc6r1" in doc_id:
    #    print "IN BASE FN for check {} {}".format(doc_id, key)
    try:
        d = unicode(data)#.decode('utf8', encoding='utf8')
    except UnicodeEncodeError, e:
        print "NON UNICODE DATA: DOC:{} key:{} value:{} {}".format(doc_id, key,
                data.encode('utf8'), e)

def report_non_unicode_data(obj, doc_id=None, key=''):
    '''Recurse the objument and report on any non-unicode data
    '''
    #print "IN REPORT id:{} key:{}".format( doc_id, key)
    if not doc_id:
        doc_id = obj['_id']
    #jif "kt4m3nc6r1" in doc_id:
    # h   print "IN REPORT id:{} key:{}".format( doc_id, key)
    if isinstance(obj, basestring):
    #    if "kt4m3nc6r1" in doc_id:
    #        print "Check key:{}".format(key)
        return non_unicode_data(obj, doc_id, key)
    elif isinstance(obj, list):
    #    if "kt4m3nc6r1" in doc_id:
    #        print "Check key:{}".format(key)
        for value in obj:
            report_non_unicode_data(value, doc_id=doc_id, key=key)
    elif isinstance(obj, dict):
    #    if "kt4m3nc6r1" in doc_id:
    #        print "Check key:{}".format(key)
        for subkey, value in obj.items():
            fullkey = '/'.join((key, subkey))
            report_non_unicode_data(value, doc_id=doc_id, key=fullkey)
    return obj


def get_id_on_queue_and_run(queue):
    cdbworker = CouchDBWorker()
    cid = queue.get_nowait()
    while cid:
        print "STARTING COLLECTION: {}".format(cid)
        cdbworker.run_by_collection(cid, report_non_unicode_data)
        print "FINISHED COLLECTION: {}".format(cid)
        cid = queue.get_nowait()

if __name__=='__main__':
    queue = Queue.Queue()
    num_threads = 2
    for l in  open('collections-check-unicode.list'):
        cid = l.strip()
        #fill q, not parallel yet
        queue.put(cid)
    threads = [threading.Thread(target=get_id_on_queue_and_run,
        args=(queue,)) for i in range(num_threads)]
    print "THREADS:{} starting next".format(threads)
    for t in threads:
        t.start()
