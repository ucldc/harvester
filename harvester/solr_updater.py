import os, sys
import urllib
from collections import defaultdict
import requests
import boto
from solr import Solr, SolrException
from couchdb.client import Server, Database, Document


URL_SOLR = os.environ.get('URL_SOLR', 'http://10.0.1.13:8080/solr/dc-collection/')
URL_COUCHDB = os.environ.get('URL_COUCHDB', 'http://localhost:5984')
COUCHDB_DB =os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_LAST_SEQ_KEY = 'couchdb_last_seq'

DPLA_COUCHDOC_TO_SOLR_MAPPING = {
        'id' : lambda d: {'id': d['_id']},
        'collection' : lambda d: { 'collection' : d['originalRecord']['collection']},
        'collection_name' : lambda d: { 'collection_name' : d['originalRecord']['collection']['name']},
        'campus' : lambda d: { 'campus' : [ c.name for c in d['originalRecord']['campus']] },
        'repository' : lambda d: { 'repository' : [ r.name for r in d['originalRecord']['repository']]},

}

def map_couch_to_solr_doc(doc):
    '''Return a json document suitable for updating the solr index
    how to make schema aware mapping?'''
    solr_doc = {}
    for p in doc.keys():
        if p in DPLA_COUCHDOC_TO_SOLR_MAPPING:
            solr_doc.update(DPLA_COUCHDOC_TO_SOLR_MAPPING[p](doc))
    #solr_doc['id'] = doc['_id']
    #collection = doc['originalRecord']['collection']
    #solr_doc['collection_name'] = collection['name']
    #solr_doc['campus'] = []
    #solr_doc['repository'] = []
    #campuses = doc['originalRecord']['campus']
    #for c in campuses:
    #    solr_doc['campus'].append(c['name'])
    #repositories = doc['originalRecord']['repository']
    #for r in repositories:
    #    solr_doc['repository'].append(r['name'])
    for k, value in doc['sourceResource'].items():
        if k not in ('subject', 'stateLocatedIn', 'spatial', 'collection'):
            solr_doc[k] = value
        if k == 'subject':
            for s in doc.get('subject', []):
                if not has_key('subject', solr_doc):
                    doc['subject'] = [ s['name'] ]
                else:
                    doc['subject'].append(s['name'])
    return solr_doc

def push_couch_doc_to_solr(solr_doc, solr_db):
    '''Push one couch doc to solr'''
    try:
        solr_db.add(solr_doc)
        print "ADDED", solr_doc
    except SolrException, e:
        print "ERROR:", solr_doc
        print e
        if not e.httpcode == 400:
            raise e
    return solr_doc

def set_couchdb_last_seq(seq_num):
    '''Set the value fof the last sequence from couchdb _changes api'''
    conn = boto.connect_s3()
    b = conn.get_bucket('ucldc')
    k = b.get_key(COUCHDB_LAST_SEQ_KEY)
    if not k:
        k = Key(b)
        k.key = COUCHDB_LAST_SEQ_KEY 
    k.set_contents_from_string(seq_num)


def get_couchdb_last_seq():
    '''Return the value stored in the s3 bucket'''
    conn = boto.connect_s3()
    b = conn.get_bucket('ucldc')
    k = b.get_key(COUCHDB_LAST_SEQ_KEY)
    return int(k.get_contents_as_string())

def main():
    server = Server(URL_COUCHDB)
    db = server[COUCHDB_DB]
    since = get_couchdb_last_seq()
    changes = db.changes(since=since)
    last_seq = int(changes['last_seq'])
    results = changes['results']
    n = 0
    solr_db = Solr(URL_SOLR)
    for row in results:
        if '_design' in row['id']:
            print("Skip {0}".format(row['id']))
            continue
        n += 1
        doc = db.get(row['id'])
        solr_doc = map_couch_to_solr_doc(doc)
        solr_doc = push_couch_doc_to_solr(solr_doc, solr_db=solr_db) 
    #TODO: set_couchdb_last_seq(last_seq)
    print("UPDATED {0} DOCUMENTS")

if __name__=='__main__':
    main()
