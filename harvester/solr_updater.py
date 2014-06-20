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

def map_couch_to_solr_doc(doc):
    '''Return a json document suitable for updating the solr index
    how to make schema aware mapping?'''
    solr_doc = {}
    solr_doc['id'] = doc['_id']
    collection = doc['originalRecord']['collection']
    solr_doc['collection_name'] = collection['name']
    solr_doc['campus'] = []
    solr_doc['repository'] = []
    campuses = doc['originalRecord']['campus']
    for c in campuses:
        solr_doc['campus'].append(c['name'])
    repositories = doc['originalRecord']['repository']
    for r in repositories:
        solr_doc['repository'].append(r['name'])
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

def push_couch_doc_to_solr(doc, solr_db):
    '''Map and push one couch doc to solr'''
    solr_doc = map_couch_to_solr_doc(doc)
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

if __name__=='__main__':
    server = Server(URL_COUCHDB)
    db = server[COUCHDB_DB]
    changes = db.changes()
    since = get_couchdb_last_seq()
    changes = db.changes(since=since)
    last_seq = int(changes['last_seq'])
    results = changes['results']
    n = 0
    changed_ids = defaultdict(int)
    for row in results:
        n += 1
        changed_ids[row['id']] += 1
        if (n % 1000) == 0:
            print row['id'], row['changes']
    ##TODO: set_couchdb_last_seq(last_seq)

    print "CHANGED IDS:", len(changed_ids)
    solr_db = Solr(URL_SOLR)
    for i in changed_ids:
        doc = db.get(i)
        solr_doc = push_couch_doc_to_solr(doc, solr_db=solr_db)
        break
    print d
    print type(d), dir(d)
    print solr_doc
