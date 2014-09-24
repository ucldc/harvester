#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys
import urllib
from collections import defaultdict
import requests
import boto
from solr import Solr, SolrException
from couchdb import Server, Database, Document


URL_SOLR = os.environ.get('URL_SOLR', 'http://10.0.1.13:8080/solr/dc-collection/')
URL_COUCHDB = os.environ.get('URL_COUCHDB', 'http://localhost:5984')
COUCHDB_DB =os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_LAST_SEQ_KEY = 'couchdb_last_seq'

COUCHDOC_TO_SOLR_MAPPING = {
    'id' : lambda d: {'id': d['_id']},
    'object' : lambda d: {'reference_image_md5': d['object']},
    'isShownAt'     : lambda d : { 'url_item' : d['isShownAt'] },
}

COUCHDOC_ORG_RECORD_TO_SOLR_MAPPING = {
    'campus'     : lambda d: { 'campus' : [ c['@id'] for c in d['campus']], 
                        'campus_name' : [ c['name'] for c in d['campus']] },
    'repository' : lambda d: { 'repository' : [ r['@id'] for r in d['repository']],
                               'repository_name': [ r['name'] for r in d['repository']]},
}

COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING = {
    #assuming one collection only, may need to change
    'collection'  : lambda d: { 'collection' : [c['@id'] for c in d['collection']],
                               'collection_name' : [c['name'] for c in d['collection']] },
    'contributor' : lambda d: {'contributor': d.get('contributor', None)},
    'coverage'    : lambda d: {'spatial': d.get('spatial', None)},
    'creator'     : lambda d: {'creator': d.get('creator', None)},
    'description' : lambda d: {'description': d.get('description', None)},
    'date'        : lambda d: {'date': d.get('date', None)},
    'identifier'  : lambda d: {'identifier': d.get('identifier', None)},
    'language'    : lambda d: {'language': d.get('language', None)},
    'publisher'   : lambda d: {'publisher': d.get('publisher', None)},
    'relation'    : lambda d: {'relation': d.get('relation', None)},
    'rights'      : lambda d: {'rights': d.get('rights', None)},
    'subject'     : lambda d: { 'subject' : [s['name'] for s in d['subject'] if s] },
    'title'       : lambda d: {'title': d.get('title', None)},
    'type'        : lambda d: {'type': d.get('type', None)},
    'format'      : lambda d: {'format': d.get('format', None)},
    'extent'      : lambda d: {'extent': d.get('extent', None)},
}

def map_couch_to_solr_doc(doc):
    '''Return a json document suitable for updating the solr index
    how to make schema aware mapping?'''
    solr_doc = {}
    for p in doc.keys():
        if p in COUCHDOC_TO_SOLR_MAPPING:
            solr_doc.update(COUCHDOC_TO_SOLR_MAPPING[p](doc))
    originalRecord = doc['originalRecord']
    for p in originalRecord.keys():
        if p in COUCHDOC_ORG_RECORD_TO_SOLR_MAPPING:
            solr_doc.update(COUCHDOC_ORG_RECORD_TO_SOLR_MAPPING[p](originalRecord))
    sourceResource = doc['sourceResource']
    for p in sourceResource.keys():
        if p in COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING:
            try:
                solr_doc.update(COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING[p](sourceResource))
            except TypeError, e:
                print('TypeError for doc {} on sourceResource {}'.format(doc['_id'], p))
                raise e
    return solr_doc

def push_doc_to_solr(solr_doc, solr_db):
    '''Push one couch doc to solr'''
    try:
        solr_db.add(solr_doc)
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
        #TODO: Handle deletions, need to look for "deleted" key in
        # change doc
        doc = db.get(row['id'])
        try:
            solr_doc = map_couch_to_solr_doc(doc)
            solr_doc = push_doc_to_solr(solr_doc, solr_db=solr_db) 
        except TypeError:
            continue
    solr_db.commit() #commit updates
    set_couchdb_last_seq(last_seq)
    print("UPDATED {0} DOCUMENTS".format(n))

if __name__=='__main__':
    main()
