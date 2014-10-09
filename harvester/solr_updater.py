#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import urllib
import argparse
from collections import defaultdict
import requests
import boto
from solr import Solr, SolrException
from couchdb import Server, Database, Document

COUCHDB_LAST_SEQ_KEY = 'couchdb_last_seq'

COUCHDOC_TO_SOLR_MAPPING = {
    'id'       : lambda d: {'id': d['_id']},
    'object'   : lambda d: {'reference_image_md5': d['object']},
    'isShownAt': lambda d: {'url_item': d['isShownAt']},
}

COUCHDOC_ORG_RECORD_TO_SOLR_MAPPING = {
    'campus'     : lambda d: {'campus': [c['@id'] for c in d['campus']],
                              'campus_name': [c['name'] for c in d['campus']]},
    'repository' : lambda d: {'repository': [r['@id'] for r in d['repository']],
                              'repository_name': [r['name'] for r in d['repository']]},
    # assuming one collection only, may need to change
    'collection'  : lambda d: {'collection': [c['@id'] for c in d['collection']],
                               'collection_name': [c['name'] for c in d['collection']]},
}

COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING = {
    'collection'  : lambda d: {'collection': [c['@id'] for c in d['collection']],
                               'collection_name': [c['name'] for c in d['collection']]},
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
    'subject'     : lambda d: {'subject': [s['name'] for s in d['subject'] if s]},
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
        print("ERROR for {0} : {1}".format(solr_doc['id'], e))
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


def main(url_couchdb=None, dbname=None, url_solr=None):
    server = Server(url_couchdb)
    db = server[dbname]
    since = get_couchdb_last_seq()
    print('Attempt to connect to {0} - db:{1}'.format(url_couchdb, dbname))
    changes = db.changes(since=since)
    last_seq = int(changes['last_seq'])
    results = changes['results']
    n_up = n_design = n_delete = 0
    solr_db = Solr(url_solr)
    for row in results:
        cur_id = row['id']
        if '_design' in cur_id:
            n_design += 1
            print("Skip {0}".format(cur_id))
            continue
        if row.get('deleted', False):
            print('====DELETING: {0}'.format(cur_id))
            solr_db.delete(id=cur_id)
            n_delete += 1
        else:
            doc = db.get(cur_id)
            try:
                solr_doc = map_couch_to_solr_doc(doc)
                solr_doc = push_doc_to_solr(solr_doc, solr_db=solr_db)
            except TypeError, e:
                print('TypeError for {0} : {1}'.format(cur_id, e))
                continue
        n_up += 1
    solr_db.commit() #commit updates
    if (since + n_up + n_design) < last_seq:
        print("OVERRIDING LAST SEQ. LAST SEQ:{0} SINCE:{1} NUP:{2} \
        NTOT:{3}".format(last_seq, since, n_up+n_design, since+n_up+n_design))
        last_seq = since + n_up + n_design  # can redo updates, safer if errs
    set_couchdb_last_seq(last_seq)
    print("UPDATED {0} DOCUMENTS. DELETED:{1}".format(n_up, n_delete))
    print("Last SEQ:{0}".format(last_seq))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='update a solr instance from the couchdb doc store')
    parser.add_argument('url_couchdb',
                        help='URL to couchdb (http://127.0.0.1:5984)')
    parser.add_argument('dbname', help='Couchdb database name')
    parser.add_argument('url_solr', help='URL to writeable solr instance')

    args = parser.parse_args()
    print('Warning: this may take some time')
    main(url_couchdb=args.url_couchdb, dbname=args.dbname,
         url_solr=args.url_solr)
