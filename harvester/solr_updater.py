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

COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING = {
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
    'subject'     : lambda d: {'subject': [s['name'] if isinstance(s, dict) else s for s in d['subject']]},
    'title'       : lambda d: {'title': d.get('title', None)},
    'type'        : lambda d: {'type': d.get('type', None)},
    'format'      : lambda d: {'format': d.get('format', None)},
    'extent'      : lambda d: {'extent': d.get('extent', None)},
}

def map_registry_data(collections):
    '''Map the collections data to corresponding data fields in the solr doc
    '''
    collection_urls = []
    collection_names = []
    collection_datas = []
    repository_urls = []
    repository_names = []
    repository_datas = []
    campus_urls = []
    campus_names = []
    campus_datas = []
    for collection in collections: #can have multiple collections
        collection_urls.append(collection['@id'])
        collection_names.append(collection['name'])
        collection_datas.append('::'.join((collection['@id'],
            collection['name'])))
        campuses = collection['campus']
        campus_urls.extend([campus['@id'] for campus in campuses])
        campus_names.extend([campus['name'] for c in campuses])
        campus_datas.extend(['::'.join((campus['@id'], campus['name']))
            for campus in campuses])
        repositories = collection['repository']
        repository_urls.extend([repo['@id'] for repo in repositories])
        repository_names.extend([repo['name'] for repo in repositories])
        repo_datas = []
        for repo in repositories:
            if repo['campus']:
                repo_data = '::'.join((repo['@id'], repo['name'],
                    repo['campus'][0]['name']))
            else:
                repo_data = '::'.join((repo['@id'], repo['name']))
            repo_datas.append(repo_data)
        repository_datas.extend(repo_datas)
    return dict(collection_url = collection_urls,
                collection_name = collection_names,
                collection_data = collection_datas,
                repository_url = repository_urls,
                repository_name = repository_names,
                repository_data = repository_datas,
                campus_url = campus_urls,
                campus_name = campus_names,
                campus_data = campus_datas
                )

def map_couch_to_solr_doc(doc):
    '''Return a json document suitable for updating the solr index
    how to make schema aware mapping?'''
    solr_doc = {}
    for p in doc.keys():
        if p in COUCHDOC_TO_SOLR_MAPPING:
            solr_doc.update(COUCHDOC_TO_SOLR_MAPPING[p](doc))
    solr_doc.update(map_registry_data(doc['originalRecord']['collection']))
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
    '''Use the _changes feed with a "since" parameter to only catch new 
    changes to docs. The _changes feed will only have the *last* event on
    a document and does not retain intermediate changes.
    Setting the "since" to 0 will result in getting a _changes record for 
    each document, essentially dumping the db to solr
    '''
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
