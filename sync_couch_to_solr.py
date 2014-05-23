import os, sys
import urllib
import requests
import solr
from solr import SolrException

URL_SOLR = os.environ.get('URL_SOLR', 'http://107.21.228.130:8080/solr/dc-collection/')

def map_couch_to_solr_doc(doc):
    '''Return a json document suitable for updating the solr index'''
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

if __name__=='__main__':
    solr_db = solr.Solr(URL_SOLR)
    resp=requests.get('http://localhost:5984/ucldc/_all_docs')
    j=resp.json()
    rows=j['rows']
    print "DOCS TO GRAB", len(rows)
    #update or create new solr doc for each couchdb doc
    for row in rows:
        if not '_design' in row['id']:
            doc = requests.get('http://localhost:5984/ucldc/'+urllib.quote_plus(row['id']))
            doc = doc.json()
            solr_doc = push_couch_doc_to_solr(doc, solr_db=solr_db)
