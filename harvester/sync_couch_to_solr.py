import os, sys
import urllib
import requests
import solr
from solr import SolrException

from solr_updater import map_couch_to_solr_doc, push_doc_to_solr
from solr_updater import URL_SOLR
from solr_updater import URL_COUCHDB, COUCHDB_DB

def main(url_solr=URL_SOLR, url_couchdb=URL_COUCHDB, couchdb_db=COUCHDB_DB):
    solr_db = solr.Solr(url_solr)
    resp=requests.get('/'.join((url_couchdb, couchdb_db, '_all_docs')))
    j=resp.json()
    rows=j['rows']
    print "DOCS TO GRAB", len(rows)
    #update or create new solr doc for each couchdb doc
    for row in rows:
        if not '_design' in row['id']:
            doc = requests.get('/'.join((url_couchdb, couchdb_db, urllib.quote_plus(row['id']))))
            doc_couch = doc.json()
            doc_solr = push_doc_to_solr(map_couch_to_solr_doc(doc_couch),
                                        solr_db=solr_db)
    solr_db.commit()

if __name__=='__main__':
    main()
