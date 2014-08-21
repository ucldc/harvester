'''Push contents of couchdb to solr index'''
import os, sys
import urllib
import requests
import solr
import couchdb
from solr import SolrException

from solr_updater import map_couch_to_solr_doc, push_doc_to_solr
from solr_updater import URL_SOLR
from solr_updater import URL_COUCHDB, COUCHDB_DB

from harvester.couchdb_pager import couchdb_pager

def main(url_solr=URL_SOLR, url_couchdb=URL_COUCHDB, couchdb_db=COUCHDB_DB):
    solr_db = solr.Solr(url_solr)
    s = couchdb.Server(url=url_couchdb)
    db = s[couchdb_db]
    v = couchdb_pager(db, include_docs='true')
    #update or create new solr doc for each couchdb doc
    for r in v:
        doc_couch = r.doc
        if not '_design' in doc_couch['_id']:
            if not isinstance(doc_couch['originalRecord']['collection'], list):
                doc_couch['originalRecord']['collection'] = [
                                doc_couch['originalRecord']['collection'],]
                print("orgRec.Collection: {}".format(doc_couch['sourceResource']['collection']))
            if not isinstance(doc_couch['sourceResource']['collection'], list):
                doc_couch['sourceResource']['collection'] = [
                                doc_couch['sourceResource']['collection'],]
                print("srcRes.Collection: {}".format(doc_couch['sourceResource']['subject']))
            if not isinstance(doc_couch['sourceResource'].get('subject', [{},])[0], dict):
                doc_couch['sourceResource']['subject'] = [ {'name':x } for x in
                doc_couch['sourceResource']['subject']]
                print("srcRes.subject: {}".format(doc_couch['sourceResource'].get('subject',None)))
            db.save(doc_couch)
            try:
                doc_solr = push_doc_to_solr(map_couch_to_solr_doc(doc_couch),
                                        solr_db=solr_db)
                print("PUSHED {} to solr".format(doc_couch['_id']))
            except TypeError:
                pass
    solr_db.commit()

if __name__=='__main__':
    main()
