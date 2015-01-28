'''Push contents of couchdb to solr index'''
import os
import sys
import urllib
import requests
import solr
import couchdb
from solr import SolrException
from solr_updater import map_couch_to_solr_doc, push_doc_to_solr
from harvester.couchdb_pager import couchdb_pager

URL_SOLR = os.environ.get('URL_SOLR', None)
URL_COUCHDB = os.environ.get('URL_COUCHDB', 'http://localhost:5984')
COUCHDB_DB = os.environ.get('COUCHDB_DB', 'ucldc')

def main(url_solr=URL_SOLR, url_couchdb=URL_COUCHDB, couchdb_db=COUCHDB_DB):
    solr_db = solr.Solr(url_solr)
    s = couchdb.Server(url=url_couchdb)
    db = s[couchdb_db]
    v = couchdb_pager(db, include_docs='true')
    # update or create new solr doc for each couchdb doc
    for r in v:
        doc_couch = r.doc
        if '_design' not in doc_couch['_id']:
            try:
                if not isinstance(doc_couch['originalRecord']['collection'], list):
                    doc_couch['originalRecord']['collection'] = [
                                    doc_couch['originalRecord']['collection'],
                                    ]
                    print("orgRec.Collection: {}".format(doc_couch['sourceResource']['collection']))
            except KeyError:
                pass
            try:
                if not isinstance(doc_couch['sourceResource']['collection'], list):
                    doc_couch['sourceResource']['collection'] = [
                                    doc_couch['sourceResource']['collection'],
                                    ]
                    print("srcRes.Collection: {}".format(doc_couch['sourceResource']['subject']))
            except KeyError:
                pass
            try:
                subject = doc_couch['sourceResource'].get('subject', None)
                if not isinstance(subject, list):
                    subject = [subject]
                subjects_norm = []
                for sub in subject:
                    if not isinstance(sub, dict):
                        subjects_norm.append({'name': sub})
                    else:
                        subjects_norm.append(sub)
                doc_couch['sourceResource']['subject'] = subjects_norm
            except KeyError:
                pass
            db.save(doc_couch)
            try:
                doc_solr = push_doc_to_solr(map_couch_to_solr_doc(doc_couch),
                                        solr_db=solr_db)
                print("PUSHED {} to solr".format(doc_couch['_id']))
            except TypeError:
                pass
    solr_db.commit()

if __name__ == '__main__':
    main()
