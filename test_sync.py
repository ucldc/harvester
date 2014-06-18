import json
import sync_couch_to_solr
import solr

bad_couch_doc = json.load(open('bad-couch-doc.json'))

solr_db = solr.Solr(sync_couch_to_solr.URL_SOLR)

solr_doc = sync_couch_to_solr.push_couch_doc_to_solr(bad_couch_doc, solr_db=solr_db)

