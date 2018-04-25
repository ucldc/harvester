#! /bin/env python
from os import environ
import sys
from copy import deepcopy
from harvester.collection_registry_client import Collection
from harvester.couchdb_init import get_couchdb
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter
from harvester.post_processing.couchdb_runner import get_collection_doc_ids
from harvester.sns_message import publish_to_harvesting
from harvester.sns_message import format_results_subject

from dplaingestion.selector import getprop, setprop

def update_by_id_list(ids, fieldname, newValue, _couchdb=None):
    '''For a list of couchdb ids, given field name and new value, update the doc[fieldname] with "new value"'''
    updated = []
    num_updated = 0
    for did in ids:
        doc = _couchdb.get(did)
        if not doc:
            continue
        value_src = getprop(doc, fieldName, keyErrorAsNone=True)
        if value_src:
            setprop(doc, field, newValue)
            _couchdb.save(doc)
        updated.append(did)
        print >> sys.stderr, "UPDATED: {0}".format(did)
        num_updated += 1
    return num_deleted, deleted

def update_couch_docs_by_collection(cid, fieldName, newValue):
    print >> sys.stderr, "UPDATING DOCS FOR COLLECTION: {}".format(cid)
    _couchdb = get_couchdb()
    rows = CouchDBCollectionFilter(collection_key=cid, couchdb_obj=_couchdb)
    ids = [row['id'] for row in rows]
    num_updated, updated_docs = update_by_id_list(ids, fieldname, newValue, _couchdb=_couchdb)
    subject = format_results_subject(cid,
                                     'Updated documents from CouchDB {env} ')
    publish_to_harvesting(
        subject,
        'Updated {} documents from CouchDB collection CID: {}'.format(
            num_updated,
            cid))
    return num_updated, updated_docs
