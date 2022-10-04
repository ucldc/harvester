#! /bin/env python
import sys
from harvester.couchdb_init import get_couchdb
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter
from harvester.sns_message import publish_to_harvesting
from harvester.sns_message import format_results_subject

PATH_DELIM = '/'

def traverse(doc, path):
    if PATH_DELIM not in path:
        return doc, path
    parent, remainder = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM, 1))
    return traverse(doc[parent], remainder)

def substring_replace(doc, path, newValue, substring):
    parent, field = traverse(doc, path)
    # this will throw a key error if the field doesn't exist
    if isinstance(parent[field], list):
        parent[field] = [val.replace(substring, newValue) for val in parent[field]]
    else:
        parent[field] = parent[field].replace(substring, newValue)
    return

def replace(doc, path, newValue):
    parent, field = traverse(doc, path)
    # this will quietly set the field to newValue if it doesn't exist
    if field in parent and isinstance(parent[field], list):
        parent[field] = [newValue]
    else:
        parent[field] = newValue
    return

def append(doc, path, newValue):
    parent, field = traverse(doc, path)
    # this will quietly set the field to [newValue] if it doesn't exist
    if field not in parent:
        parent[field] = []
    if isinstance(parent[field], list):
        parent[field].append(newValue)
    else:
        parent[field] = [parent[field], newValue]

def update_by_id_list(ids, fieldName, newValue, substring, append=False, _couchdb=None):
    '''For a list of couchdb ids, given field name and new value, update the doc[fieldname] with "new value"'''
    updated = []
    num_updated = 0
    for did in ids:
        doc = _couchdb.get(did)
        if not doc:
            continue
        if substring:
            print >> sys.stderr, "SUBSTRING 1: {}".format(substring)
            substring_replace(doc, fieldName, newValue, substring)
        if append:
            append(doc, fieldName, newValue)
        else:
            replace(doc, fieldName, newValue)
        _couchdb.save(doc)
        updated.append(did)
        print >> sys.stderr, "UPDATED: {0}".format(did)
        num_updated += 1
    return num_updated, updated

def update_couch_docs_by_collection(cid, fieldName, newValue, substring, append=False):
    print >> sys.stderr, "UPDATING DOCS FOR COLLECTION: {}".format(cid)
    _couchdb = get_couchdb()
    rows = CouchDBCollectionFilter(collection_key=cid, couchdb_obj=_couchdb)
    ids = [row['id'] for row in rows]
    num_updated, updated_docs = update_by_id_list(
        ids, fieldName, newValue, substring, append, _couchdb=_couchdb)
    subject = format_results_subject(cid,
                                     'Updated documents from CouchDB {env} ')
    publish_to_harvesting(
        subject, 'Updated {} documents from CouchDB collection CID: {}'.format(
            num_updated, cid))
    return num_updated, updated_docs