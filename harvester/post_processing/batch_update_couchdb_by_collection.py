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
from akara import logger

PATH_DELIM = '/'

def setprop(obj,path,val,keyErrorAsNone=False):
    """
    Sets the value of the key identified by interpreting
    the path as a delimited hierarchy of keys. Enumerate
    if object is list.
    """
    if '/' not in path:
        logger.error(type(obj))
        logger.error(obj)
        if type(obj) == list:
            logger.error("HERE")
            logger.error(obj)
            for t in (t for t in obj if t[path]):
                logger.error(t)
                logger.error(path)
                t[path] = val
                return
        else:
            if path not in obj:
                if not keyErrorAsNone:
                    raise KeyError('Path not found in object: %s'%(path))
                else:
                    return None
            else:
                logger.error("THERE")
                logger.error(obj)
                logger.error(path)
                obj[path] = val
                return

    pp,pn = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM,1))
    logger.error(pp)
    if pp not in obj:
        if not keyErrorAsNone:
            raise KeyError('Path not found in object: %s (%s)'%(path,pp))
        else:
            return None

    return setprop(obj[pp],pn,val,keyErrorAsNone)

def update_by_id_list(ids, fieldName, newValue, _couchdb=None):
    '''For a list of couchdb ids, given field name and new value, update the doc[fieldname] with "new value"'''
    updated = []
    num_updated = 0
    for did in ids:
        doc = _couchdb.get(did)
        if not doc:
            continue
        setprop(doc, fieldName, newValue)
        _couchdb.save(doc)
        updated.append(did)
        print >> sys.stderr, "UPDATED: {0}".format(did)
        num_updated += 1
    return num_updated, updated

def update_couch_docs_by_collection(cid, fieldName, newValue):
    print >> sys.stderr, "UPDATING DOCS FOR COLLECTION: {}".format(cid)
    _couchdb = get_couchdb()
    rows = CouchDBCollectionFilter(collection_key=cid, couchdb_obj=_couchdb)
    ids = [row['id'] for row in rows]
    num_updated, updated_docs = update_by_id_list(ids, fieldName, newValue, _couchdb=_couchdb)
    subject = format_results_subject(cid,
                                     'Updated documents from CouchDB {env} ')
    publish_to_harvesting(
        subject,
        'Updated {} documents from CouchDB collection CID: {}'.format(
            num_updated,
            cid))
    return num_updated, updated_docs
