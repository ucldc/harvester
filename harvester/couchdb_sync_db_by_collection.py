'''This is a script and class? to sync 2 couchdb databases by a registry
collection.
It uses the view all_provider_docs/_view/by_provider_name_wdoc with the key
given by the id of the collection.
It checks that the collection has "ready_for_publication" set before syncing.
It then syncs to the harvesting environments default couchdb instance by just
adding the documents from the source.
'''
from copy import copy
from harvester.collection_registry_client import Collection
from harvester.couchdb_init import get_couchdb
from harvester.couchdb_pager import couchdb_pager
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter

COUCHDB_VIEW_COLL_IDS = 'all_provider_docs/by_provider_name'

# use couchdb_init to get environment couchdb

def collection_ready_for_publication(url_api_collection):
    '''Check if the collection has "ready_for_publication" currently 
    checked
    '''
    try:
        collection = Collection(url_api_collection)
    except Exception, e:
        msg = 'Exception in Collection {}, init {}'.format(url_api_collection,
                                                           str(e))
        logbook.error(msg)
        raise e
    return collection.ready_for_publication

def update_from_remote(doc_id, url_remote_couchdb=None, couchdb_remote=None,
        couchdb_env=None):
    '''Update the environment's couchdb from a remote couchdb document
    '''
    if not couchdb_remote:
        couchdb_remote = get_couchdb(url_remote_couchdb)
    if not couchdb_env:
        couchdb_env = get_couchdb()
    doc = couchdb_remote.get(doc_id)
    # need to remove the revision data, as will be different
    del doc['_rev']
    #if doc exists, need to update metadata for the existing document
    #and then save that, due to revision number in couch
    doc_in_target = couchdb_env.get(doc_id)
    if doc_in_target:
        doc_in_target.update(doc)
        couchdb_env[doc_id] = doc_in_target
    else:
        doc_no_rev = doc.copy()
        couchdb_env[doc_id] = doc_no_rev

def queue_update_from_remote(queue, url_api_collection, url_couchdb_source=None):
    '''for this environment, put a couchdb doc id on another environments
    queue. This environment's couchdb url becomes the remote in the call to
    update_from_remote
    This can be overridden to run from target environment
    '''
    pass

def get_collection_doc_ids(collection_id, url_couchdb_source=None):
    '''Use the by_provider_name view to get doc ids for a given collection
    '''
    print "XXXXXXXXXXXX", str(collection_id), url_couchdb_source
    _couchdb = get_couchdb(url=url_couchdb_source)
    v = CouchDBCollectionFilter(couchdb_obj=_couchdb,
                                    collection_key=str(collection_id),
                                    include_docs=False)

    doc_ids = []
    for r in v:
        doc_ids.append(r.id)
    return doc_ids

def update_collection_from_remote(url_remote_couchdb, url_api_collection):
    '''Update a collection from a remote couchdb.
    '''
    collection = Collection(url_api_collection)
    doc_ids = get_collection_doc_ids(collection.id, url_remote_couchdb)
    couchdb_remote = get_couchdb(url_remote_couchdb)
    couchdb_env = get_couchdb()
    for doc_id in doc_ids:
        update_from_remote(doc_id,
                couchdb_remote=couchdb_remote,
                couchdb_env=couchdb_env)

def main(url_remote_couchdb, url_api_collection):
    '''Update to the current environment's couchdb a remote couchdb collection
    '''
    update_collection_from_remote(url_remote_couchdb, url_api_collection)

if __name__=='__main__':
    import argparse
    import sys
    parser = argparse.ArgumentParser(
        description='Update current env couchdb from a remote couchdb for \
                given collection')
    parser.add_argument('url_remote_couchdb',
                        help='URL to the remote (source) couchdb')
    parser.add_argument('url_api_collection',
                        help='Registry api endpoint for the collection')
    args = parser.parse_args(sys.argv[1:])
    main(args.url_remote_couchdb, args.url_api_collection)
