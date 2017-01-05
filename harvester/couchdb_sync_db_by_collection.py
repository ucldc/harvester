'''This is a script and class? to sync 2 couchdb databases by a registry
collection.
It uses the view all_provider_docs/_view/by_provider_name_wdoc with the key
given by the id of the collection.
It checks that the collection has "ready_for_publication" set before syncing.
It then syncs to the harvesting environments default couchdb instance by just
adding the documents from the source.
'''
from os import environ
import sys
from harvester.collection_registry_client import Collection
from harvester.couchdb_init import get_couchdb
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter
from harvester.post_processing.couchdb_runner import get_collection_doc_ids
from harvester.sns_message import publish_to_harvesting

COUCHDB_VIEW_COLL_IDS = 'all_provider_docs/by_provider_name'

# use couchdb_init to get environment couchdb


def delete_id_list(ids, _couchdb=None):
    '''For a list of couchdb ids & given couchdb, delete the docs'''
    deleted = []
    num_deleted = 0
    for did in ids:
        doc = _couchdb.get(did)
        if not doc:
            continue
        _couchdb.delete(doc)
        deleted.append(did)
        print >> sys.stderr, "DELETED: {0}".format(did)
        num_deleted += 1
    return num_deleted, deleted


def delete_collection(cid):
    print >> sys.stderr, "DELETING COLLECTION: {}".format(cid)
    _couchdb = get_couchdb()
    rows = CouchDBCollectionFilter(collection_key=cid, couchdb_obj=_couchdb)
    ids = [row['id'] for row in rows]
    num_deleted, deleted_docs = delete_id_list(ids, _couchdb=_couchdb)
    publish_to_harvesting(
        'Deleted CouchDB Collection {}'.format(cid),
        'Deleted {} documents from CouchDB collection {}'.format(num_deleted,
                                                                 cid))
    return num_deleted, deleted_docs


def collection_ready_for_publication(url_api_collection):
    '''Check if the collection has "ready_for_publication" currently
    checked
    '''
    collection = Collection(url_api_collection)
    return collection.ready_for_publication


def update_from_remote(doc_id,
                       url_remote_couchdb=None,
                       couchdb_remote=None,
                       couchdb_env=None):
    '''Update the environment's couchdb from a remote couchdb document
    '''
    msg = None
    if not couchdb_remote:
        couchdb_remote = get_couchdb(url_remote_couchdb)
    if not couchdb_env:
        couchdb_env = get_couchdb()
    doc = couchdb_remote.get(doc_id)
    # need to remove the revision data, as will be different
    del doc['_rev']
    # if doc exists, need to update metadata for the existing document
    # and then save that, due to revision number in couch
    doc_in_target = couchdb_env.get(doc_id)
    if doc_in_target:
        doc_in_target.update(doc)
        couchdb_env[doc_id] = doc_in_target
        msg = "updated {}".format(doc_id)
    else:
        doc_no_rev = doc.copy()
        couchdb_env[doc_id] = doc_no_rev
        msg = "created {}".format(doc_id)
    print >> sys.stderr, msg
    return msg


def queue_update_from_remote(queue,
                             url_api_collection,
                             url_couchdb_source=None):
    '''for this environment, put a couchdb doc id on another environments
    queue. This environment's couchdb url becomes the remote in the call to
    update_from_remote
    This can be overridden to run from target environment
    '''
    pass


def update_collection_from_remote(url_remote_couchdb,
                                  url_api_collection,
                                  delete_first=True):
    '''Update a collection from a remote couchdb.
    '''
    if delete_first:
        delete_collection(url_api_collection.rsplit('/', 2)[1])
    collection = Collection(url_api_collection)
    # guard against updating production for not ready_for_publication
    # collections
    if 'prod' in environ.get('DATA_BRANCH', ''):
        if not collection.ready_for_publication:
            raise Exception(
                'In PRODUCTION ENV and collection {} not ready for '
                'publication'.format(collection.id))
    doc_ids = get_collection_doc_ids(collection.id, url_remote_couchdb)
    couchdb_remote = get_couchdb(url_remote_couchdb)
    couchdb_env = get_couchdb()
    created = 0
    updated = 0

    for doc_id in doc_ids:
        msg = update_from_remote(
            doc_id, couchdb_remote=couchdb_remote, couchdb_env=couchdb_env)
        if 'created' in msg:
            created += 1
        else:
            updated += 1

    return len(doc_ids), updated, created


def main(url_remote_couchdb, url_api_collection):
    '''Update to the current environment's couchdb a remote couchdb collection
    '''
    collection = Collection(url_api_collection)
    total, updated, created = update_collection_from_remote(
        url_remote_couchdb, url_api_collection)
    msg = 'Synced {} documents to production for CouchDB collection {}'.format(
        total,
        collection.id)
    msg += '\nUpdated {} documents, created {} documents.'.format(
        updated,
        created)
    publish_to_harvesting(
        'Synced CouchDB Collection {}'.format(collection.id),
        msg)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Update current env couchdb from a remote couchdb for \
                given collection')
    parser.add_argument(
        'url_remote_couchdb', help='URL to the remote (source) couchdb')
    parser.add_argument(
        'url_api_collection', help='Registry api endpoint for the collection')
    args = parser.parse_args(sys.argv[1:])
    main(args.url_remote_couchdb, args.url_api_collection)
