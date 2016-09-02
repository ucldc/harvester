#! /bin/env python
'''create a script to feed to the redis-cli to delete cached
harvested images information'''
import sys
import os
from harvester.post_processing.couchdb_runner import get_collection_doc_ids

redis_hash_key = 'ucldc:harvester:harvested-images'

def create_redis_deletion_script(url_remote_couchdb, collection_id):
    doc_ids = get_collection_doc_ids(collection_id, url_remote_couchdb)
    redis_cmd_base = 'HDEL {}'.format(redis_hash_key)
    #for every 100 ids create a new line
    with open('delete_image_cache-{}'.format(collection_id), 'w') as foo:
        for doc_id in doc_ids:
            redis_cmd = ' '.join((redis_cmd_base, doc_id, '\n'))
            foo.write(redis_cmd)

def main(url_remote_couchdb, collection_id):
    '''Update to the current environment's couchdb a remote couchdb collection
    '''
    create_redis_deletion_script(url_remote_couchdb, collection_id)

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Create script to delete harvested images cache for collection')
    parser.add_argument('--url_remote_couchdb',
                        help='URL to the remote (source) couchdb')
    parser.add_argument('collection_id',
                        help='collection id')
    args = parser.parse_args(sys.argv[1:])
    url_remote_couchdb = args.url_remote_couchdb
    if not url_remote_couchdb:
        url_remote_couchdb = os.environ.get('COUCHDB_URL', None)
    if not url_remote_couchdb:
        raise Exception("Need to pass in url_remote_couchdb or have $COUCHDB_URL set")
    main(url_remote_couchdb, args.collection_id)
