#! /bin/env python
'''Rerun a collection's stored enrichments

'''
import sys
import argparse
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.collection_registry_client import Collection
import harvester.post_processing.enrich_existing_couch_doc

def main(args):
    parser = argparse.ArgumentParser(
        description='run the enrichments stored for a collection.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--collection_id',
                        help='Registry id for the collection')
    group.add_argument('--cid_file',
                        help='File with collection ids for running')
    parser.add_argument('--rq_queue',
			help='Override queue for jobs, normal-stage is default')

    args = parser.parse_args(args)
    Q = 'normal-stage'
    if args.rq_queue:
        Q = args.rq_queue
    enq = CouchDBJobEnqueue(Q)
    timeout = 10000

    cids = []
    if args.collection_id:
        cids = [ args.collection_id ]
    else: #cid file
        with open(args.cid_file) as foo:
            lines = foo.readlines()
        cids = [ l.strip() for l in lines]
    print "CIDS:{}".format(cids)

    for cid in cids:
        url_api = ''.join(('https://registry.cdlib.org/api/v1/collection/',
                    cid, '/'))
        coll = Collection(url_api)
        print coll.id
        enrichments = coll.enrichments_item
        enq.queue_collection(cid, timeout,
                     harvester.post_processing.enrich_existing_couch_doc.main,
                     enrichments
                     )


if __name__=='__main__':
    main(sys.argv[1:])

