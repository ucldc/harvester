'''Enqueue a docuement for re-enriching.

'''
import sys
import argparse
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
import harvester.post_processing.enrich_existing_couch_doc

def main(args):
    parser = argparse.ArgumentParser(
        description='run an Akara enrichment chain on documents in a \
                collection.')
    parser.add_argument('document_id',
                        help='Registry id for the collection')
    parser.add_argument('enrichment', help='enrichment chain to run')

    args = parser.parse_args(args)
    print(args.collection_id)
    print(args.enrichment)
    enq = CouchDBJobEnqueue()
    timeout = 10000
    enq.queue_collection(args.collection_id, timeout,
                     harvester.post_processing.enrich_existing_couch_doc.main,
                     args.enrichment
                     )


if __name__=='__main__':
    main(sys.argv)

