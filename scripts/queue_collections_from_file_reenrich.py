'''For collection ids in the file, queue the passed in enrichments

'''
import sys
import argparse
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
import harvester.post_processing.enrich_existing_couch_doc

ENQ = CouchDBJobEnqueue()

def q_collection(collection_id, enrichment):
    timeout = 10000
    print "ENRICH {} with {}".format(collection_id, enrichment)
    ENQ.queue_collection(collection_id, timeout,
                     harvester.post_processing.enrich_existing_couch_doc.main,
                     enrichment
                     )


def main(cid_file, enrichment):
    for l in open(args.collection_id_file):
        cid = l.strip()
        q_collection(cid, enrichment)

if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description='run an Akara enrichment chain on documents in a \
                collection.')
    parser.add_argument('collection_id_file',
                        help='File with registry ids for enrichment')
    parser.add_argument('enrichment', help='enrichment chain to run')

    args = parser.parse_args()
    print(args.collection_id_file)
    print(args.enrichment)
    main(args.collection_id_file, args.enrichment)

