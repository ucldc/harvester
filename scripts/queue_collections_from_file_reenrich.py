'''For collection ids in the file, queue the passed in enrichments

'''
import sys
import argparse
import Queue
import threading

from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
import harvester.post_processing.enrich_existing_couch_doc


def q_collection(collection_id, enrichment):
    timeout = 10000
    print "ENRICH {} with {}".format(collection_id, enrichment)
    ENQ = CouchDBJobEnqueue()
    ENQ.queue_collection(collection_id, timeout,
                     harvester.post_processing.enrich_existing_couch_doc.main,
                     enrichment
                     )


def get_id_on_queue_and_run(queue, enrichment):
    '''Pulls cid from queue and runs q_collection on the id'''
    cid = queue.get_nowait()
    while cid:
        q_collection(cid, enrichment)
        cid = queue.get_nowait()

def main(cid_file, enrichment, num_threads):
    queue = Queue.Queue()
    for l in open(args.collection_id_file):
        cid = l.strip()
        #fill q, not parallel yet
        queue.put(cid)
    # setup threads, run on objects in queue....
    # need wrapper callable that takes queue, consumes and call q_collection
    threads = [threading.Thread(target=get_id_on_queue_and_run,
        args=(queue, enrichment)) for i in range(num_threads)]
    print "THREADS:{} starting next".format(threads)
    for t in threads:
        t.start()


if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description='run an Akara enrichment chain on documents in a \
                collection.')
    parser.add_argument('collection_id_file',
                        help='File with registry ids for enrichment')
    parser.add_argument('enrichment', help='enrichment chain to run')
    parser.add_argument('--threads', nargs='?', default=1, type=int,
            help='number of threads to run on (default:1)')
    parser.add_argument('--rq_queue',
			help='Override queue for jobs, normal-stage is default')

    args = parser.parse_args()
    print(args.collection_id_file)
    print "ENRICH FILE:{}".format(args.enrichment)
    with open(args.enrichment) as enrichfoo:
        enrichments = enrichfoo.read() 
    main(args.collection_id_file, enrichments, args.threads)

