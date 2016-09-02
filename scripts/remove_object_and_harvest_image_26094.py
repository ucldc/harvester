#! /bin/env python
import sys
import os
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.post_processing.couchdb_runner import CouchDBWorker
from harvester.image_harvest import harvest_image_for_doc
from harvester.couchdb_init import get_couchdb
import couchdb #couchdb-python

from dplaingestion.selector import delprop

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)

def delete_field_and_queue_image_harvest(doc, field, cdb, enq):
    print 'Delete {} for {}'.format(field, doc['_id'])
    delprop(doc, field, keyErrorAsNone=True)
    cdb.save(doc)
    timeout = 10000
    results = enq.queue_list_of_ids([doc['_id']],
                     timeout,
                     harvest_image_for_doc,
                     )

def main(cid):
    worker = CouchDBWorker()
    enq = CouchDBJobEnqueue()
    timeout = 100000
    cdb = get_couchdb()
    worker.run_by_collection(cid,
                     delete_field_and_queue_image_harvest,
                     'object',
                     cdb,
                     enq
                     )

if __name__ == '__main__':
    main('26094')
