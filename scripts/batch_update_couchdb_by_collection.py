#! /bin/env python
import sys
import argparse
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
import couchdb #couchdb-python
from dplaingestion.selector import setprop
from akara import logger

def update_field(doc, fieldName, newValue, cdb):
    setprop(doc, fieldName, newValue, keyErrorAsNone=True)
    logger.error("3")


def def_args():
    import argparse
    parser = argparse.ArgumentParser(
            description='Batch update field with new value for a given couchdb collection')
    parser.add_argument('user_email', type=str, help='user email')
    #parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument('cid', type=str,
            help='Collection ID')
    parser.add_argument('fieldName', type=str,
            help='Field to update')
    parser.add_argument('newValue', type=str,
            help='New value to insert in field')
    return parser

def main(user_email, cid, fieldName, newValue):

    print(cid)
    logger.error("1")
    enq = CouchDBJobEnqueue()
    logger.error("2")
    timeout = 10000
    enq.queue_collection(args.cid,
                     timeout,
                     scripts.batch_update_couchdb_by_collection.update_field,
                     args.fieldName,
                     args.newValue
                     )
    logger.error("2")

if __name__=='__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.cid:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    field_list = [ x for x in args.field_list.split(',')]
    main(args.user_email,
            args.cid,
            args.fieldname,
            args.newValue,
            **kwargs)
