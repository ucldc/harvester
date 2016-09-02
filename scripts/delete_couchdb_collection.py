#! /bin/env python
import sys
import argparse
import os
from harvester.couchdb_init import get_couchdb
import couchdb
from harvester.couchdb_sync_db_by_collection import delete_collection

def confirm_deletion(cid):
    prompt = "Are you sure you want to delete all couchdb " + \
             "documents for %s? yes to confirm\n" % cid
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        else:
            return False

if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description='Delete all documents in given collection')
    parser.add_argument('collection_id',
                        help='Registry id for the collection')
    parser.add_argument('--yes', action='store_true',
                     help="Don't prompt for deletion, just do it")
    args = parser.parse_args(sys.argv[1:])
    if args.yes or confirm_deletion(args.collection_id):
        print 'DELETING COLLECTION {}'.format(args.collection_id)
        num, deleted_ids = delete_collection(args.collection_id)
        print "DELETED {} DOCS".format(num)
    else:
        print "Exiting without deleting"
