#! /bin/env python
import sys
import os
from harvester.post_processing.couchdb_runner import CouchDBWorker
from harvester.image_harvest import harvest_image_for_doc
from harvester.couchdb_init import get_couchdb
import couchdb #couchdb-python

from dplaingestion.selector import delprop

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)

def delete_field(doc, field):
    delprop(doc, field, keyErrorAsNone=True)

def delete_field_list(doc, field_list, cdb):
    for field in field_list:
        delete_field(doc, field)
    doc.save()

def def_args():
    import argparse
    parser = argparse.ArgumentParser(
            description='Delete fields from couchdb docs for a collection')
    parser.add_argument('user_email', type=str, help='user email')
    #parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument('cid', type=str,
            help='Collection ID')
    parser.add_argument('field_list', type=str,
            help='List of fields to delete over')
    return parser

def main(user_email, cid, field_list):
    worker = CouchDBWorker()
    timeout = 100000
    cdb = get_couchdb()
    worker.run_by_collection(cid,
                     delete_field_list,
                     field_list,
                     cdb
                     )

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.cid:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    field_list = [ x for x in args.field_list.split(',')]
    main(args.user_email,
            args.cid,
            args.url_couchdb_src,
            field_list,
            **kwargs)

