import sys
import os
from harvester.post_processing.couchdb_runner import CouchDBWorker
from harvester.image_harvest import harvest_image_for_doc
from harvester.couchdb_init import get_couchdb
import couchdb #couchdb-python

from dplaingestion.selector import getprop, setprop

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)

def copy_fields_for_doc(doc, couchdb_src, field_list,
        couchdb_dest):
    doc_id = doc['_id']
    doc_src = couchdb_src[doc_id]
    for field in field_list:
        value_src = getprop(doc_src, field, keyErrorAsNone=True)
        print "SRC ID:{} FIELD {} VALUE:{}".format(doc_id, field, value_src)
        if value_src:
            setprop(doc, field, value_src)
            couchdb_dest.save(doc)
        else:
            print 'SRC DOC {} missing {}'.format(doc_id, field)

def def_args():
    import argparse
    parser = argparse.ArgumentParser(
            description='Copy fields from one couchdb to another')
    parser.add_argument('user_email', type=str, help='user email')
    #parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument('cid', type=str,
            help='Collection ID')
    parser.add_argument('url_couchdb_src', type=str,
            help='Source couchdb')
    parser.add_argument('field_list', type=str,
            help='List of fields to copy over')
    parser.add_argument('--url_couchdb_dest', type=str,
            help='Destination couchdb (defaults to environment couch)')
    return parser

def main(user_email, cid, url_couchdb_src, field_list, url_couchdb_dest=None):
    worker = CouchDBWorker()
    timeout = 100000
    cdb_src = get_couchdb(url=url_couchdb_src, username=False, password=False)
    if url_couchdb_dest:
        cdb_dest= get_couchdb(url=url_couchdb_dest)
    else:
        cdb_dest= get_couchdb()
    worker.run_by_collection(cid,
                     copy_fields_for_doc,
                     cdb_src,
                     field_list,
                     cdb_dest
                     )

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.cid:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    field_list = [ x for x in args.field_list.split(',')]
    if args.url_couchdb_dest:
        kwargs['url_couchdb_dest'] = args.url_couchdb_dest
    main(args.user_email,
            args.cid,
            args.url_couchdb_src,
            field_list,
            **kwargs)
