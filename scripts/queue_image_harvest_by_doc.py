import sys
import os
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.image_harvest import harvest_image_for_doc

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)
IMAGE_HARVEST_TIMEOUT = 144000


def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('cid', type=str,
            help='Collection ID')
    parser.add_argument('--object_auth', nargs='?',
            help='HTTP Auth needed to download images - username:password')
    parser.add_argument('--url_couchdb', nargs='?',
            help='Override url to couchdb')
    parser.add_argument('--timeout', nargs='?',
            help='set image harvest timeout in sec (14400 - 4hrs default)')
    parser.add_argument('--no_get_if_object', action='store_true',
                        default=False,
            help='Should image harvester not get image if the object field exists for the doc (default: False, always get)')
    return parser

def main(user_email, cid, url_couchdb=None):
    enq = CouchDBJobEnqueue()
    timeout = 10000
    enq.queue_collection(cid,
                     timeout,
                     harvest_image_for_doc,
                     url_couchdb=url_couchdb,
                     )

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.cid:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    if args.object_auth:
        kwargs['object_auth'] = args.object_auth
    if args.timeout:
        kwargs['harvest_timeout'] = int(args.timeout)
    if args.no_get_if_object:
        kwargs['no_get_if_object'] = args.no_get_if_object
    main(args.user_email,
            args.cid,
            **kwargs)

