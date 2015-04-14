
""""
Usage:
    $ python run_ingest.py user-email url_collection_api
"""
import sys
import os
from email.mime.text import MIMEText
from dplaingestion.scripts import enrich_records
from dplaingestion.scripts import save_records
from dplaingestion.scripts import remove_deleted_records
from dplaingestion.scripts import dashboard_cleanup
from dplaingestion.scripts import check_ingestion_counts
import logbook
from harvester import fetcher
from harvester.config import config as config_harvest
from harvester.collection_registry_client import Collection
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from rq import Queue
from harvester import solr_updater
from harvester import grab_solr_index
import harvester.image_harvest

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)
IMAGE_HARVEST_TIMEOUT = 144000


def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('url_api_collection', type=str,
            help='URL for the collection Django tastypie api resource')
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


def queue_image_harvest(redis_host, redis_port, redis_pswd, redis_timeout,
                        collection_key, url_couchdb=None, object_auth=None,
                        no_get_if_object=False,
                        harvest_timeout=IMAGE_HARVEST_TIMEOUT):
    rQ = Queue(connection=Redis(host=redis_host, port=redis_port,
                                password=redis_pswd,
                                socket_connect_timeout=redis_timeout)
    )
    job = rQ.enqueue_call(func=harvester.image_harvest.main,
                          kwargs=dict(collection_key=collection_key,
                                      url_couchdb=url_couchdb,
                                      object_auth=object_auth,
                                      no_get_if_object=no_get_if_object),
                                      timeout=harvest_timeout
                          )
    return job


def main(user_email, url_api_collection, log_handler=None,
         mail_handler=None, dir_profile='profiles', profile_path=None,
         config_file='akara.ini',
         **kwargs):
    '''Runs a UCLDC ingest process for the given collection'''
    emails = [user_email]
    if EMAIL_SYS_ADMIN:
        emails.extend([u for u in EMAIL_SYS_ADMIN.split(',')])
    if not mail_handler:
        mail_handler = logbook.MailHandler(EMAIL_RETURN_ADDRESS,
                                           emails,
                                           level='ERROR',
                                           bubble=True)
    mail_handler.push_application()
    config = config_harvest(config_file=config_file)

    try:
        collection = Collection(url_api_collection)
    except Exception, e:
        msg = 'Exception in Collection {}, init {}'.format(url_api_collection,
                                                           str(e))
        logbook.error(msg)
        raise e
    if not log_handler:
        log_handler = logbook.StderrHandler(level='DEBUG')

    print log_handler
    log_handler.push_application()
    print config
    # the image_harvest should be a separate job, with a long timeout
    job = queue_image_harvest(config.redis_host, config.redis_port,
                              config.redis_pswd, config.redis_timeout,
                              collection_key=collection.provider,
                              object_auth=collection.auth,
                              **kwargs)

    log_handler.pop_application()
    mail_handler.pop_application()

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection:
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
            args.url_api_collection,
            **kwargs)
