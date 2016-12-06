"""
Script to run the ingest process.


Usage:
    $ python run_ingest.py user-email url_collection_api
"""
import sys
import os
from dplaingestion.scripts import enrich_records
from dplaingestion.scripts import save_records
from dplaingestion.scripts import remove_deleted_records
from dplaingestion.scripts import dashboard_cleanup
from dplaingestion.scripts import check_ingestion_counts
import logbook
from harvester import fetcher
from harvester.config import config as config_harvest
from harvester.collection_registry_client import Collection
from harvester.sns_message import publish_to_harvesting
from redis import Redis
from rq import Queue
import harvester.image_harvest
from harvester.cleanup_dir import cleanup_work_dir

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)
IMAGE_HARVEST_TIMEOUT = 259200  # 3 days


def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('rq_queue', type=str, help='RQ queue to put job on')
    parser.add_argument(
        'url_api_collection',
        type=str,
        help='URL for the collection Django tastypie api resource')
    return parser


def queue_image_harvest(redis_host,
                        redis_port,
                        redis_pswd,
                        redis_timeout,
                        url_couchdb,
                        collection_key,
                        rq_queue,
                        object_auth=None):
    rQ = Queue(
        rq_queue,
        connection=Redis(
            host=redis_host,
            port=redis_port,
            password=redis_pswd,
            socket_connect_timeout=redis_timeout))
    job = rQ.enqueue_call(
        func=harvester.image_harvest.main,
        kwargs=dict(
            collection_key=collection_key,
            url_couchdb=url_couchdb,
            object_auth=object_auth,
            get_if_object=False),
        # default to no harvesting image
        # if object field has data
        timeout=IMAGE_HARVEST_TIMEOUT, )
    return job


def main(user_email,
         url_api_collection,
         log_handler=None,
         mail_handler=None,
         dir_profile='profiles',
         profile_path=None,
         config_file=None,
         redis_host=None,
         redis_port=None,
         redis_pswd=None,
         redis_timeout=600,
         rq_queue=None,
         run_image_harvest=False):
    '''Runs a UCLDC ingest process for the given collection'''
    cleanup_work_dir()  # remove files from /tmp
    emails = [user_email]
    if EMAIL_SYS_ADMIN:
        emails.extend([u for u in EMAIL_SYS_ADMIN.split(',')])
    if not mail_handler:
        mail_handler = logbook.MailHandler(
            EMAIL_RETURN_ADDRESS, emails, level='ERROR', bubble=True)
    mail_handler.push_application()
    if not config_file:
        config_file = os.environ.get('DPLA_CONFIG_FILE', 'akara.ini')
    if not (redis_host and redis_port and redis_pswd):
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

    log_handler.push_application()
    logger = logbook.Logger('run_ingest')
    ingest_doc_id, num_recs, dir_save, harvester = fetcher.main(
        emails,
        url_api_collection,
        log_handler=log_handler,
        mail_handler=mail_handler)
    if 'prod' in os.environ['DATA_BRANCH'].lower():
        if not collection.ready_for_publication:
            raise Exception(''.join(
                ('Collection {} is not ready for publication.',
                 ' Run on stage and QA first, then set',
                 ' ready_for_publication')).format(collection.id))
    logger.info("INGEST DOC ID:{0}".format(ingest_doc_id))
    logger.info('HARVESTED {0} RECORDS'.format(num_recs))
    logger.info('IN DIR:{0}'.format(dir_save))
    resp = enrich_records.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error enriching records {0}".format(resp))
        raise Exception('Failed during enrichment process: {0}'.format(resp))
    logger.info('Enriched records')

    resp = save_records.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error saving records {0}".format(str(resp)))
        raise Exception("Error saving records {0}".format(str(resp)))
    logger.info("SAVED RECS")

    resp = remove_deleted_records.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error deleting records {0}".format(resp))
        raise Exception("Error deleting records {0}".format(resp))

    resp = check_ingestion_counts.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error checking counts {0}".format(resp))
        raise Exception("Error checking counts {0}".format(resp))

    resp = dashboard_cleanup.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error cleaning up dashboard {0}".format(resp))
        raise Exception("Error cleaning up dashboard {0}".format(resp))

    publish_to_harvesting('Harvesting completed for {}'.format(collection.id),
                          'Finished harvest for {}'.format(collection.id))
    # the image_harvest should be a separate job, with a long timeout
    if run_image_harvest:
        job = queue_image_harvest(
            config['redis_host'],
            config['redis_port'],
            config['redis_password'],
            config['redis_connect_timeout'],
            config['couchdb_url'],
            collection.id,
            rq_queue,
            object_auth=collection.auth)
        logger.info("Started job for image_harvest:{}".format(job.result))

    log_handler.pop_application()
    mail_handler.pop_application()


if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection or not args.rq_queue:
        parser.print_help()
        sys.exit(27)
    conf = config_harvest()
    main(
        args.user_email,
        args.url_api_collection,
        redis_host=conf['redis_host'],
        redis_port=conf['redis_port'],
        redis_pswd=conf['redis_password'],
        redis_timeout=conf['redis_connect_timeout'],
        rq_queue=args.rq_queue)
