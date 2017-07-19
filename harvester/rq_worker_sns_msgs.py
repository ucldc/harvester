'''A custom rq worker class to add start & stop SNS messages to all jobs'''

import logging
import os
import re
from rq.worker import Worker
from harvester.sns_message import publish_to_harvesting

logger = logging.getLogger(__name__)

# need tuple of tuple pairs, regex string to msg template
# the regex needs to match the function called
# and parse out the collection id
message_match_list = (
    ("sync_couch_collection_to_solr\(collection_key='(?P<cid>\d+)'\)",
     "{status}: Sync from Couchdb to Solr {env} on "
     ":worker: {worker} for CID: {cid}"),
    ("run_ingest.main.*/collection/(?P<cid>\d+)/",
     "{status}: Metadata Harvest to Couchdb {env} on "
     ":worker: {worker} for CID: {cid}"),
    ("image_harvest.main\(collection_key=.*'(?P<cid>\d+)'",
     "{status}: Image Harvest {env} on "
     ":worker: {worker} for CID: {cid}"),
    ("delete_solr_collection\(collection_key='(?P<cid>\d+)'\)",
     "{status}: Delete from Solr {env} on "
     ":worker: {worker} for CID: {cid}"),
    ("s3stash.stash_collection.main\(registry_id=(?P<cid>\d+)",
     "{status}: Nuxeo Deep Harvest on "
     ":worker: {env} {worker} for CID: {cid}"),
    ("delete_collection\((?P<cid>\d+)\)",
     "{status}: Delete CouchDB {env} on "
     ":worker: {worker} for CID: {cid}"),
    ("couchdb_sync_db_by_collection.main\(url_api_collection="
     "'https://registry.cdlib.org/api/v1/collection/(?P<cid>\d+)/'",
     "{status}: Sync CouchDB to production on "
     ":worker: {env} {worker} for CID: {cid}"),
    ("<fn--name> -- parse out collection id as cid ",
     "replacement template for message- needs cid env variables"))

re_object_auth = re.compile("object_auth=(\('\w+', '\S+'\))")`

def create_execute_job_message(status, worker, job):
    '''Create a formatted message for the job.
    Searches for a match to function, then fills in values
    '''
    env = os.environ.get('DATA_BRANCH')
    message_template = "{status}: {env} {worker} {job}"
    message = message_template.format(
        status=status, env=env, worker=worker, job=job.description)
    subject = message
    for regex, msg_template in message_match_list:
        m = re.search(regex, job.description)
        if m:
            mdict = m.groupdict()
            subject = msg_template.format(
                status=status,
                env=env,
                worker=worker,
                cid=mdict.get('cid', '?'))
            message = ''.join((subject, '\n', job.description))
            break
    message = re_object_auth.sub('object_auth=<REDACTED>', message)
    return subject, message


def exception_to_sns(job, *exc_info):
    '''Make an exception handler to report exceptions to SNS msg queue'''
    subject = 'FAILED: job {}'.format(job.description)
    message = 'ERROR: job {} failed\n{}'.format(job.description, exc_info[1])
    logging.error(message)
    publish_to_harvesting(subject, message)


class SNSWorker(Worker):
    def execute_job(self, job, queue):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        worker_name = (self.key.rsplit(':', 1)[1]).rsplit('.', 1)[0]
        subject, msg = create_execute_job_message("Started", worker_name, job)
        logging.info(msg)
        publish_to_harvesting(subject, msg)
        self.set_state('busy')
        self.fork_work_horse(job, queue)
        self.monitor_work_horse(job)
        subject, msg = create_execute_job_message("Completed", worker_name,
                                                  job)
        logging.info(msg)
        publish_to_harvesting(subject, msg)
        self.set_state('idle')
