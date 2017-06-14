'''A custom rq worker class to add start & stop SNS messages to all jobs'''

import logging
from rq.worker import Worker
from harvester.sns_message import publish_to_harvesting

logger = logging.getLogger(__name__)


def exception_to_sns(job, *exc_info):
    '''Make an exception handler to report exceptions to SNS msg queue'''
    subject = 'FAILED: job {}'.format(job.description)
    message = 'ERROR: job {} failed\n{}'.format(
        job.description,
        exc_info[1])
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
        subject = 'Worker {} starting job {}'.format(
            worker_name,
            job.description)
        publish_to_harvesting(subject, subject)
        self.set_state('busy')
        self.fork_work_horse(job, queue)
        self.monitor_work_horse(job)
        subject = 'Worker {} finished job {}'.format(
            worker_name,
            job.description)
        publish_to_harvesting(subject, subject)
        self.set_state('idle')
