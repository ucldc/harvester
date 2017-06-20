import os
import boto3
import botocore.exceptions
import logging
import requests


logger = logging.getLogger(__name__)


def format_results_subject(cid, registry_action):
    '''Format the "subject" part of the harvesting message for
    results from the various processes.

    Results: [Action from Registry] on [Worker IP] for Collection ID [###]
    '''
    if '{env}' in registry_action:
        registry_action = registry_action.format(
            env=os.environ.get('DATA_BRANCH'))
    resp = requests.get('http://169.254.169.254/latest/meta-data/local-ipv4')
    worker_ip = resp.text
    worker_id = worker_ip.replace('.', '-')
    return 'Results: {} on {} for CID: {}'.format(
        registry_action,
        worker_id,
        cid)


def publish_to_harvesting(subject, message):
    '''Publish a SNS message to the harvesting topic channel'''
    client = boto3.client('sns')
    # NOTE: this appears to raise exceptions if problem
    try:
        client.publish(
            TopicArn=os.environ['ARN_TOPIC_HARVESTING_REPORT'],
            Message=message,
            Subject=subject if len(subject) <= 100 else subject[:100]
            )
    except botocore.exceptions.BotoCoreError, e:
        logger.error('Exception in Boto SNS: {}'.format(e))
