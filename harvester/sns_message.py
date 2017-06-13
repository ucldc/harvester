import os
import boto3
import botocore.exceptions
import logging

logger = logging.getLogger(__name__)

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
