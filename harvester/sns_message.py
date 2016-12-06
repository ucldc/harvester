import boto3
import botocore.exceptions


def publish_to_harvesting(subject, message):
    '''Publish a SNS message to the harvesting topic channel'''
    client = boto3.client('sns')
    # NOTE: this appears to raise exceptions if problem
    try:
        client.publish(
            TopicArn='arn:aws:sns:us-west-2:563907706919:ucldc-harvesting',
            Message=message,
            Subject=subject
            )
    except botocore.exceptions.BotoCoreError, e:
        import sys
        print >> sys.stderr, 'Exception in Boto SNS: {}'.format(e)
