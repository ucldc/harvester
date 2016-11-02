#! /bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import logbook
from harvester.config import config as config_harvest
from redis import Redis
from rq import Queue

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)
IMAGE_HARVEST_TIMEOUT = 144000


def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument(
        'collection_key',
        type=int,
        help='URL for the collection Django tastypie api resource')
    return parser


def queue_image_harvest(redis_host,
                        redis_port,
                        redis_password,
                        redis_timeout,
                        rq_queue,
                        collection_key,
                        url_couchdb=None,
                        object_auth=None,
                        get_if_object=False,
                        harvest_timeout=IMAGE_HARVEST_TIMEOUT):
    rQ = Queue(
        rq_queue,
        connection=Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            socket_connect_timeout=redis_timeout))
    job = rQ.enqueue_call(
        func=harvester.scripts.sync_couch_collection_to_solr.main
        kwargs=dict(
            collection_key=collection_key,
            )
    return job


def main(user_email,
         collection_key,
         log_handler=None,
         mail_handler=None,
         config_file='akara.ini',
         rq_queue=None,
         **kwargs):
    '''Runs a UCLDC sync to solr for collection key'''
    emails = [user_email]
    if EMAIL_SYS_ADMIN:
        emails.extend([u for u in EMAIL_SYS_ADMIN.split(',')])
    if not mail_handler:
        mail_handler = logbook.MailHandler(
            EMAIL_RETURN_ADDRESS, emails, level='ERROR', bubble=True)
    mail_handler.push_application()
    config = config_harvest(config_file=config_file)

    if not log_handler:
        log_handler = logbook.StderrHandler(level='DEBUG')

    log_handler.push_application()
    print config
    # the image_harvest should be a separate job, with a long timeout
    job = queue_image_harvest(
        config['redis_host'],
        config['redis_port'],
        config['redis_password'],
        config['redis_connect_timeout'],
        rq_queue=rq_queue,
        collection_key=collection_key,
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
    if args.timeout:
        kwargs['harvest_timeout'] = int(args.timeout)
    if args.get_if_object:
        kwargs['get_if_object'] = args.get_if_object
    main(
        args.collection_key,
        rq_queue=args.rq_queue,
        **kwargs)


# Copyright © 2016, Regents of the University of California
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# - Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# - Neither the name of the University of California nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
