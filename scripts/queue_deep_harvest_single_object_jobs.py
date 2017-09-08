#! /bin/env python
# -*- coding: utf-8 -*-
import sys
import logbook
from rq import Queue
from redis import Redis
from harvester.config import parse_env
from harvester.collection_registry_client import Collection
from deepharvest.deepharvest_nuxeo import DeepHarvestNuxeo

JOB_TIMEOUT = 345600  # 96 hrs

def queue_deep_harvest_path(redis_host,
                       redis_port,
                       redis_password,
                       redis_timeout,
                       rq_queue,
                       path,
                       replace=False,
                       timeout=JOB_TIMEOUT):
    '''Queue job onto RQ queue'''
    rQ = Queue(
        rq_queue,
        connection=Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            socket_connect_timeout=redis_timeout))
    job = rQ.enqueue_call(
        func='s3stash.stash_single_rqworker.stash_file',
        args=(path, ),
        kwargs={'replace': replace},
        timeout=timeout)
    job = rQ.enqueue_call(
        func='s3stash.stash_single_rqworker.stash_image',
        args=(path,),
        kwargs={'replace': replace},
        timeout=timeout)
    job = rQ.enqueue_call(
        func='s3stash.stash_single_rqworker.stash_media_json',
        args=(path,),
        kwargs={'replace': replace},
        timeout=timeout)
    job = rQ.enqueue_call(
        func='s3stash.stash_single_rqworker.stash_thumb',
        args=(path,),
        kwargs={'replace': replace},
        timeout=timeout)


def main(collection_ids, rq_queue='dh-q', config=None, pynuxrc=None,
        replace=False, timeout=JOB_TIMEOUT, log_handler=None):
    ''' Queue a deep harvest of a nuxeo object on a worker'''
    if not log_handler:
        log_handler = logbook.StderrHandler(level='DEBUG')
    log_handler.push_application()
    log = logbook.Logger('QDH')
    for cid in [x for x in collection_ids.split(';')]:
        url_api = ''.join(('https://registry.cdlib.org/api/v1/collection/',
                    cid, '/'))
        coll = Collection(url_api)

        dh = DeepHarvestNuxeo(coll.harvest_extra_data, '', pynuxrc=pynuxrc)

        for obj in dh.fetch_objects():
            log.info('Queueing TOPLEVEL {} :-: {}'.format(
                obj['uid'],
                obj['path']))
            # deep harvest top level object
            queue_deep_harvest_path(
                config['redis_host'],
                config['redis_port'],
                config['redis_password'],
                config['redis_connect_timeout'],
                rq_queue=rq_queue,
                path=obj['path'],
                replace=replace,
                timeout=timeout)
            # deep harvest component sub-objects
            for c in dh.fetch_components(obj):
                log.info('Queueing {} :-: {}'.format(
                    c['uid'],
                    c['path']))
                queue_deep_harvest_path(
                    config['redis_host'],
                    config['redis_port'],
                    config['redis_password'],
                    config['redis_connect_timeout'],
                    rq_queue=rq_queue,
                    path=c['path'],
                    replace=replace,
                    timeout=timeout)

    log_handler.pop_application()

def def_args():
    import argparse
    parser = argparse.ArgumentParser(
        description='Queue a Nuxeo deep harvesting job for a single object')
    parser.add_argument('--rq_queue', type=str, help='RQ Queue to put job in',
            default='dh-q')
    parser.add_argument(
        'collection_ids', type=str, help='Collection ids, ";" delimited')
    #parser.add_argument(
    #    'path', type=str, help='Nuxeo path to root folder')
    parser.add_argument('--job_timeout', type=int, default=JOB_TIMEOUT,
        help='Timeout for the RQ job')
    parser.add_argument(
        '--pynuxrc', default='~/.pynuxrc', help='rc file for use by pynux')
    parser.add_argument(
        '--replace',
        action='store_true',
        help='replace files on s3 if they already exist')

    return parser

if __name__=='__main__':
    parser = def_args()
    args = parser.parse_args()
    config = parse_env(None)
    main(args.collection_ids, rq_queue=args.rq_queue, config=config,
        replace=args.replace, timeout=args.job_timeout,
        pynuxrc=args.pynuxrc)

# Copyright © 2017, Regents of the University of California
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
