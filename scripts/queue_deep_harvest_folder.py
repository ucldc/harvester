#! /bin/env python
# -*- coding: utf-8 -*-
import sys
import logbook
from rq import Queue
from redis import Redis
from harvester.config import parse_env

JOB_TIMEOUT = 345600  # 96 hrs


def queue_deep_harvest_folder(redis_host,
                       redis_port,
                       redis_password,
                       redis_timeout,
                       rq_queue,
                       path,
                       replace=True,
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
        func='s3stash.stash_folder.main',
        args=(path,),
        kwargs=dict(replace=replace),
        timeout=timeout)
    return job


def main(path, log_handler=None, rq_queue='normal-stage',
        timeout=JOB_TIMEOUT, replace=True):
    ''' Queue a deep harvest of a nuxeo folder on a worker'''
    if not log_handler:
        log_handler = logbook.StderrHandler(level='DEBUG')
    log_handler.push_application()
    queue_deep_harvest_folder(
        config['redis_host'],
        config['redis_port'],
        config['redis_password'],
        config['redis_connect_timeout'],
        rq_queue=rq_queue,
        path=path,
        replace=replace,
        timeout=timeout)
    log_handler.pop_application()


def def_args():
    import argparse
    parser = argparse.ArgumentParser(
        description='Queue a Nuxeo deep harvesting job for a given folder (path)')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument('path', type=str, help='Nuxeo path')
    parser.add_argument(
        '--replace',
        action='store_true',
        help='replace files on s3 if they already exist')
    parser.add_argument('--job_timeout', type=int, default=JOB_TIMEOUT,
                        help='Timeout for the RQ job')
    return parser


if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args()
    config = parse_env(None)
    if not args.path or not args.rq_queue:
        parser.print_help()
        sys.exit(27)
    if args.job_timeout:
        job_timeout = args.job_timeout
    else:
        job_timeout = JOB_TIMEOUT
    main(args.path, rq_queue=args.rq_queue, replace=args.replace,
            timeout=job_timeout)

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
