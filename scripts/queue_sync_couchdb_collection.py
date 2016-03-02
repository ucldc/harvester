# -*- coding: utf-8 -*-
import sys
import logbook
from rq import Queue
from redis import Redis
from harvester.config import parse_env
from harvester.collection_registry_client import Collection
import harvester.couchdb_sync_db_by_collection

JOB_TIMEOUT = 28800 #8 hrs
URL_REMOTE_COUCHDB = 'https://52.10.100.133/couchdb'

def queue_couch_sync(redis_host, redis_port, redis_password, redis_timeout,
                        rq_queue,
                        url_api_collection,
                        url_remote_couchdb=None,
                        timeout=JOB_TIMEOUT):
    '''Queue job onto RQ queue'''
    rQ = Queue(rq_queue, connection=Redis(host=redis_host, port=redis_port,
                                password=redis_password,
                                socket_connect_timeout=redis_timeout)
    )
    job = rQ.enqueue_call(func=harvester.couchdb_sync_db_by_collection.main,
                          kwargs=dict(url_remote_couchdb=url_remote_couchdb,
                                      url_api_collection=url_api_collection),
                                      timeout=timeout
                          )
    return job


def main(url_api_collection, url_remote_couchdb=URL_REMOTE_COUCHDB,
        log_handler=None):
    '''This should only be run in production env!
    Queue is hard coded to normal-prod so that it will be run there
    '''
    config = parse_env(None)
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
    job = queue_couch_sync(config['redis_host'], config['redis_port'],
                              config['redis_password'],
                              config['redis_connect_timeout'],
                              rq_queue='normal-prod',
                              url_api_collection=url_api_collection,
                              url_remote_couchdb=url_remote_couchdb,
                              )

    log_handler.pop_application()

def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Sync collection to production couchdb')
    parser.add_argument('url_api_collection', type=str,
            help='URL for the collection Django tastypie api resource')
    return parser

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.url_api_collection:
        parser.print_help()
        sys.exit(27)
    main(args.url_api_collection, URL_REMOTE_COUCHDB)
    

"""
Copyright © 2016, Regents of the University of California
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from this
  software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""
