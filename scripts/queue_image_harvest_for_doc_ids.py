# -*- coding: utf-8 -*-
#! /bin/env python
import sys
import os
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.image_harvest import harvest_image_for_doc

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)
IMAGE_HARVEST_TIMEOUT = 144000


def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument('--object_auth', nargs='?',
            help='HTTP Auth needed to download images - username:password')
    parser.add_argument('--url_couchdb', nargs='?',
            help='Override url to couchdb')
    parser.add_argument('--timeout', nargs='?',
            help='set image harvest timeout in sec (14400 - 4hrs default)')
    parser.add_argument('doc_ids', type=str,
            help='Comma separated CouchDB document ids')
    return parser

def main(user_email, doc_ids, url_couchdb=None):
    enq = CouchDBJobEnqueue()
    timeout = 10000
    enq.queue_list_of_ids(doc_ids,
                     timeout,
                     harvest_image_for_doc,
                     url_couchdb=url_couchdb,
                     force=True
                     )

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.doc_ids:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    id_list = [s for s in args.doc_ids.split(',')]
    main(args.user_email,
            id_list,
            **kwargs)




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
