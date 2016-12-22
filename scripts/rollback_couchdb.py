# -*- coding: utf-8 -*-
'''Rollback docs to previous rev for given collection'''
import os
import json
import couchdb
from harvester.post_processing.couchdb_runner import get_collection_doc_ids
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

couch_stg = 'https://{}harvest-stg.cdlib.org/couchdb'
couch_prd = 'https://{}harvest-prd.cdlib.org/couchdb'


def rollback_collection_docs(collection_key, dry_run=False, auth=''):
    dids = get_collection_doc_ids(collection_key, couch_stg.format(''))
    url = couch_stg.format(auth)
    cserver_stg = couchdb.Server(url)
    cdb_stg = cserver_stg['ucldc']
    i = 0
    try:
        os.mkdir(collection_key)
    except OSError:
        pass
    for did in dids:
        print did
        doc = cdb_stg[did]
        revs = cdb_stg.revisions(doc['_id'])
        rlist = []
        for i, x in enumerate(revs):
            print x['_rev']
            rlist.append(x)
            if i > 1:
                break
        rev_doc = rlist[2]
        rev_doc['_rev'] = rlist[0]['_rev']
        with open('{}/{}_old.json'.format(collection_key, doc['id']),
                  'w') as foo:
            json.dump(rlist[0], foo)
        # revert by updating
        if not dry_run:
            pass
            cdb_stg[did] = rev_doc


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
            description='Compare stage to production couchdb collection')
    parser.add_argument('collection_key', type=str,
                        help='Numeric ID for collection')
    args = parser.parse_args()
    auth = ''
    if os.environ.get('COUCHDB_USER'):
        auth = '{}:{}@'.format(
                os.environ['COUCHDB_USER'],
                os.environ.get('COUCHDB_PASSWORD'))
    print "AUTH:{}".format(auth)
    results = rollback_collection_docs(args.collection_key, auth=auth)
    print results


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
