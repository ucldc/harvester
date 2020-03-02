#! /bin/env python
# -*- coding: utf-8 -*-

# Use when Calisphere Object URLs for a collection change, to generate
# a redirect file mapping 'old' (on SOLR-PROD) to 'new' (on SOLR-TEST) URLs.
#
# This script takes a Collection ID and a 'match' field in SOLR (i.e. best
# field to use for matching SOLR-PROD record to corresponding SOLR-TEST)
# and generates a JSON file containing the SOLR-PROD Solr ID value and
# 'match' field value for each object in given Collection, to use as input
# for external-redirect-generate-URL-redirect-map.py

import os
import argparse
import json
import requests
import solr

# to get rid of ssl key warning
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

URL_REGISTRY_API='https://registry.cdlib.org/api/v1/collection/'
SOLR_URL='https://solr.calisphere.org/solr/'
SOLR_API_KEY = os.environ.get('SOLR_API_KEY', '')

def get_solr_id(cid, matchVal, url_solr=SOLR_URL, api_key=SOLR_API_KEY):
    solr_auth = { 'X-Authentication-Token': api_key } if api_key else None
    url_collection = URL_REGISTRY_API + cid + '/'
    query = { 'q': 'collection_url:{}'.format(url_collection), 'rows':1000000,             'fl': 'id,{}'.format(matchVal)}
    solr_endpoint = url_solr + 'query'
    print "Getting ids from : {}\n{}".format(solr_endpoint, query)
    resp_obj =  requests.get(solr_endpoint,
                                    headers=solr_auth,
                                    params=query,
                                    verify=False)
    results = resp_obj.json()
    if results:
        with open('prod-URLs-{}.json'.format(cid), 'w') as foo:
            foo.write(json.dumps(results, sort_keys=True, indent=4))

def main(cid, matchVal):
    get_solr_id(cid, matchVal)

if __name__=='__main__':
    parser = argparse.ArgumentParser('This script takes a Collection ID ' \
'and a "match" field in SOLR (i.e. best field to use for matching SOLR-PROD ' \
'record to corresponding SOLR-TEST) and generates a JSON file containing ' \
'the SOLR-PROD Solr ID value and "match" field value for each object in ' \
'given Collection, to use as input for ' \
'external-redirect-generate-redirect-map.py' \
'\nUsage: external-redirect-get-solr_prod-id.py [Collection ID] [match field]' )
    parser.add_argument('cid')
    parser.add_argument('matchVal')
    argv = parser.parse_args()
    if not argv.cid:
        raise Exception(
            "Please include valid Registry Collection ID")
    if not argv.matchVal:
        raise Exception(
            "Please include valid SOLR metadata match field")

    print "CID: {} MATCH FIELD: {}".format(
        argv.cid,
        argv.matchVal)

    main(argv.cid, argv.matchVal)


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
