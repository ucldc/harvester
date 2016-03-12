#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" for a given collection, identify records in first index that aren't 
in the other.
Meant to run based on results of the qa report
"""

import sys
import argparse
import re
from datetime import date
import itertools
import json
import xlsxwriter
import requests
from pprint import pprint as pp
import ConfigParser
import time
import datetime
import os
from get_solr_json import get_solr_json

base_query = {
    'rows': 1000000, # get all rows
    'fl': 'id,harvest_id_s',
}

def get_collection_solr_json(solr_url, collection_url, **kwargs):
    q = base_query.copy().update({'collection_url':collection_url})
    return get_solr_json(solr_url, q, **kwargs)

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('collection_url', nargs=1,)

    if argv is None:
        argv = parser.parse_args()
    collection_url = argv.collection_url[0]
    config = ConfigParser.SafeConfigParser()
    config.read('report.ini')

    #get calisphere current index data
    solr_url = config.get('calisphere', 'solrUrl')
    api_key = config.get('calisphere', 'solrAuth')
    production_json = get_collection_solr_json(solr_url, collection_url,
            api_key=api_key)
    prod_docs = production_json.get('response').get('docs')
    solr_url = config.get('new-index', 'solrUrl')
    digest_user = config.get('new-index', 'digestUser')
    digest_pswd = config.get('new-index', 'digestPswd')
    new_json = get_collection_solr_json(solr_url, collection_url,
            digest_user=digest_user, digest_pswd=digest_pswd)
    new_docs = new_json.get('response').get('docs')
    prod_doc_set = set([ x['id'] for x in prod_docs])
    new_doc_set = set([x['id'] for x in new_docs])
    missing_doc_set = prod_doc_set.difference(new_doc_set)
    missing_docs = []
    for sid in missing_doc_set:
        d = [ x for x in prod_docs if x['id'] == sid]
        missing_docs.append(d[0])

    pp(missing_docs)


if __name__ == "__main__":
    sys.exit(main())

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
