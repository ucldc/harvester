# -*- coding: utf-8 -*-
import os
import json
import requests
from requests.auth import HTTPDigestAuth
from collections import OrderedDict

def get_solr_json(solr_url,
        query,
        api_key=None,
        digest_user=None,
        digest_pswd=None):
    '''get the solr json response for the given URL.
    Use the requests library. The production API uses headers for auth,
    while the ingest environment uses http digest auth
    Returns an python object created from the json response.
    Query is a dictionary of parameters
    '''
    solr_auth = { 'X-Authentication-Token': api_key } if api_key else None
    digest_auth = HTTPDigestAuth(
            digest_user, digest_pswd) if digest_user else None
    return json.loads(requests.get(solr_url,
                                    headers=solr_auth,
                                    auth=digest_auth,
                                    params=query,
                                    verify=False).content)

def create_facet_dict(json_results, facet_field):
    '''Create a dictionary consisting of keys = facet_field_values, values =
    facet_field coutns
    Takes a json result that must have the given facet field in it & the
    '''
    results = json_results.get('facet_counts').get('facet_fields').get(facet_field)
    #zip into array with elements of ('collection_data_value', count)
    facet_list = zip(*[iter(results)]*2)
    d = OrderedDict()
    for val, count in facet_list:
        if count > 0: #reject ones that have 0 records?
            d[val] = count
    return d

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

