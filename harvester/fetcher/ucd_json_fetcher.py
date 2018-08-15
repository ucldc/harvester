# -*- coding: utf-8 -*-
from collections import defaultdict
from .fetcher import Fetcher
import urllib
import extruct
import requests
import re
from xml.etree import ElementTree as ET
from w3lib.html import get_base_url
from rdflib.plugin import register, Serializer
register('json-ld', Serializer, 'rdflib_jsonld.serializer', 'JsonLDSerializer')


class UCD_JSON_Fetcher(Fetcher):
    '''Retrieve JSON from each page listed on
    UC Davis XML sitemap given as url_harvest'''

    def __init__(self, url_harvest, extra_data, **kwargs):
        self.url_base = url_harvest
        self.docs_fetched = 0
        xml = urllib.urlopen(self.url_base).read()
        total = re.findall('<url>', xml)
        self.docs_total = len(total)

        # need to declare the sitemap namespace for ElementTree
        tree = ET.fromstring(xml)
        namespaces = {'xmlns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        self.hits = tree.findall('.//xmlns:url/xmlns:loc', namespaces)

    def _dochits_to_objset(self, docHits):
        '''Returns list of objects.
        '''

        objset = []
        for d in docHits:
            r = requests.get(d.text)

            # get JSON-LD from the page
            base_url = get_base_url(r.text)
            data = extruct.extract(r.text, base_url)
            jsld = data.get('json-ld')[0]

            obj = {}
            obj_mdata = defaultdict(list)
            for mdata in jsld:
                obj_mdata[mdata] = jsld[mdata]
            obj['metadata'] = dict(obj_mdata)
            objset.append(obj)
            self.docs_fetched += 1
        return objset

    def next(self):
        '''get next objset, use etree to pythonize'''
        if self.docs_fetched >= self.docs_total:
            raise StopIteration
        return self._dochits_to_objset(self.hits)

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
