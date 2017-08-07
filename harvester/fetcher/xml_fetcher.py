# -*- coding: utf-8 -*-
import urllib
import re
from xml.etree import ElementTree as ET
from collections import defaultdict
from .fetcher import Fetcher
from itertools import islice


class XML_Fetcher(Fetcher):
    '''Harvests 1,000 records at a time from
    static XML document at url_harvest'''

    def __init__(self, url_harvest, extra_data):
        self.url_base = url_harvest
        self.doc_current = 1
        self.docs_fetched = 0
        xml = urllib.urlopen(self.url_base).read()
        total = re.findall('<record>', xml)
        self.docs_total = len(total)
        # Use etree to pythonize
        tree = ET.fromstring(xml)
        self.hits = tree.findall(".//record")
        self.re_ns_strip = re.compile('{.*}(?P<tag>.*)$')

    def _dochits_to_objset(self, docHits):
        '''Returns list of objects.
        '''
        objset = []
        # Use islice w/ self.docs_fetched  to iterate through docHits
        for d in islice(docHits, self.docs_fetched, None):
            obj = {}
            obj_mdata = defaultdict(list)
            for mdata in d.iter():
                # Find elements w/ text value and no children
                if mdata.text and (len(mdata) is 0):
                    if self.re_ns_strip.match(mdata.tag):
                        key = self.re_ns_strip.match(mdata.tag).group('tag')
                    else:
                        key = mdata.tag
                    obj_mdata[key].append(mdata.text)
                # Find elements w/ attribute value and no children
                if mdata.attrib and (len(mdata) is 0):
                    for elem in mdata.attrib:
                        obj_mdata[elem].append(mdata.get(elem))
            obj['metadata'] = dict(obj_mdata)
            objset.append(obj)
            self.docs_fetched += 1
            self.doc_current += 1
            # Once 1000 records in objset, reset self.doc_current & break/return
            if self.doc_current == 1000 or self.docs_fetched >= self.docs_total:
                self.doc_current = 0
                break
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
