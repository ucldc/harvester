# -*- coding: utf-8 -*-
import urllib
import re
from xml.etree import ElementTree as ET
from collections import defaultdict
from .fetcher import Fetcher


class UCSF_XML_Fetcher(Fetcher):
    def __init__(self, url_harvest, extra_data, page_size=100):
        self.url_base = url_harvest
        self.page_size = page_size
        self.page_current = 1
        self.doc_current = 1
        self.docs_fetched = 0
        xml = urllib.urlopen(self.url_current).read()
        total = re.search('search-hits pages="(?P<pages>\d+)" page="'
                          '(?P<page>\d+)" total="(?P<total>\d+)"', xml)
        self.docs_total = int(total.group('total'))
        self.re_ns_strip = re.compile('{.*}(?P<tag>.*)$')

    @property
    def url_current(self):
        return '{0}&ps={1}&p={2}'.format(self.url_base, self.page_size,
                                         self.page_current)

    def _dochits_to_objset(self, docHits):
        '''Returns list of objecs.
        Objects are dictionary with tid, uri & metadata keys.
        tid & uri are strings
        metadata is a dict with keys matching UCSF xml metadata tags.
        '''
        objset = []
        for d in docHits:
            doc = d.find('./{http://legacy.library.ucsf.edu/document/1.1}'
                         'document')
            obj = {}
            obj['tid'] = doc.attrib['tid']
            obj['uri'] = doc.find('./{http://legacy.library.ucsf.edu/document/'
                                  '1.1}uri').text
            mdata = doc.find('./{http://legacy.library.ucsf.edu/document/1.1}'
                             'metadata')
            obj_mdata = defaultdict(list)
            for md in mdata:
                # strip namespace
                key = self.re_ns_strip.match(md.tag).group('tag')
                obj_mdata[key].append(md.text)
            obj['metadata'] = obj_mdata
            self.docs_fetched += 1
            objset.append(obj)
        return objset

    def next(self):
        '''get next objset, use etree to pythonize'''
        if self.doc_current == self.docs_total:
            if self.docs_fetched != self.docs_total:
                raise ValueError(
                   "Number of documents fetched ({0}) doesn't match \
                    total reported by server ({1})".format(
                        self.docs_fetched,
                        self.docs_total)
                    )
            else:
                raise StopIteration
        tree = ET.fromstring(urllib.urlopen(self.url_current).read())
        hits = tree.findall(
                ".//{http://legacy.library.ucsf.edu/search/1.0}search-hit")
        self.page_current += 1
        self.doc_current = int(hits[-1].attrib['number'])
        return self._dochits_to_objset(hits)


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
