# -*- coding: utf-8 -*-
import datetime
import time
import requests
import re
from xml.etree import ElementTree as ET
from collections import defaultdict
from .fetcher import Fetcher

class eMuseum_Fetcher(Fetcher):
    '''Paginates through eMuseum API XML search results until
        no more records are found'''

    def __init__(self, url_harvest, extra_data, **kwargs):
        self.url_base = url_harvest
        self.page_current = 1
        self.doc_current = 1
        self.docs_fetched = 0

    @property
    def url_current(self):
        quote_param = '/search/*/objects/xml?filter=approved%3Atrue&page='
        return '{0}{1}{2}'.format(self.url_base, quote_param,
                                  self.page_current)

    def _dochits_to_objset(self, docHits):
        '''Returns list of objects. Use 'name' attribute
        as JSON field name and 'value' as main 'text' field in CouchDB; save any other attributes as nested dict 'attrib'
        '''
        objset = []
        # iterate through docHits
        for d in docHits:
            fieldnumb = 1
            textnumb = 1
            obj = {}
            attributes = {}
            obj_mdata = defaultdict(list)
            for mdata in d:
                '''assign CouchDB fieldname from what's available, else use iterative unknown'''
                if 'name' in mdata.attrib:
                    md_fieldname = mdata.attrib['name']
                elif 'label' in mdata.attrib:
                    md_fieldname = mdata.attrib['label']
                else:
                    md_fieldname = ''.join(('unknown', str(fieldnumb)))
                    fieldnumb += 1
                for value in mdata:
                    if len(mdata) > 1:
                        txt_fieldname = ''.join(('text', str(textnumb)))
                        obj_mdata[txt_fieldname] = value.text
                        textnumb += 1
                    else:
                        obj_mdata['text'] = value.text
                for att in mdata.attrib:
                    if 'name' not in att:
                        att_dict = {att: mdata.attrib[att]}
                        attributes.update(att_dict)
                if mdata.attrib:
                    obj_mdata['attrib'] = dict(attributes)
                attributes.clear()
                obj[md_fieldname] = dict(obj_mdata)
                obj_mdata.clear()
            objset.append(obj)
        return objset

    def next(self):
        '''get next objset, use etree to pythonize. Stop
        iterating when no more <object>s are found'''
        dt_start = dt_end = datetime.datetime.now()
        xml = requests.get(self.url_current).text
        dt_end = datetime.datetime.now()
        time.sleep((dt_end-dt_start).total_seconds())
        tree = ET.fromstring(xml.encode('utf-8'))
        hits = tree.findall("objects/object")
        self.docs_total = len(hits)
        if self.docs_total == 0:
            raise StopIteration
        self.page_current += 1
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
