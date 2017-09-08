# -*- coding: utf-8 -*-
import urllib
from collections import defaultdict
from xml.etree import ElementTree as ET
import time
import logbook
import requests
from requests.packages.urllib3.exceptions import DecodeError
from .fetcher import Fetcher

CONTENT_SERVER = 'http://content.cdlib.org/'


class BunchDict(dict):
    def __init__(self, **kwds):
        dict.__init__(self, kwds)
        self.__dict__ = self


class OAC_XML_Fetcher(Fetcher):
    '''Fetcher for the OAC
    The results are returned in 3 groups, image, text and website.
    Image and text are the ones we care about.
    '''

    def __init__(self, url_harvest, extra_data, docsPerPage=100, **kwargs):
        super(OAC_XML_Fetcher, self).__init__(url_harvest, extra_data)
        self.logger = logbook.Logger('FetcherOACXML')
        self.docsPerPage = docsPerPage
        self.url = self.url + '&docsPerPage=' + str(self.docsPerPage)
        self._url_current = self.url
        self.currentDoc = 0
        self.currentGroup = ('image', 0)
        # this will be used to track counts for the 3 groups
        self.groups = dict(
            image=BunchDict(), text=BunchDict(), website=BunchDict())
        facet_type_tab = self._get_next_result_set()
        # set total number of hits across the 3 groups
        self.totalDocs = int(facet_type_tab.attrib['totalDocs'])
        if self.totalDocs <= 0:
            raise ValueError(self.url + ' yields no results')
        self.totalGroups = int(facet_type_tab.attrib['totalGroups'])
        self._update_groups(facet_type_tab.findall('group'))
        # set initial group to image, if there are any
        for key, hitgroup in self.groups.items():
            hitgroup.end = 0
            hitgroup.currentDoc = 1
        if self.groups['image'].start != 0:
            self.currentGroup = 'image'
        elif self.groups['text'].start != 0:
            self.currentGroup = 'text'
        else:
            self.currentGroup = None

    def _get_doc_ark(self, docHit):
        '''Return the object's ark from the xml etree docHit'''
        ids = docHit.find('meta').findall('identifier')
        ark = None
        for i in ids:
            if i.attrib.get('q', None) != 'local':
                try:
                    split = i.text.split('ark:')
                except AttributeError:
                    continue
                if len(split) > 1:
                    ark = ''.join(('ark:', split[1]))
        return ark

    def _docHits_to_objset(self, docHits):
        '''Transform the ElementTree docHits into a python object list
        ready to be jsonfied

        Any elements with sub-elements (just seem to be the snippets in the
        relation field for the findaid ark) the innertext of the element +
        subelements becomes the value of the output.

        '''
        objset = []
        for d in docHits:
            obj = defaultdict(list)
            meta = d.find('meta')
            ark = self._get_doc_ark(d)
            for t in meta:
                if t.tag == 'google_analytics_tracking_code':
                    continue
                data = ''
                if t.tag == 'reference-image':
                    # ref image & thumbnail have data in attribs
                    # return as dicts
                    try:
                        x = int(t.attrib['X'])
                    except ValueError:
                        x = 0
                    try:
                        y = int(t.attrib['Y'])
                    except ValueError:
                        y = 0
                    src = ''.join((CONTENT_SERVER, t.attrib['src']))
                    src = src.replace('//', '/').replace('/', '//', 1)
                    data = {
                        'X': x,
                        'Y': y,
                        'src': src,
                    }
                    obj[t.tag].append(data)
                elif t.tag == 'thumbnail':
                    try:
                        x = int(t.attrib['X'])
                    except ValueError:
                        x = 0
                    try:
                        y = int(t.attrib['Y'])
                    except ValueError:
                        y = 0
                    src = ''.join((CONTENT_SERVER, '/', ark, '/thumbnail'))
                    src = src.replace('//', '/').replace('/', '//', 1)
                    data = {
                        'X': x,
                        'Y': y,
                        'src': src,
                    }
                    obj[t.tag] = data
                elif len(list(t)) > 0:
                    # <snippet> tag breaks up text for findaid <relation>
                    for innertext in t.itertext():
                        data = ''.join((data, innertext.strip()))
                    if data:
                        obj[t.tag].append({'attrib': t.attrib, 'text': data})
                else:
                    if t.text:  # don't add blank ones
                        obj[t.tag].append({'attrib': t.attrib, 'text': t.text})
            objset.append(obj)
        return objset

    def _update_groups(self, group_elements):
        '''Update the internal data structure with the count from
        the current ElementTree results for the search groups
        '''
        for g in group_elements:
            v = g.attrib['value']
            self.groups[v].total = int(g.attrib['totalDocs'])
            self.groups[v].start = int(g.attrib['startDoc'])
            self.groups[v].end = int(g.attrib['endDoc'])

    def _get_next_result_set(self):
        '''get the next result set
        Return the facet element, only one were interested in'''
        n_tries = 0
        pause = 5
        while True:
            try:
                resp = urllib.urlopen(self._url_current)
                break
            except DecodeError, e:
                n_tries += 1
                if n_tries > 5:
                    raise e
                # backoff
                time.sleep(pause)
                pause = pause * 2
        # resp.encoding = 'utf-8'  # thinks it's ISO-8859-1
        crossQueryResult = ET.fromstring(resp.read())
        # crossQueryResult = ET.fromstring(resp.text.encode('utf-8'))
        return crossQueryResult.find('facet')

    def next(self):
        '''Get the next page of search results
        '''
        if self.currentDoc >= self.totalDocs:
            raise StopIteration
        if self.currentGroup == 'image':
            if self.groups['image']['end'] == self.groups['image']['total']:
                self.currentGroup = 'text'
                if self.groups['text']['total'] == 0:
                    raise StopIteration
        self._url_current = ''.join(
            (self.url, '&startDoc=',
             str(self.groups[self.currentGroup]['currentDoc']), '&group=',
             self.currentGroup))
        facet_type_tab = self._get_next_result_set()
        self._update_groups(facet_type_tab.findall('group'))
        objset = self._docHits_to_objset(
            facet_type_tab.findall('./group/docHit'))
        self.currentDoc += len(objset)
        self.groups[self.currentGroup]['currentDoc'] += len(objset)
        return objset


class OAC_JSON_Fetcher(Fetcher):
    '''Fetcher for oac, using the JSON objset interface
    This is being deprecated in favor of the xml interface'''

    def __init__(self, url_harvest, extra_data):
        super(OAC_JSON_Fetcher, self).__init__(url_harvest, extra_data)
        self.oac_findaid_ark = self._parse_oac_findaid_ark(self.url)
        self.headers = {'content-type': 'application/json'}
        self.objset_last = False
        self.resp = requests.get(self.url, headers=self.headers)
        api_resp = self.resp.json()
        # for key in api_resp.keys():
        #    self.__dict__[key] = api_resp[key]
        self.objset_total = api_resp[u'objset_total']
        self.objset_start = api_resp['objset_start']
        self.objset_end = api_resp['objset_end']
        self.objset = api_resp['objset']
        n_objset = []
        for rec in self.objset:
            rec_orig = rec
            rec = rec['qdc']
            rec['files'] = rec_orig['files']
            n_objset.append(rec)
        self.objset = n_objset

    def _parse_oac_findaid_ark(self, url_findaid):
        return ''.join(('ark:', url_findaid.split('ark:')[1]))

    def next(self):
        '''Point to which function we want as main'''
        return self.next_objset()

    def next_record(self):
        '''Return the next record'''
        while self.resp:
            try:
                rec = self.objset.pop()
                return rec
            except IndexError:
                if self.objset_end == self.objset_total:
                    self.resp = None
                    raise StopIteration
            url_next = ''.join((self.url, '&startDoc=',
                                unicode(self.objset_end + 1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            # self.objset_total = api_resp['objset_total']
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
            n_objset = []
            for rec in self.objset:
                rec_orig = rec
                rec = rec['qdc']
                rec['files'] = rec_orig['files']
                n_objset.append(rec)
            self.objset = n_objset

    def next_objset(self):
        '''Return records in objset batches. More efficient and makes
        sense when storing to file in DPLA type ingest'''
        if self.objset_last:
            raise StopIteration
        cur_objset = self.objset
        if self.objset_end == self.objset_total:
            self.objset_last = True
        else:
            url_next = ''.join((self.url, '&startDoc=',
                                unicode(self.objset_end + 1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
            n_objset = []
            for rec in self.objset:
                rec_orig = rec
                rec = rec['qdc']
                rec['files'] = rec_orig['files']
                n_objset.append(rec)
            self.objset = n_objset
        return cur_objset


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
