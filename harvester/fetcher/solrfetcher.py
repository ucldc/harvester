# -*- coding: utf-8 -*-
import solr
import pysolr
from .fetcher import Fetcher


class SolrFetcher(Fetcher):
    def __init__(self, url_harvest, query, **query_params):
        super(SolrFetcher, self).__init__(url_harvest, query)
        self.solr = solr.Solr(url_harvest)  # , debug=True)
        self.query = query
        self.resp = self.solr.select(self.query)
        self.numFound = self.resp.numFound
        self.index = 0

    def next(self):
        if self.index < len(self.resp.results):
            self.index += 1
            return self.resp.results[self.index-1]
        self.index = 1
        self.resp = self.resp.next_batch()
        if not len(self.resp.results):
            raise StopIteration
        return self.resp.results[self.index-1]


class PySolrFetcher(Fetcher):
    def __init__(self, url_harvest, query, handler_path='select',
                 **query_params):
        super(PySolrFetcher, self).__init__(url_harvest, query)
        self.solr = pysolr.Solr(url_harvest, timeout=1)
        self._handler_path = handler_path
        self._query_params = {
                'q': query,
                'wt': 'json',
                'sort': 'id asc',
                'cursorMark': '*'}
        self._query_params.update(query_params)
        self._query_params_encoded = pysolr.safe_urlencode(self._query_params)
        self._query_path = '{}?{}'.format(self._handler_path,
                                          self._query_params_encoded)
        self.get_next_results()
        self.numFound = self.results['response'].get('numFound')
        self.index = 0

    def get_next_results(self):
        resp = self.solr._send_request('get', path=self._query_path)
        self.results = self.solr.decoder.decode(resp)
        self.nextCursorMark = self.results.get('nextCursorMark')
        self.iter = self.results['response']['docs'].__iter__()

    def next(self):
        try:
            next_result = self.iter.next()
            self.index += 1
            return next_result
        except StopIteration:
            if self.index >= self.numFound:
                raise StopIteration
        self._query_params['cursorMark'] = self.nextCursorMark
        self.get_next_results()
        if self.nextCursorMark == self._query_params['cursorMark']:
            if self.index >= self.numFound:
                raise StopIteration
        if len(self.results['response']['docs']) == 0:
            raise StopIteration
        self.index += 1
        return self.iter.next()


class PySolrQueryFetcher(PySolrFetcher):
    ''' Use the `select` url path for querying instead of 'query'. This is
    more typical for most Solr applications.
    '''
    def __init__(self, url_harvest, query, **query_params):
        super(PySolrQueryFetcher, self).__init__(url_harvest,
                                                 query,
                                                 handler_path='query',
                                                 **query_params)


class PySolrUCBFetcher(PySolrFetcher):
    '''Add the qt=document parameter for UCB blacklight'''
    def __init__(self, url_harvest, query, **query_params):
        query_params = {'qt': 'document'}
        super(PySolrUCBFetcher, self).__init__(url_harvest, query,
                                                 **query_params)


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
