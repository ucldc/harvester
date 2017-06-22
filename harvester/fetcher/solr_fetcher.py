# -*- coding: utf-8 -*-
import solr
import pysolr
from .fetcher import Fetcher
import urlparse
import requests


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
            return self.resp.results[self.index - 1]
        self.index = 1
        self.resp = self.resp.next_batch()
        if not len(self.resp.results):
            raise StopIteration
        return self.resp.results[self.index - 1]


class PySolrFetcher(Fetcher):
    def __init__(self,
                 url_harvest,
                 query,
                 handler_path='select',
                 **query_params):
        super(PySolrFetcher, self).__init__(url_harvest, query)
        self.solr = pysolr.Solr(url_harvest, timeout=1)
        self._handler_path = handler_path
        self._query_params = {
            'q': query,
            'wt': 'json',
            'sort': 'id asc',
            'rows': 100,
            'cursorMark': '*'
        }
        self._query_params.update(query_params)
        self._nextCursorMark = '*'
        self.get_next_results()
        self.numFound = self.results['response'].get('numFound')
        self.index = 0

    @property
    def _query_path(self):
        self._query_params_encoded = pysolr.safe_urlencode(self._query_params)
        return '{}?{}'.format(self._handler_path, self._query_params_encoded)

    def get_next_results(self):
        self._query_params['cursorMark'] = self._nextCursorMark
        resp = self.solr._send_request('get', path=self._query_path)
        self.results = self.solr.decoder.decode(resp)
        self._nextCursorMark = self.results.get('nextCursorMark')
        self.iter = self.results['response']['docs'].__iter__()

    def next(self):
        try:
            next_result = self.iter.next()
            self.index += 1
            return next_result
        except StopIteration:
            if self.index >= self.numFound:
                raise StopIteration
        self.get_next_results()
        if self._nextCursorMark == self._query_params['cursorMark']:
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
        super(PySolrQueryFetcher, self).__init__(
            url_harvest, query, handler_path='query', **query_params)


class PySolrUCBFetcher(PySolrFetcher):
    '''Add the qt=document parameter for UCB blacklight'''

    def __init__(self, url_harvest, query, **query_params):
        query_params.update({'qt': 'document'})
        super(PySolrUCBFetcher, self).__init__(url_harvest, query,
                                               **query_params)


class RequestsSolrFetcher(Fetcher):
    '''A fetcher for solr that uses just the requests library
    The URL is the URL up to the "select" bit (may change in future)
    Extra_data is one of 2 formats:
        just a string -- it is the "q" string
        URL encoded query string ->
        q=<query>&header=<name>:<value>&header=<name>:<value>
    The auth parameter will be parsed to figure out type of authentication
    needed, right now just deal with "header" token authentication
    '''

    def __init__(self, url_harvest, extra_data):
        super(RequestsSolrFetcher, self).__init__(url_harvest, extra_data)
        if '/select' not in self.url and '/query' not in self.url:
            self.url = ''.join((self.url, '/select'))
        self._query_iter_template = \
            '?rows={rows}&cursorMark={cursorMark}'
        self._query_params = urlparse.parse_qs(extra_data)
        if not self._query_params:  # Old style, just "q" bit of query
            self._query_params = {'q': [extra_data]}
        self._page_size = 1000
        self._cursorMark = None
        self._nextCursorMark = '*'
        self._query_params.update({
            'wt': ['json'],
            'sort': ['id asc'],
        })
        self._headers = {}
        for name, value in self._query_params.items():
            if name == 'header':
                for value in self._query_params[name]:  # its a list
                    header_name, header_value = value.split(':', 1)
                    self._headers[header_name] = header_value
                del self._query_params[name]

    @property
    def end_of_feed(self):
        return self._cursorMark == self._nextCursorMark

    @property
    def url_request(self):
        # build current URL
        url_request = ''.join((
            self.url,
            self._query_iter_template.format(
                rows=self._page_size,
                cursorMark=self._cursorMark),
            ))
        # join 'q' and all other params
        for name, value in self._query_params.items():
            url_request = ''.join((url_request, '&', name, '=', value[0]))
        return url_request

    def get_response(self):
        '''Get the correct response for the given combo of params'''
        return requests.get(self.url_request, headers=self._headers)

    def next(self):
        '''get the next page of solr data, using the cursor mark to build
        URL
        '''
        if (self.end_of_feed):
            raise StopIteration
        # get resp
        self._cursorMark = self._nextCursorMark
        resp = self.get_response()
        resp.raise_for_status()
        resp_obj = resp.json()
        self._nextCursorMark = resp_obj['nextCursorMark']
        return resp_obj['response']['docs']


# Copyright © 2017, Regents of the University of California
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
