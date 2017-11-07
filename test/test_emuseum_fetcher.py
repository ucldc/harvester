# -*- coding: utf-8 -*-
from unittest import TestCase
from mypretty import httpretty
# import httpretty
import harvester.fetcher as fetcher
from test.utils import DIR_FIXTURES
from test.utils import LogOverrideMixin
from akara import logger


class eMuseumFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for eMuseum API interface'''

    @httpretty.activate
    def testFetch(self):
        '''Test the httpretty mocked fetching of documents'''
        url = 'http://digitalcollections.hoover.org'
        httpretty.register_uri(
                httpretty.GET,
                url,
                responses=[
                    httpretty.Response(
                        body=open(
                        DIR_FIXTURES +
                        '/eMuseum-page-1.xml')
                        .read()),
                    httpretty.Response(
                        body=open(
                        DIR_FIXTURES +
                        '/eMuseum-page-2.xml')
                        .read()),
                    httpretty.Response(
                        body=open(
                        DIR_FIXTURES +
                        '/eMuseum-page-3.xml')
                        .read())
                ]
        )
        h = fetcher.eMuseum_Fetcher(url, None)
        self.assertEqual(h.url_base, url)
        docs = []
        d = h.next()
        self.assertEqual(len(d), 12)
        docs.extend(d)
        for d in h:
            docs.extend(d)
        self.assertEqual(len(docs), 64)
        test1 = docs[0]
        self.assertIn('title', test1['metadata'])
        self.assertEqual(test1['metadata']['title'], [
            'AF Afghani Service ID'
        ])

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
