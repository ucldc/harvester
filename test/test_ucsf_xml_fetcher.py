# -*- coding: utf-8 -*-
from unittest import TestCase
import httpretty
import harvester.fetcher as fetcher
from test.utils import DIR_FIXTURES
from test.utils import LogOverrideMixin


class UCSFXMLFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for the UCSF xml search interface'''
    @httpretty.activate
    def testInit(self):
        '''Basic tdd start'''
        url = 'https://example.edu/action/search/xml?q=ddu%3A20*&' \
              'asf=ddu&asd=&fd=1&_hd=&hd=on&sf=&_rs=&_ef=&ef=on&sd=&ed=&c=ga'
        httpretty.register_uri(
                httpretty.GET,
                url,
                body=open(DIR_FIXTURES+'/ucsf-page-1.xml').read())
        h = fetcher.UCSF_XML_Fetcher(url, None, page_size=3)
        self.assertEqual(h.url_base, url)
        self.assertEqual(h.page_size, 3)
        self.assertEqual(h.page_current, 1)
        self.assertEqual(h.url_current, url+'&ps=3&p=1')
        self.assertEqual(h.docs_total, 7)

    @httpretty.activate
    def testFetch(self):
        '''Test the httpretty mocked fetching of documents'''
        url = 'https://example.edu/action/search/xml?q=ddu%3A20*&' \
              'asf=ddu&asd=&fd=1&_hd=&hd=on&sf=&_rs=&_ef=&ef=on&sd=&ed=&c=ga'
        httpretty.register_uri(
                httpretty.GET,
                url,
                responses=[
                    httpretty.Response(
                        open(DIR_FIXTURES+'/ucsf-page-1.xml').read(),
                        status=200),
                    httpretty.Response(
                        open(DIR_FIXTURES+'/ucsf-page-1.xml').read(),
                        status=200),
                    httpretty.Response(
                        open(DIR_FIXTURES+'/ucsf-page-2.xml').read(),
                        status=200),
                    httpretty.Response(
                        open(DIR_FIXTURES+'/ucsf-page-3.xml').read(),
                        status=200),
                ]
        )
        h = fetcher.UCSF_XML_Fetcher(url, None, page_size=3)
        docs = []
        for d in h:
            docs.extend(d)
        self.assertEqual(len(docs), 7)
        testy = docs[0]
        self.assertIn('tid', testy)
        self.assertEqual(testy['tid'], "nga13j00")
        self.assertEqual(testy['uri'],
                         'http://legacy.library.ucsf.edu/tid/nga13j00')
        self.assertIn('aup', testy['metadata'])
        self.assertEqual(testy['metadata']['aup'], ['Whent, Peter'])


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
