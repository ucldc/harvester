# -*- coding: utf-8 -*-
from unittest import TestCase
from mypretty import httpretty
# import httpretty
import harvester.fetcher as fetcher
from test.utils import DIR_FIXTURES
from test.utils import LogOverrideMixin


class UCDFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for UCD sitemap/JSON retrieval'''

    @httpretty.activate
    def testInit(self):
        '''Basic tdd start'''
        url = 'https://digital.ucdavis.edu/sitemap-eastman.xml'

        # Ugly but works
        httpretty.register_uri(
            httpretty.GET,
            url,
            body=open(DIR_FIXTURES + '/ucd-sitemap.xml').read(),
            status=200)
        httpretty.register_uri(
            httpretty.GET,
            'https://digital.ucdavis.edu/record/collection/eastman/B-1/B-1022',
            body=open(DIR_FIXTURES + '/ucd-recpage-1.xml').read(),
            status=200)
        httpretty.register_uri(
            httpretty.GET,
            'https://digital.ucdavis.edu/record/collection/eastman/B-1/B-1912',
            body=open(DIR_FIXTURES + '/ucd-recpage-2.xml').read(),
            status=200)
        httpretty.register_uri(
            httpretty.GET,
            'https://digital.ucdavis.edu/record/collection/eastman/B-1/B-1160',
            body=open(DIR_FIXTURES + '/ucd-recpage-3.xml').read(),
            status=200)
        h = fetcher.UCD_JSON_Fetcher(url, None)
        self.assertEqual(h.url_base, url)
        docs = []
        d = h.next()
        self.assertEqual(len(d), 3)
        docs.extend(d)
        test1 = docs[0]
        test2 = docs[2]

        self.assertIn('name', test1['metadata'])
        self.assertEqual(
            test1['metadata']['name'],
            '"Post Office," At Old Station Near Lassen Park, Calif')

        self.assertIn('@context', test2['metadata'])
        self.assertEqual(test2['metadata']['license'],
                         'http://rightsstatements.org/vocab/InC-NC/1.0/')

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
