# -*- coding: utf-8 -*-
from __future__ import print_function
from unittest import TestCase
import harvester.fetcher as fetcher
from test.utils import DIR_FIXTURES
from test.utils import LogOverrideMixin
from mypretty import httpretty
# import httpretty


class InternetArchiveTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for the Internet Archive API'''

    @httpretty.activate
    def test_fetching(self):
        '''Basic tdd start'''
        url = 'https://example.edu'
        extra_data = 'collection:environmentaldesignarchive AND subject:"edith heath"'
        page_current = 1
        url_first = fetcher.IA_Fetcher.url_advsearch.format(
            page_current=page_current, search_query=extra_data)
        httpretty.register_uri(
            httpretty.GET,
            url_first,
            responses=[
                httpretty.Response(body=open(DIR_FIXTURES +
                                             '/ia-results-1.json').read()),
                httpretty.Response(body=open(DIR_FIXTURES +
                                             '/ia-results-2.json').read()),
                httpretty.Response(body=open(DIR_FIXTURES +
                                             '/ia-results-3.json').read()),
            ])
        h = fetcher.IA_Fetcher(url, extra_data)
        results = []
        for v in h:
            results.extend(v)
        self.assertEqual(h.url_base, url)
        self.assertEqual(
            h.url_advsearch, 'https://archive.org/advancedsearch.php?'
            'q={search_query}&rows=500&page={page_current}&output=json')
        self.assertEqual(len(results), 1285)
        self.assertEqual(
            results[1284], {
                u'week':
                0,
                u'publicdate':
                u'2014-02-28T03:17:59Z',
                u'format': [
                    u'Archive BitTorrent', u'JPEG', u'JPEG Thumb', u'JSON',
                    u'Metadata'
                ],
                u'title':
                u'Upright Cabinet Piano',
                u'downloads':
                68,
                u'indexflag': [u'index', u'nonoindex'],
                u'mediatype':
                u'image',
                u'collection': [
                    u'metropolitanmuseumofart-gallery',
                    u'fav-mar_a_luisa_guevara_tirado', u'fav-drewblanco'
                ],
                u'month':
                1,
                u'btih':
                u'e16555eb5474d2543c7ad27a1cfd145195ce05bf',
                u'item_size':
                353871,
                u'backup_location':
                u'ia905804_31',
                u'year':
                u'1835',
                u'date':
                u'1835-01-01T00:00:00Z',
                u'oai_updatedate': [
                    u'2014-02-28T03:17:59Z', u'2014-02-28T03:17:59Z',
                    u'2016-08-31T20:56:29Z'
                ],
                u'identifier':
                u'mma_upright_cabinet_piano_504395',
                u'subject': [
                    u'North and Central America', u'Wood, various materials',
                    u'Cabinets', u'Case furniture', u'1835', u'Pianos',
                    u'New York City', u'Metropolitan Museum of Art',
                    u'Zithers', u'United States', u'Brooklyn',
                    u'Musical instruments', u'Chordophones', u'New York',
                    u'Furniture'
                ]
            })


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
