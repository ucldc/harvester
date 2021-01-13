# -*- coding: utf-8 -*-
from unittest import TestCase
from mypretty import httpretty
# import httpretty
import harvester.fetcher as fetcher
from test.utils import LogOverrideMixin
from test.utils import DIR_FIXTURES


class PreservicaFetcherTestCase(LogOverrideMixin, TestCase):
    @httpretty.activate
    def testPreservicaFetch(self):
        httpretty.register_uri(
            httpretty.GET,
            'https://us.preservica.com/api/entity/v6.0/structural-objects/eb2416ec-ac1e-4e5e-baee-84e3371c03e9/children',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/preservica-page-1.xml').read())
            ])
        httpretty.register_uri(
            httpretty.GET,
            'https://us.preservica.com/api/entity/v6.0/structural-objects/eb2416ec-ac1e-4e5e-baee-84e3371c03e9/children/?start=100&amp;max=100',
            match_querystring=True,
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/preservica-page-2.xml').read())
            ])
        httpretty.register_uri(
            httpretty.GET,
            'https://us.preservica.com/api/entity/v6.0/information-objects/8c81f065-b6e4-457e-8b76-d18176f74bee',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/preservica-child-1.xml').read())
            ])
        httpretty.register_uri(
            httpretty.GET,
            'https://us.preservica.com/api/entity/v6.0/information-objects/8c81f065-b6e4-457e-8b76-d18176f74bee/metadata/37db4583-8e8e-4778-ac90-ad443664c5cb',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/preservica-child-2.xml').read())
            ])
        httpretty.register_uri(
            httpretty.GET,
            'https://us.preservica.com/api/entity/v6.0/information-objects/9501e09f-1ae8-4abc-a9ec-6c705ff8fdbe',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/preservica-child-3.xml').read())
            ])
        httpretty.register_uri(
            httpretty.GET,
            'https://us.preservica.com/api/entity/v6.0/information-objects/9501e09f-1ae8-4abc-a9ec-6c705ff8fdbe/metadata/ec5c46e5-443e-4b6d-81b9-ec2a5252a50c',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/preservica-child-4.xml').read())
            ])
        h = fetcher.PreservicaFetcher(
                'https://oakland.access.preservica.com/v6.0/uncategorized/SO_eb2416ec-ac1e-4e5e-baee-84e3371c03e9/',
                'usr, pwd')
        docs = []
        d = h.next()
        docs.extend(d)
        logger.error(docs[0])
        for d in h:
            docs.extend(d)
        self.assertEqual(len(docs), 17)

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
