# -*- coding: utf-8 -*-
import shutil
from unittest import TestCase
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES
from harvester.collection_registry_client import Collection
import harvester.fetcher as fetcher
import httpretty


class HarvestOAIControllerTestCase(ConfigFileOverrideMixin,
                                   LogOverrideMixin, TestCase):
    '''Test the function of an OAI fetcher'''
    def setUp(self):
        super(HarvestOAIControllerTestCase, self).setUp()

    def tearDown(self):
        super(HarvestOAIControllerTestCase, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testOAIHarvest(self):
        '''Test the function of the OAI harvest'''
        httpretty.register_uri(
                httpretty.GET,
                'http://registry.cdlib.org/api/v1/collection/',
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(
                httpretty.GET,
                'http://content.cdlib.org/oai',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        self.collection = Collection(
                'http://registry.cdlib.org/api/v1/collection/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController(
                'email@example.com', self.collection,
                config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        # TODO: fix why logbook.TestHandler not working for previous logging
        # self.assertEqual(len(self.test_log_handler.records), 2)
        self.tearDown_config()


class OAIFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the OAIFetcher
    '''
    @httpretty.activate
    def setUp(self):
        super(OAIFetcherTestCase, self).setUp()
        httpretty.register_uri(
                httpretty.GET,
                'http://content.cdlib.org/oai',
                body=open(DIR_FIXTURES+'/testOAI.xml').read())
        self.fetcher = fetcher.OAIFetcher('http://content.cdlib.org/oai',
                                          'oac:images')

    def tearDown(self):
        super(OAIFetcherTestCase, self).tearDown()
        httpretty.disable()

    def testHarvestIsIter(self):
        self.assertTrue(hasattr(self.fetcher, '__iter__'))
        self.assertEqual(self.fetcher, self.fetcher.__iter__())
        self.fetcher.next()

    def testOAIFetcherReturnedData(self):
        '''test that the data returned by the OAI Fetcher is a proper dc
        dictionary
        '''
        rec = self.fetcher.next()
        self.assertIsInstance(rec, dict)
        self.assertIn('id', rec)
        self.assertEqual(rec['id'], '13030/hb796nb5mn')
        self.assertIn('datestamp', rec)
        self.assertIn(rec['datestamp'], '2005-12-13')

    def testDeletedRecords(self):
        '''Test that the OAI harvest handles "deleted" records.
        For now that means skipping over them.
        '''
        recs = []
        for r in self.fetcher:
            recs.append(r)
        # skips over 5 "deleted" records
        self.assertEqual(len(recs), 3)

    @httpretty.activate
    def testOverrideMetadataPrefix(self):
        '''test that the metadataPrefix for an OAI feed can be overridden.
        The extra_data for OAI can be either just a set spec or a html query
        string of set= &metadataPrefix=
        '''
        httpretty.register_uri(
                httpretty.GET,
                'http://content.cdlib.org/oai',
                body=open(DIR_FIXTURES+'/testOAI.xml').read())
        set_fetcher = fetcher.OAIFetcher('http://content.cdlib.org/oai',
                                         'set=oac:images')
        self.assertEqual(set_fetcher._set, 'oac:images')
        rec = set_fetcher.next()
        self.assertIsInstance(rec, dict)
        self.assertIn('id', rec)
        self.assertEqual(rec['id'], '13030/hb796nb5mn')
        self.assertIn('datestamp', rec)
        self.assertIn(rec['datestamp'], '2005-12-13')
        self.assertEqual(httpretty.last_request().querystring,
                         {u'verb': [u'ListRecords'], u'set': [u'oac:images'],
                         u'metadataPrefix': [u'oai_dc']})
        httpretty.register_uri(
                httpretty.GET,
                'http://content.cdlib.org/oai',
                body=open(DIR_FIXTURES+'/testOAI-didl.xml').read())
        didl_fetcher = fetcher.OAIFetcher('http://content.cdlib.org/oai',
                                          'set=oac:images&metadataPrefix=didl')
        self.assertEqual(didl_fetcher._set, 'oac:images')
        self.assertEqual(didl_fetcher._metadataPrefix, 'didl')
        rec = didl_fetcher.next()
        self.assertIsInstance(rec, dict)
        self.assertIn('id', rec)
        self.assertEqual(rec['id'], 'oai:ucispace-prod.lib.uci.edu:10575/25')
        self.assertEqual(rec['title'], ['Schedule of lectures'])
        self.assertIn('datestamp', rec)
        self.assertEqual(rec['datestamp'], '2015-05-20T11:04:23Z')
        self.assertEqual(httpretty.last_request().querystring,
                         {u'verb': [u'ListRecords'], u'set': [u'oac:images'],
                         u'metadataPrefix': [u'didl']})
        self.assertEqual(rec['Resource']['@ref'],
                         'http://ucispace-prod.lib.uci.edu/xmlui/bitstream/' +
                         '10575/25/1/!COLLOQU.IA.pdf')
        self.assertEqual(rec['Item']['@id'],
                         'uuid-640925bd-9cdf-46be-babb-b2138c3fce9c')
        self.assertEqual(rec['Component']['@id'],
                         'uuid-897984d8-9392-4a68-912f-ffdf6fd7ce59')
        self.assertIn('Descriptor', rec)
        self.assertEqual(rec['Statement']['@mimeType'],
                         'application/xml; charset=utf-8')
        self.assertEqual(
                rec['DIDLInfo']
                ['{urn:mpeg:mpeg21:2002:02-DIDL-NS}DIDLInfo'][0]['text'],
                '2015-05-20T20:30:26Z')
        del didl_fetcher


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
