# -*- coding: utf-8 -*-
from unittest import TestCase
import shutil
from mock import patch
from harvester.collection_registry_client import Collection
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES
from mypretty import httpretty
# import httpretty
import harvester.fetcher as fetcher
import pprint

class MARCFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test MARC fetching'''

    def testInit(self):
        '''Basic tdd start'''

        h = fetcher.MARCFetcher('file:' + DIR_FIXTURES + '/marc-test', None)
        self.assertTrue(hasattr(h, 'url_marc_file'))
        self.assertTrue(hasattr(h, 'marc_file'))
        self.assertIsInstance(h.marc_file, file)
        self.assertTrue(hasattr(h, 'marc_reader'))
        self.assertEqual(
            str(type(h.marc_reader)), "<class 'pymarc.reader.MARCReader'>")

    def testLocalFileLoad(self):
        h = fetcher.MARCFetcher('file:' + DIR_FIXTURES + '/marc-test', None)
        for n, rec in enumerate(h):  # enum starts at 0
            pass
            # print("NUM->{}:{}".format(n,rec))
        self.assertEqual(n, 9)
        self.assertIsInstance(rec, dict)
        self.assertEqual(rec['leader'], '01914nkm a2200277ia 4500')
        self.assertEqual(len(rec['fields']), 21)


class AlephMARCXMLFetcherTestCase(LogOverrideMixin, TestCase):
    @httpretty.activate
    def testInit(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://ucsb-fake-aleph/endpoint&maximumRecords=3&startRecord=1',
            body=open(DIR_FIXTURES + '/ucsb-aleph-resp-1-3.xml').read())
        h = fetcher.AlephMARCXMLFetcher(
            'http://ucsb-fake-aleph/endpoint', None, page_size=3)
        self.assertTrue(hasattr(h, 'ns'))
        self.assertTrue(hasattr(h, 'url_base'))
        self.assertEqual(h.page_size, 3)
        self.assertEqual(h.num_records, 8)

    @httpretty.activate
    def testFetching(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://ucsb-fake-aleph/endpoint&maximumRecords=3&startRecord=1',
            body=open(DIR_FIXTURES + '/ucsb-aleph-resp-1-3.xml').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://ucsb-fake-aleph/endpoint&maximumRecords=3&startRecord=4',
            body=open(DIR_FIXTURES + '/ucsb-aleph-resp-4-6.xml').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://ucsb-fake-aleph/endpoint&maximumRecords=3&startRecord=7',
            body=open(DIR_FIXTURES + '/ucsb-aleph-resp-7-8.xml').read())
        h = fetcher.AlephMARCXMLFetcher(
            'http://ucsb-fake-aleph/endpoint', None, page_size=3)
        num_fetched = 0
        for objset in h:
            num_fetched += len(objset)
        self.assertEqual(num_fetched, 8)


class Harvest_MARC_ControllerTestCase(ConfigFileOverrideMixin,
                                      LogOverrideMixin, TestCase):
    '''Test the function of an MARC harvest controller'''

    def setUp(self):
        super(Harvest_MARC_ControllerTestCase, self).setUp()

    def tearDown(self):
        super(Harvest_MARC_ControllerTestCase, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    @patch('boto3.resource', autospec=True)
    def testMARCHarvest(self, mock_boto3):
        '''Test the function of the MARC harvest'''
        httpretty.register_uri(
            httpretty.GET,
            'http://registry.cdlib.org/api/v1/collection/',
            body=open(DIR_FIXTURES + '/collection_api_test_marc.json').read())
        self.collection = Collection(
            'http://registry.cdlib.org/api/v1/collection/')
        self.collection.url_harvest = 'file:' + DIR_FIXTURES + '/marc-test'
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController(
            'email@example.com',
            self.collection,
            config_file=self.config_file,
            profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        num = self.controller.harvest()
        self.assertEqual(num, 10)
        self.tearDown_config()


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
