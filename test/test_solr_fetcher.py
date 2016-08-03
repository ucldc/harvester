# -*- coding: utf-8 -*-
from unittest import TestCase
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES
from harvester.collection_registry_client import Collection
import solr
import pysolr
import harvester.fetcher as fetcher
import httpretty


class SolrFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the harvesting of solr baed data.'''
    # URL:/solr/select body:q=extra_data&version=2.2&fl=%2A%2Cscore&wt=standard
    @httpretty.activate
    def testClassInit(self):
        '''Test that the class exists and gives good error messages
        if initial data not correct'''
        httpretty.register_uri(
            httpretty.POST,
            'http://example.edu/solr/select',
            body=open(
                DIR_FIXTURES +
                '/ucsd-new-feed-missions-bb3038949s-0.xml').read()
            )
        self.assertRaises(TypeError, fetcher.SolrFetcher)
        h = fetcher.SolrFetcher('http://example.edu/solr', 'extra_data',
                                rows=3)
        self.assertTrue(hasattr(h, 'solr'))
        self.assertTrue(isinstance(h.solr, solr.Solr))
        self.assertEqual(h.solr.url, 'http://example.edu/solr')
        self.assertTrue(hasattr(h, 'query'))
        self.assertEqual(h.query, 'extra_data')
        self.assertTrue(hasattr(h, 'resp'))
        self.assertEqual(h.resp.start, 0)
        self.assertEqual(len(h.resp.results), 3)
        self.assertTrue(hasattr(h, 'numFound'))
        self.assertEqual(h.numFound, 10)
        self.assertTrue(hasattr(h, 'index'))

    @httpretty.activate
    def testIterateOverResults(self):
        '''Test the iteration over a mock set of data'''
        httpretty.register_uri(
            httpretty.POST,
            'http://example.edu/solr/select',
            responses=[
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-0.xml').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-1.xml').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-2.xml').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-3.xml').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-4.xml').read())
            ]
            )
        h = fetcher.SolrFetcher('http://example.edu/solr', 'extra_data',
                                rows=3)
        self.assertEqual(len(h.resp.results), 3)
        n = 0
        for r in h:
            n += 1
        self.assertEqual(['Mission at Santa Barbara'], r['title_tesim'])
        self.assertEqual(n, 10)


class PySolrQueryFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the harvesting of solr baed data.'''
    # URL:/solr/select body:q=extra_data&version=2.2&fl=%2A%2Cscore&wt=standard
    @httpretty.activate
    def testClassInit(self):
        '''Test that the class exists and gives good error messages
        if initial data not correct'''
        httpretty.register_uri(
                httpretty.GET,
                'http://example.edu/solr/query',
                body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-0.json').read()
                )
        self.assertRaises(TypeError, fetcher.PySolrQueryFetcher)
        h = fetcher.PySolrQueryFetcher('http://example.edu/solr',
                                       'extra_data',)
        self.assertTrue(hasattr(h, 'solr'))
        self.assertTrue(isinstance(h.solr, pysolr.Solr))
        self.assertEqual(h.solr.url, 'http://example.edu/solr')
        self.assertTrue(hasattr(h, 'results'))
        self.assertEqual(len(h.results), 4)
        self.assertEqual(h.results['response']['numFound'], 10)
        self.assertEqual(h.numFound, 10)
        self.assertTrue(hasattr(h, 'index'))

    @httpretty.activate
    def testIterateOverResults(self):
        '''Test the iteration over a mock set of data'''
        httpretty.register_uri(
            httpretty.GET,
            'http://example.edu/solr/query',
            responses=[
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-0.json').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-1.json').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-2.json').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-3.json').read()),
            ]
        )
        self.assertRaises(TypeError, fetcher.PySolrFetcher)
        h = fetcher.PySolrQueryFetcher('http://example.edu/solr', 'extra_data',
                                       **{'rows': 3})
        self.assertEqual(
                h._query_path,
                'query?q=extra_data&rows=3&wt=json&cursorMark=%2A')
        n = 0
        for r in h:
            n += 1
        self.assertEqual(n, 10)
        self.assertEqual(['Mission Santa Ynez'], r['title_tesim'])


class PySolrUCBFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the UCB fetcher'''
    @httpretty.activate
    def testClassInit(self):
        '''Test that the class exists and gives good error messages
        if initial data not correct'''
        httpretty.register_uri(
                httpretty.GET,
                'http://example.edu/solr/select',
                body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-0.json').read()
                )
        self.assertRaises(TypeError, fetcher.PySolrUCBFetcher)
        h = fetcher.PySolrUCBFetcher('http://example.edu/solr',
                                     'extra_data',)
        self.assertTrue(hasattr(h, 'solr'))
        self.assertTrue(isinstance(h.solr, pysolr.Solr))
        self.assertEqual(h.solr.url, 'http://example.edu/solr')
        self.assertTrue(hasattr(h, 'results'))
        self.assertEqual(len(h.results), 4)
        self.assertEqual(h.results['response']['numFound'], 10)
        self.assertEqual(h.numFound, 10)
        self.assertTrue(hasattr(h, 'index'))

    @httpretty.activate
    def testIterateOverResults(self):
        '''Test the SolrUCBFetcher iteration over a mock set of data'''
        httpretty.register_uri(
            httpretty.GET,
            'http://example.edu/solr/select',
            responses=[
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-0.json').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-1.json').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-2.json').read()),
                httpretty.Response(body=open(
                    DIR_FIXTURES +
                    '/ucsd-new-feed-missions-bb3038949s-3.json').read()),
            ]
        )
        h = fetcher.PySolrUCBFetcher('http://example.edu/solr', 'extra_data',
                                     **{'rows': 3})
        self.assertEqual(
                h._query_path,
                'select?q=extra_data&wt=json&cursorMark=%2A&qt=document')
        n = 0
        for r in h:
            n += 1
        self.assertEqual(n, 10)
        self.assertEqual(['Mission Santa Ynez'], r['title_tesim'])


class HarvestSolr_ControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin,
                                     TestCase):
    '''Test the function of Solr harvest controller'''
    @httpretty.activate
    def setUp(self):
        super(HarvestSolr_ControllerTestCase, self).setUp()
        # self.testFile = DIR_FIXTURES+'/collection_api_test_oac.json'
        httpretty.register_uri(
            httpretty.GET,
            "https://registry.cdlib.org/api/v1/collection/183/",
            body=open(DIR_FIXTURES+'/collection_api_solr_harvest.json').read())
        httpretty.register_uri(
            httpretty.POST,
            'http://example.edu/solr/blacklight/select',
            body=open(
                DIR_FIXTURES +
                '/ucsd-new-feed-missions-bb3038949s-0.xml').read()
                )
        self.collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/183/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController(
                'email@example.com', self.collection,
                config_file=self.config_file, profile_path=self.profile_path)
        print "DIR SAVE::::: {}".format(self.controller.dir_save)

    def tearDown(self):
        super(HarvestSolr_ControllerTestCase, self).tearDown()
        self.tearDown_config()
        # shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testSolrHarvest(self):
        '''Test the function of the Solr harvest with <date> objects
        in stream'''
        httpretty.register_uri(
            httpretty.POST,
            'http://example.edu/solr/blacklight/select',
            responses=[
               httpretty.Response(body=open(
                   DIR_FIXTURES +
                   '/ucsd-new-feed-missions-bb3038949s-0.xml').read()),
               httpretty.Response(body=open(
                   DIR_FIXTURES +
                   '/ucsd-new-feed-missions-bb3038949s-1.xml').read()),
               httpretty.Response(body=open(
                   DIR_FIXTURES +
                   '/ucsd-new-feed-missions-bb3038949s-2.xml').read()),
               httpretty.Response(body=open(
                   DIR_FIXTURES +
                   '/ucsd-new-feed-missions-bb3038949s-3.xml').read()),
               httpretty.Response(body=open(
                   DIR_FIXTURES +
                   '/ucsd-new-feed-missions-bb3038949s-4.xml').read())
            ]
            )
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.controller.harvest()
        print "LOGS:{}".format(self.test_log_handler.formatted_records)
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue(
                'UC San Diego' in self.test_log_handler.formatted_records[0])
        self.assertEqual(
                self.test_log_handler.formatted_records[1],
                '[INFO] HarvestController: 13 records harvested')

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
