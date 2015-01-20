import os
from unittest import TestCase
import re
import httpretty
from mock import patch
from test.utils import DIR_FIXTURES
from harvester.config import config
from harvester.post_processing.couchdb_runner import COUCHDB_VIEW
from harvester.post_processing.couchdb_runner import CouchDBWorker
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue


class CouchDBWorkerTestCase(TestCase):
    '''Test the running of functions against sets of couchdb documents.
    '''
    @httpretty.activate
    def setUp(self):
        conf = config()
        self.url_couch_base = conf.DPLA.get('CouchDb', 'URL')
        self.cdb = conf.DPLA.get('CouchDb', 'ItemDatabase')
        url_head = os.path.join(self.url_couch_base, self.cdb)
        httpretty.register_uri(httpretty.HEAD,
                url_head,
                body='',
                content_length='0',
                content_type='text/plain; charset=utf-8',
                connection='close',
                server='CouchDB/1.5.0 (Erlang OTP/R16B03)',
                cache_control='must-revalidate',
                date='Mon, 24 Nov 2014 21:30:38 GMT'
                )
        self._cdbworker = CouchDBWorker()
        def func_for_test(doc, *args, **kwargs):
            return doc, args, kwargs
        self.function = func_for_test


    @httpretty.activate
    def testCollectionSlice(self):
        '''Test that results are correct for a known couchdb result'''
        url_to_pretty = os.path.join(self.url_couch_base, self.cdb,
                '_design', COUCHDB_VIEW.split('/')[0],
                '_view', COUCHDB_VIEW.split('/')[1])
        httpretty.register_uri(httpretty.GET,
                re.compile(url_to_pretty+".*$"),
                body=open(DIR_FIXTURES+'/couchdb_by_provider_name-5112.json').read(),
                etag="2U5BW2TDDX9EHZJOO0DNE29D1",
                content_type='application/json',
                )

        results = self._cdbworker.run_by_collection('5112', self.function,
                'arg1', 'arg2', kwarg1='1', kwarg2=2)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[1][0], '5112--http://ark.cdlib.org/ark:/13030/kt7779r8zj')
        self.assertEqual(results[1][1][1], ('arg1', 'arg2'))
        self.assertEqual(results[1][1][2], {'kwarg1':'1', 'kwarg2':2})
        doc = results[0][1][0]
        self.assertEqual(doc['isShownAt'], 'http://www.coronado.ca.us/library/')


class CouchDBJobEnqueueTestCase(TestCase):
    #@patch('redis.client.Redis', autospec=True)
    @patch('harvester.post_processing.couchdb_runner.Redis', autospec=True)
    @httpretty.activate
    def setUp(self, mock_redis):
        conf = config()
        self.url_couch_base = conf.DPLA.get('CouchDb', 'URL')
        self.cdb = conf.DPLA.get('CouchDb', 'ItemDatabase')
        url_head = os.path.join(self.url_couch_base, self.cdb)
        httpretty.register_uri(httpretty.HEAD,
                url_head,
                body='',
                content_length='0',
                content_type='text/plain; charset=utf-8',
                connection='close',
                server='CouchDB/1.5.0 (Erlang OTP/R16B03)',
                cache_control='must-revalidate',
                date='Mon, 24 Nov 2014 21:30:38 GMT'
                )

        self._cdbrunner = CouchDBJobEnqueue()
        def func_for_test(doc, *args, **kwargs):
            return doc, args, kwargs
        self.function = func_for_test

    @httpretty.activate
    def testCollectionSlice(self):
        '''Test that results are correct for a known couchdb result'''
        url_to_pretty = os.path.join(self.url_couch_base, self.cdb,
                '_design', COUCHDB_VIEW.split('/')[0],
                '_view', COUCHDB_VIEW.split('/')[1])
        httpretty.register_uri(httpretty.GET,
                re.compile(url_to_pretty+".*$"),
                body=open(DIR_FIXTURES+'/couchdb_by_provider_name-5112.json').read(),
                etag="2U5BW2TDDX9EHZJOO0DNE29D1",
                content_type='application/json',
                )
                #transfer_encoding='chunked', #NOTE: doesn't work with httpretty
        results = self._cdbrunner.queue_collection('5112', 6000, self.function,
                'arg1', 'arg2', kwarg1='1', kwarg2=2)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].args, ('5112--http://ark.cdlib.org/ark:/13030/kt7580382j', 'arg1', 'arg2'))
        self.assertEqual(results[0].kwargs, {'kwarg1': '1', 'kwarg2': 2})
        self.assertEqual(results[0].func_name, 'test.test_couchdb_runner.func_for_test')
