import os
from unittest import TestCase
import shutil
import re
from xml.etree import ElementTree as ET
import json
import solr
import httpretty
from mock import patch, call
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES, TEST_COUCH_DASHBOARD, TEST_COUCH_DB
import harvester.fetcher as fetcher
from harvester.collection_registry_client import Collection
import pynux.utils
from requests.packages.urllib3.exceptions import DecodeError

class HarvestOAC_JSON_ControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the function of an OAC harvest controller'''
    @httpretty.activate
    def setUp(self):
        super(HarvestOAC_JSON_ControllerTestCase, self).setUp()
        # self.testFile = DIR_FIXTURES+'/collection_api_test_oac.json'
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/178/",
                body=open(DIR_FIXTURES+'/collection_api_test_oac.json').read())
        httpretty.register_uri(httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open(DIR_FIXTURES+'/testOAC.json').read())
        self.collection = Collection('https://registry.cdlib.org/api/v1/collection/178/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)

    def tearDown(self):
        super(HarvestOAC_JSON_ControllerTestCase, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testOAC_JSON_Harvest(self):
        '''Test the function of the OAC harvest'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue('UCB Department of Statistics' in self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 28 records harvested')

    @httpretty.activate
    def testObjectsHaveRegistryData(self):
        # test OAC objsets
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928&startDoc=26',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.testFile = DIR_FIXTURES+'/testOAC-url_next-1.json'
        self.ranGet = False
        self.controller.harvest()
        dir_list = os.listdir(self.controller.dir_save)
        self.assertEqual(len(dir_list), 2)
        objset_saved = json.loads(open(os.path.join(self.controller.dir_save, dir_list[0])).read())
        obj = objset_saved[2]
        self.assertIn('collection', obj)
        self.assertEqual(obj['collection'], [
            {'@id': 'https://registry.cdlib.org/api/v1/collection/178/',
             'name': 'Harry Crosby Collection',
             'title': 'Harry Crosby Collection',
             'id': '178',
             'ingestType': 'collection',
             'description': 'Black & white photographs of Sonora and Baja California taken by Harry Crosby. These materials are used widely by programs in Mexican history and ethnic studies.',
        }])
        self.assertIn('campus', obj)
        self.assertEqual(obj['campus'], [{u'@id': u'https://registry.cdlib.org/api/v1/campus/6/', u'name': u'UC San Diego'}, {u'@id': u'https://registry.cdlib.org/api/v1/campus/1/', u'name': u'UC Berkeley'}])
        self.assertIn('repository', obj)
        self.assertEqual(obj['repository'], [{u'@id': u'https://registry.cdlib.org/api/v1/repository/22/',
            u'name': u'Mandeville Special Collections Library'}, {u'@id': u'https://registry.cdlib.org/api/v1/repository/36/', u'name': u'UCB Department of Statistics'}])


class HarvestOAIControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the function of an OAI fetcher'''
    def setUp(self):
        super(HarvestOAIControllerTestCase, self).setUp()

    def tearDown(self):
        super(HarvestOAIControllerTestCase, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testOAIHarvest(self):
        '''Test the function of the OAI harvest'''
        httpretty.register_uri(httpretty.GET,
                'http://registry.cdlib.org/api/v1/collection/',
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://content.cdlib.org/oai',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        self.collection = Collection('http://registry.cdlib.org/api/v1/collection/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        # TODO: fix why logbook.TestHandler not working for the previous logging
        # self.assertEqual(len(self.test_log_handler.records), 2)
        self.tearDown_config()


class HarvestControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the harvest controller class'''
    @httpretty.activate
    def setUp(self):
        super(HarvestControllerTestCase, self).setUp()
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        self.collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        config_file, profile_path = self.setUp_config(self.collection)
        self.controller_oai = fetcher.HarvestController('email@example.com', self.collection, profile_path=profile_path, config_file=config_file)
        self.objset_test_doc = json.load(open(DIR_FIXTURES+'/objset_test_doc.json'))

    def tearDown(self):
        super(HarvestControllerTestCase, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller_oai.dir_save)

    @httpretty.activate
    def testHarvestControllerExists(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/101/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        collection = Collection('https://registry.cdlib.org/api/v1/collection/101/')
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(controller, 'fetcher'))
        self.assertIsInstance(controller.fetcher, fetcher.OAIFetcher)
        self.assertTrue(hasattr(controller, 'campus_valid'))
        self.assertTrue(hasattr(controller, 'dc_elements'))
        shutil.rmtree(controller.dir_save)

    def testOAIFetcherType(self):
        '''Check the correct object returned for type of harvest'''
        self.assertIsInstance(self.controller_oai.fetcher, fetcher.OAIFetcher)
        self.assertEqual(self.controller_oai.collection.campus[0]['slug'], 'UCDL')

    @httpretty.activate
    def testIDCreation(self):
        '''Test how the id for the index is created'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        self.assertTrue(hasattr(self.controller_oai, 'create_id'))
        identifier = 'x'
        self.assertRaises(TypeError, self.controller_oai.create_id, identifier)
        identifier = ['x', ]
        sid = self.controller_oai.create_id(identifier)
        self.assertIn(self.controller_oai.collection.slug, sid)
        self.assertIn(self.controller_oai.collection.campus[0]['slug'], sid)
        self.assertIn(self.controller_oai.collection.repository[0]['slug'], sid)
        self.assertEqual(sid, 'UCDL-Calisphere-calisphere-santa-clara-university-digital-objects-x')
        collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        sid = controller.create_id(identifier)
        self.assertEqual(sid, 'UCDL-Calisphere-calisphere-santa-clara-university-digital-objects-x')
        shutil.rmtree(controller.dir_save)

    def testUpdateIngestDoc(self):
        '''Test that the update to the ingest doc in couch is called correctly
        '''
        self.assertTrue(hasattr(self.controller_oai, 'update_ingest_doc'))
        self.assertRaises(TypeError, self.controller_oai.update_ingest_doc)
        self.assertRaises(ValueError, self.controller_oai.update_ingest_doc, 'error')
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            foo = {}
            with patch.dict(foo, {'test-id': 'test-ingest-doc'}):
                instance.dashboard_db = foo
                self.controller_oai.update_ingest_doc('error', error_msg="BOOM!")
            call_args = unicode(instance.update_ingestion_doc.call_args)
            self.assertIn('test-ingest-doc', call_args)
            self.assertIn("fetch_process/error='BOOM!'", call_args)
            self.assertIn("fetch_process/end_time", call_args)
            self.assertIn("fetch_process/total_items=0", call_args)
            self.assertIn("fetch_process/total_collections=None", call_args)

    @patch('dplaingestion.couch.Couch')
    def testCreateIngestCouch(self, mock_couch):
        '''Test the integration of the DPLA couch lib'''
        self.assertTrue(hasattr(self.controller_oai, 'ingest_doc_id'))
        self.assertTrue(hasattr(self.controller_oai, 'create_ingest_doc'))
        self.assertTrue(hasattr(self.controller_oai, 'config_dpla'))
        ingest_doc_id = self.controller_oai.create_ingest_doc()
        mock_couch.assert_called_with(config_file=self.config_file, dashboard_db_name=TEST_COUCH_DASHBOARD, dpla_db_name=TEST_COUCH_DB)

    def testUpdateFailInCreateIngestDoc(self):
        '''Test the failure of the update to the ingest doc'''
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            instance.update_ingestion_doc.side_effect = Exception('Boom!')
            self.assertRaises(Exception,  self.controller_oai.create_ingest_doc)

    def testCreateIngestDoc(self):
        '''Test the creation of the DPLA style ingest document in couch.
        This will call _create_ingestion_document, dashboard_db and update_ingestion_doc'''
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            instance.update_ingestion_doc.return_value = None
            foo = {}
            with patch.dict(foo, {'test-id': 'test-ingest-doc'}):
                instance.dashboard_db = foo
                ingest_doc_id = self.controller_oai.create_ingest_doc()
            self.assertIsNotNone(ingest_doc_id)
            self.assertEqual(ingest_doc_id, 'test-id')
            instance._create_ingestion_document.assert_called_with(self.collection.provider, 'http://localhost:8889', self.profile_path, self.collection.dpla_profile_obj['thresholds'])
            instance.update_ingestion_doc.assert_called()
            self.assertEqual(instance.update_ingestion_doc.call_count, 1)
            call_args = unicode(instance.update_ingestion_doc.call_args)
            self.assertIn('test-ingest-doc', call_args)
            self.assertIn("fetch_process/data_dir=u'/tmp/", call_args)
            self.assertIn("santa-clara-university-digital-objects", call_args)
            self.assertIn("fetch_process/end_time=None", call_args)
            self.assertIn("fetch_process/status='running'", call_args)
            self.assertIn("fetch_process/total_collections=None", call_args)
            self.assertIn("fetch_process/start_time=", call_args)
            self.assertIn("fetch_process/error=None", call_args)
            self.assertIn("fetch_process/total_items=None", call_args)

    def testNoTitleInRecord(self):
        '''Test that the process continues if it finds a record with no "title"
        THIS IS NOW HANDLED DOWNSTREAM'''
        pass

    def testFileSave(self):
        '''Test saving objset to file'''
        self.assertTrue(hasattr(self.controller_oai, 'dir_save'))
        self.assertTrue(hasattr(self.controller_oai, 'save_objset'))
        self.controller_oai.save_objset(self.objset_test_doc)
        # did it save?
        dir_list = os.listdir(self.controller_oai.dir_save)
        self.assertEqual(len(dir_list), 1)
        objset_saved = json.loads(open(os.path.join(self.controller_oai.dir_save, dir_list[0])).read())
        self.assertEqual(self.objset_test_doc, objset_saved)

    @httpretty.activate
    def testLoggingMoreThan1000(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/198/",
                body=open(DIR_FIXTURES+'/collection_api_big_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-2400-records.xml').read())
        collection = Collection('https://registry.cdlib.org/api/v1/collection/198/')
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 13)
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 100 records harvested')
        shutil.rmtree(controller.dir_save)
        self.assertEqual(self.test_log_handler.formatted_records[10], '[INFO] HarvestController: 1000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[11], '[INFO] HarvestController: 2000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[12], '[INFO] HarvestController: 2400 records harvested')

    @httpretty.activate
    def testAddRegistryData(self):
        '''Unittest the _add_registry_data function'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())

        collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        self.tearDown_config()  # remove ones setup in setUp
        self.setUp_config(collection)
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        obj = {'id': 'fakey', 'otherdata': 'test'}
        self.assertNotIn('collection', obj)
        objnew = controller._add_registry_data(obj)
        self.assertIn('collection', obj)
        self.assertEqual(obj['collection'][0]['@id'], 'https://registry.cdlib.org/api/v1/collection/197/')
        self.assertIn('campus', obj)
        self.assertIn('repository', obj)
        # need to test one without campus
        self.assertEqual(obj['campus'][0]['@id'], 'https://registry.cdlib.org/api/v1/campus/12/')
        self.assertEqual(obj['repository'][0]['@id'], 'https://registry.cdlib.org/api/v1/repository/37/')

    def testObjectsHaveRegistryData(self):
        '''Test that the registry data is being attached to objects from
        the harvest controller'''
        self.controller_oai.harvest()
        dir_list = os.listdir(self.controller_oai.dir_save)
        self.assertEqual(len(dir_list), 128)
        objset_saved = json.loads(open(os.path.join(self.controller_oai.dir_save, dir_list[0])).read())
        obj_saved = objset_saved[0]
        self.assertIn('collection', obj_saved)
        self.assertEqual(obj_saved['collection'], [
            {'@id': 'https://registry.cdlib.org/api/v1/collection/197/',
            'name': 'Calisphere - Santa Clara University: Digital Objects',
            'title': 'Calisphere - Santa Clara University: Digital Objects',
            'id': '197',
            'ingestType': 'collection',
            'description': '',
        }])
        self.assertIn('campus', obj_saved)
        self.assertEqual(obj_saved['campus'], [{'@id': 'https://registry.cdlib.org/api/v1/campus/12/',
            'name': 'California Digital Library'}])
        self.assertIn('repository', obj_saved)
        self.assertEqual(obj_saved['repository'], [{'@id': 'https://registry.cdlib.org/api/v1/repository/37/',
            'name': 'Calisphere'}])


class FetcherClassTestCase(TestCase):
    '''Test the abstract Fetcher class'''
    def testClassExists(self):
        h = fetcher.Fetcher
        h = h('url_harvest', 'extra_data')


class OAIFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the OAIFetcher
    '''
    @httpretty.activate
    def setUp(self):
        super(OAIFetcherTestCase, self).setUp()
        httpretty.register_uri(httpretty.GET,
                'http://content.cdlib.org/oai?verb=ListRecords&metadataPrefix=oai_dc&set=oac:images',
                body=open(DIR_FIXTURES+'/testOAI.xml').read())
        self.fetcher = fetcher.OAIFetcher('http://content.cdlib.org/oai', 'oac:images')

    def tearDown(self):
        super(OAIFetcherTestCase, self).tearDown()

    def testHarvestIsIter(self):
        self.assertTrue(hasattr(self.fetcher, '__iter__'))
        self.assertEqual(self.fetcher, self.fetcher.__iter__())
        rec1 = self.fetcher.next()

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


class SolrFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the harvesting of solr baed data.'''
    # URL:/solr/select body:q=extra_data&version=2.2&fl=%2A%2Cscore&wt=standard
    @httpretty.activate
    def testClassInit(self):
        '''Test that the class exists and gives good error messages
        if initial data not correct'''
        httpretty.register_uri(httpretty.POST,
            'http://example.edu/solr/select',
            body=open(DIR_FIXTURES+'/ucsd_bb5837608z-1.xml').read()
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
        httpretty.register_uri(httpretty.POST,
            'http://example.edu/solr/select',
            responses=[
                    httpretty.Response(body=open(DIR_FIXTURES+'/ucsd_bb5837608z-1.xml').read()),
                    httpretty.Response(body=open(DIR_FIXTURES+'/ucsd_bb5837608z-2.xml').read()),
                    httpretty.Response(body=open(DIR_FIXTURES+'/ucsd_bb5837608z-3.xml').read()),
                    httpretty.Response(body=open(DIR_FIXTURES+'/ucsd_bb5837608z-4.xml').read()),
                    httpretty.Response(body=open(DIR_FIXTURES+'/ucsd_bb5837608z-5.xml').read()),
            ]
            )
        h = fetcher.SolrFetcher('http://example.edu/solr', 'extra_data',
            rows=3)
        self.assertEqual(len(h.resp.results), 3)
        n = 0
        for r in h:
            n += 1
        self.assertEqual(['Urey Hall and Mayer Hall'], r['title_tesim'])
        self.assertEqual(n, 10)


class MARCFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test MARC fetching'''
    def testInit(self):
        '''Basic tdd start'''

        h = fetcher.MARCFetcher('file:'+DIR_FIXTURES+'/marc-test', None)
        self.assertTrue(hasattr(h, 'url_marc_file'))
        self.assertTrue(hasattr(h, 'marc_file'))
        self.assertIsInstance(h.marc_file, file)
        self.assertTrue(hasattr(h, 'marc_reader'))
        self.assertEqual(str(type(h.marc_reader)), "<class 'pymarc.reader.MARCReader'>")

    def testLocalFileLoad(self):
        h = fetcher.MARCFetcher('file:'+DIR_FIXTURES+'/marc-test', None)
        for n, rec in enumerate(h):  # enum starts at 0
            pass
            # print("NUM->{}:{}".format(n,rec))
        self.assertEqual(n, 9)
        self.assertIsInstance(rec, dict)
        self.assertEqual(rec['leader'], '01914nkm a2200277ia 4500')
        self.assertEqual(len(rec['fields']), 21)


class Harvest_MARC_ControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the function of an MARC harvest controller'''
    def setUp(self):
        super(Harvest_MARC_ControllerTestCase, self).setUp()

    def tearDown(self):
        super(Harvest_MARC_ControllerTestCase, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testMARCHarvest(self):
        '''Test the function of the MARC harvest'''
        httpretty.register_uri(httpretty.GET,
                'http://registry.cdlib.org/api/v1/collection/',
                body=open(DIR_FIXTURES+'/collection_api_test_marc.json').read())
        self.collection = Collection('http://registry.cdlib.org/api/v1/collection/')
        self.collection.url_harvest = 'file:'+DIR_FIXTURES+'/marc-test'
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        num = self.controller.harvest()
        self.assertEqual(num, 10)
        self.tearDown_config()


class NuxeoFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test Nuxeo fetching'''
    # put httppretty here, have sample outputs.
    @httpretty.activate
    def testInit(self):
        '''Basic tdd start'''
        httpretty.register_uri(httpretty.GET,
                'https://example.edu/api/v1/path/path-to-asset/here/@children',
                body=open(DIR_FIXTURES+'/nuxeo_folder.json').read())
        h = fetcher.NuxeoFetcher('https://example.edu/api/v1/', 'path-to-asset/here')
        self.assertTrue(hasattr(h, '_url'))  # assert in called next repeatedly
        self.assertEqual(h.url, 'https://example.edu/api/v1/')
        self.assertTrue(hasattr(h, '_nx'))
        self.assertIsInstance(h._nx, pynux.utils.Nuxeo)
        self.assertTrue(hasattr(h, '_children'))
        self.assertTrue(hasattr(h, 'next'))

    @httpretty.activate
    def testFetch(self):
        '''Test the httpretty mocked fetching of documents'''
        httpretty.register_uri(httpretty.GET,
                'https://example.edu/api/v1/path/path-to-asset/here/@children',
                responses=[
                    httpretty.Response(
                        body=open(DIR_FIXTURES+'/nuxeo_folder.json').read(),
                        status=200),
                    httpretty.Response(
                        body=open(DIR_FIXTURES+'/nuxeo_folder-1.json').read(),
                        status=200),
                ]
        )
        httpretty.register_uri(httpretty.GET,
                re.compile('https://example.edu/api/v1/id/.*'),
                body=open(DIR_FIXTURES+'/nuxeo_doc.json').read())
        h = fetcher.NuxeoFetcher('https://example.edu/api/v1/', 'path-to-asset/here')
        docs = []
        for d in h:
            docs.append(d)
        self.assertEqual(10, len(docs))
        self.assertEqual(docs[0], json.load(open(DIR_FIXTURES+'/nuxeo_doc.json')))
        self.assertIn('picture:views', docs[0]['properties'])
        self.assertIn('dc:subjects', docs[0]['properties'])


class UCLDCNuxeoFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test that the UCLDC Nuxeo Fetcher errors if necessary
    Nuxeo document schema header property not set.
    '''
    @httpretty.activate
    def testNuxeoPropHeader(self):
        '''Test that the Nuxeo document property header has necessary
        settings. This will test the base UCLDC schemas
        '''
        httpretty.register_uri(httpretty.GET,
                'https://example.edu/api/v1/path/path-to-asset/here/@children',
                body=open(DIR_FIXTURES+'/nuxeo_folder.json').read())
        # can test adding a prop, but what if prop needed not there.
        # need to remove ~/.pynuxrc
        self.assertRaises(AssertionError, fetcher.UCLDCNuxeoFetcher,
                'https://example.edu/api/v1/',
                'path-to-asset/here',
                conf_pynux={'X-NXDocumentProperties': ''}
        )
        self.assertRaises(AssertionError, fetcher.UCLDCNuxeoFetcher,
                'https://example.edu/api/v1/',
                'path-to-asset/here',
                conf_pynux={'X-NXDocumentProperties': 'dublincore'}
        )
        self.assertRaises(AssertionError, fetcher.UCLDCNuxeoFetcher,
                'https://example.edu/api/v1/',
                'path-to-asset/here',
                conf_pynux={'X-NXDocumentProperties': 'dublincore,ucldc_schema'}
        )
        h = fetcher.UCLDCNuxeoFetcher('https://example.edu/api/v1/',
                'path-to-asset/here',
                conf_pynux={'X-NXDocumentProperties': 'dublincore,ucldc_schema,picture'}
                )
        self.assertIn('dublincore', h._nx.conf['X-NXDocumentProperties'])
        self.assertIn('ucldc_schema', h._nx.conf['X-NXDocumentProperties'])
        self.assertIn('picture', h._nx.conf['X-NXDocumentProperties'])


class Harvest_UCLDCNuxeo_ControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the function of an Nuxeo harvest controller'''
    def setUp(self):
        super(Harvest_UCLDCNuxeo_ControllerTestCase, self).setUp()

    def tearDown(self):
        super(Harvest_UCLDCNuxeo_ControllerTestCase, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    # need to mock out ConfigParser to override pynuxrc
    @patch('ConfigParser.SafeConfigParser', autospec=True)
    @httpretty.activate
    def testNuxeoHarvest(self, mock_configparser):
        '''Test the function of the Nuxeo harvest'''
        config_inst = mock_configparser.return_value
        config_inst.get.return_value = 'dublincore,ucldc_schema,picture'
        httpretty.register_uri(httpretty.GET,
                'http://registry.cdlib.org/api/v1/collection/19/',
                body=open(DIR_FIXTURES+'/collection_api_test_nuxeo.json').read())
        httpretty.register_uri(httpretty.GET,
                'https://example.edu/Nuxeo/site/api/v1/path/asset-library/UCI/Cochems/@children',
                responses=[
                    httpretty.Response(
                        body=open(DIR_FIXTURES+'/nuxeo_folder.json').read(),
                        status=200),
                    httpretty.Response(
                        body=open(DIR_FIXTURES+'/nuxeo_folder-1.json').read(),
                        status=200),
                ]
        )
        httpretty.register_uri(httpretty.GET,
                re.compile('https://example.edu/Nuxeo/site/api/v1/id/.*'),
                body=open(DIR_FIXTURES+'/nuxeo_doc.json').read())
        self.collection = Collection('http://registry.cdlib.org/api/v1/collection/19/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController(
                'email@example.com',
                self.collection,
                config_file=self.config_file,
                profile_path=self.profile_path
        )
        self.assertTrue(hasattr(self.controller, 'harvest'))
        num = self.controller.harvest()
        self.assertEqual(num, 10)
        self.tearDown_config()
        # verify one record has collection and such filled in
        fname = os.listdir(self.controller.dir_save)[0]
        saved_objset = json.load(open(os.path.join(self.controller.dir_save, fname)))
        saved_obj = saved_objset[0]
        self.assertEqual(saved_obj['collection'], [
            {u'@id': u'http://registry.cdlib.org/api/v1/collection/19/',
                u'name': u'Cochems (Edward W.) Photographs',
                u'title': u'Cochems (Edward W.) Photographs',
                u'id': u'19',
                u'ingestType': u'collection',
                u'description': u'This collection is comprised of approximately 1,200 photographs taken by Edward W. Cochems, a prominent commercial photographer in Santa Ana, California, between ca. 1919-ca. 1949. The images are principally views of Southern California, primarily Orange County locations.'
            }]
        )
        self.assertEqual(saved_obj['campus'], [{u'@id': u'http://registry.cdlib.org/api/v1/campus/3/', u'name': u'UC Irvine'}])
        self.assertEqual(saved_obj['state'], 'project')
        self.assertEqual(saved_obj['title'], 'Adeline Cochems having her portrait taken by her father Edward W, Cochems in Santa Ana, California: Photograph')


class OAC_XML_FetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the OAC_XML_Fetcher
    '''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        super(OAC_XML_FetcherTestCase, self).setUp()
        self.fetcher = fetcher.OAC_XML_Fetcher('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134', 'extra_data')

    def tearDown(self):
        super(OAC_XML_FetcherTestCase, self).tearDown()

    @httpretty.activate
    def testBadOACSearch(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj--xxxx',
                body=open(DIR_FIXTURES+'/testOAC-badsearch.xml').read())
        self.assertRaises(ValueError, fetcher.OAC_XML_Fetcher, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj--xxxx', 'extra_data')

    @httpretty.activate
    def testOnlyTextResults(self):
        '''Test when only texts are in result'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-noimages-in-results.xml').read())
        h = fetcher.OAC_XML_Fetcher('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 11)
        recs = self.fetcher.next()
        self.assertEqual(self.fetcher.groups['text']['end'], 10)
        self.assertEqual(len(recs), 10)

    @httpretty.activate
    def testUTF8ResultsContent(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-utf8-content.xml').read())
        h = fetcher.OAC_XML_Fetcher('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 0)
        objset = h.next()
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 25)
        self.assertEqual(len(objset), 25)

    def testDocHitsToObjset(self):
        '''Check that the _docHits_to_objset to function returns expected
        object for a given input'''
        docHits = ET.parse(open(DIR_FIXTURES+'/docHit.xml')).getroot()
        objset = self.fetcher._docHits_to_objset([docHits])
        obj = objset[0]
        self.assertEqual(obj['relation'][0], {'attrib':{},
            'text':'http://www.oac.cdlib.org/findaid/ark:/13030/tf0c600134'})
        self.assertIsInstance(obj['relation'], list)
        self.assertIsNone(obj.get('google_analytics_tracking_code'))
        self.assertIsInstance(obj['reference-image'][0], dict)
        self.assertEqual(len(obj['reference-image']), 2)
        self.assertIn('X', obj['reference-image'][0])
        self.assertEqual(750, obj['reference-image'][0]['X'])
        self.assertIn('Y', obj['reference-image'][0])
        self.assertEqual(564, obj['reference-image'][0]['Y'])
        self.assertIn('src', obj['reference-image'][0])
        self.assertEqual('http://content.cdlib.org/ark:/13030/kt40000501/FID3', obj['reference-image'][0]['src'])
        self.assertIsInstance(obj['thumbnail'], dict)
        self.assertIn('X', obj['thumbnail'])
        self.assertEqual(125, obj['thumbnail']['X'])
        self.assertIn('Y', obj['thumbnail'])
        self.assertEqual(93, obj['thumbnail']['Y'])
        self.assertIn('src', obj['thumbnail'])
        self.assertEqual('http://content.cdlib.org/ark:/13030/kt40000501/thumbnail', obj['thumbnail']['src'])
        self.assertIsInstance(obj['publisher'][0], dict)
        self.assertEqual(obj['date'], [
            {'attrib':{'q':'created'}, 'text':'7/21/42'},
            {'attrib':{'q':'published'}, 'text':'7/21/72'}])

    def testDocHitsToObjsetBadImageData(self):
        '''Check when the X & Y for thumbnail or reference image is not an
        integer. Text have value of "" for X & Y'''
        docHits = ET.parse(open(DIR_FIXTURES+'/docHit-blank-image-sizes.xml')).getroot()
        objset = self.fetcher._docHits_to_objset([docHits])
        obj = objset[0]
        self.assertEqual(0, obj['reference-image'][0]['X'])
        self.assertEqual(0, obj['reference-image'][0]['Y'])
        self.assertEqual(0, obj['thumbnail']['X'])
        self.assertEqual(0, obj['thumbnail']['Y'])

    def testDocHitsToObjsetRemovesBlanks(self):
        '''Blank xml tags (no text value) get propagated through the system
        as "null" values. Eliminate them here to make data cleaner.
        '''
        docHit = ET.parse(open(DIR_FIXTURES+'/testOAC-blank-value.xml')).getroot()
        objset = self.fetcher._docHits_to_objset([docHit])
        obj = objset[0]
        self.assertEqual(obj['title'], [
            {'attrib':{},
             'text':'Main Street, Cisko Placer Co. [California]. Popular American Scenery.'}])

    @httpretty.activate
    def testFetchOnePage(self):
        '''Test fetching one "page" of results where no return trips are
        necessary
        '''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        self.assertTrue(hasattr(self.fetcher, 'totalDocs'))
        self.assertTrue(hasattr(self.fetcher, 'totalGroups'))
        self.assertTrue(hasattr(self.fetcher, 'groups'))
        self.assertIsInstance(self.fetcher.totalDocs, int)
        self.assertEqual(self.fetcher.totalDocs, 24)
        self.assertEqual(self.fetcher.groups['image']['total'], 13)
        self.assertEqual(self.fetcher.groups['image']['start'], 1)
        self.assertEqual(self.fetcher.groups['image']['end'], 0)
        self.assertEqual(self.fetcher.groups['text']['total'], 11)
        self.assertEqual(self.fetcher.groups['text']['start'], 0)
        self.assertEqual(self.fetcher.groups['text']['end'], 0)
        recs = self.fetcher.next()
        self.assertEqual(self.fetcher.groups['image']['end'], 10)
        self.assertEqual(len(recs), 10)

    @patch('requests.get', side_effect=DecodeError())
    @patch('time.sleep')
    def testDecodeErrorHandling(self, mock_sleep, mock_get):
        '''Test that the requests download tries 5 times if 
        it gets a DecodeError when decoding the gzip'd content.
        This occaisionally crops up when harvesting from OAC
        '''
        self.assertRaises(DecodeError, fetcher.OAC_XML_Fetcher, 'http://bogus', 'extra_data')
        mock_get.assert_has_calls([call('http://bogus&docsPerPage=100'),
                                   call('http://bogus&docsPerPage=100'),
                                   call('http://bogus&docsPerPage=100'),
                                   call('http://bogus&docsPerPage=100'),
                                   call('http://bogus&docsPerPage=100'),
                                   call('http://bogus&docsPerPage=100')]
                                  )

class OAC_XML_Fetcher_text_contentTestCase(LogOverrideMixin, TestCase):
    '''Test when results only contain texts'''
    @httpretty.activate
    def testFetchTextOnlyContent(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10',
                body=open(DIR_FIXTURES+'/testOAC-noimages-in-results.xml').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10&startDoc=1&group=text',
                body=open(DIR_FIXTURES+'/testOAC-noimages-in-results.xml').read())
        oac_fetcher = fetcher.OAC_XML_Fetcher('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_fetcher.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(oac_fetcher._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10&startDoc=11&group=text',
                body=open(DIR_FIXTURES+'/testOAC-noimages-in-results-1.xml').read())
        second_set = oac_fetcher.next()
        self.assertEqual(len(second_set), 1)
        self.assertEqual(oac_fetcher._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text')
        self.assertRaises(StopIteration, oac_fetcher.next)


class OAC_XML_Fetcher_mixed_contentTestCase(LogOverrideMixin, TestCase):
    @httpretty.activate
    def testFetchMixedContent(self):
        '''This interface gets tricky when image & text data are in the
        collection.
        My test Mock object will return an xml with 10 images
        then with 3 images
        then 10 texts
        then 1 text then quit
        '''
        httpretty.register_uri(httpretty.GET,
                 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=image',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        oac_fetcher = fetcher.OAC_XML_Fetcher('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_fetcher.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(oac_fetcher._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=image')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=image',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.xml').read())
        second_set = oac_fetcher.next()
        self.assertEqual(len(second_set), 3)
        self.assertEqual(oac_fetcher._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=image')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text',
                body=open(DIR_FIXTURES+'/testOAC-url_next-2.xml').read())
        third_set = oac_fetcher.next()
        self.assertEqual(len(third_set), 10)
        self.assertEqual(oac_fetcher._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text',
                body=open(DIR_FIXTURES+'/testOAC-url_next-3.xml').read())
        fourth_set = oac_fetcher.next()
        self.assertEqual(len(fourth_set), 1)
        self.assertEqual(oac_fetcher._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text')
        self.assertRaises(StopIteration, oac_fetcher.next)


class OAC_JSON_FetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the OAC_JSON_Fetcher
    '''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        super(OAC_JSON_FetcherTestCase, self).setUp()
        self.fetcher = fetcher.OAC_JSON_Fetcher('http://dsc.cdlib.org/search?rmode=json&facet=type-tab&style=cui&relation=ark:/13030/hb5d5nb7dj', 'extra_data')

    def tearDown(self):
        super(OAC_JSON_FetcherTestCase, self).tearDown()

    def testParseArk(self):
        self.assertEqual(self.fetcher._parse_oac_findaid_ark(self.fetcher.url), 'ark:/13030/hb5d5nb7dj')

    @httpretty.activate
    def testOAC_JSON_FetcherReturnedData(self):
        '''test that the data returned by the OAI Fetcher is a proper dc
        dictionary
        '''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        rec = self.fetcher.next()[0]
        self.assertIsInstance(rec, dict)

    @httpretty.activate
    def testHarvestByRecord(self):
        '''Test the older by single record interface'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.testFile = DIR_FIXTURES+'/testOAC-url_next-1.json'
        records = []
        r = self.fetcher.next_record()
        try:
            while True:
                records.append(r)
                r = self.fetcher.next_record()
        except StopIteration:
            pass
        self.assertEqual(len(records), 28)

    @httpretty.activate
    def testHarvestIsIter(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.fetcher, '__iter__'))
        self.assertEqual(self.fetcher, self.fetcher.__iter__())
        rec1 = self.fetcher.next_record()
        objset = self.fetcher.next()

    @httpretty.activate
    def testNextGroupFetch(self):
        '''Test that the OAC Fetcher will fetch more records when current
        response set records are all consumed'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.testFile = DIR_FIXTURES+'/testOAC-url_next-1.json'
        records = []
        self.ranGet = False
        for r in self.fetcher:
            records.extend(r)
        self.assertEqual(len(records), 28)

    @httpretty.activate
    def testObjsetFetch(self):
        '''Test fetching data in whole objsets'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.fetcher, 'next_objset'))
        self.assertTrue(hasattr(self.fetcher.next_objset, '__call__'))
        objset = self.fetcher.next_objset()
        self.assertIsNotNone(objset)
        self.assertIsInstance(objset, list)
        self.assertEqual(len(objset), 25)
        objset2 = self.fetcher.next_objset()
        self.assertTrue(objset != objset2)
        self.assertRaises(StopIteration, self.fetcher.next_objset)


