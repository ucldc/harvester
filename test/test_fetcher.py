import os
from unittest import TestCase
from unittest import skip
import shutil
import re
from xml.etree import ElementTree as ET
import json
import httpretty
from mock import patch, call
from mock import Mock
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES, TEST_COUCH_DASHBOARD, TEST_COUCH_DB
import harvester.fetcher as fetcher
from harvester.collection_registry_client import Collection
import pynux.utils
from requests.packages.urllib3.exceptions import DecodeError
import pickle

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
        self.assertIn('source_collection_name', obj)
        self.assertEqual(obj['source_collection_name'],
                'record source collection name')
        self.assertIn('collection', obj)
        self.assertIn('@id', obj['collection'][0])
        self.assertIn('title', obj['collection'][0])
        self.assertIn('campus', obj['collection'][0])
        self.assertIn('dcmi_type', obj['collection'][0])
        self.assertIn('description', obj['collection'][0])
        self.assertIn('enrichments_item', obj['collection'][0])
        self.assertIn('name', obj['collection'][0])
        self.assertIn('harvest_extra_data', obj['collection'][0])
        self.assertIn('repository', obj['collection'][0])
        self.assertIn('rights_statement', obj['collection'][0])
        self.assertIn('rights_status', obj['collection'][0])
        self.assertIn('url_harvest', obj['collection'][0])
        self.assertEqual(obj['collection'][0]['@id'], 'https://registry.cdlib.org/api/v1/collection/178/')
        self.assertNotIn('campus', obj)
        self.assertEqual(obj['collection'][0]['campus'],
                            [
                               {
                                   '@id': 'https://registry.cdlib.org/api/v1/campus/6/',
                                 'slug': 'UCSD',
                                 'resource_uri': '/api/v1/campus/6/',
                                 'position': 6,
                                 'name': 'UC San Diego'
                               },
                               {
                                 '@id': 'https://registry.cdlib.org/api/v1/campus/1/',
                                 'slug': 'UCB',
                                 'resource_uri': '/api/v1/campus/1/',
                                 'position': 0,
                                 'name': 'UC Berkeley'
                               }
                             ])
        self.assertNotIn('repository', obj)
        self.assertEqual(obj['collection'][0]['repository'],
                        [
                          {
                            '@id': 'https://registry.cdlib.org/api/v1/repository/22/',
                            'resource_uri': '/api/v1/repository/22/',
                            'name': 'Mandeville Special Collections Library',
                            'slug': 'Mandeville-Special-Collections-Library',
                            'campus': [
                              {
                                'slug': 'UCSD',
                                'resource_uri': '/api/v1/campus/6/',
                                'position': 6,
                                'name': 'UC San Diego'
                              },
                              {
                                'slug': 'UCB',
                                'resource_uri': '/api/v1/campus/1/',
                                'position': 0,
                                'name': 'UC Berkeley'
                              }
                            ]
                          },
                          {
                            '@id': 'https://registry.cdlib.org/api/v1/repository/36/',
                            'resource_uri': '/api/v1/repository/36/',
                            'name': 'UCB Department of Statistics',
                            'slug': 'UCB-Department-of-Statistics',
                            'campus': {
                                'slug': 'UCB',
                                'resource_uri': '/api/v1/campus/1/',
                                'position': 0,
                                'name': 'UC Berkeley'
                              }
                          }
                        ])


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
        self.assertTrue(hasattr(self.controller_oai, '_config'))
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
            self.assertIn("fetch_process/data_dir", call_args)
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
        objset_saved = json.loads(
            open(os.path.join(self.controller_oai.dir_save, dir_list[0])
                ).read()
            )
        self.assertEqual(self.objset_test_doc, objset_saved)

    @skip('Takes too long')
    @httpretty.activate
    def testLoggingMoreThan1000(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/198/",
                body=open(DIR_FIXTURES+'/collection_api_big_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-2400-records.xml').read())
        collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/198/')
        controller = fetcher.HarvestController('email@example.com', collection,
                config_file=self.config_file, profile_path=self.profile_path)
        controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 13)
        self.assertEqual(self.test_log_handler.formatted_records[1],
                '[INFO] HarvestController: 100 records harvested')
        shutil.rmtree(controller.dir_save)
        self.assertEqual(self.test_log_handler.formatted_records[10],
                '[INFO] HarvestController: 1000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[11],
                '[INFO] HarvestController: 2000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[12],
                '[INFO] HarvestController: 2400 records harvested')

    @httpretty.activate
    def testAddRegistryData(self):
        '''Unittest the _add_registry_data function'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())

        collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/197/')
        self.tearDown_config()  # remove ones setup in setUp
        self.setUp_config(collection)
        controller = fetcher.HarvestController('email@example.com', collection,
                config_file=self.config_file, profile_path=self.profile_path)
        obj = {'id': 'fakey', 'otherdata': 'test'}
        self.assertNotIn('collection', obj)
        objnew = controller._add_registry_data(obj)
        self.assertIn('collection', obj)
        self.assertEqual(obj['collection'][0]['@id'],
                'https://registry.cdlib.org/api/v1/collection/197/')
        self.assertNotIn('campus', obj)
        self.assertIn('campus', obj['collection'][0])
        self.assertNotIn('repository', obj)
        self.assertIn('repository', obj['collection'][0])
        # need to test one without campus
        self.assertEqual(obj['collection'][0]['campus'][0]['@id'],
                'https://registry.cdlib.org/api/v1/campus/12/')
        self.assertEqual(obj['collection'][0]['repository'][0]['@id'],
                'https://registry.cdlib.org/api/v1/repository/37/')

    def testObjectsHaveRegistryData(self):
        '''Test that the registry data is being attached to objects from
        the harvest controller'''
        self.controller_oai.harvest()
        dir_list = os.listdir(self.controller_oai.dir_save)
        self.assertEqual(len(dir_list), 128)
        objset_saved = json.loads(open(os.path.join(self.controller_oai.dir_save, dir_list[0])).read())
        obj_saved = objset_saved[0]
        self.assertIn('collection', obj_saved)
        self.assertEqual(obj_saved['collection'][0]['@id'],
                    'https://registry.cdlib.org/api/v1/collection/197/')
        self.assertEqual(obj_saved['collection'][0]['title'],
                    'Calisphere - Santa Clara University: Digital Objects')
        self.assertEqual(obj_saved['collection'][0]['ingestType'],
                                    'collection')
        self.assertNotIn('campus', obj_saved)
        self.assertEqual(obj_saved['collection'][0]['campus'],
                [{'@id': 'https://registry.cdlib.org/api/v1/campus/12/',
                  "slug": "UCDL",
                  "resource_uri": "/api/v1/campus/12/",
                  "position": 11,
                  "name": "California Digital Library"
                            }
                        ])
        self.assertNotIn('repository', obj_saved)
        self.assertEqual(obj_saved['collection'][0]['repository'],
                [{'@id': 'https://registry.cdlib.org/api/v1/repository/37/',
                  "resource_uri": "/api/v1/repository/37/",
                  "name": "Calisphere",
                  "slug": "Calisphere",
                  "campus": [
                                {
                                    "slug": "UCDL",
                                    "resource_uri": "/api/v1/campus/12/",
                                    "position": 11,
                                    "name": "California Digital Library"
                                }
                            ]
                    }])

    @httpretty.activate
    def testFailsIfNoRecords(self):
        '''Test that the Controller throws an error if no records come back
        from fetcher
        '''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/101/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-no-records.xml').read())
        collection = Collection('https://registry.cdlib.org/api/v1/collection/101/')
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        self.assertRaises(fetcher.NoRecordsFetchedException, controller.harvest)


class FetcherClassTestCase(TestCase):
    '''Test the abstract Fetcher class'''
    def testClassExists(self):
        h = fetcher.Fetcher
        h = h('url_harvest', 'extra_data')






class CMISAtomFeedFetcherTestCase(LogOverrideMixin, TestCase):
    @httpretty.activate
    def testCMISFetch(self):
        httpretty.register_uri(httpretty.GET,
                'http://cmis-atom-endpoint/descendants',
                body=open(DIR_FIXTURES+'/cmis-atom-descendants.xml').read())
        h = fetcher.CMISAtomFeedFetcher('http://cmis-atom-endpoint/descendants',
                'uname, pswd')
        self.assertTrue(hasattr(h, 'objects'))
        self.assertEqual(42, len(h.objects))

    @httpretty.activate
    def testFetching(self):
        httpretty.register_uri(httpretty.GET,
                'http://cmis-atom-endpoint/descendants',
                body=open(DIR_FIXTURES+'/cmis-atom-descendants.xml').read())
        h = fetcher.CMISAtomFeedFetcher('http://cmis-atom-endpoint/descendants',
                'uname, pswd')
        num_fetched = 0
        for obj in h:
            num_fetched += 1
        self.assertEqual(num_fetched, 42)




class UCSFXMLFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for the UCSF xml search interface'''
    @httpretty.activate
    def testInit(self):
        '''Basic tdd start'''
        url = 'https://example.edu/action/search/xml?q=ddu%3A20*&asf=ddu&asd=&fd=1&_hd=&hd=on&sf=&_rs=&_ef=&ef=on&sd=&ed=&c=ga'
        httpretty.register_uri(httpretty.GET,
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
        url = 'https://example.edu/action/search/xml?q=ddu%3A20*&asf=ddu&asd=&fd=1&_hd=&hd=on&sf=&_rs=&_ef=&ef=on&sd=&ed=&c=ga'
        httpretty.register_uri(httpretty.GET,
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




class HarvestOAC_XML_ControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the function of an OAC XML harvest controller'''
    @httpretty.activate
    def setUp(self):
        super(HarvestOAC_XML_ControllerTestCase, self).setUp()
        # self.testFile = DIR_FIXTURES+'/collection_api_test_oac.json'
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/178/",
                body=open(DIR_FIXTURES+'/collection_api_test_oac_xml.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134',
                body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        self.collection = Collection('https://registry.cdlib.org/api/v1/collection/178/')
        #print "COLLECTION DIR:{}".format(dir(self.collection))
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)
        print "DIR SAVE::::: {}".format(self.controller.dir_save)

    def tearDown(self):
        super(HarvestOAC_XML_ControllerTestCase, self).tearDown()
        self.tearDown_config()
        #shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testOAC_XML_Harvest(self):
        '''Test the function of the OAC harvest'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134',
                #'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.xml').read())
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.controller.harvest()
        print "LOGS:{}".format(self.test_log_handler.formatted_records)
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue('UCB Department of Statistics' in self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 24 records harvested')


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

    @httpretty.activate
    def testAmpersandInDoc(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open(DIR_FIXTURES+'/testOAC-utf8-content.xml').read())
        h = fetcher.OAC_XML_Fetcher('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 0)
        objset = h.next()


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
        '''test that the data returned by the OAC Fetcher is a proper dc
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


