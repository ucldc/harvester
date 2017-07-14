import os
from unittest import TestCase
from unittest import skip
import shutil
import re
import json
import datetime
from mypretty import httpretty
# import httpretty
from mock import patch
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES, TEST_COUCH_DASHBOARD, TEST_COUCH_DB
import harvester.fetcher as fetcher
from harvester.collection_registry_client import Collection


class HarvestControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin,
                                TestCase):
    '''Test the harvest controller class'''
    @httpretty.activate
    def setUp(self):
        super(HarvestControllerTestCase, self).setUp()
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(
                httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        self.collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/197/')
        config_file, profile_path = self.setUp_config(self.collection)
        self.controller_oai = fetcher.HarvestController(
                'email@example.com', self.collection,
                profile_path=profile_path, config_file=config_file)
        self.objset_test_doc = json.load(open(
            DIR_FIXTURES + '/objset_test_doc.json'))

    def tearDown(self):
        super(HarvestControllerTestCase, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller_oai.dir_save)

    @httpretty.activate
    def testHarvestControllerExists(self):
        class myNow(datetime.datetime):
            @classmethod
            def now(cls):
                return cls(2017, 7, 14, 12, 1)
        datetime.datetime = myNow
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(
                httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/197/')
        controller = fetcher.HarvestController(
                'email@example.com', collection,
                config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(controller, 'fetcher'))
        self.assertIsInstance(controller.fetcher, fetcher.OAIFetcher)
        self.assertTrue(hasattr(controller, 'campus_valid'))
        self.assertTrue(hasattr(controller, 'dc_elements'))
        self.assertTrue(hasattr(controller, 'datetime_start'))
        print(controller.s3path)
        self.assertEqual(controller.s3path,
                'data-fetched/197/2017-07-14-1201/')
        shutil.rmtree(controller.dir_save)

    def testOAIFetcherType(self):
        '''Check the correct object returned for type of harvest'''
        self.assertIsInstance(self.controller_oai.fetcher, fetcher.OAIFetcher)
        self.assertEqual(self.controller_oai.collection.campus[0]['slug'],
                         'UCDL')

    def testUpdateIngestDoc(self):
        '''Test that the update to the ingest doc in couch is called correctly
        '''
        self.assertTrue(hasattr(self.controller_oai, 'update_ingest_doc'))
        self.assertRaises(TypeError, self.controller_oai.update_ingest_doc)
        self.assertRaises(ValueError, self.controller_oai.update_ingest_doc,
                          'error')
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            foo = {}
            with patch.dict(foo, {'test-id': 'test-ingest-doc'}):
                instance.dashboard_db = foo
                self.controller_oai.update_ingest_doc('error',
                                                      error_msg="BOOM!")
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
        self.controller_oai.create_ingest_doc()
        mock_couch.assert_called_with(config_file=self.config_file,
                                      dashboard_db_name=TEST_COUCH_DASHBOARD,
                                      dpla_db_name=TEST_COUCH_DB)

    def testUpdateFailInCreateIngestDoc(self):
        '''Test the failure of the update to the ingest doc'''
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            instance.update_ingestion_doc.side_effect = Exception('Boom!')
            self.assertRaises(Exception,
                              self.controller_oai.create_ingest_doc)

    def testCreateIngestDoc(self):
        '''Test the creation of the DPLA style ingest document in couch.
        This will call _create_ingestion_document, dashboard_db and
        update_ingestion_doc
        '''
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
            instance._create_ingestion_document.assert_called_with(
                    self.collection.provider, 'http://localhost:8889',
                    self.profile_path,
                    self.collection.dpla_profile_obj['thresholds'])
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
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/198/",
                body=open(DIR_FIXTURES+'/collection_api_big_test.json').read())
        httpretty.register_uri(
                httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-2400-records.xml').read())
        collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/198/')
        controller = fetcher.HarvestController(
                'email@example.com', collection,
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
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(
                httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/197/')
        self.tearDown_config()  # remove ones setup in setUp
        self.setUp_config(collection)
        controller = fetcher.HarvestController(
                'email@example.com', collection,
                config_file=self.config_file, profile_path=self.profile_path)
        obj = {'id': 'fakey', 'otherdata': 'test'}
        self.assertNotIn('collection', obj)
        controller._add_registry_data(obj)
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
        objset_saved = json.loads(open(os.path.join(
            self.controller_oai.dir_save, dir_list[0])).read())
        obj_saved = objset_saved[0]
        self.assertIn('collection', obj_saved)
        self.assertEqual(obj_saved['collection'][0]['@id'],
                         'https://registry.cdlib.org/api/v1/collection/197/')
        self.assertEqual(
                obj_saved['collection'][0]['title'],
                'Calisphere - Santa Clara University: Digital Objects')
        self.assertEqual(obj_saved['collection'][0]['ingestType'],
                         'collection')
        self.assertNotIn('campus', obj_saved)
        self.assertEqual(
                obj_saved['collection'][0]['campus'],
                [{'@id': 'https://registry.cdlib.org/api/v1/campus/12/',
                  "slug": "UCDL",
                  "resource_uri": "/api/v1/campus/12/",
                  "position": 11,
                  "name": "California Digital Library"
                  }
                 ])
        self.assertNotIn('repository', obj_saved)
        self.assertEqual(
                obj_saved['collection'][0]['repository'],
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
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/101/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(
                httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-no-records.xml').read())
        collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/101/')
        controller = fetcher.HarvestController(
                'email@example.com', collection,
                config_file=self.config_file, profile_path=self.profile_path)
        self.assertRaises(fetcher.NoRecordsFetchedException,
                          controller.harvest)


class FetcherClassTestCase(TestCase):
    '''Test the abstract Fetcher class'''
    def testClassExists(self):
        h = fetcher.Fetcher
        h = h('url_harvest', 'extra_data')
