import os
import sys
import unittest
from unittest import TestCase
import codecs
import re
import json
import shutil
import tempfile
import pickle
from mock import MagicMock
from mock import Mock
from mock import call as mcall
from mock import patch
from mock import call, ANY
from xml.etree import ElementTree as ET
import requests
import logbook
import httpretty
from redis import Redis
import harvester
from harvester.fetcher import get_log_file_path
from harvester.collection_registry_client import Registry, Collection
from harvester.queue_harvest import main as queue_harvest_main
from harvester.queue_harvest import get_redis_connection, check_redis_queue
from harvester.queue_harvest import start_ec2_instances
from harvester.parse_env import parse_env
from harvester.solr_updater import main as solr_updater_main
from harvester.solr_updater import push_doc_to_solr, map_couch_to_solr_doc
from harvester.solr_updater import set_couchdb_last_seq, get_couchdb_last_seq
from harvester import grab_solr_index

#from harvester import Collection
from dplaingestion.couch import Couch
import harvester.run_ingest as run_ingest
import harvester.fetcher as fetcher

#NOTE: these are used in integration test runs
TEST_COUCH_DB = 'test-ucldc'
TEST_COUCH_DASHBOARD = 'test-dashboard'

def skipUnlessIntegrationTest(selfobj=None):
    '''Skip the test unless the environmen variable RUN_INTEGRATION_TESTS is set
    TO run integration tests need the following:
    - Registry server
    - couchdb server with databases setup
    - redis server
    - solr server with schema
    '''
    if os.environ.get('RUN_INTEGRATION_TESTS', False):
        return lambda func: func
    return unittest.skip('RUN_INTEGRATION_TESTS not set. Skipping integration tests.')

class LogOverrideMixin(object):
    '''Mixin to use logbook test_handler for logging'''
    def setUp(self):
        '''Use test_handler'''
        super(LogOverrideMixin, self).setUp()
        self.test_log_handler = logbook.TestHandler()
        def deliver(msg, email):
            #print ' '.join(('Mail sent to ', email, ' MSG: ', msg))
            pass
        self.test_log_handler.deliver = deliver
        self.test_log_handler.push_thread()

    def tearDown(self):
        self.test_log_handler.pop_thread()


class ConfigFileOverrideMixin(object):
    '''Create temporary config and profile files for use by the DPLA couch
    module when creating the ingest doc.
    Returns names of 2 tempfiles for use as config and profile.'''
    def setUp_config(self, collection):
        f, self.config_file = tempfile.mkstemp()
        with open(self.config_file, 'w') as f:
            f.write(CONFIG_FILE_DPLA)
        f, self.profile_path = tempfile.mkstemp()
        with open(self.profile_path, 'w') as f:
            f.write(collection.dpla_profile)
        return self.config_file, self.profile_path

    def tearDown_config(self):
        os.remove(self.config_file)
        os.remove(self.profile_path)

class RegistryApiTestCase(TestCase):
    '''Test that the registry api works for our purposes'''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/',
                body='''{"campus": {"list_endpoint": "/api/v1/campus/", "schema": "/api/v1/campus/schema/"}, "collection": {"list_endpoint": "/api/v1/collection/", "schema": "/api/v1/collection/schema/"}, "repository": {"list_endpoint": "/api/v1/repository/", "schema": "/api/v1/repository/schema/"}}''')
        self.registry = Registry()

    def testRegistryListEndpoints(self):
        self.assertEqual(set(self.registry.endpoints.keys()), set(['collection', 'repository', 'campus'])) #use set so order independent
        self.assertRaises(ValueError, self.registry.resource_iter, 'x')

    @httpretty.activate
    def testResourceIteratorOnePage(self):
        '''Test when less than one page worth of objects fetched'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/campus/',
                body=open('./fixtures/registry_api_campus.json').read())
        l = []
        for c in self.registry.resource_iter('campus'):
            l.append(c)
        self.assertEqual(len(l), 10)
        self.assertEqual(l[0]['slug'], 'UCB')

    @httpretty.activate
    def testResourceIteratoreMultiPage(self):
        '''Test when less than one page worth of objects fetched'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/repository/?limit=20&offset=20',
                body=open('./fixtures/registry_api_repository-page-2.json').read())
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/repository/',
                body=open('./fixtures/registry_api_repository.json').read())

        riter = self.registry.resource_iter('repository')
        self.assertEqual(riter.url, 'https://registry.cdlib.org/api/v1/repository/')
        self.assertEqual(riter.path_next, '/api/v1/repository/?limit=20&offset=20')
        r = ''
        for x in range(0,38):
            r = riter.next()
        self.assertFalse(isinstance(r, Collection))
        self.assertEqual(r['resource_uri'], '/api/v1/repository/42/')
        self.assertEqual(riter.url, 'https://registry.cdlib.org/api/v1/repository/?limit=20&offset=20')
        self.assertEqual(riter.path_next, None)
        self.assertRaises(StopIteration, riter.next)

    def testResourceIteratorReturnsCollection(self):
        '''Test that the resource iterator returns a Collection object
        for library collection resources'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/',
                body=open('./fixtures/registry_api_collection.json').read())
        riter = self.registry.resource_iter('collection')
        c = riter.next()
        self.assertTrue(isinstance(c, Collection))


class ApiCollectionTestCase(TestCase):
    '''Test that the Collection object is complete from the api
    '''
    @httpretty.activate
    def testOAICollectionAPI(self):
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/197',
                body=open('./fixtures/collection_api_test.json').read())
        c = Collection('https://registry.cdlib.org/api/v1/collection/197')
        self.assertEqual(c['harvest_type'], 'OAI')
        self.assertEqual(c.harvest_type, 'OAI')
        self.assertEqual(c['name'], 'Calisphere - Santa Clara University: Digital Objects')
        self.assertEqual(c.name, 'Calisphere - Santa Clara University: Digital Objects')
        self.assertEqual(c['url_oai'], 'fixtures/testOAI-128-records.xml')
        self.assertEqual(c.url_oai, 'fixtures/testOAI-128-records.xml')
        self.assertEqual(c.campus[0]['resource_uri'], '/api/v1/campus/12/')
        self.assertEqual(c.campus[0]['slug'], 'UCDL')

    @httpretty.activate
    def testOACApiCollection(self):
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/178',
                body=open('./fixtures/collection_api_test_oac.json').read())
        c = Collection('https://registry.cdlib.org/api/v1/collection/178')
        self.assertEqual(c['harvest_type'], 'OAJ')
        self.assertEqual(c.harvest_type, 'OAJ')
        self.assertEqual(c['name'], 'Harry Crosby Collection')
        self.assertEqual(c.name, 'Harry Crosby Collection')
        self.assertEqual(c['url_oac'], 'fixtures/testOAC.json')
        self.assertEqual(c.url_oac, 'fixtures/testOAC.json')
        self.assertEqual(c.campus[0]['resource_uri'], '/api/v1/campus/6/')
        self.assertEqual(c.campus[0]['slug'], 'UCSD')

    @httpretty.activate
    def testCreateProfile(self):
        '''Test the creation of a DPLA style proflie file'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/178',
                body=open('./fixtures/collection_api_test_oac.json').read())
        c = Collection('https://registry.cdlib.org/api/v1/collection/178')
        self.assertTrue(hasattr(c, 'dpla_profile'))
        self.assertIsInstance(c.dpla_profile, str)
        j = json.loads(c.dpla_profile)
        self.assertEqual(j['name'], 'harry-crosby-collection-black-white-photographs-of')
        self.assertEqual(j['enrichments_coll'], [ '/compare_with_schema' ])
        self.assertTrue('enrichments_item' in j)
        self.assertIsInstance(j['enrichments_item'], list)
        self.assertEqual(len(j['enrichments_item']), 30)
        self.assertIn('contributor', j)
        self.assertIsInstance(j['contributor'], list)
        self.assertEqual(len(j['contributor']) , 4)
        self.assertEqual(j['contributor'][1] , {u'@id': u'/api/v1/campus/1/', u'name': u'UCB'})
        self.assertTrue(hasattr(c, 'dpla_profile_obj'))
        self.assertIsInstance(c.dpla_profile_obj, dict)
        self.assertIsInstance(c.dpla_profile_obj['enrichments_item'], list)
        e = c.dpla_profile_obj['enrichments_item']
        self.assertEqual(e[0], '/oai-to-dpla')
        self.assertEqual(e[1], '/shred?prop=sourceResource/contributor%2CsourceResource/creator%2CsourceResource/date')


class HarvestOAC_JSON_ControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the function of an OAC harvest controller'''
    @httpretty.activate
    def setUp(self):
        super(HarvestOAC_JSON_ControllerTestCase, self).setUp()
        #self.testFile = 'fixtures/collection_api_test_oac.json'
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/178/",
                body=open('./fixtures/collection_api_test_oac.json').read())
        httpretty.register_uri(httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open('./fixtures/testOAC.json').read())
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
                body=open('./fixtures/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue('UCB Department of Statistics' in self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 28 records harvested')

    @httpretty.activate
    def testObjectsHaveRegistryData(self):
        #test OAC objsets
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open('./fixtures/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928&startDoc=26',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        self.ranGet = False
        self.controller.harvest()
        dir_list = os.listdir(self.controller.dir_save)
        self.assertEqual(len(dir_list), 2)
        objset_saved = json.loads(open(os.path.join(self.controller.dir_save, dir_list[0])).read())
        obj = objset_saved[2]
        self.assertIn('collection', obj)
        self.assertEqual(obj['collection'], {'@id':'https://registry.cdlib.org/api/v1/collection/178/', 'name':'Harry Crosby Collection'})
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
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://content.cdlib.org/oai',
                body=open('./fixtures/testOAC-url_next-0.xml').read())
        self.collection = Collection('http://registry.cdlib.org/api/v1/collection/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        #TODO: fix why logbook.TestHandler not working for the previous logging
        #self.assertEqual(len(self.test_log_handler.records), 2)
        self.tearDown_config()


class HarvestControllerTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the harvest controller class'''
    @httpretty.activate
    def setUp(self):
        super(HarvestControllerTestCase, self).setUp()
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        #self.collection = Collection('fixtures/collection_api_test.json')
        self.collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        config_file, profile_path = self.setUp_config(self.collection) 
        self.controller_oai = fetcher.HarvestController('email@example.com', self.collection, profile_path=profile_path, config_file=config_file)
        self.objset_test_doc = json.load(open('objset_test_doc.json'))

    def tearDown(self):
        super(HarvestControllerTestCase, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller_oai.dir_save)

    @httpretty.activate
    def testHarvestControllerExists(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/101/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        collection = Collection('https://registry.cdlib.org/api/v1/collection/101/')
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path) 
        self.assertTrue(hasattr(controller, 'harvester'))
        self.assertIsInstance(controller.harvester, fetcher.OAIHarvester)
        self.assertTrue(hasattr(controller, 'campus_valid'))
        self.assertTrue(hasattr(controller, 'dc_elements'))
        shutil.rmtree(controller.dir_save)

    def testOAIHarvesterType(self):
        '''Check the correct object returned for type of harvest'''
        self.assertIsInstance(self.controller_oai.harvester, fetcher.OAIHarvester)
        self.assertEqual(self.controller_oai.collection.campus[0]['slug'], 'UCDL')

    @httpretty.activate
    def testIDCreation(self):
        '''Test how the id for the index is created'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        self.assertTrue(hasattr(self.controller_oai, 'create_id'))
        identifier = 'x'
        self.assertRaises(TypeError, self.controller_oai.create_id, identifier)
        identifier = ['x',]
        sid = self.controller_oai.create_id(identifier)
        self.assertIn(self.controller_oai.collection.slug, sid)
        self.assertIn(self.controller_oai.collection.campus[0]['slug'], sid)
        self.assertIn(self.controller_oai.collection.repository[0]['slug'], sid)
        self.assertEqual(sid, 'UCDL-Calisphere-calisphere-santa-clara-university-digital-objects-x')
        collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        #collection = Collection('fixtures/collection_api_test.json')
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
            with patch.dict(foo, {'test-id':'test-ingest-doc'}):
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
            with patch.dict(foo, {'test-id':'test-ingest-doc'}):
                instance.dashboard_db = foo
                ingest_doc_id = self.controller_oai.create_ingest_doc()
            self.assertIsNotNone(ingest_doc_id)
            self.assertEqual(ingest_doc_id, 'test-id')
            instance._create_ingestion_document.assert_called_with(self.collection.slug, 'http://localhost:8889', self.profile_path, self.collection.dpla_profile_obj['thresholds'])
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
        #did it save?
        dir_list = os.listdir(self.controller_oai.dir_save)
        self.assertEqual(len(dir_list), 1)
        objset_saved = json.loads(open(os.path.join(self.controller_oai.dir_save, dir_list[0])).read())
        self.assertEqual(self.objset_test_doc, objset_saved)

    @httpretty.activate
    def testLoggingMoreThan1000(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/198/",
                body=open('./fixtures/collection_api_big_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-2400-records.xml').read())
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
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())

        collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        self.tearDown_config() # remove ones setup in setUp
        self.setUp_config(collection)
        controller = fetcher.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        obj = {'id':'fakey', 'otherdata':'test'}
        self.assertNotIn('collection', obj)
        objnew = controller._add_registry_data(obj)
        self.assertIn('collection', obj)
        self.assertEqual(obj['collection']['@id'], 'https://registry.cdlib.org/api/v1/collection/197/')
        self.assertIn('campus', obj)
        self.assertIn('repository', obj)
        #need to test one without campus
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
        self.assertEqual(obj_saved['collection'], {'@id':'https://registry.cdlib.org/api/v1/collection/197/',
            'name':'Calisphere - Santa Clara University: Digital Objects'})
        self.assertIn('campus', obj_saved)
        self.assertEqual(obj_saved['campus'], [{'@id':'https://registry.cdlib.org/api/v1/campus/12/',
            'name':'California Digital Library'}])
        self.assertIn('repository', obj_saved)
        self.assertEqual(obj_saved['repository'], [{'@id':'https://registry.cdlib.org/api/v1/repository/37/',
            'name':'Calisphere'}])

@skipUnlessIntegrationTest()
class CouchIntegrationTestCase(ConfigFileOverrideMixin, TestCase):
    def setUp(self):
        super(CouchIntegrationTestCase, self).setUp()
        self.collection = Collection('fixtures/collection_api_test.json')
        config_file, profile_path = self.setUp_config(self.collection) 
        self.controller_oai = fetcher.HarvestController('email@example.com', self.collection, profile_path=profile_path, config_file=config_file)
        self.remove_log_dir = False
        if not os.path.isdir('logs'):
            os.makedirs('logs')
            self.remove_log_dir = True

    def tearDown(self):
        super(CouchIntegrationTestCase, self).tearDown()
###        couch = Couch(config_file=self.config_file,
###                dpla_db_name = TEST_COUCH_DB,
###                dashboard_db_name = TEST_COUCH_DASHBOARD
###            )
###        db = couch.server[TEST_COUCH_DASHBOARD]
###        doc = db.get(self.ingest_doc_id)
###        db.delete(doc)
###        self.tearDown_config()
        if self.remove_log_dir:
            shutil.rmtree('logs')


    def testCouchDocIntegration(self):
        '''Test the couch document creation in a test environment'''
        self.ingest_doc_id = self.controller_oai.create_ingest_doc()
        self.controller_oai.update_ingest_doc('error', error_msg='This is an error')

class HarvesterClassTestCase(TestCase):
    '''Test the abstract Harvester class'''
    def testClassExists(self):
        h = fetcher.Harvester
        h = h('url_harvest', 'extra_data')


class OAIHarvesterTestCase(LogOverrideMixin, TestCase):
    '''Test the OAIHarvester
    '''
    @httpretty.activate
    def setUp(self):
        super(OAIHarvesterTestCase, self).setUp()
        httpretty.register_uri(httpretty.GET,
                'http://content.cdlib.org/oai?verb=ListRecords&metadataPrefix=oai_dc&set=oac:images',
                body=open('./fixtures/testOAI.xml').read())
        self.harvester = fetcher.OAIHarvester('http://content.cdlib.org/oai', 'oac:images')

    def tearDown(self):
        super(OAIHarvesterTestCase, self).tearDown()

    def testHarvestIsIter(self):
        self.assertTrue(hasattr(self.harvester, '__iter__')) 
        self.assertEqual(self.harvester, self.harvester.__iter__())
        rec1 = self.harvester.next()

    def testOAIHarvesterReturnedData(self):
        '''test that the data returned by the OAI harvester is a proper dc
        dictionary
        '''
        rec = self.harvester.next()
        self.assertIsInstance(rec, dict)
        self.assertIn('handle', rec)

class OAC_XML_HarvesterTestCase(LogOverrideMixin, TestCase):
    '''Test the OAC_XML_Harvester
    '''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134',
                body=open('./fixtures/testOAC-url_next-0.xml').read())
        #self.testFile = 'fixtures/testOAC-url_next-0.xml'
        super(OAC_XML_HarvesterTestCase, self).setUp()
        self.harvester = fetcher.OAC_XML_Harvester('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134', 'extra_data')

    def tearDown(self):
        super(OAC_XML_HarvesterTestCase, self).tearDown()

    @httpretty.activate
    def testBadOACSearch(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj--xxxx',
                body=open('./fixtures/testOAC-badsearch.xml').read())
        #self.testFile = 'fixtures/testOAC-badsearch.xml'
        self.assertRaises(ValueError, fetcher.OAC_XML_Harvester, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj--xxxx', 'extra_data')

    @httpretty.activate
    def testOnlyTextResults(self):
        '''Test when only texts are in result'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-noimages-in-results.xml').read())
        #self.testFile = 'fixtures/testOAC-noimages-in-results.xml'
        h = fetcher.OAC_XML_Harvester( 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 11)
        recs = self.harvester.next()
        self.assertEqual(self.harvester.groups['text']['end'], 10)
        self.assertEqual(len(recs), 10)

    @httpretty.activate
    def testUTF8ResultsContent(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-utf8-content.xml').read())
        #self.testFile = 'fixtures/testOAC-utf8-content.xml'
        h = fetcher.OAC_XML_Harvester( 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 0)
        objset = h.next()
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 25)
        self.assertEqual(len(objset), 25)

    def testDocHitsToObjset(self):
        '''Check that the _docHits_to_objset to function returns expected
        object for a given input'''
        docHits = ET.parse(open('fixtures/docHit.xml')).getroot()
        objset = self.harvester._docHits_to_objset([docHits])
        obj = objset[0]
        self.assertIsNotNone(obj.get('handle'))
        self.assertEqual(obj['handle'][0], 'http://ark.cdlib.org/ark:/13030/kt40000501')
        self.assertEqual(obj['handle'][1], '[15]')
        self.assertEqual(obj['handle'][2], 'brk00000755_7a.tif')
        self.assertEqual(obj['relation'][0], 'http://www.oac.cdlib.org/findaid/ark:/13030/tf0c600134')
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
        self.assertIsInstance(obj['publisher'], str)

    def testDocHitsToObjsetBadImageData(self):
        '''Check when the X & Y for thumbnail or reference image is not an 
        integer. Text have value of "" for X & Y'''
        docHits = ET.parse(open('fixtures/docHit-blank-image-sizes.xml')).getroot()
        objset = self.harvester._docHits_to_objset([docHits])
        obj = objset[0]
        self.assertEqual(0, obj['reference-image'][0]['X'])
        self.assertEqual(0, obj['reference-image'][0]['Y'])
        self.assertEqual(0, obj['thumbnail']['X'])
        self.assertEqual(0, obj['thumbnail']['Y'])

    @httpretty.activate
    def testFetchOnePage(self):
        '''Test fetching one "page" of results where no return trips are
        necessary
        '''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-url_next-0.xml').read())
        self.assertTrue(hasattr(self.harvester, 'totalDocs'))
        self.assertTrue(hasattr(self.harvester, 'totalGroups'))
        self.assertTrue(hasattr(self.harvester, 'groups'))
        self.assertIsInstance(self.harvester.totalDocs, int)
        self.assertEqual(self.harvester.totalDocs, 24)
        self.assertEqual(self.harvester.groups['image']['total'], 13)
        self.assertEqual(self.harvester.groups['image']['start'], 1)
        self.assertEqual(self.harvester.groups['image']['end'], 0)
        self.assertEqual(self.harvester.groups['text']['total'], 11)
        self.assertEqual(self.harvester.groups['text']['start'], 0)
        self.assertEqual(self.harvester.groups['text']['end'], 0)
        recs = self.harvester.next()
        self.assertEqual(self.harvester.groups['image']['end'], 10)
        self.assertEqual(len(recs), 10)

class OAC_XML_Harvester_text_contentTestCase(LogOverrideMixin, TestCase):
    '''Test when results only contain texts'''
    @httpretty.activate
    def testFetchTextOnlyContent(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10',
                body=open('./fixtures/testOAC-noimages-in-results.xml').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10&startDoc=1&group=text',
                body=open('./fixtures/testOAC-noimages-in-results.xml').read())
        oac_harvester = fetcher.OAC_XML_Harvester('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_harvester.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10&startDoc=11&group=text',
                body=open('./fixtures/testOAC-noimages-in-results-1.xml').read())
        second_set = oac_harvester.next()
        self.assertEqual(len(second_set), 1)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text')
        self.assertRaises(StopIteration, oac_harvester.next)


class OAC_XML_Harvester_mixed_contentTestCase(LogOverrideMixin, TestCase):
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
                body=open('./fixtures/testOAC-url_next-0.xml').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=image',
                body=open('./fixtures/testOAC-url_next-0.xml').read())
        oac_harvester = fetcher.OAC_XML_Harvester('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_harvester.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=image')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=image',
                body=open('./fixtures/testOAC-url_next-1.xml').read())
        second_set = oac_harvester.next()
        self.assertEqual(len(second_set), 3)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=image')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text',
                body=open('./fixtures/testOAC-url_next-2.xml').read())
        third_set = oac_harvester.next()
        self.assertEqual(len(third_set), 10)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text')
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text',
                body=open('./fixtures/testOAC-url_next-3.xml').read())
        fourth_set = oac_harvester.next()
        self.assertEqual(len(fourth_set), 1)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text')
        self.assertRaises(StopIteration, oac_harvester.next)


class OAC_JSON_HarvesterTestCase(LogOverrideMixin, TestCase):
    '''Test the OAC_JSON_Harvester
    '''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-url_next-0.json').read())
        super(OAC_JSON_HarvesterTestCase, self).setUp()
        self.harvester = fetcher.OAC_JSON_Harvester('http://dsc.cdlib.org/search?rmode=json&facet=type-tab&style=cui&relation=ark:/13030/hb5d5nb7dj', 'extra_data')

    def tearDown(self):
        super(OAC_JSON_HarvesterTestCase, self).tearDown()

    def testParseArk(self):
        self.assertEqual(self.harvester._parse_oac_findaid_ark(self.harvester.url), 'ark:/13030/hb5d5nb7dj')

    @httpretty.activate
    def testOAC_JSON_HarvesterReturnedData(self):
        '''test that the data returned by the OAI harvester is a proper dc
        dictionary
        '''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        rec = self.harvester.next()[0]
        self.assertIsInstance(rec, dict)
        self.assertIn('handle', rec)

    @httpretty.activate
    def testHarvestByRecord(self):
        '''Test the older by single record interface'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        records = []
        r = self.harvester.next_record()
        try:
            while True:
                records.append(r)
                r = self.harvester.next_record()
        except StopIteration:
            pass
        self.assertEqual(len(records), 28)

    @httpretty.activate
    def testHarvestIsIter(self):
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.harvester, '__iter__')) 
        self.assertEqual(self.harvester, self.harvester.__iter__())
        rec1 = self.harvester.next_record()
        objset = self.harvester.next()

    @httpretty.activate
    def testNextGroupFetch(self):
        '''Test that the OAC harvester will fetch more records when current
        response set records are all consumed'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        records = []
        self.ranGet = False
        for r in self.harvester:
            records.extend(r)
        self.assertEqual(len(records), 28)

    @httpretty.activate
    def testObjsetFetch(self):
        '''Test fetching data in whole objsets'''
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj',
                body=open('./fixtures/testOAC-url_next-0.json').read())
        httpretty.register_uri(httpretty.GET,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&startDoc=26',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.harvester, 'next_objset'))
        self.assertTrue(hasattr(self.harvester.next_objset, '__call__'))
        objset = self.harvester.next_objset()
        self.assertIsNotNone(objset)
        self.assertIsInstance(objset, list)
        self.assertEqual(len(objset), 25)
        objset2 = self.harvester.next_objset()
        self.assertTrue(objset != objset2)
        self.assertRaises(StopIteration, self.harvester.next_objset)

class MainTestCase(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
    '''Test the main function'''
    @httpretty.activate
    def setUp(self):
        super(MainTestCase, self).setUp()
        self.dir_test_profile = '/tmp/profiles/test'
        self.dir_save = None
        if not os.path.isdir(self.dir_test_profile):
            os.makedirs(self.dir_test_profile)
        self.user_email = 'email@example.com'
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        self.url_api_collection = "https://registry.cdlib.org/api/v1/collection/197/"
        sys.argv = ['thisexe', self.user_email, self.url_api_collection]
        self.collection = Collection(self.url_api_collection)
        self.setUp_config(self.collection)
        self.mail_handler = logbook.TestHandler(bubble=True)

    def tearDown(self):
        super(MainTestCase, self).tearDown()
        self.tearDown_config()
        if self.dir_save:
            shutil.rmtree(self.dir_save)
        shutil.rmtree(self.dir_test_profile)

    def testReturnAdd(self):
        self.assertTrue(hasattr(fetcher, 'EMAIL_RETURN_ADDRESS'))

    @httpretty.activate
    def testMainCreatesCollectionProfile(self):
        '''Test that the main function produces a collection profile
        file for DPLA. The path to this file is needed when creating a 
        DPLA ingestion document.
        '''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        c = Collection("https://registry.cdlib.org/api/v1/collection/197/")
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save = fetcher.main(
                    self.user_email,
                    self.url_api_collection,
                    log_handler=self.test_log_handler,
                    mail_handler=self.test_log_handler,
                    dir_profile=self.dir_test_profile,
                    profile_path=self.profile_path,
                    config_file=self.config_file)
        self.assertEqual(ingest_doc_id, 'test-id')
        self.assertEqual(num, 128)
        self.assertTrue(os.path.exists(os.path.join(self.profile_path)))

    @patch('dplaingestion.couch.Couch')
    def testMainCollection__init__Error(self, mock_couch):
        self.assertRaises(ValueError, fetcher.main,
                                    self.user_email,
                                    'this-is-a-bad-url',
                                    log_handler=self.test_log_handler,
                                    mail_handler=self.mail_handler,
                                    dir_profile=self.dir_test_profile,
                                    config_file=self.config_file
                         )
        self.assertEqual(len(self.test_log_handler.records), 1)
        self.assertEqual(len(self.mail_handler.records), 1)

    @httpretty.activate
    @patch('dplaingestion.couch.Couch')
    def testMainCollectionWrongType(self, mock_couch):
        '''Test what happens with wrong type of harvest'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test_bad_type.json').read())
        self.assertRaises(ValueError, fetcher.main,
                    self.user_email,
                    "https://registry.cdlib.org/api/v1/collection/197/",
                                    log_handler=self.test_log_handler,
                                    mail_handler=self.mail_handler,
                                    dir_profile=self.dir_test_profile,
                                    config_file=self.config_file
                         )
        self.assertEqual(len(self.test_log_handler.records), 1)
        self.assertEqual(len(self.mail_handler.records), 1)


    @httpretty.activate
    def testCollectionNoEnrichItems(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/36/",
                body=open('./fixtures/collection_api_no_enrich_item.json').read())
        c = Collection("https://registry.cdlib.org/api/v1/collection/36/")
        with self.assertRaises(ValueError):
            c.dpla_profile_obj

    @httpretty.activate
    @patch('harvester.fetcher.HarvestController.__init__', side_effect=Exception('Boom!'), autospec=True)
    def testMainHarvestController__init__Error(self, mock_method):
        '''Test the try-except block in main when HarvestController not created
        correctly'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        sys.argv = ['thisexe', 'email@example.com', 'https://registry.cdlib.org/api/v1/collection/197/']
        self.assertRaises(Exception, fetcher.main, self.user_email, self.url_api_collection, log_handler=self.test_log_handler, mail_handler=self.test_log_handler, dir_profile=self.dir_test_profile)
        self.assertEqual(len(self.test_log_handler.records), 4)
        self.assertTrue("[ERROR] HarvestMain: Exception in harvester init" in self.test_log_handler.formatted_records[3])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[3])
        c = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        os.remove(os.path.abspath(os.path.join(self.dir_test_profile, c.slug+'.pjs')))

    @httpretty.activate
    @patch('harvester.fetcher.HarvestController.harvest', side_effect=Exception('Boom!'), autospec=True)
    def testMainFnWithException(self, mock_method):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save = fetcher.main(
                    self.user_email,
                    self.url_api_collection,
                    log_handler=self.test_log_handler,
                    mail_handler=self.test_log_handler,
                    profile_path=self.profile_path,
                    config_file=self.config_file)
        self.assertEqual(len(self.test_log_handler.records), 7)
        self.assertTrue("[ERROR] HarvestMain: Error while harvesting:" in self.test_log_handler.formatted_records[6])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[6])

    @httpretty.activate
    def testMainFn(self):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save = fetcher.main(
                    self.user_email,
                    self.url_api_collection,
                    log_handler=self.test_log_handler,
                    mail_handler=self.test_log_handler,
                    dir_profile=self.dir_test_profile,
                    profile_path=self.profile_path,
                    config_file=self.config_file)
        self.assertEqual(len(self.test_log_handler.records), 10)
        self.assertIn(u'[INFO] HarvestMain: Init harvester next', self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1], u'[INFO] HarvestMain: Create DPLA profile document')
        self.assertTrue(u'[INFO] HarvestMain: DPLA profile document' in self.test_log_handler.formatted_records[2])
        self.assertEqual(self.test_log_handler.formatted_records[3], u'[INFO] HarvestMain: Create ingest doc in couch')
        self.assertEqual(self.test_log_handler.formatted_records[4], u'[INFO] HarvestMain: Ingest DOC ID: test-id')
        self.assertEqual(self.test_log_handler.formatted_records[5], u'[INFO] HarvestMain: Start harvesting next')
        self.assertTrue(u"[INFO] HarvestController: Starting harvest for: email@example.com Santa Clara University: Digital Objects ['UCDL'] ['Calisphere']", self.test_log_handler.formatted_records[6])
        self.assertEqual(self.test_log_handler.formatted_records[7], u'[INFO] HarvestController: 100 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[8], u'[INFO] HarvestController: 128 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[9], u'[INFO] HarvestMain: Finished harvest of calisphere-santa-clara-university-digital-objects. 128 records harvested.')


class LogFileNameTestCase(TestCase):
    '''Test the log file name function'''
    def setUp(self):
        self.old_dir = os.environ.get('DIR_HARVESTER_LOG')
        os.environ['DIR_HARVESTER_LOG'] = 'test/log/dir'

    def tearDown(self):
        os.environ.pop('DIR_HARVESTER_LOG')
        if self.old_dir:
            os.environ['DIR_HARVESTER_LOG'] = self.old_dir

    def testLogName(self):
        n = get_log_file_path('test_collection_slug')
        self.assertTrue(re.match('test/log/dir/harvester-test_collection_slug-\d{8}-\d{6}.log', n))
        

@skipUnlessIntegrationTest()
class HarvesterLogSetupTestCase(TestCase):
    '''Test that the log gets setup and run'''
    def testLogDirExists(self):
        log_file_path = fetcher.get_log_file_path('x')
        log_file_dir = log_file_path.rsplit('/', 1)[0]
        self.assertTrue(os.path.isdir(log_file_dir))

@skipUnlessIntegrationTest()
class MainMailIntegrationTestCase(TestCase):
    '''Test that the main function emails?'''
    def setUp(self):
        '''Need to run fakesmtp server on local host'''
        sys.argv = ['thisexe', 'email@example.com', 'https://xregistry-dev.cdlib.org/api/v1/collection/197/' ]

    def testMainFunctionMail(self):
        '''This should error out and send mail through error handler'''
        self.assertRaises(requests.exceptions.ConnectionError, fetcher.main, 'email@example.com', 'https://xregistry-dev.cdlib.org/api/v1/collection/197/')

@skipUnlessIntegrationTest()
class ScriptFileTestCase(TestCase):
    '''Test that the script file exists and is executable. Check that it 
    starts the correct proecss
    '''
    def testScriptFileExists(self):
        '''Test that the ScriptFile exists'''
        path_script = os.environ.get('HARVEST_SCRIPT', os.path.join(os.environ['HOME'], 'code/harvester/start_harvest.bash'))
        self.assertTrue(os.path.exists(path_script))

@skipUnlessIntegrationTest()
class FullOACHarvestTestCase(ConfigFileOverrideMixin, TestCase):
    def setUp(self):
        self.collection = Collection('http://localhost:8000/api/v1/collection/200/')
        self.setUp_config(self.collection)

    def tearDown(self):
        self.tearDown_config()
        #shutil.rmtree(self.controller.dir_save)

    def testFullOACHarvest(self):
        self.assertIsNotNone(self.collection)
        self.controller = fetcher.HarvestController('email@example.com',
               self.collection,
               config_file=self.config_file,
               profile_path=self.profile_path
                )
        n = self.controller.harvest()
        self.assertEqual(n, 26)


@skipUnlessIntegrationTest()
class FullOAIHarvestTestCase(ConfigFileOverrideMixin, TestCase):
    def setUp(self):
        self.collection = Collection('http://localhost:8000/api/v1/collection/197/')
        self.setUp_config(self.collection)

    def tearDown(self):
        self.tearDown_config()
        shutil.rmtree(self.controller.dir_save)

    def testFullOAIHarvest(self):
        self.assertIsNotNone(self.collection)
        self.controller = fetcher.HarvestController('email@example.com',
               self.collection,
               config_file=self.config_file,
               profile_path=self.profile_path
                )
        n = self.controller.harvest()
        self.assertEqual(n, 128)

class ParseEnvTestCase(TestCase):
    '''test the environment variable parsing'''
    def tearDown(self):
        # remove env vars if created?
        del os.environ['REDIS_PASSWORD']
        del os.environ['ID_EC2_INGEST']
        del os.environ['ID_EC2_SOLR_BUILD']

    def testParseEnv(self):
        with self.assertRaises(KeyError) as cm:
            parse_env()
        self.assertEqual(cm.exception.message, 'Please set environment variable REDIS_PASSWORD to redis password!')
        os.environ['REDIS_PASSWORD'] = 'XX'
        with self.assertRaises(KeyError) as cm:
            parse_env()
        self.assertEqual(cm.exception.message, 'Please set environment variable ID_EC2_INGEST to main ingest ec2 instance id.')
        os.environ['ID_EC2_INGEST'] = 'INGEST'
        with self.assertRaises(KeyError) as cm:
            parse_env()
        self.assertEqual(cm.exception.message, 'Please set environment variable ID_EC2_SOLR_BUILD to ingest solr instance id.')
        os.environ['ID_EC2_SOLR_BUILD'] = 'BUILD'
        redis_host, redis_port, redis_pswd, redis_connect_timeout, id_ec2_ingest, id_ec2_solr_build = parse_env()
        self.assertEqual(redis_host, '127.0.0.1')
        self.assertEqual(redis_port, '6379')
        self.assertEqual(redis_pswd, 'XX')
        self.assertEqual(redis_connect_timeout, 10)
        self.assertEqual(id_ec2_ingest, 'INGEST')
        self.assertEqual(id_ec2_solr_build, 'BUILD')


class RunIngestTestCase(LogOverrideMixin, TestCase):
    '''Test the run_ingest script. Wraps harvesting with rest of DPLA
    ingest process.
    '''
    def setUp(self):
        os.environ['REDIS_PASSWORD'] = 'XX'
        os.environ['ID_EC2_INGEST'] = 'INGEST'
        os.environ['ID_EC2_SOLR_BUILD'] = 'BUILD'

    def tearDown(self):
        # remove env vars if created?
        del os.environ['REDIS_PASSWORD']
        del os.environ['ID_EC2_INGEST']
        del os.environ['ID_EC2_SOLR_BUILD']

    @patch('dplaingestion.scripts.enrich_records.main', return_value=0)
    @patch('dplaingestion.scripts.save_records.main', return_value=0)
    @patch('dplaingestion.scripts.remove_deleted_records.main', return_value=0)
    @patch('dplaingestion.scripts.check_ingestion_counts.main', return_value=0)
    @patch('dplaingestion.scripts.dashboard_cleanup.main', return_value=0)
    @patch('dplaingestion.couch.Couch')
    def testRunIngest(self, mock_couch, mock_dash_clean, mock_check, mock_remove, mock_save, mock_enrich):
        mock_couch.return_value._create_ingestion_document.return_value = 'test-id'
        mail_handler = MagicMock()
        httpretty.enable()
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/178/',
                body=open('./fixtures/collection_api_test_oac.json').read())
        httpretty.register_uri(httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open('./fixtures/testOAC-url_next-1.json').read())
        run_ingest.main('mark.redar@ucop.edu',
                'https://registry.cdlib.org/api/v1/collection/178/',
                mail_handler=mail_handler)
        mock_couch.assert_called_with(config_file='akara.ini', dashboard_db_name='dashboard', dpla_db_name='ucldc')
        mock_enrich.assert_called_with([None, 'test-id'])

class QueueHarvestTestCase(TestCase):
    '''Test the queue harvester. 
    For now will mock the RQ library.
    '''
    def testGetRedisConnection(self):
        r = get_redis_connection('127.0.0.1', '6379', 'PASS')
        self.assertEqual(str(type(r)), "<class 'redis.client.Redis'>")

    def testCheckRedisQ(self):
        with patch('redis.Redis.ping', return_value=False) as mock_redis:
            res = check_redis_queue('127.0.0.1', '6379', 'PASS')
            self.assertEqual(res, False)
        with patch('redis.Redis.ping', return_value=True) as mock_redis:
            res = check_redis_queue('127.0.0.1', '6379', 'PASS')
            self.assertEqual(res, True)

    @patch('boto.ec2')
    def testStartEC2(self, mock_boto):
        start_ec2_instances('XXXX', 'YYYY')
        mock_boto.connect_to_region.assert_called_with('us-east-1')
        mock_boto.connect_to_region().start_instances.assert_called_with(('XXXX', 'YYYY'))

    @patch('boto.ec2')
    def testMain(self, mock_boto):
        with self.assertRaises(Exception) as cm:
            with patch('harvester.queue_harvest.Redis') as mock_redis:
                mock_redis().ping.return_value = False
                queue_harvest_main('mark.redar@ucop.edu',
                    'https://registry.cdlib.org/api/v1/collection/178/; \
                            https://registry.cdlib.org/api/v1/collection/189',
                    redis_host='127.0.0.1',
                    redis_port='6379',
                    redis_pswd='X',
                    timeout=1,
                    poll_interval=1
                )
        self.assertIn('TIMEOUT (1s) WAITING FOR QUEUE. TODO: EMAIL USER', cm.exception.message)
        with patch('harvester.queue_harvest.Redis', autospec=True) as mock_redis:
            mock_redis().ping.return_value = True
            results = queue_harvest_main('mark.redar@ucop.edu',
                'https://registry.cdlib.org/api/v1/collection/178/; \
                            https://registry.cdlib.org/api/v1/collection/189',
                redis_host='127.0.0.1',
                redis_port='6379',
                redis_pswd='X',
                timeout=1,
                poll_interval=1
                )
        mock_calls = [ str(x) for x in mock_redis.mock_calls]
        self.assertEqual(len(mock_calls), 12)
        self.assertEqual(mock_redis.call_count, 4)
        self.assertIn('call().ping()', mock_calls)
        self.assertEqual("call().sadd(u'rq:queues', u'rq:queue:default')", mock_calls[6])
        self.assertEqual("call().sadd(u'rq:queues', u'rq:queue:default')", mock_calls[9])
        self.assertIn("call().rpush(u'rq:queue:default", mock_calls[8])
        self.assertIn("call().rpush(u'rq:queue:default", mock_calls[11])
        self.assertIn("call().hmset('rq:job", mock_calls[7])
        self.assertIn(results[0].id, mock_calls[7])
        self.assertIn("call().hmset('rq:job", mock_calls[10])
        self.assertIn(results[1].id, mock_calls[10])


class SolrUpdaterTestCase(TestCase):
    '''Test the solr update from couchdb changes feed'''
#    def testMain(self):
#        '''Test running of main fn'''
#solr_updater_main
    @patch('solr.Solr', autospec=True)
    def test_push_doc_to_solr(self, mock_solr):
        '''Unit test calls to solr'''
        f = open('pickled_couchdb_doc')
        doc = pickle.load(f)
        sdoc = map_couch_to_solr_doc(doc)
        push_doc_to_solr(sdoc, mock_solr)
        mock_solr.add.assert_called_with({'rights': [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'], 'repository_name': [u'Bancroft Library'], 'url_item': u'http://ark.cdlib.org/ark:/13030/ft009nb05r', 'repository': [u'https://registry.cdlib.org/api/v1/repository/4/'], 'publisher': u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/', 'collection_name': u'Uchida (Yoshiko) photograph collection', 'format': u'mods', 'title': u'Neighbor', 'collection': u'https://registry.cdlib.org/api/v1/collection/23066', 'campus': [u'https://registry.cdlib.org/api/v1/campus/1/'], 'campus_name': [u'UC Berkeley'], 'relation': [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://bancroft.berkeley.edu/collections/jarda.html', u'hb158005k9', u'BANC PIC 1986.059--PIC', u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://calisphere.universityofcalifornia.edu/', u'http://bancroft.berkeley.edu/'], 'type': u'image', 'id': u'uchida-yoshiko-photograph-collection--http://ark.cdlib.org/ark:/13030/ft009nb05r', 'subject': [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive']})

    def test_map_couch_to_solr_doc(self):
        '''Test the mapping of a couch db source json doc to a solr schema
        compatible doc.
        '''
        f = open('pickled_couchdb_doc')
        doc = pickle.load(f)
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], doc['_id'])
        self.assertEqual(sdoc['id'], 'uchida-yoshiko-photograph-collection--http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertEqual(sdoc['campus'], [u'https://registry.cdlib.org/api/v1/campus/1/'])
        self.assertEqual(sdoc['campus_name'], [u'UC Berkeley'])
        self.assertEqual(sdoc['repository'],  [u'https://registry.cdlib.org/api/v1/repository/4/'])
        self.assertEqual(sdoc['repository_name'], [u'Bancroft Library'])
        self.assertEqual(sdoc['collection'],'https://registry.cdlib.org/api/v1/collection/23066') 
        self.assertEqual(sdoc['collection_name'], 'Uchida (Yoshiko) photograph collection')
        self.assertEqual(sdoc['url_item'], u'http://ark.cdlib.org/ark:/13030/ft009nb05r')
        #self.assertEqual(sdoc['contributor'], '')
        #self.assertEqual(sdoc['coverage'], '')
        #self.assertEqual(sdoc['creator'], '')
        #self.assertEqual(sdoc['description'], '')
        #self.assertEqual(sdoc['date'], '')
        #self.assertEqual(sdoc['language'], '')
        self.assertEqual(sdoc['publisher'], u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/'),
        self.assertEqual(sdoc['relation'], [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://bancroft.berkeley.edu/collections/jarda.html', u'hb158005k9', u'BANC PIC 1986.059--PIC', u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://calisphere.universityofcalifornia.edu/', u'http://bancroft.berkeley.edu/'])
        self.assertEqual(sdoc['rights'], [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'])
        self.assertEqual(sdoc['subject'], [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive'])
        self.assertEqual(sdoc['title'], u'Neighbor')
        self.assertEqual(sdoc['type'], u'image')
        self.assertEqual(sdoc['format'], 'mods')
        #self.assertEqual(sdoc['extent'], '')

    @patch('boto.connect_s3', autospec=True)
    def test_set_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq setting'''
        set_couchdb_last_seq(5)
        mock_boto.assert_called_with()
        mock_boto().get_bucket.assert_called_with('ucldc')
        mock_boto().get_bucket().get_key.assert_called_with('couchdb_last_seq')
        mock_boto().get_bucket().get_key().set_contents_from_string.assert_called_with(5)

    @patch('boto.connect_s3', autospec=True)
    def test_get_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq getting'''
        get_couchdb_last_seq()
        mock_boto.assert_called_with()
        mock_boto().get_bucket.assert_called_with('ucldc')
        mock_boto().get_bucket().get_key.assert_called_with('couchdb_last_seq')
        mock_boto().get_bucket().get_key().get_contents_as_string.assert_called_with()

class GrabSolrIndexTestCase(TestCase):
    '''Basic test for grabbing solr index. Like others, heavily mocked
    '''
    def setUp(self):
        os.environ['DIR_CODE'] = os.path.abspath(os.path.dirname(__file__))

    def tearDown(self):
        del os.environ['DIR_CODE']

    @patch('ansible.callbacks.PlaybookRunnerCallbacks', autospec=True)
    @patch('ansible.callbacks.PlaybookCallbacks', autospec=True)
    @patch('ansible.callbacks.AggregateStats', autospec=True)
    @patch('ansible.inventory.Inventory', autospec=True)
    @patch('ansible.playbook.PlayBook', autospec=True)
    def test_grab_solr_main(self, mock_pb, mock_inv, mock_stats, mock_cb, mock_cbr):
        inventory = mock_inv.return_value
        inventory.list_hosts.return_value = ['test-host']
        grab_solr_index.main()
        mock_pb.assert_called_with(playbook=os.path.join(os.environ['DIR_CODE'], 'harvester/grab-solr-index-playbook.yml'),
                inventory=inventory,
                callbacks=mock_cb.return_value,
                runner_callbacks=mock_cbr.return_value,
                stats=mock_stats.return_value
        )
        self.assertEqual(mock_pb.return_value.run.called, True)
        self.assertEqual(mock_pb.return_value.run.call_count, 1)
       

CONFIG_FILE_DPLA = '''
[Akara]
Port=8889

[CouchDb]
URL=http://127.0.0.1:5984/
Username=mark
Password=mark
ItemDatabase='''+ TEST_COUCH_DB + '''
DashboardDatabase='''+ TEST_COUCH_DASHBOARD

if __name__=='__main__':
    unittest.main()
