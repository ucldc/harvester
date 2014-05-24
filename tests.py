import os
import sys
import unittest
from unittest import TestCase
import codecs
import re
import json
import shutil
import tempfile
from mock import MagicMock
from mock import patch
from mock import call, ANY
from xml.etree import ElementTree as ET
import requests
import harvester
import logbook
import vcr
import httpretty
from harvester import get_log_file_path
from harvester import Collection
from dplaingestion.couch import Couch

#NOTE: these are used in integration test runs
TEST_COUCH_DB = 'test-ucldc'
TEST_COUCH_DASHBOARD = 'test-dashboard'

def skipUnlessIntegrationTest(selfobj=None):
    '''Skip the test unless the environmen variable RUN_INTEGRATION_TESTS is set
    '''
    if os.environ.get('RUN_INTEGRATION_TESTS', False):
        return lambda func: func
    return unittest.skip('RUN_INTEGRATION_TESTS not set. Skipping integration tests.')

class MockResponse(object):
    """Mimics the response object returned by HTTP requests."""
    def __init__(self, text):
        # request's response object carry an attribute 'text' which contains
        # the server's response data encoded as unicode.
        self.text = text
        self.status_code = 200

    def json(self):
        try:
           return json.loads(text)
        except:
           return {}

    def raise_for_status(self):
        pass


def mockRequestsGet(filename, **kwargs):
    '''Replaces requests.get for testing the sickle interface on semi-real
    data
    '''
    with codecs.open(filename, 'r', 'utf8') as foo:
        resp = MockResponse(foo.read())
    return resp

class MockRequestsGetMixin(object):
    '''Mixin to use mockRequestsGet file grabber'''
    def setUp(self):
        '''Use file base request mock'''
        super(MockRequestsGetMixin, self).setUp()
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet

    def tearDown(self):
        super(MockRequestsGetMixin, self).tearDown()
        requests.get = self.real_requests_get


class MockOACRequestsGetMixin(object):
    '''Mixin to use to Mock OAC requests'''
    class MockOACapi(object):
        def __init__(self, json):
            self._json = json

        @property
        def text(self):
            return self._json

        def json(self):
            return  json.loads(self._json)

    def mockOACRequestsGet(self, url, **kwargs):
        if getattr(self, 'ranGet', False):
            return None
        with codecs.open(self.testFile, 'r', 'utf8') as foo:
            return  TestOAC_JSON_Harvester.MockOACapi(foo.read())

    def setUp(self):
        '''Use mockOACRequestsGet'''
        super(MockOACRequestsGetMixin, self).setUp()
        self.real_requests_get = requests.get
        requests.get = self.mockOACRequestsGet

    def tearDown(self):
        super(MockOACRequestsGetMixin, self).tearDown()
        requests.get = self.real_requests_get

class Mock_for_testFetchTextOnlyContent_Mixin(object):
    '''Mixin to use to in test of text only content fetching'''
    def __init__(self, *args, **kwargs):
        super(Mock_for_testFetchTextOnlyContent_Mixin, self).__init__(*args, **kwargs)
        self.times_run = 0

    class MockOACapi(object):
        def __init__(self, text):
            self._text = text

        @property
        def text(self):
            return self._text

    def mockOACRequestsGet(self, url, **kwargs):
        self.times_run += 1
        if self.times_run <= 2:
            #first time initializes data, but does not return it
            return self.MockOACapi(codecs.open('fixtures/testOAC-noimages-in-results.xml', 'r', 'utf8').read())
        elif self.times_run == 3:
            return self.MockOACapi(codecs.open('fixtures/testOAC-noimages-in-results-1.xml', 'r', 'utf8').read())
        else:
            return None

    def setUp(self):
        '''Use mockOACRequestsGet'''
        super(Mock_for_testFetchTextOnlyContent_Mixin, self).setUp()
        self.real_requests_get = requests.get
        requests.get = self.mockOACRequestsGet

    def tearDown(self):
        super(Mock_for_testFetchTextOnlyContent_Mixin, self).tearDown()
        requests.get = self.real_requests_get

class Mock_for_testFetchMixedContent_Mixin(object):
    '''Mixin to use to in test of mixed content fetching'''
    def __init__(self, *args, **kwargs):
        super(Mock_for_testFetchMixedContent_Mixin, self).__init__(*args, **kwargs)
        self.times_run = 0

    class MockOACapi(object):
        def __init__(self, text):
            self._text = text

        @property
        def text(self):
            return self._text

    def mockOACRequestsGet(self, url, **kwargs):
        self.times_run += 1
        if self.times_run <= 2:
            #first time initializes data, but does not return it
            return self.MockOACapi(codecs.open('fixtures/testOAC-url_next-0.xml', 'r', 'utf8').read())
        elif self.times_run == 3:
            return self.MockOACapi(codecs.open('fixtures/testOAC-url_next-1.xml', 'r', 'utf8').read())
        elif self.times_run == 4:
            return self.MockOACapi(codecs.open('fixtures/testOAC-url_next-2.xml', 'r', 'utf8').read())
        elif self.times_run == 5:
            return self.MockOACapi(codecs.open('fixtures/testOAC-url_next-3.xml', 'r', 'utf8').read())
        else:
            return None

    def setUp(self):
        '''Use mockOACRequestsGet'''
        super(Mock_for_testFetchMixedContent_Mixin, self).setUp()
        self.real_requests_get = requests.get
        requests.get = self.mockOACRequestsGet

    def tearDown(self):
        super(Mock_for_testFetchMixedContent_Mixin, self).tearDown()
        requests.get = self.real_requests_get

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


class TestApiCollection(TestCase):
    '''Test that the Collection object is complete from the api
    '''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet

    def tearDown(self):
        requests.get = self.real_requests_get

    def testOAICollectionAPI(self):
        c = Collection('fixtures/collection_api_test.json')
        self.assertEqual(c['harvest_type'], 'OAI')
        self.assertEqual(c.harvest_type, 'OAI')
        self.assertEqual(c['name'], 'Calisphere - Santa Clara University: Digital Objects')
        self.assertEqual(c.name, 'Calisphere - Santa Clara University: Digital Objects')
        self.assertEqual(c['url_oai'], 'fixtures/testOAI-128-records.xml')
        self.assertEqual(c.url_oai, 'fixtures/testOAI-128-records.xml')
        self.assertEqual(c.campus[0]['resource_uri'], '/api/v1/campus/12/')
        self.assertEqual(c.campus[0]['slug'], 'UCDL')

    def testOACApiCollection(self):
        c = Collection('fixtures/collection_api_test_oac.json')
        self.assertEqual(c['harvest_type'], 'OAJ')
        self.assertEqual(c.harvest_type, 'OAJ')
        self.assertEqual(c['name'], 'Harry Crosby Collection')
        self.assertEqual(c.name, 'Harry Crosby Collection')
        self.assertEqual(c['url_oac'], 'fixtures/testOAC.json')
        self.assertEqual(c.url_oac, 'fixtures/testOAC.json')
        self.assertEqual(c.campus[0]['resource_uri'], '/api/v1/campus/6/')
        self.assertEqual(c.campus[0]['slug'], 'UCSD')

    def testWrongTypeCollection(self):
        '''Test a collection that is not OAI or OAC
        '''
        self.assertRaises(ValueError,Collection, 'fixtures/collection_api_test_bad_type.json')

    def testCreateProfile(self):
        '''Test the creation of a DPLA style proflie file'''
        c = Collection('fixtures/collection_api_test_oac.json')
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
        #self.assertEqual(e[1], '/shred?prop=sourceResource%2Fcontributor%2CsourceResource%2Fcreator%2CsourceResource%2Fdate')
        self.assertEqual(e[1], '/shred?prop=sourceResource/contributor%2CsourceResource/creator%2CsourceResource/date')


class TestHarvestOAC_JSON_Controller(ConfigFileOverrideMixin, MockOACRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the function of an OAC harvest controller'''
    @patch('harvester.OAC_JSON_Harvester._parse_oac_findaid_ark', return_value='ark:/13030/tf2v19n928/', autospec=True)
    def setUp(self, mock_method):
        super(TestHarvestOAC_JSON_Controller, self).setUp()
        self.testFile = 'fixtures/collection_api_test_oac.json'
        self.collection = Collection('fixtures/collection_api_test_oac.json')
        self.setUp_config(self.collection)
        self.testFile = 'fixtures/testOAC-url_next-0.json'
        self.controller = harvester.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)

    def tearDown(self):
        super(TestHarvestOAC_JSON_Controller, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller.dir_save)

    def testOAC_JSON_Harvest(self):
        '''Test the function of the OAC harvest'''
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        self.ranGet = False
        self.controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue('fixtures/collection_api' in self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 28 records harvested')

    def testObjectsHaveRegistryData(self):
        #test OAC objsets
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        self.ranGet = False
        self.controller.harvest()
        dir_list = os.listdir(self.controller.dir_save)
        self.assertEqual(len(dir_list), 2)
        objset_saved = json.loads(open(os.path.join(self.controller.dir_save, dir_list[0])).read())
        obj = objset_saved[2]
        self.assertIn('collection', obj)
        self.assertEqual(obj['collection'], {'@id':'fixtures/collection_api_test_oac.json', 'name':'Harry Crosby Collection'})
        self.assertIn('campus', obj)
        self.assertEqual(obj['campus'], [{u'@id': u'/api/v1/campus/6/', u'name': u'UC San Diego'}, {u'@id': u'/api/v1/campus/1/', u'name': u'UC Berkeley'}])
        self.assertIn('repository', obj)
        self.assertEqual(obj['repository'], [{u'@id': u'/api/v1/repository/22/',
   u'name': u'Mandeville Special Collections Library'}, {u'@id': u'/api/v1/repository/36/', u'name': u'UCB Department of Statistics'}])


class TestHarvestOAIController(ConfigFileOverrideMixin, MockRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the function of an OAI harvester'''
    def setUp(self):
        super(TestHarvestOAIController, self).setUp()

    def tearDown(self):
        super(TestHarvestOAIController, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    def testOAIHarvest(self):
        '''Test the function of the OAI harvest'''
        self.collection = Collection('fixtures/collection_api_test.json')
        self.setUp_config(self.collection)
        self.controller = harvester.HarvestController('email@example.com', self.collection, config_file=self.config_file, profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        #TODO: fix why logbook.TestHandler not working for the previous logging
        #self.assertEqual(len(self.test_log_handler.records), 2)
        self.tearDown_config()


class TestHarvestController(ConfigFileOverrideMixin, LogOverrideMixin, TestCase):
#class TestHarvestController(ConfigFileOverrideMixin, MockRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the harvest controller class'''
    @httpretty.activate
    def setUp(self):
        super(TestHarvestController, self).setUp()
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open('./fixtures/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open('./fixtures/testOAI-128-records.xml').read())
        #self.collection = Collection('fixtures/collection_api_test.json')
        self.collection = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        config_file, profile_path = self.setUp_config(self.collection) 
        self.controller_oai = harvester.HarvestController('email@example.com', self.collection, profile_path=profile_path, config_file=config_file)
        self.objset_test_doc = json.load(open('objset_test_doc.json'))

    def tearDown(self):
        super(TestHarvestController, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller_oai.dir_save)

    def testHarvestControllerExists(self):
        collection = Collection('https://registry.cdlib.org/api/v1/collection/101/')
        controller = harvester.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path) 
        self.assertTrue(hasattr(controller, 'harvester'))
        self.assertIsInstance(controller.harvester, harvester.OAIHarvester)
        self.assertTrue(hasattr(controller, 'campus_valid'))
        self.assertTrue(hasattr(controller, 'dc_elements'))
        shutil.rmtree(controller.dir_save)

    def testOAIHarvesterType(self):
        '''Check the correct object returned for type of harvest'''
        self.assertIsInstance(self.controller_oai.harvester, harvester.OAIHarvester)
        self.assertEqual(self.controller_oai.collection.campus[0]['slug'], 'UCDL')

    def testIDCreation(self):
        '''Test how the id for the index is created'''
        self.assertTrue(hasattr(self.controller_oai, 'create_id'))
        identifier = 'x'
        self.assertRaises(TypeError, self.controller_oai.create_id, identifier)
        identifier = ['x',]
        sid = self.controller_oai.create_id(identifier)
        self.assertIn(self.controller_oai.collection.slug, sid)
        self.assertIn(self.controller_oai.collection.campus[0]['slug'], sid)
        self.assertIn(self.controller_oai.collection.repository[0]['slug'], sid)
        self.assertEqual(sid, 'UCDL-Calisphere-calisphere-santa-clara-university-digital-objects-x')
        collection = Collection('fixtures/collection_api_test.json')
        controller = harvester.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
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

    def testLoggingMoreThan1000(self):
        collection = Collection('fixtures/collection_api_big_test.json')
        controller = harvester.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
        controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 13)
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 100 records harvested')
        shutil.rmtree(controller.dir_save)
        self.assertEqual(self.test_log_handler.formatted_records[10], '[INFO] HarvestController: 1000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[11], '[INFO] HarvestController: 2000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[12], '[INFO] HarvestController: 2400 records harvested')

    #@vcr.use_cassette('fixtures/vcr_cassettes/registry_collection-23065.yaml')
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
        controller = harvester.HarvestController('email@example.com', collection, config_file=self.config_file, profile_path=self.profile_path)
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
        obj_saved = json.loads(open(os.path.join(self.controller_oai.dir_save, dir_list[0])).read())
        self.assertIn('collection', obj_saved)
        self.assertEqual(obj_saved['collection'], {'@id':'fixtures/collection_api_test.json',
            'name':'Calisphere - Santa Clara University: Digital Objects'})
        self.assertIn('campus', obj_saved)
        self.assertEqual(obj_saved['campus'], [{'@id':'/api/v1/campus/12/',
            'name':'California Digital Library'}])
        self.assertIn('repository', obj_saved)
        self.assertEqual(obj_saved['repository'], [{'@id':'/api/v1/repository/37/',
            'name':'Calisphere'}])

@skipUnlessIntegrationTest()
class TestCouchIntegration(ConfigFileOverrideMixin, MockRequestsGetMixin, TestCase):
    def setUp(self):
        super(TestCouchIntegration, self).setUp()
        self.collection = Collection('fixtures/collection_api_test.json')
        config_file, profile_path = self.setUp_config(self.collection) 
        self.controller_oai = harvester.HarvestController('email@example.com', self.collection, profile_path=profile_path, config_file=config_file)
        self.remove_log_dir = False
        if not os.path.isdir('logs'):
            os.makedirs('logs')
            self.remove_log_dir = True

    def tearDown(self):
        super(TestCouchIntegration, self).tearDown()
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

class TestHarvesterClass(TestCase):
    '''Test the abstract Harvester class'''
    def testClassExists(self):
        h = harvester.Harvester
        h = h('url_harvest', 'extra_data')


class TestOAIHarvester(MockRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the OAIHarvester
    '''
    def setUp(self):
        super(TestOAIHarvester, self).setUp()
        self.harvester = harvester.OAIHarvester('fixtures/testOAI.xml', 'latimes')
##        self.harvester = harvester.OAIHarvester('fixtures/collection_api_test.json', 'latimes')

    def tearDown(self):
        super(TestOAIHarvester, self).tearDown()

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

class TestOAC_XML_Harvester(LogOverrideMixin, TestCase):
#class TestOAC_XML_Harvester(MockOACRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the OAC_XML_Harvester
    '''
    @vcr.use_cassette('fixtures/vcr_cassettes/TestOAC_XML_Harvester/tf0c600134.yaml')
    def setUp(self):
        #self.testFile = 'fixtures/testOAC-url_next-0.xml'
        super(TestOAC_XML_Harvester, self).setUp()
        self.harvester = harvester.OAC_XML_Harvester('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf0c600134', 'extra_data')

    def tearDown(self):
        super(TestOAC_XML_Harvester, self).tearDown()

    @vcr.use_cassette('fixtures/vcr_cassettes/TestOAC_XML_Harvester/testBadOACSearch.yaml')
    def testBadOACSearch(self):
        self.testFile = 'fixtures/testOAC-badsearch.xml'
        self.assertRaises(ValueError, harvester.OAC_XML_Harvester, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj--xxxx', 'extra_data')

    @vcr.use_cassette('fixtures/vcr_cassettes/TestOAC_XML_Harvester/testOnlyTextResults.yaml')
    def testOnlyTextResults(self):
        '''Test when only texts are in result'''
        self.testFile = 'fixtures/testOAC-noimages-in-results.xml'
        h = harvester.OAC_XML_Harvester( 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 11)
        recs = self.harvester.next()
        self.assertEqual(self.harvester.groups['text']['end'], 10)
        self.assertEqual(len(recs), 10)

    @vcr.use_cassette('fixtures/vcr_cassettes/TestOAC_XML_Harvester/testUTF8ResultsContent.yaml')
    def testUTF8ResultsContent(self):
        self.testFile = 'fixtures/testOAC-utf8-content.xml'
        h = harvester.OAC_XML_Harvester( 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
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



    def testFetchOnePage(self):
        '''Test fetching one "page" of results where no return trips are
        necessary
        '''
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

class TestOAC_XML_Harvester_text_content(Mock_for_testFetchTextOnlyContent_Mixin,  LogOverrideMixin, TestCase):
    '''Test when results only contain texts'''
    def testFetchTextOnlyContent(self):
        oac_harvester = harvester.OAC_XML_Harvester('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_harvester.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text')
        second_set = oac_harvester.next()
        self.assertEqual(len(second_set), 1)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text')
        self.assertRaises(StopIteration, oac_harvester.next)


class TestOAC_XML_Harvester_mixed_content(Mock_for_testFetchMixedContent_Mixin,  LogOverrideMixin, TestCase):
    def testFetchMixedContent(self):
        '''This interface gets tricky when image & text data are in the
        collection.
        My test Mock object will return an xml with 10 images
        then with 3 images
        then 10 texts
        then 1 text then quit 
        '''
        oac_harvester = harvester.OAC_XML_Harvester('http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_harvester.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=image')
        second_set = oac_harvester.next()
        self.assertEqual(len(second_set), 3)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=image')
        third_set = oac_harvester.next()
        self.assertEqual(len(third_set), 10)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&group=text')
        fourth_set = oac_harvester.next()
        self.assertEqual(len(fourth_set), 1)
        self.assertEqual(oac_harvester._url_current, 'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&group=text')
        self.assertRaises(StopIteration, oac_harvester.next)


class TestOAC_JSON_Harvester(MockOACRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the OAC_JSON_Harvester
    '''
    def setUp(self):
        self.testFile = 'fixtures/testOAC-url_next-0.json'
        super(TestOAC_JSON_Harvester, self).setUp()
        self.harvester = harvester.OAC_JSON_Harvester('http://dsc.cdlib.org/search?rmode=json&facet=type-tab&style=cui&relation=ark:/13030/hb5d5nb7dj', 'extra_data')

    def tearDown(self):
        super(TestOAC_JSON_Harvester, self).tearDown()

    def testParseArk(self):
        self.assertEqual(self.harvester._parse_oac_findaid_ark(self.harvester.url), 'ark:/13030/hb5d5nb7dj')

    def testOAC_JSON_HarvesterReturnedData(self):
        '''test that the data returned by the OAI harvester is a proper dc
        dictionary
        '''
        rec = self.harvester.next()[0]
        self.assertIsInstance(rec, dict)
        self.assertIn('handle', rec)

    def testHarvestByRecord(self):
        '''Test the older by single record interface'''
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

    def testHarvestIsIter(self):
        self.assertTrue(hasattr(self.harvester, '__iter__')) 
        self.assertEqual(self.harvester, self.harvester.__iter__())
        rec1 = self.harvester.next_record()
        objset = self.harvester.next()

    def testNextGroupFetch(self):
        '''Test that the OAC harvester will fetch more records when current
        response set records are all consumed'''
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        records = []
        self.ranGet = False
        for r in self.harvester:
            records.extend(r)
        self.assertEqual(len(records), 28)

    def testObjsetFetch(self):
        '''Test fetching data in whole objsets'''
        self.assertTrue(hasattr(self.harvester, 'next_objset'))
        self.assertTrue(hasattr(self.harvester.next_objset, '__call__'))
        self.testFile = 'fixtures/testOAC-url_next-1.json' #so next resp is next
        objset = self.harvester.next_objset()
        self.assertIsNotNone(objset)
        self.assertIsInstance(objset, list)
        self.assertEqual(len(objset), 25)
        objset2 = self.harvester.next_objset()
        self.assertTrue(objset != objset2)
        self.assertRaises(StopIteration, self.harvester.next_objset)

class TestMain(ConfigFileOverrideMixin, MockRequestsGetMixin, LogOverrideMixin, TestCase):
    '''Test the main function'''
    def setUp(self):
        super(TestMain, self).setUp()
        self.dir_test_profile = '/tmp/profiles/test'
        self.dir_save = None
        if not os.path.isdir(self.dir_test_profile):
            os.makedirs(self.dir_test_profile)
        self.user_email = 'email@example.com'
        self.url_api_collection = 'fixtures/collection_api_test.json'
        sys.argv = ['thisexe', self.user_email, self.url_api_collection]
        self.collection = Collection(self.url_api_collection)
        self.setUp_config(self.collection)

    def tearDown(self):
        super(TestMain, self).tearDown()
        self.tearDown_config()
        if self.dir_save:
            shutil.rmtree(self.dir_save)
        os.removedirs(self.dir_test_profile)

    def testReturnAdd(self):
        self.assertTrue(hasattr(harvester, 'EMAIL_RETURN_ADDRESS'))

    def testMainCreatesCollectionProfile(self):
        '''Test that the main function produces a collection profile
        file for DPLA. The path to this file is needed when creating a 
        DPLA ingestion document.
        '''
        c = Collection('fixtures/collection_api_test.json')
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save = harvester.main(
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
        self.mail_handler = MagicMock()
        self.assertRaises(ValueError, harvester.main,
                                    self.user_email,
                                    'fixtures/collection_api_test_bad_type.json',
                                    log_handler=self.test_log_handler,
                                    mail_handler=self.mail_handler,
                                    dir_profile=self.dir_test_profile,
                                    config_file=self.config_file
                         )
        self.assertEqual(len(self.test_log_handler.records), 0)
        self.mail_handler.deliver.assert_called()
        self.assertEqual(self.mail_handler.deliver.call_count, 1)

    def testCollectionNoEnrichItems(self):
        c = Collection('fixtures/collection_api_no_enrich_item.json')
        with self.assertRaises(ValueError):
            c.dpla_profile_obj

    @patch('harvester.HarvestController.__init__', side_effect=Exception('Boom!'), autospec=True)
    def testMainHarvestController__init__Error(self, mock_method):
        '''Test the try-except block in main when HarvestController not created
        correctly'''
        sys.argv = ['thisexe', 'email@example.com', 'fixtures/collection_api_test.json']
        self.assertRaises(Exception, harvester.main, self.user_email, self.url_api_collection, log_handler=self.test_log_handler, mail_handler=self.test_log_handler, dir_profile=self.dir_test_profile)
        self.assertEqual(len(self.test_log_handler.records), 5)
        self.assertTrue("[ERROR] HarvestMain: Exception in harvester init" in self.test_log_handler.formatted_records[4])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[4])
        c = Collection('fixtures/collection_api_test.json')
        os.remove(os.path.abspath(os.path.join(self.dir_test_profile, c.slug+'.pjs')))

    @patch('harvester.HarvestController.harvest', side_effect=Exception('Boom!'), autospec=True)
    def testMainFnWithException(self, mock_method):
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save = harvester.main(
                    self.user_email,
                    self.url_api_collection,
                    log_handler=self.test_log_handler,
                    mail_handler=self.test_log_handler,
                    profile_path=self.profile_path,
                    config_file=self.config_file)
        self.assertEqual(len(self.test_log_handler.records), 8)
        self.assertTrue("[ERROR] HarvestMain: Error while harvesting:" in self.test_log_handler.formatted_records[7])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[7])

    def testMainFn(self):
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save = harvester.main(
                    self.user_email,
                    self.url_api_collection,
                    log_handler=self.test_log_handler,
                    mail_handler=self.test_log_handler,
                    dir_profile=self.dir_test_profile,
                    profile_path=self.profile_path,
                    config_file=self.config_file)
        #print len(self.test_log_handler.records), self.test_log_handler.formatted_records
        self.assertEqual(len(self.test_log_handler.records), 11)
        self.assertEqual(self.test_log_handler.formatted_records[0], u'[INFO] HarvestMain: Init harvester next')
        self.assertEqual(self.test_log_handler.formatted_records[1], u'[INFO] HarvestMain: ARGS: email@example.com fixtures/collection_api_test.json')
        self.assertEqual(self.test_log_handler.formatted_records[2], u'[INFO] HarvestMain: Create DPLA profile document')
        self.assertTrue(u'[INFO] HarvestMain: DPLA profile document' in self.test_log_handler.formatted_records[3])
        self.assertEqual(self.test_log_handler.formatted_records[4], u'[INFO] HarvestMain: Create ingest doc in couch')
        self.assertEqual(self.test_log_handler.formatted_records[5], u'[INFO] HarvestMain: Ingest DOC ID: test-id')
        self.assertEqual(self.test_log_handler.formatted_records[6], u'[INFO] HarvestMain: Start harvesting next')
        self.assertTrue(u"[INFO] HarvestController: Starting harvest for: email@example.com Santa Clara University: Digital Objects ['UCDL'] ['Calisphere']", self.test_log_handler.formatted_records[7])
        self.assertEqual(self.test_log_handler.formatted_records[8], u'[INFO] HarvestController: 100 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[9], u'[INFO] HarvestController: 128 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[10], u'[INFO] HarvestMain: Finished harvest of calisphere-santa-clara-university-digital-objects. 128 records harvested.')


class TestLogFileName(TestCase):
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
class TestHarvesterLogSetup(TestCase):
    '''Test that the log gets setup and run'''
    def testLogDirExists(self):
        log_file_path = harvester.get_log_file_path('x')
        log_file_dir = log_file_path.rsplit('/', 1)[0]
        self.assertTrue(os.path.isdir(log_file_dir))

@skipUnlessIntegrationTest()
class TestMainMailIntegration(TestCase):
    '''Test that the main function emails?'''
    def setUp(self):
        '''Need to run fakesmtp server on local host'''
        sys.argv = ['thisexe', 'email@example.com', 'https://xregistry-dev.cdlib.org/api/v1/collection/197/' ]

    def testMainFunctionMail(self):
        '''This should error out and send mail through error handler'''
        self.assertRaises(requests.exceptions.ConnectionError, harvester.main, 'email@example.com', 'https://xregistry-dev.cdlib.org/api/v1/collection/197/')

@skipUnlessIntegrationTest()
class ScriptFileTestCase(TestCase):
    '''Test that the script file exists and is executable. Check that it 
    starts the correct proecss
    '''
    def testScriptFileExists(self):
        '''Test that the ScriptFile exists'''
        path_script = os.environ.get('HARVEST_SCRIPT', os.path.join(os.environ['HOME'], 'code/ucldc_harvester/start_harvest.bash'))
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
        self.controller = harvester.HarvestController('email@example.com',
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
        self.controller = harvester.HarvestController('email@example.com',
               self.collection,
               config_file=self.config_file,
               profile_path=self.profile_path
                )
        n = self.controller.harvest()
        self.assertEqual(n, 128)



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
