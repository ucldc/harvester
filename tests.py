import os
import sys
import unittest
from unittest import TestCase
import codecs
import re
import json
import shutil
from mock import MagicMock
from mock import patch
from mock import call, ANY
import requests
import harvester
import logbook
from harvester import get_log_file_path
from harvester import Collection

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
        self.assertEqual(c['harvest_type'], 'OAC')
        self.assertEqual(c.harvest_type, 'OAC')
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
        self.assertTrue(isinstance(c.dpla_profile, str))
        #print c.dpla_profile
        j = json.loads(c.dpla_profile)
        self.assertTrue(j['name'] == 'harry-crosby-collection-black-white-photographs-of')
        self.assertTrue(j['enrichments_coll'] == [ '/compare_with_schema' ])
        self.assertTrue('enrichments_item' in j)
        self.assertTrue(len(j['enrichments_item']) == 31)
        self.assertTrue('contributor' in j)
        self.assertTrue(isinstance(j['contributor'], list))
        self.assertTrue(len(j['contributor']) == 4)
        self.assertTrue(j['contributor'][1] == {u'@id': u'/api/v1/campus/1/', u'name': u'UCB'})


class TestHarvestOAIController(TestCase):
    '''Test the function of an OAI harvester'''
    def setUp(self):
        self.test_log_handler = logbook.TestHandler()
        self.test_log_handler.push_thread()
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet

    def tearDown(self):
        self.test_log_handler.pop_thread()
        requests.get = self.real_requests_get
        shutil.rmtree(self.controller.dir_save)

    def testOAIHarvest(self):
        '''Test the function of the OAI harvest'''
        #with logbook.TestHandler('HarvestController') as log_handler:
        collection = Collection('fixtures/collection_api_test.json')
        self.controller = harvester.HarvestController('email@example.com', collection)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        #TODO: fix why logbook.TestHandler not working for the previous logging
        #self.assertEqual(len(self.test_log_handler.records), 2)


class TestHarvestOACController(TestCase):
    '''Test the function of an OAC harvesterController'''
    class MockOACapi(object):
        def __init__(self, json):
            self._json = json

        def json(self):
            return  json.loads(self._json)

    def mockRequestsGet(self, url, **kwargs):
        with codecs.open(self.testFile, 'r', 'utf8') as foo:
            return  TestOACHarvester.MockOACapi(foo.read())

    @patch('harvester.OACHarvester._parse_oac_findaid_ark', return_value='ark:/13030/tf2v19n928/', autospec=True)
    def setUp(self, mock_method):
        self.test_log_handler = logbook.TestHandler()
        self.test_log_handler.push_thread()
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        self.collection = Collection('fixtures/collection_api_test_oac.json')
        self.testFile = 'fixtures/testOAC-url_next-0.json'
        requests.get = self.mockRequestsGet
        self.controller = harvester.HarvestController('email@example.com', self.collection)

    def tearDown(self):
        self.test_log_handler.pop_thread()
        requests.get = self.real_requests_get
        shutil.rmtree(self.controller.dir_save)

    def testOACHarvest(self):
        '''Test the function of the OAC harvest'''
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.testFile = 'fixtures/testOAC-url_next-1.json'
        self.controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue('fixtures/collection_api' in self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 28 records harvested')


class TestHarvestController(TestCase):
    '''Test the harvest controller class'''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        self.collection = Collection('fixtures/collection_api_test.json')
        self.controller_oai = harvester.HarvestController('email@example.com', self.collection)
        self.test_log_handler = logbook.TestHandler()
        self.test_log_handler.push_thread()
        self.objset_test_doc = json.load(open('objset_test_doc.json'))

    def tearDown(self):
        self.test_log_handler.pop_thread()
        requests.get = self.real_requests_get
        shutil.rmtree(self.controller_oai.dir_save)

    def testHarvestControllerExists(self):
        collection = Collection('fixtures/collection_api_test.json')
        controller = harvester.HarvestController('email@example.com', collection) 
        self.assertTrue(hasattr(controller, 'harvester'))
        self.assertIsInstance(controller.harvester, harvester.OAIHarvester)
        self.assertTrue(hasattr(controller, 'campus_valid'))
        self.assertTrue(hasattr(controller, 'harvest_types'))
        self.assertTrue(hasattr(controller, 'dc_elements'))
        shutil.rmtree(controller.dir_save)

    def testOAIHarvesterType(self):
        '''Check the correct object returned for type of harvest'''
        self.assertIsInstance(self.controller_oai.harvester, harvester.OAIHarvester)
        self.assertTrue(self.controller_oai.collection.campus[0]['slug'] == 'UCDL')

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
        controller = harvester.HarvestController('email@example.com', collection)
        sid = controller.create_id(identifier)
        self.assertEqual(sid, 'UCDL-Calisphere-calisphere-santa-clara-university-digital-objects-x')
        shutil.rmtree(controller.dir_save)

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
        controller = harvester.HarvestController('email@example.com', collection)
        controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 13)
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 100 records harvested')
        shutil.rmtree(controller.dir_save)
        self.assertEqual(self.test_log_handler.formatted_records[10], '[INFO] HarvestController: 1000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[11], '[INFO] HarvestController: 2000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[12], '[INFO] HarvestController: 2400 records harvested')

class TestHarvesterClass(TestCase):
    '''Test the abstract Harvester class'''
    def testClassExists(self):
        h = harvester.Harvester
        h = h('url_harvest', 'extra_data')


class TestOAIHarvester(TestCase):
    '''Test the OAIHarvester
    '''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        self.harvester = harvester.OAIHarvester('fixtures/testOAI.xml', 'latimes')
        self.test_log_handler = logbook.TestHandler()
        self.test_log_handler.push_thread()

    def tearDown(self):
        self.test_log_handler.pop_thread()
        requests.get = self.real_requests_get

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
        for key, value in rec.items():
            self.assertIn(key, harvester.HarvestController.dc_elements)

class TestOACHarvester(TestCase):
    '''Test the OACHarvester
    '''
    class MockOACapi(object):
        def __init__(self, json):
            self._json = json

        def json(self):
            return  json.loads(self._json)

    def mockRequestsGet(self, url, **kwargs):
        with codecs.open(self.testFile, 'r', 'utf8') as foo:
            return  TestOACHarvester.MockOACapi(foo.read())

    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = self.mockRequestsGet
        self.testFile = 'fixtures/testOAC-url_next-0.json'
        self.harvester = harvester.OACHarvester('http://dsc.cdlib.org/search?rmode=json&facet=type-tab&style=cui&relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.test_log_handler = logbook.TestHandler()
        self.test_log_handler.push_thread()

    def tearDown(self):
        self.test_log_handler.pop_thread()
        requests.get = self.real_requests_get

    def testParseArk(self):
        self.assertEqual(self.harvester._parse_oac_findaid_ark(self.harvester.url_harvest), 'ark:/13030/hb5d5nb7dj')

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
        self.assertTrue(len(objset) == 25)
        objset2 = self.harvester.next_objset()
        self.assertTrue(objset != objset2)
        self.assertRaises(StopIteration, self.harvester.next_objset)

class TestMain(TestCase):
    '''Test the main function'''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        sys.argv = ['thisexe', 'email@example.com', 'fixtures/collection_api_test.json']
        self.test_log_handler = logbook.TestHandler()
        def deliver(msg, email):
            #print ' '.join(('Mail sent to ', email, ' MSG: ', msg))
            pass
        self.test_log_handler.deliver = deliver
        self.test_log_handler.push_thread()
        self.dir_test_profile = 'profiles/test'
        if not os.path.isdir(self.dir_test_profile):
            os.makedirs(self.dir_test_profile) 
        #self.mail_handler = logbook.TestHandler()
        #self.mail_handler.push_thread()

    def tearDown(self):
        requests.get = self.real_requests_get
        #self.mail_handler.pop_thread()
        self.test_log_handler.pop_thread()
        dir_list = os.listdir('/tmp')
        for d in dir_list:
            if "Santa" in d:
                shutil.rmtree(os.path.join('/tmp', d))
        shutil.rmtree(self.dir_test_profile)

    def testReturnAdd(self):
        self.assertTrue(hasattr(harvester, 'EMAIL_RETURN_ADDRESS'))

    def testMainCreatesCollectionProfile(self):
        '''Test that the main function produces a collection profile
        file for DPLA. The path to this file is needed when creating a 
        DPLA ingestion document.
        '''
        c = Collection('fixtures/collection_api_test.json')
        harvester.main(log_handler=self.test_log_handler, mail_handler=self.test_log_handler, dir_profile=self.dir_test_profile)
        self.assertTrue(os.path.exists(os.path.join(self.dir_test_profile, c.slug+'.pjs')))

    def testMainCollection__init__Error(self):
        sys.argv = ['thisexe', 'email@example.com', 'fixtures/collection_api_test_bad_type.json']
        self.mail_handler = MagicMock()
        self.assertRaises(ValueError, harvester.main, log_handler=self.test_log_handler, mail_handler=self.mail_handler, dir_profile=self.dir_test_profile)
        self.assertEqual(len(self.test_log_handler.records), 0)
        self.mail_handler.deliver.assert_called()
        self.assertEqual(self.mail_handler.deliver.call_count, 1)

    @patch('harvester.HarvestController.__init__', side_effect=Exception('Boom!'), autospec=True)
    def testMainHarvestController__init__Error(self, mock_method):
        '''Test the try-except block in main when HarvestController not created
        correctly'''
        sys.argv = ['thisexe', 'email@example.com', 'fixtures/collection_api_test.json']
        self.assertRaises(Exception, harvester.main, log_handler=self.test_log_handler, mail_handler=self.test_log_handler, dir_profile=self.dir_test_profile)
        self.assertEqual(len(self.test_log_handler.records), 3)
        self.assertTrue("[ERROR] HarvestMain: Exception in harvester init" in self.test_log_handler.formatted_records[2])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[2])

    @patch('harvester.HarvestController.harvest', side_effect=Exception('Boom!'), autospec=True)
    def testMainFnWithException(self, mock_method):
        harvester.main(log_handler=self.test_log_handler, mail_handler=self.test_log_handler, dir_profile=self.dir_test_profile)
        self.assertEqual(len(self.test_log_handler.records), 5)
        self.assertTrue("[ERROR] HarvestMain: Error while harvesting:" in self.test_log_handler.formatted_records[4])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[4])

    def testMainFn(self):
        harvester.main(log_handler=self.test_log_handler, mail_handler=self.test_log_handler, dir_profile=self.dir_test_profile)
        #print len(self.test_log_handler.records), self.test_log_handler.formatted_records
        self.assertEqual(len(self.test_log_handler.records), 8)
        self.assertEqual(self.test_log_handler.formatted_records[0], u'[INFO] HarvestMain: Init harvester next')
        self.assertEqual(self.test_log_handler.formatted_records[1], u'[INFO] HarvestMain: ARGS: email@example.com fixtures/collection_api_test.json')
        self.assertEqual(self.test_log_handler.formatted_records[2], u'[INFO] HarvestMain: Create DPLA profile document')
        self.assertEqual(self.test_log_handler.formatted_records[3], u'[INFO] HarvestMain: Start harvesting next')
        self.assertTrue(u"[INFO] HarvestController: Starting harvest for: email@example.com Santa Clara University: Digital Objects ['UCDL'] ['Calisphere']", self.test_log_handler.formatted_records[4])
        self.assertEqual(self.test_log_handler.formatted_records[5], u'[INFO] HarvestController: 100 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[6], u'[INFO] HarvestController: 128 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[7], u'[INFO] HarvestMain: Finished harvest of calisphere-santa-clara-university-digital-objects. 128 records harvested.')


def skipUnlessIntegrationTest(selfobj=None):
    '''Skip the test unless the environmen variable RUN_INTEGRATION_TESTS is set
    '''
    if os.environ.get('RUN_INTEGRATION_TESTS', False):
        return lambda func: func
    return unittest.skip('RUN_INTEGRATION_TESTS not set. Skipping integration tests.')

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
        self.assertRaises(requests.exceptions.ConnectionError, harvester.main)

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
class FullOAIHarvestTestCase(TestCase):
    def testFullHarvest(self):
        collection = Collection('https://registry-dev.cdlib.org/api/v1/collection/197/')
        controller = harvester.HarvestController('email@example.com',
               collection 
                )
        controller.harvest()


if __name__=='__main__':
    unittest.main()
