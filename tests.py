import os
import sys
import unittest
from unittest import TestCase
import codecs
import re
import solr
from mock import MagicMock
from mock import patch
from mock import call, ANY
import requests
import harvester
import logbook
from harvester import get_log_file_path

class MockResponse(object):
    """Mimics the response object returned by HTTP requests."""
    def __init__(self, text):
        # request's response object carry an attribute 'text' which contains
        # the server's response data encoded as unicode.
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        pass


def mockRequestsGet(filename, **kwargs):
    '''Replaces requests.get for testing the sickle interface on semi-real
    data
    '''
    with codecs.open(filename, 'r', 'utf8') as foo:
        resp = MockResponse(foo.read())
    return resp

class TestHarvestOAIController(TestCase):
    '''Test the function of an OAI harvester'''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        self.controller = harvester.HarvestController('email@example.com', 'test_collection_name', ['UCLA'], ['test_repo_name'], 'OAI', 'testOAI.xml', 'extra_data')

    def tearDown(self):
        requests.get = self.real_requests_get

    def testOAIHarvest(self):
        '''Test the function of the OAI harvest'''
        self.assertTrue(hasattr(self.controller, 'harvest'))


class TestHarvestController(TestCase):
    '''Test the harvest controller class'''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        self.controller_oai = harvester.HarvestController('email@example.com', 'test_collection_name', ['UCLA'], ['test_repo_name'], 'OAI', 'testOAI.xml', 'extra_data')
        self.test_log_handler = logbook.TestHandler()
        self.test_log_handler.push_thread()

    def tearDown(self):
        self.test_log_handler.pop_thread()
        requests.get = self.real_requests_get

    def testHarvestControllerExists(self):
        from harvester import HarvestController
        controller = HarvestController('email@example.com', 'Los Angeles Times Photographic Archive', ['UCLA'], ['UCLA yLibrary Special Collections, Charles E. Young Research Library'], 'OAI', 'testOAI.xml', 'latimes')
        self.assertTrue(hasattr(controller, 'harvester'))
        self.assertIsInstance(controller.harvester, harvester.OAIHarvester)
        self.assertTrue(hasattr(controller, 'campus_valid'))
        self.assertTrue(hasattr(controller, 'harvest_types'))
        self.assertTrue(hasattr(controller, 'dc_elements'))
        self.assertTrue(hasattr(controller, 'solr'))

    def testOAIHarvesterType(self):
        '''Check the correct object returned for type of harvest'''
        self.assertRaises(ValueError, harvester.HarvestController, 'email@example.com', 'test_collection_name', ['campuses'], ['test_repo_name'], 'OAI', 'url_harvest', 'extra_data')
        self.assertIsInstance(self.controller_oai.harvester, harvester.OAIHarvester)
        self.assertTrue(self.controller_oai.campuses == ['UCLA'])

    def testInputDataValidation(self):
        '''Test the validation of input dictionaries'''
        self.assertTrue(hasattr(self.controller_oai, 'validate_input_dict'))
        d = 'x'
        self.assertRaises(TypeError, self.controller_oai.validate_input_dict, d)
        d = {'notdc':'x'}
        self.assertRaises(ValueError, self.controller_oai.validate_input_dict, d)

    def testSolrIDCreation(self):
        '''Test how the id for the solr index is created'''
        self.assertTrue(hasattr(self.controller_oai, 'create_solr_id'))
        identifier = 'x'
        self.assertRaises(TypeError, self.controller_oai.create_solr_id, identifier)
        identifier = ['x',]
        sid = self.controller_oai.create_solr_id(identifier)
        self.assertIn(self.controller_oai.collection_name, sid)
        self.assertIn(self.controller_oai.campuses[0], sid)
        self.assertIn(self.controller_oai.repositories[0], sid)
        self.assertEqual(sid, 'UCLA-test_repo_name-test_collection_name-x')
        controller = harvester.HarvestController('email@example.com', 'Los Angeles Times Photographic Archive', ['UCLA','UCD'], ['Spec Coll', 'Archive'], 'OAI', 'testOAI.xml', 'latimes')
        sid = controller.create_solr_id(identifier)
        self.assertEqual(sid, 'UCLA-UCD-Spec Coll-Archive-Los Angeles Times Photographic Archive-x')

    def testSolrDocCreation(self):
        '''Test that a dc elements dictionary gets the required extra
        elements needed to update the solr index'''
        self.assertTrue(hasattr(self.controller_oai, 'create_solr_doc'))
        d = 'x'
        self.assertRaises(TypeError, self.controller_oai.create_solr_doc, d)
        d = {'notdc':'x'}
        self.assertRaises(ValueError, self.controller_oai.create_solr_doc, d)
        d = {'publisher':['test-pub',]}
        self.assertRaises(ValueError, self.controller_oai.create_solr_doc, d)
        d['title'] = ['test-title',]
        self.assertRaises(KeyError, self.controller_oai.create_solr_doc, d)
        d['identifier'] = ['x', 'y']
        sDoc = self.controller_oai.create_solr_doc(d)
        self.assertIn('title', sDoc)
        self.assertIn('id', sDoc)
        self.assertEqual(sDoc['id'], 'UCLA-test_repo_name-test_collection_name-x')
        self.assertEqual(sDoc['collection_name'], 'test_collection_name')
        self.assertEqual(sDoc['campus'], ['UCLA'])
        self.assertEqual(sDoc['repository'], ['test_repo_name'])


    def testSolrCall(self):
        '''Test that the call to solr.add in harvesting is correct
        '''
        self.controller_oai.solr = MagicMock()
        mock = self.controller_oai.solr
        self.controller_oai.harvest()
        self.assertTrue(mock.add.call_count == 3)
        #last doc in test doc
        mock.add.assert_called_with(solr_test_doc , commit=True)

    @unittest.skip('Takes too long to run')
    def testLoggingMoreThan1000(self):
        controller = harvester.HarvestController('email@example.com', 'test_collection_name', ['UCLA'], ['test_repo_name'], 'OAI', 'testOAI-2400-records.xml', 'extra_data')
        controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 12)
        self.assertEqual(self.test_log_handler.formatted_records[1], '[INFO] HarvestController: 100 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[10], '[INFO] HarvestController: 1000 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[11], '[INFO] HarvestController: 2000 records harvested')
        print len(self.test_log_handler.records), self.test_log_handler.formatted_records

class TestHarvesterClass(TestCase):
    '''Test the abstract Harvester class'''
    def testClassExists(self):
        self.assertTrue(hasattr(harvester, 'URL_SOLR'))
        h = harvester.Harvester
        h = h('url_harvest', 'extra_data')


class TestOAIHarvester(TestCase):
    '''Test the OAIHarvester
    '''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        self.harvester = harvester.OAIHarvester('testOAI.xml', 'latimes')
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
        #print rec

class TestMain(TestCase):
    '''Test the main function'''
    def setUp(self):
        self.real_requests_get = requests.get
        requests.get = mockRequestsGet
        sys.argv = ['thisexe', 'email@example.com', 'Santa Clara University: Digital Objects', 'UCDL', 'Calisphere', 'OAI', 'testOAI-128-records.xml', 'extra_data="scu:objects"']
        self.test_log_handler = logbook.TestHandler()
        def deliver(msg, email):
            #print ' '.join(('Mail sent to ', email, ' MSG: ', msg))
            pass
        self.test_log_handler.deliver = deliver
        self.test_log_handler.push_thread()
        #self.mail_handler = logbook.TestHandler()
        #self.mail_handler.push_thread()

    def tearDown(self):
        requests.get = self.real_requests_get
        #self.mail_handler.pop_thread()
        self.test_log_handler.pop_thread()

    def testReturnAdd(self):
        self.assertTrue(hasattr(harvester, 'EMAIL_RETURN_ADDRESS'))

    @patch('harvester.HarvestController.harvest', side_effect=Exception('Boom!'), autospec=True)
    def testMainFnWithException(self, mock_method):
        harvester.main(log_handler=self.test_log_handler, mail_handler=self.test_log_handler)
        self.assertEqual(len(self.test_log_handler.records), 4)
        self.assertEqual(self.test_log_handler.formatted_records[3], '[ERROR] HarvestMain: Error while harvesting:Boom!')

    def testMainFn(self):
        with patch('harvester.solr.Solr', autospec=True) as mock:
            instance = mock.return_value
            with patch.object(instance, 'add') as mock_method:
                harvester.main(log_handler=self.test_log_handler, mail_handler=self.test_log_handler)
        mock.assert_called_once_with('http://107.21.228.130:8080/solr/dc-collection/')
        self.assertEqual(mock_method.call_count, 128)
        self.assertEqual(len(self.test_log_handler.records), 6)
        self.assertEqual(self.test_log_handler.formatted_records[0], u'[INFO] HarvestMain: Init harvester next')
        self.assertEqual(self.test_log_handler.formatted_records[1], u'[INFO] HarvestMain: ARGS: email@example.com Santa Clara University: Digital Objects [\'UCDL\'] [\'Calisphere\'] OAI testOAI-128-records.xml extra_data="scu:objects"')
        self.assertEqual(self.test_log_handler.formatted_records[2], u'[INFO] HarvestMain: Start harvesting next')
        self.assertTrue(u"[INFO] HarvestController: Starting harvest for: email@example.com Santa Clara University: Digital Objects ['UCDL'] ['Calisphere']", self.test_log_handler.formatted_records[3])
        self.assertEqual(self.test_log_handler.formatted_records[4], u'[INFO] HarvestController: 100 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[5], u'[INFO] HarvestMain: Finished harvest of Santa Clara University: Digital Objects. 128 records harvested.')


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
        n = get_log_file_path('test_collection_name')
        self.assertTrue(re.match('test/log/dir/harvester-test_collection_name-\d{8}-\d{6}.log', n))
        

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
        sys.argv = ['thisexe', 'email@example.com', 'Santa Clara University: Digital Objects', 'UCDL', 'Calisphere', 'OAI', 'http://content.cdlib.com/bogus', 'extra_data="scu:objects"']

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
    '''Test that the full integration with sickle and solr work. 
    The scu:objects content.cdlib.org OAI set work well for full integration
    test with the solr instance.
    Should use appstrap to create local solr loaded with known docs to make
    this work best.
    NOTE: As of 2013-12-17 there are 126 Santa Clara University Objects
    '''
    def testFullHarvest(self):
        controller = harvester.HarvestController('email@example.com',
                'Santa Clara University: Digital Objects', ['UCDL'],
                ['Calisphere'], 'OAI',
                'http://content.cdlib.org/oai', extra_data='scu:objects'
                )
        controller.harvest()


solr_test_doc = {
        'publisher': ['California Historical Society, North Baker Research Library, 678 Mission Street, San Francisco, CA 94105-4014; http://www.californiahistoricalsociety.org/collections/northbaker_research.html'],
'description': ["Describes Rankin's personal experience of the earthquake, his trip that morning into San Francisco, damage to landmark buildings, and creation of a refugee camp on California Field, a football field in Berkeley, which he helped to patrol."],
    'repository': ['test_repo_name'],
    'creator': ['Rankin, Ivan S.'],
    'collection_name': 'test_collection_name', 'format': ['mods'],
    'campus': ['UCLA'],
    'contributor': ['Rankin, Ivan S.'],
    'relation': ['http://oac.cdlib.org/findaid/ark:/13030/hb8779p2cx', 'http://bancroft.berkeley.edu/collections/earthquakeandfire', 'eqf', 'MS 3497', 'http://calisphere.universityofcalifornia.edu/', 'http://www.californiahistoricalsociety.org/collections/northbaker_research.html'],
    'date': ['1969, April 25', '1969-04-25'],
    'title': ['Recollections of the earthquake and fire in San Francisco, April 18, 19, 20 and 21, 1906.'],
    'identifier': ['http://ark.cdlib.org/ark:/13030/hb367nb2vx', 'MS 3497', 'chs00000479_42a.xml'],
    'type': ['text', 'still image'],
    'id': 'UCLA-test_repo_name-test_collection_name-http://ark.cdlib.org/ark:/13030/hb367nb2vx', 'subject': ['Buildings--Earthquake effects--California--San Francisco', 'Earthquakes--California--San Francisco--Personal narratives', 'Fires--California--San Francisco--Personal narratives', 'Refugee camps--California--Oakland', 'San Francisco Earthquake, Calif., 1906--Personal narratives', 'The 1906 San Francisco Earthquake and Fire Digital Collection', 'Recollections of the earthquake and fire in San Francisco, April 18, 19, 20 and 21, 1906.']
}


if __name__=='__main__':
    unittest.main()
