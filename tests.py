import os
import unittest
from unittest import TestCase
import harvester

def skipUnlessIntegrationTest(selfobj=None):
    '''Skip the test unless the environmen variable RUN_INTEGRATION_TESTS is set.
    '''
    if os.environ.get('RUN_INTEGRATION_TESTS', False):
        return lambda func: func
    return unittest.skip('RUN_INTEGRATION_TESTS not set. Skipping integration tests.')


@skipUnlessIntegrationTest()
class ScriptFileTestCase(TestCase):
    '''Test that the script file exists and is executable. Check that it 
    starts the correct proecss
    '''
    def testScriptFileExists(self):
        '''Test that the ScriptFile exists'''
        path_script = os.environ.get('HARVEST_SCRIPT', os.path.join(os.environ['HOME'], 'code/ucldc_harvester/start_harvest.bash'))
        self.assertTrue(os.path.exists(path_script))


class testHarvestController(TestCase):
    '''Test the harvest controller class'''
    def testHarvestControllerExists(self):
        from harvester import HarvestController
        harvester = HarvestController()
        self.assertTrue(hasattr(harvester, 'harvest_collection'))
        self.assertTrue(callable(harvester.harvest_collection))
        harvester.harvest_collection('email@example.com', 'collectionname', 'campuses', 'repositories', 'type', 'url_harvest', 'extra_data')
        
    def testOAIHarvesterType(self):
        '''Check the correct object returned for type of harvest'''
        harvest_cls = harvester.HarvestController().get_harvester_for_collection_type('OAI')
        self.assertRaises(ValueError, harvest_cls, 'email@example.com', 'collectionname', ['campuses'], ['repositories'], 'url_harvest', 'extra_data')
        harvest_obj = harvest_cls('email@example.com', 'collectionname', ['UCLA'], ['repositories'], 'http://oai.ucsd.edu/Oai/Oai-script', 'set_spec=mscl_scheffler')
        self.assertIsInstance(harvest_obj, harvester.OAIHarvester)
        self.assertTrue(harvest_obj.campuses == ['UCLA'])
        self.assertTrue(harvest_obj.type == 'OAI')


class testHarvesterClass(TestCase):
    '''Test the abstract Harvester class'''
    def testClassExists(self):
        self.assertTrue(hasattr(harvester, 'URL_SOLR'))
        #self.assertTrue(hasattr(harvester, 'email_user'))
        h = harvester.Harvester
        h = h('email@example.com', 'collectionname', ['UCLA'], ['repositories'], 'OAI', 'url_harvest', 'extra_data')


class testOAIHarvester(TestCase):
    '''Test the OAIHarvester
    Currently using a live site as test, must be better way
    '''
    def setUp(self):
        self.harvester = harvester.HarvestController().get_harvester_for_collection_type('OAI')('email@example.com', 'Los Angeles Times Photographic Archive', ['UCLA'], ['UCLA yLibrary Special Collections, Charles E. Young Research Library'], 'http://digital2.library.ucla.edu/oai2_0.do', 'latimes')

    def testHarvestIsIter(self):
        self.assertTrue(hasattr(self.harvester, '__iter__')) 
        self.assertEqual(self.harvester, self.harvester.__iter__())
        rec1 = self.harvester.next()
        print rec1


class testHarvestFunctionEmail(TestCase):
    '''Check for call to emailer.'''
    def testStartEmail(self):
        '''Test that when called the harvester sends an email to the user.
        '''


if __name__=='__main__':
    unittest.main()
