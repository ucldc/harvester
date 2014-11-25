from unittest import TestCase
from test.utils import skipUnlessIntegrationTest
from test.utils import ConfigFileOverrideMixin
from harvester.fetcher import get_log_file_path

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
        if self.remove_log_dir:
            shutil.rmtree('logs')

    def testCouchDocIntegration(self):
        '''Test the couch document creation in a test environment'''
        self.ingest_doc_id = self.controller_oai.create_ingest_doc()
        self.controller_oai.update_ingest_doc('error', error_msg='This is an error')


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
        sys.argv = ['thisexe', 'email@example.com', 'https://xregistry-dev.cdlib.org/api/v1/collection/197/']

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



