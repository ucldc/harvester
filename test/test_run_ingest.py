import os
import sys
from unittest import TestCase
import shutil
import re
import pickle
import httpretty
import logbook
from mock import patch
from mock import MagicMock
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES
from harvester.collection_registry_client import Collection
import harvester.fetcher as fetcher
from harvester.fetcher import get_log_file_path
from harvester.config import config
import harvester.run_ingest as run_ingest


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
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        self.url_api_collection = \
                "https://registry.cdlib.org/api/v1/collection/197/"
        sys.argv = ['thisexe', self.user_email, self.url_api_collection]
        self.collection = Collection(self.url_api_collection)
        self.setUp_config(self.collection)
        self.mail_handler = logbook.TestHandler(bubble=True)
        self.mail_handler.push_thread()

    def tearDown(self):
        self.mail_handler.pop_thread()
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
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        c = Collection("https://registry.cdlib.org/api/v1/collection/197/")
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save, self.fetcher = fetcher.main(
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
                body=open(DIR_FIXTURES+'/collection_api_test_bad_type.json').read())
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
                body=open(DIR_FIXTURES+'/collection_api_no_enrich_item.json').read())
        c = Collection("https://registry.cdlib.org/api/v1/collection/36/")
        with self.assertRaises(ValueError):
            c.dpla_profile_obj

    @httpretty.activate
    @patch('harvester.fetcher.HarvestController.__init__',
            side_effect=Exception('Boom!'), autospec=True)
    def testMainHarvestController__init__Error(self, mock_method):
        '''Test the try-except block in main when HarvestController not created
        correctly'''
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        sys.argv = ['thisexe', 'email@example.com',
                    'https://registry.cdlib.org/api/v1/collection/197/']
        self.assertRaises(Exception, fetcher.main, self.user_email,
                                     self.url_api_collection,
                                     log_handler=self.test_log_handler,
                                     mail_handler=self.test_log_handler,
                                     dir_profile=self.dir_test_profile)
        self.assertEqual(len(self.test_log_handler.records), 4)
        self.assertTrue("[ERROR] HarvestMain: Exception in harvester init" in
                self.test_log_handler.formatted_records[3])
        self.assertTrue("Boom!" in self.test_log_handler.formatted_records[3])
        c = Collection('https://registry.cdlib.org/api/v1/collection/197/')
        os.remove(os.path.abspath(os.path.join(
                  self.dir_test_profile, c.id+'.pjs')))
                  #self.dir_test_profile, c.slug+'.pjs')))

    @httpretty.activate
    @patch('harvester.fetcher.HarvestController.harvest',
            side_effect=Exception('Boom!'), autospec=True)
    def testMainFnWithException(self, mock_method):
        httpretty.register_uri(httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/197/",
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            self.assertRaises(Exception, fetcher.main,
#            ingest_doc_id, num, self.dir_save, self.harvester = fetcher.main(
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
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
        httpretty.register_uri(httpretty.GET,
                re.compile("http://content.cdlib.org/oai?.*"),
                body=open(DIR_FIXTURES+'/testOAI-128-records.xml').read())
        with patch('dplaingestion.couch.Couch') as mock_couch:
            instance = mock_couch.return_value
            instance._create_ingestion_document.return_value = 'test-id'
            ingest_doc_id, num, self.dir_save, self.harvester = fetcher.main(
                    self.user_email,
                    self.url_api_collection,
                    log_handler=self.test_log_handler,
                    mail_handler=self.test_log_handler,
                    dir_profile=self.dir_test_profile,
                    profile_path=self.profile_path,
                    config_file=self.config_file)
        self.assertEqual(len(self.test_log_handler.records), 10)
        self.assertIn(u'[INFO] HarvestMain: Init harvester next', self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1],
                u'[INFO] HarvestMain: Create DPLA profile document')
        self.assertTrue(u'[INFO] HarvestMain: DPLA profile document' in
                self.test_log_handler.formatted_records[2])
        self.assertEqual(self.test_log_handler.formatted_records[3],
                u'[INFO] HarvestMain: Create ingest doc in couch')
        self.assertEqual(self.test_log_handler.formatted_records[4],
                u'[INFO] HarvestMain: Ingest DOC ID: test-id')
        self.assertEqual(self.test_log_handler.formatted_records[5],
                u'[INFO] HarvestMain: Start harvesting next')
        self.assertTrue(u"[INFO] HarvestController: Starting harvest for: email@example.com Santa Clara University: Digital Objects ['UCDL'] ['Calisphere']",
                self.test_log_handler.formatted_records[6])
        self.assertEqual(self.test_log_handler.formatted_records[7],
                u'[INFO] HarvestController: 100 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[8],
                u'[INFO] HarvestController: 128 records harvested')
        self.assertEqual(self.test_log_handler.formatted_records[9],
                u'[INFO] HarvestMain: Finished harvest of calisphere-santa-clara-university-digital-objects. 128 records harvested.')


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
        print(n)
        self.assertTrue(re.match(
            'test/log/dir/harvester-test_collection_slug-\d{8}-\d{6}-.log', n))


class ConfigTestCase(TestCase):
    '''test the environment variable parsing and confg file init'''
    def setUp(self):
        self.rpwd = self.rhost = self.ec2ingest = self.ec2solr = None
        if 'REDIS_PASSWORD' in os.environ:
            self.rpwd = os.environ['REDIS_PASSWORD']
            del os.environ['REDIS_PASSWORD']
        if 'REDIS_HOST' in os.environ:
            self.rhost = os.environ['REDIS_HOST']
            del os.environ['REDIS_HOST']
        if 'ID_EC2_INGEST' in os.environ:
            self.ec2ingest = os.environ['ID_EC2_INGEST']
            del os.environ['ID_EC2_INGEST']
        if 'ID_EC2_SOLR_BUILD' in os.environ:
            self.ec2solr = os.environ['ID_EC2_SOLR_BUILD']
            del os.environ['ID_EC2_SOLR_BUILD']

    def tearDown(self):
        # remove env vars if created?
        if self.rpwd:
            os.environ['REDIS_PASSWORD'] = self.rpwd
        else:
            del os.environ['REDIS_PASSWORD']
        if self.rhost:
            os.environ['REDIS_HOST'] = self.rhost
        if self.ec2ingest:
            os.environ['ID_EC2_INGEST'] = self.ec2ingest
        else:
            del os.environ['ID_EC2_INGEST']
        if self.ec2solr:
            os.environ['ID_EC2_SOLR_BUILD'] = self.ec2solr
        else:
            del os.environ['ID_EC2_SOLR_BUILD']

    def testConfig(self):
        with self.assertRaises(KeyError) as cm:
            config(redis_required=True)
        self.assertEqual(str(cm.exception.message),
                'Please set environment variable REDIS_PASSWORD to redis password!')
        os.environ['REDIS_HOST'] = 'redis_host_ip'
        os.environ['REDIS_PASSWORD'] = 'XX'
        with self.assertRaises(KeyError) as cm:
            config(redis_required=True, ec2_required=True)
        self.assertEqual(cm.exception.message,
                'Please set environment variable ID_EC2_INGEST to main ingest ec2 instance id.')
        os.environ['ID_EC2_INGEST'] = 'INGEST'
        with self.assertRaises(KeyError) as cm:
            config(redis_required=True, ec2_required=True)
        self.assertEqual(cm.exception.message,
                'Please set environment variable ID_EC2_SOLR_BUILD to ingest solr instance id.')
        os.environ['ID_EC2_SOLR_BUILD'] = 'BUILD'
        redis_host, redis_port, redis_pswd, redis_connect_timeout, \
            id_ec2_ingest, id_ec2_solr_build, DPLA = config()
        self.assertEqual(redis_host, 'redis_host_ip')
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
        super(RunIngestTestCase, self).setUp()
        os.environ['REDIS_PASSWORD'] = 'XX'
        os.environ['ID_EC2_INGEST'] = 'INGEST'
        os.environ['ID_EC2_SOLR_BUILD'] = 'BUILD'

    def tearDown(self):
        # remove env vars if created?
        super(RunIngestTestCase, self).tearDown()
        del os.environ['REDIS_PASSWORD']
        del os.environ['ID_EC2_INGEST']
        del os.environ['ID_EC2_SOLR_BUILD']

    @patch('harvester.run_ingest.Redis', autospec=True)
    @patch('couchdb.Server')
    @patch('dplaingestion.scripts.enrich_records.main', return_value=0)
    @patch('dplaingestion.scripts.save_records.main', return_value=0)
    @patch('dplaingestion.scripts.remove_deleted_records.main', return_value=0)
    @patch('dplaingestion.scripts.check_ingestion_counts.main', return_value=0)
    @patch('dplaingestion.scripts.dashboard_cleanup.main', return_value=0)
    @patch('dplaingestion.couch.Couch')
    def testRunIngest(self, mock_couch, mock_dash_clean, mock_check,
                mock_remove, mock_save, mock_enrich, mock_couchdb, mock_redis):
        mock_couch.return_value._create_ingestion_document.return_value = 'test-id'
        # this next is because the redis client unpickles....
        mock_redis.return_value.hget.return_value = pickle.dumps('RQ-result!')
        mail_handler = MagicMock()
        httpretty.enable()
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/178/',
                body=open(DIR_FIXTURES+'/collection_api_test_oac.json').read())
        httpretty.register_uri(httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&relation=ark:/13030/tf2v19n928',
                body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        run_ingest.main('mark.redar@ucop.edu',
                'https://registry.cdlib.org/api/v1/collection/178/',
                log_handler=self.test_log_handler,
                mail_handler=mail_handler)
        mock_couch.assert_called_with(config_file='akara.ini', dashboard_db_name='dashboard', dpla_db_name='ucldc')
        mock_enrich.assert_called_with([None, 'test-id'])
        self.assertEqual(len(self.test_log_handler.records), 14)
        # no longer run image_harvest by default
        #self.assertEqual(self.test_log_handler.formatted_records[14],
        #    u'[INFO] run_ingest: Started job for image_harvest:RQ-result!')
