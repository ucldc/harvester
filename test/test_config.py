import os
from unittest import TestCase

from harvester import config

class QueueListTestCase(TestCase):
    '''Check config has queue list static member'''
    def testQueueList(self):
        self.assertTrue(hasattr(config, 'RQ_Q_LIST'))

class ConfigReturnTestCase(TestCase):
    '''Verify config returns expected values from env'''
    def setUp(self):
        self.os_cached = dict(os.environ)
        os.environ['REDIS_HOST'] = 'test_redis_host'
        os.environ['REDIS_PORT'] = 'test_redis_port'
        os.environ['REDIS_CONNECT_TIMEOUT'] = 'test_redis_timeout'
        os.environ['REDIS_PASSWORD'] = 'test_redis_password'
        os.environ['COUCHDB_URL'] = 'test_couchdb_url'
        os.environ['COUCHDB_USER'] = 'test_couchdb_user'
        os.environ['COUCHDB_PASSWORD'] = 'test_couchdb_password'
        os.environ['COUCHDB_DB'] = 'test_couchdb_dbname'
        os.environ['COUCHDB_DASHBOARD'] = 'test_couchdb_dashname'

    def tearDown(self):
        del os.environ['REDIS_HOST'] 
        del os.environ['REDIS_PORT'] 
        del os.environ['REDIS_CONNECT_TIMEOUT'] 
        del os.environ['COUCHDB_URL'] 
        del os.environ['COUCHDB_USER'] 
        del os.environ['COUCHDB_PASSWORD'] 
        del os.environ['COUCHDB_DB'] 
        del os.environ['COUCHDB_DASHBOARD'] 
        os.environ = self.os_cached

    def testConfigValues(self):
        cfg = config.config()
        self.assertEqual(cfg['redis_host'], 'test_redis_host')
        self.assertEqual(cfg['redis_port'], 'test_redis_port') 
        self.assertEqual(cfg['redis_connect_timeout'], 'test_redis_timeout') 
        self.assertEqual(cfg['redis_password'], 'test_redis_password')
        self.assertEqual(cfg['couchdb_url'], 'test_couchdb_url')
        self.assertEqual(cfg['couchdb_username'], 'test_couchdb_user')
        self.assertEqual(cfg['couchdb_password'], 'test_couchdb_password')
        self.assertEqual(cfg['couchdb_dbname'], 'test_couchdb_dbname')
        self.assertEqual(cfg['couchdb_dashboard'], 'test_couchdb_dashname')
