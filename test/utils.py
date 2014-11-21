import os
import unittest
import tempfile
import logbook

DIR_THIS_FILE = os.path.abspath(os.path.split(__file__)[0])
DIR_FIXTURES = os.path.join(DIR_THIS_FILE, 'fixtures')

#  NOTE: these are used in integration test runs
TEST_COUCH_DB = 'test-ucldc'
TEST_COUCH_DASHBOARD = 'test-dashboard'


CONFIG_FILE_DPLA = '''
[Akara]
Port=8889

[CouchDb]
URL=http://127.0.0.1:5984/
Username=mark
Password=mark
ItemDatabase=''' + TEST_COUCH_DB + '''
DashboardDatabase=''' + TEST_COUCH_DASHBOARD

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
            # print ' '.join(('Mail sent to ', email, ' MSG: ', msg))
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



