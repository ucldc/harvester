from unittest import TestCase
import json
import StringIO
from mypretty import httpretty
# import httpretty
from mock import patch
from harvester.collection_registry_client import Registry, Collection
from test.utils import DIR_FIXTURES

class RegistryApiTestCase(TestCase):
    '''Test that the registry api works for our purposes'''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(httpretty.GET,
           'https://registry.cdlib.org/api/v1/',
           body='''{"campus": {"list_endpoint": "/api/v1/campus/", "schema": "/api/v1/campus/schema/"}, "collection": {"list_endpoint": "/api/v1/collection/", "schema": "/api/v1/collection/schema/"}, "repository": {"list_endpoint": "/api/v1/repository/", "schema": "/api/v1/repository/schema/"}}''')
        self.registry = Registry()

    def testRegistryListEndpoints(self):
        # use set so order independent
        self.assertEqual(set(self.registry.endpoints.keys()),
                         set(['collection', 'repository', 'campus']))
        self.assertRaises(ValueError, self.registry.resource_iter, 'x')

    @httpretty.activate
    def testResourceIteratorOnePage(self):
        '''Test when less than one page worth of objects fetched'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/campus/',
                body=open(DIR_FIXTURES+'/registry_api_campus.json').read())
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
                body=open(DIR_FIXTURES+'/registry_api_repository-page-2.json').read())
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/repository/',
                body=open(DIR_FIXTURES+'/registry_api_repository.json').read())

        riter = self.registry.resource_iter('repository')
        self.assertEqual(riter.url, 'https://registry.cdlib.org/api/v1/repository/')
        self.assertEqual(riter.path_next, '/api/v1/repository/?limit=20&offset=20')
        r = ''
        for x in range(0, 38):
            r = riter.next()
        self.assertFalse(isinstance(r, Collection))
        self.assertEqual(r['resource_uri'], '/api/v1/repository/42/')
        self.assertEqual(riter.url, 'https://registry.cdlib.org/api/v1/repository/?limit=20&offset=20')
        self.assertEqual(riter.path_next, None)
        self.assertRaises(StopIteration, riter.next)

    @httpretty.activate
    def testResourceIteratorReturnsCollection(self):
        '''Test that the resource iterator returns a Collection object
        for library collection resources'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/',
                body=open(DIR_FIXTURES+'/registry_api_collection.json').read())
        riter = self.registry.resource_iter('collection')
        c = riter.next()
        self.assertTrue(isinstance(c, Collection))
        self.assertTrue(hasattr(c, 'auth'))
        self.assertEqual(c.auth, None)

    @httpretty.activate
    def testNuxeoCollectionAuth(self):
        '''Test that a Nuxeo harvest collection returns an
        authentication tuple, not None
        '''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/19',
                body=open(DIR_FIXTURES+'/registry_api_collection_nuxeo.json').read())
        c = Collection('https://registry.cdlib.org/api/v1/collection/19')
        self.assertTrue(c.harvest_type, 'NUX')
        defaultrc = """\
[nuxeo_account]
user = TestUser
password = TestPass

[platform_importer]
base = http://localhost:8080/nuxeo/site/fileImporter
"""

        with patch('__builtin__.open') as fakeopen:
            fakeopen.return_value = StringIO.StringIO(defaultrc)
            self.assertEqual(c.auth[0], 'TestUser')
            self.assertEqual(c.auth[1], 'TestPass')


class ApiCollectionTestCase(TestCase):
    '''Test that the Collection object is complete from the api
    '''
    @httpretty.activate
    def testOAICollectionAPI(self):
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/197',
                body=open(DIR_FIXTURES+'/collection_api_test.json').read())
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
                body=open(DIR_FIXTURES+'/collection_api_test_oac.json').read())
        c = Collection('https://registry.cdlib.org/api/v1/collection/178')
        self.assertEqual(c['harvest_type'], 'OAJ')
        self.assertEqual(c.harvest_type, 'OAJ')
        self.assertEqual(c['name'], 'Harry Crosby Collection')
        self.assertEqual(c.name, 'Harry Crosby Collection')
        self.assertEqual(c['url_oac'], 'fixtures/testOAC.json')
        self.assertEqual(c.url_oac, 'fixtures/testOAC.json')
        self.assertEqual(c.campus[0]['resource_uri'], '/api/v1/campus/6/')
        self.assertEqual(c.campus[0]['slug'], 'UCSD')
        self.assertEqual(c.dcmi_type, 'I')
        self.assertEqual(c.rights_statement, "a sample rights statement")
        self.assertEqual(c.rights_status, "PD")

    @httpretty.activate
    def testCreateProfile(self):
        '''Test the creation of a DPLA style proflie file'''
        httpretty.register_uri(httpretty.GET,
                'https://registry.cdlib.org/api/v1/collection/178',
                body=open(DIR_FIXTURES+'/collection_api_test_oac.json').read())
        c = Collection('https://registry.cdlib.org/api/v1/collection/178')
        self.assertTrue(hasattr(c, 'dpla_profile'))
        self.assertIsInstance(c.dpla_profile, str)
        j = json.loads(c.dpla_profile)
        self.assertEqual(j['name'], '178')
        self.assertEqual(j['enrichments_coll'], ['/compare_with_schema'])
        self.assertTrue('enrichments_item' in j)
        self.assertIsInstance(j['enrichments_item'], list)
        self.assertEqual(len(j['enrichments_item']), 30)
        self.assertIn('contributor', j)
        self.assertIsInstance(j['contributor'], list)
        self.assertEqual(len(j['contributor']), 4)
        self.assertEqual(j['contributor'][1], {u'@id': u'/api/v1/campus/1/', u'name': u'UCB'})
        self.assertTrue(hasattr(c, 'dpla_profile_obj'))
        self.assertIsInstance(c.dpla_profile_obj, dict)
        self.assertIsInstance(c.dpla_profile_obj['enrichments_item'], list)
        e = c.dpla_profile_obj['enrichments_item']
        self.assertEqual(e[0], '/oai-to-dpla')
        self.assertEqual(e[1], '/shred?prop=sourceResource/contributor%2CsourceResource/creator%2CsourceResource/date')


