# -*- coding: utf-8 -*-
import os
import json
from unittest import TestCase
import shutil
from xml.etree import ElementTree as ET
from mypretty import httpretty
# import httpretty
import harvester.fetcher as fetcher
from harvester.collection_registry_client import Collection
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
from test.utils import DIR_FIXTURES


class HarvestOAC_JSON_ControllerTestCase(ConfigFileOverrideMixin,
                                         LogOverrideMixin, TestCase):
    '''Test the function of an OAC harvest controller'''
    @httpretty.activate
    def setUp(self):
        super(HarvestOAC_JSON_ControllerTestCase, self).setUp()
        # self.testFile = DIR_FIXTURES+'/collection_api_test_oac.json'
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/178/",
                body=open(DIR_FIXTURES+'/collection_api_test_oac.json').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/tf2v19n928',
            body=open(DIR_FIXTURES+'/testOAC.json').read())
        self.collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/178/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController(
                'email@example.com', self.collection,
                config_file=self.config_file, profile_path=self.profile_path)

    def tearDown(self):
        super(HarvestOAC_JSON_ControllerTestCase, self).tearDown()
        self.tearDown_config()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testOAC_JSON_Harvest(self):
        '''Test the function of the OAC harvest'''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/tf2v19n928',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.controller.harvest()
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue(
                'UCB Department of Statistics' in
                self.test_log_handler.formatted_records[0])
        self.assertEqual(self.test_log_handler.formatted_records[1],
                         '[INFO] HarvestController: 28 records harvested')

    @httpretty.activate
    def testObjectsHaveRegistryData(self):
        # test OAC objsets
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/tf2v19n928',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/tf2v19n928&startDoc=26',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.testFile = DIR_FIXTURES+'/testOAC-url_next-1.json'
        self.ranGet = False
        self.controller.harvest()
        dir_list = os.listdir(self.controller.dir_save)
        self.assertEqual(len(dir_list), 2)
        objset_saved = json.loads(open(
            os.path.join(self.controller.dir_save, dir_list[0])).read())
        obj = objset_saved[2]
        self.assertIn('source_collection_name', obj)
        self.assertEqual(obj['source_collection_name'],
                         'record source collection name')
        self.assertIn('collection', obj)
        self.assertIn('@id', obj['collection'][0])
        self.assertIn('title', obj['collection'][0])
        self.assertIn('campus', obj['collection'][0])
        self.assertIn('dcmi_type', obj['collection'][0])
        self.assertIn('description', obj['collection'][0])
        self.assertIn('enrichments_item', obj['collection'][0])
        self.assertIn('name', obj['collection'][0])
        self.assertIn('harvest_extra_data', obj['collection'][0])
        self.assertIn('repository', obj['collection'][0])
        self.assertIn('rights_statement', obj['collection'][0])
        self.assertIn('rights_status', obj['collection'][0])
        self.assertIn('url_harvest', obj['collection'][0])
        self.assertEqual(obj['collection'][0]['@id'],
                         'https://registry.cdlib.org/api/v1/collection/178/')
        self.assertNotIn('campus', obj)
        self.assertEqual(obj['collection'][0]['campus'],
                            [
                               {
                                 '@id': 'https://registry.cdlib.org/api/v1/'
                                 'campus/6/',
                                 'slug': 'UCSD',
                                 'resource_uri': '/api/v1/campus/6/',
                                 'position': 6,
                                 'name': 'UC San Diego'
                               },
                               {
                                 '@id': 'https://registry.cdlib.org/api/v1/'
                                 'campus/1/',
                                 'slug': 'UCB',
                                 'resource_uri': '/api/v1/campus/1/',
                                 'position': 0,
                                 'name': 'UC Berkeley'
                               }
                             ])
        self.assertNotIn('repository', obj)
        self.assertEqual(obj['collection'][0]['repository'],
                         [
                          {
                            '@id': 'https://registry.cdlib.org/api/v1/'
                            'repository/22/',
                            'resource_uri': '/api/v1/repository/22/',
                            'name': 'Mandeville Special Collections Library',
                            'slug': 'Mandeville-Special-Collections-Library',
                            'campus': [
                              {
                                'slug': 'UCSD',
                                'resource_uri': '/api/v1/campus/6/',
                                'position': 6,
                                'name': 'UC San Diego'
                              },
                              {
                                'slug': 'UCB',
                                'resource_uri': '/api/v1/campus/1/',
                                'position': 0,
                                'name': 'UC Berkeley'
                              }
                            ]
                          },
                          {
                            '@id': 'https://registry.cdlib.org/api/v1/'
                            'repository/36/',
                            'resource_uri': '/api/v1/repository/36/',
                            'name': 'UCB Department of Statistics',
                            'slug': 'UCB-Department-of-Statistics',
                            'campus': {
                                'slug': 'UCB',
                                'resource_uri': '/api/v1/campus/1/',
                                'position': 0,
                                'name': 'UC Berkeley'
                              }
                          }
                        ])


class HarvestOAC_XML_ControllerTestCase(ConfigFileOverrideMixin,
                                        LogOverrideMixin, TestCase):
    '''Test the function of an OAC XML harvest controller'''
    @httpretty.activate
    def setUp(self):
        super(HarvestOAC_XML_ControllerTestCase, self).setUp()
        # self.testFile = DIR_FIXTURES+'/collection_api_test_oac.json'
        httpretty.register_uri(
                httpretty.GET,
                "https://registry.cdlib.org/api/v1/collection/178/",
                body=open(
                    DIR_FIXTURES +
                    '/collection_api_test_oac_xml.json').read())
        httpretty.register_uri(
           httpretty.GET,
           'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
           'relation=ark:/13030/tf0c600134',
           body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        self.collection = Collection(
                'https://registry.cdlib.org/api/v1/collection/178/')
        self.setUp_config(self.collection)
        self.controller = fetcher.HarvestController(
                'email@example.com', self.collection,
                config_file=self.config_file, profile_path=self.profile_path)
        print "DIR SAVE::::: {}".format(self.controller.dir_save)

    def tearDown(self):
        super(HarvestOAC_XML_ControllerTestCase, self).tearDown()
        self.tearDown_config()
        # shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    def testOAC_XML_Harvest(self):
        '''Test the function of the OAC harvest'''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/tf0c600134',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.xml').read())
        self.assertTrue(hasattr(self.controller, 'harvest'))
        self.controller.harvest()
        print "LOGS:{}".format(self.test_log_handler.formatted_records)
        self.assertEqual(len(self.test_log_handler.records), 2)
        self.assertTrue(
                'UCB Department of Statistics' in
                self.test_log_handler.formatted_records[0])
        self.assertEqual(
                self.test_log_handler.formatted_records[1],
                '[INFO] HarvestController: 24 records harvested')


class OAC_XML_FetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the OAC_XML_Fetcher
    '''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/tf0c600134',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        super(OAC_XML_FetcherTestCase, self).setUp()
        self.fetcher = fetcher.OAC_XML_Fetcher(
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/tf0c600134',
                'extra_data')

    def tearDown(self):
        super(OAC_XML_FetcherTestCase, self).tearDown()

    @httpretty.activate
    def testBadOACSearch(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj--xxxx',
            body=open(DIR_FIXTURES+'/testOAC-badsearch.xml').read())
        self.assertRaises(
                ValueError,
                fetcher.OAC_XML_Fetcher,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj--xxxx', 'extra_data')

    @httpretty.activate
    def testOnlyTextResults(self):
        '''Test when only texts are in result'''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-noimages-in-results.xml').read())
        h = fetcher.OAC_XML_Fetcher(
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 11)
        recs = self.fetcher.next()
        self.assertEqual(self.fetcher.groups['text']['end'], 10)
        self.assertEqual(len(recs), 10)

    @httpretty.activate
    def testUTF8ResultsContent(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-utf8-content.xml').read())
        h = fetcher.OAC_XML_Fetcher(
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 0)
        objset = h.next()
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 25)
        self.assertEqual(len(objset), 25)

    @httpretty.activate
    def testAmpersandInDoc(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-utf8-content.xml').read())
        h = fetcher.OAC_XML_Fetcher(
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj', 'extra_data')
        self.assertEqual(h.totalDocs, 25)
        self.assertEqual(h.currentDoc, 0)
        h.next()

    def testDocHitsToObjset(self):
        '''Check that the _docHits_to_objset to function returns expected
        object for a given input'''
        docHits = ET.parse(open(DIR_FIXTURES+'/docHit.xml')).getroot()
        objset = self.fetcher._docHits_to_objset([docHits])
        obj = objset[0]
        self.assertEqual(
            obj['relation'][0],
            {'attrib': {},
             'text': 'http://www.oac.cdlib.org/findaid/ark:/13030/tf0c600134'})
        self.assertIsInstance(obj['relation'], list)
        self.assertIsNone(obj.get('google_analytics_tracking_code'))
        self.assertIsInstance(obj['reference-image'][0], dict)
        self.assertEqual(len(obj['reference-image']), 2)
        self.assertIn('X', obj['reference-image'][0])
        self.assertEqual(750, obj['reference-image'][0]['X'])
        self.assertIn('Y', obj['reference-image'][0])
        self.assertEqual(564, obj['reference-image'][0]['Y'])
        self.assertIn('src', obj['reference-image'][0])
        self.assertEqual(
                'http://content.cdlib.org/ark:/13030/kt40000501/FID3',
                obj['reference-image'][0]['src'])
        self.assertIsInstance(obj['thumbnail'], dict)
        self.assertIn('X', obj['thumbnail'])
        self.assertEqual(125, obj['thumbnail']['X'])
        self.assertIn('Y', obj['thumbnail'])
        self.assertEqual(93, obj['thumbnail']['Y'])
        self.assertIn('src', obj['thumbnail'])
        self.assertEqual(
                'http://content.cdlib.org/ark:/13030/kt40000501/thumbnail',
                obj['thumbnail']['src'])
        self.assertIsInstance(obj['publisher'][0], dict)
        self.assertEqual(
                obj['date'],
                [
                    {'attrib': {'q': 'created'}, 'text': '7/21/42'},
                    {'attrib': {'q': 'published'}, 'text': '7/21/72'}])

    def testDocHitsToObjsetBadImageData(self):
        '''Check when the X & Y for thumbnail or reference image is not an
        integer. Text have value of "" for X & Y'''
        docHits = ET.parse(open(
            DIR_FIXTURES + '/docHit-blank-image-sizes.xml')).getroot()
        objset = self.fetcher._docHits_to_objset([docHits])
        obj = objset[0]
        self.assertEqual(0, obj['reference-image'][0]['X'])
        self.assertEqual(0, obj['reference-image'][0]['Y'])
        self.assertEqual(0, obj['thumbnail']['X'])
        self.assertEqual(0, obj['thumbnail']['Y'])

    def testDocHitsToObjsetRemovesBlanks(self):
        '''Blank xml tags (no text value) get propagated through the system
        as "null" values. Eliminate them here to make data cleaner.
        '''
        docHit = ET.parse(open(
            DIR_FIXTURES + '/testOAC-blank-value.xml')).getroot()
        objset = self.fetcher._docHits_to_objset([docHit])
        obj = objset[0]
        self.assertEqual(obj['title'], [
            {'attrib': {},
             'text': 'Main Street, Cisko Placer Co. [California]. '
             'Popular American Scenery.'}])

    @httpretty.activate
    def testFetchOnePage(self):
        '''Test fetching one "page" of results where no return trips are
        necessary
        '''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        self.assertTrue(hasattr(self.fetcher, 'totalDocs'))
        self.assertTrue(hasattr(self.fetcher, 'totalGroups'))
        self.assertTrue(hasattr(self.fetcher, 'groups'))
        self.assertIsInstance(self.fetcher.totalDocs, int)
        self.assertEqual(self.fetcher.totalDocs, 24)
        self.assertEqual(self.fetcher.groups['image']['total'], 13)
        self.assertEqual(self.fetcher.groups['image']['start'], 1)
        self.assertEqual(self.fetcher.groups['image']['end'], 0)
        self.assertEqual(self.fetcher.groups['text']['total'], 11)
        self.assertEqual(self.fetcher.groups['text']['start'], 0)
        self.assertEqual(self.fetcher.groups['text']['end'], 0)
        recs = self.fetcher.next()
        self.assertEqual(self.fetcher.groups['image']['end'], 10)
        self.assertEqual(len(recs), 10)


class OAC_XML_Fetcher_text_contentTestCase(LogOverrideMixin, TestCase):
    '''Test when results only contain texts'''
    @httpretty.activate
    def testFetchTextOnlyContent(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10',
            body=open(DIR_FIXTURES+'/testOAC-noimages-in-results.xml').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10&startDoc=1&'
            'group=text',
            body=open(DIR_FIXTURES+'/testOAC-noimages-in-results.xml').read())
        oac_fetcher = fetcher.OAC_XML_Fetcher(
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_fetcher.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(
                oac_fetcher._url_current,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&'
                'group=text')
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&DocsPerPage=10&startDoc=11&'
            'group=text',
            body=open(
                DIR_FIXTURES + '/testOAC-noimages-in-results-1.xml').read())
        second_set = oac_fetcher.next()
        self.assertEqual(len(second_set), 1)
        self.assertEqual(
            oac_fetcher._url_current,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&'
            'group=text')
        self.assertRaises(StopIteration, oac_fetcher.next)


class OAC_XML_Fetcher_mixed_contentTestCase(LogOverrideMixin, TestCase):
    @httpretty.activate
    def testFetchMixedContent(self):
        '''This interface gets tricky when image & text data are in the
        collection.
        My test Mock object will return an xml with 10 images
        then with 3 images
        then 10 texts
        then 1 text then quit
        '''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&'
            'group=image',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.xml').read())
        oac_fetcher = fetcher.OAC_XML_Fetcher(
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj', 'extra_data', docsPerPage=10)
        first_set = oac_fetcher.next()
        self.assertEqual(len(first_set), 10)
        self.assertEqual(
                oac_fetcher._url_current,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&'
                'group=image')
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&'
            'group=image',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.xml').read())
        second_set = oac_fetcher.next()
        self.assertEqual(len(second_set), 3)
        self.assertEqual(
                oac_fetcher._url_current,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&'
                'group=image')
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&'
            'group=text',
            body=open(DIR_FIXTURES+'/testOAC-url_next-2.xml').read())
        third_set = oac_fetcher.next()
        self.assertEqual(len(third_set), 10)
        self.assertEqual(
                oac_fetcher._url_current,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=1&'
                'group=text')
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&'
            'group=text',
            body=open(DIR_FIXTURES+'/testOAC-url_next-3.xml').read())
        fourth_set = oac_fetcher.next()
        self.assertEqual(len(fourth_set), 1)
        self.assertEqual(
                oac_fetcher._url_current,
                'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
                'relation=ark:/13030/hb5d5nb7dj&docsPerPage=10&startDoc=11&'
                'group=text')
        self.assertRaises(StopIteration, oac_fetcher.next)


class OAC_JSON_FetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the OAC_JSON_Fetcher
    '''
    @httpretty.activate
    def setUp(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        super(OAC_JSON_FetcherTestCase, self).setUp()
        self.fetcher = fetcher.OAC_JSON_Fetcher(
                'http://dsc.cdlib.org/search?rmode=json&facet=type-tab&'
                'style=cui&relation=ark:/13030/hb5d5nb7dj', 'extra_data')

    def tearDown(self):
        super(OAC_JSON_FetcherTestCase, self).tearDown()

    def testParseArk(self):
        self.assertEqual(
                self.fetcher._parse_oac_findaid_ark(self.fetcher.url),
                'ark:/13030/hb5d5nb7dj')

    @httpretty.activate
    def testOAC_JSON_FetcherReturnedData(self):
        '''test that the data returned by the OAC Fetcher is a proper dc
        dictionary
        '''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&startDoc=26',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        rec = self.fetcher.next()[0]
        self.assertIsInstance(rec, dict)

    @httpretty.activate
    def testHarvestByRecord(self):
        '''Test the older by single record interface'''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&startDoc=26',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.testFile = DIR_FIXTURES + '/testOAC-url_next-1.json'
        records = []
        r = self.fetcher.next_record()
        try:
            while True:
                records.append(r)
                r = self.fetcher.next_record()
        except StopIteration:
            pass
        self.assertEqual(len(records), 28)

    @httpretty.activate
    def testHarvestIsIter(self):
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&startDoc=26',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.fetcher, '__iter__'))
        self.assertEqual(self.fetcher, self.fetcher.__iter__())
        self.fetcher.next_record()
        self.fetcher.next()

    @httpretty.activate
    def testNextGroupFetch(self):
        '''Test that the OAC Fetcher will fetch more records when current
        response set records are all consumed'''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&startDoc=26',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.testFile = DIR_FIXTURES+'/testOAC-url_next-1.json'
        records = []
        self.ranGet = False
        for r in self.fetcher:
            records.extend(r)
        self.assertEqual(len(records), 28)

    @httpretty.activate
    def testObjsetFetch(self):
        '''Test fetching data in whole objsets'''
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj',
            body=open(DIR_FIXTURES+'/testOAC-url_next-0.json').read())
        httpretty.register_uri(
            httpretty.GET,
            'http://dsc.cdlib.org/search?facet=type-tab&style=cui&raw=1&'
            'relation=ark:/13030/hb5d5nb7dj&startDoc=26',
            body=open(DIR_FIXTURES+'/testOAC-url_next-1.json').read())
        self.assertTrue(hasattr(self.fetcher, 'next_objset'))
        self.assertTrue(hasattr(self.fetcher.next_objset, '__call__'))
        objset = self.fetcher.next_objset()
        self.assertIsNotNone(objset)
        self.assertIsInstance(objset, list)
        self.assertEqual(len(objset), 25)
        objset2 = self.fetcher.next_objset()
        self.assertTrue(objset != objset2)
        self.assertRaises(StopIteration, self.fetcher.next_objset)


# Copyright © 2016, Regents of the University of California
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# - Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# - Neither the name of the University of California nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
