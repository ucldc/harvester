# -*- coding: utf-8 -*-
import os
from unittest import TestCase
import pickle
import json
import re
import shutil
from mock import patch
from mypretty import httpretty
# import httpretty
import pynux.utils
from harvester.collection_registry_client import Collection
from test.utils import DIR_FIXTURES
from test.utils import ConfigFileOverrideMixin, LogOverrideMixin
import harvester.fetcher as fetcher


class Bunch(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def deepharvest_mocker(mock_deepharvest):
    ''' mock deepharvest class '''
    dh_instance = mock_deepharvest.return_value
    with open(DIR_FIXTURES + '/nuxeo_doc_pickled', 'r') as f:
        dh_instance.fetch_objects.return_value = pickle.load(f)
    dh_instance.nx = Bunch(conf={'api': 'testapi'})


class NuxeoFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test Nuxeo fetching'''
    # put httppretty here, have sample outputs.
    @httpretty.activate
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def testInit(self, mock_deepharvest, mock_boto):
        '''Basic tdd start'''
        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/path-to-asset/here/@children',
            body=open(DIR_FIXTURES + '/nuxeo_folder.json').read())
        deepharvest_mocker(mock_deepharvest)
        h = fetcher.NuxeoFetcher('https://example.edu/api/v1/',
                                 'path-to-asset/here')
        mock_deepharvest.assert_called_with(
            'path-to-asset/here',
            '',
            conf_pynux={'api': 'https://example.edu/api/v1/'})
        self.assertTrue(hasattr(h, '_url'))  # assert in called next repeatedly
        self.assertEqual(h.url, 'https://example.edu/api/v1/')
        self.assertTrue(hasattr(h, '_nx'))
        self.assertIsInstance(h._nx, pynux.utils.Nuxeo)
        self.assertTrue(hasattr(h, '_children'))
        self.assertTrue(hasattr(h, 'next'))
        self.assertTrue(hasattr(h, '_structmap_bucket'))

    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def test_get_structmap_text(self, mock_deepharvest, mock_boto):
        '''Mock test s3 structmap_text getting'''
        media_json = open(DIR_FIXTURES + '/nuxeo_media_structmap.json').read()
        deepharvest_mocker(mock_deepharvest)
        mock_boto.return_value.get_bucket.return_value.\
            get_key.return_value.\
            get_contents_as_string.return_value = media_json
        h = fetcher.NuxeoFetcher('https://example.edu/api/v1/',
                                 'path-to-asset/here')
        mock_deepharvest.assert_called_with(
            'path-to-asset/here',
            '',
            conf_pynux={'api': 'https://example.edu/api/v1/'})
        structmap_text = h._get_structmap_text(
            's3://static.ucldc.cdlib.org/media_json/'
            '81249b9c-5a87-43af-877c-fb161325b1a0-media.json')
        mock_boto.assert_called_with()
        mock_boto().get_bucket.assert_called_with('static.ucldc.cdlib.org')
        mock_boto().get_bucket().get_key.assert_called_with(
            '/media_json/81249b9c-5a87-43af-877c-fb161325b1a0-media.json')
        self.assertEqual(structmap_text, "Angela Davis socializing with "
                         "students at UC Irvine AS-061_A69-013_001.tif "
                         "AS-061_A69-013_002.tif AS-061_A69-013_003.tif "
                         "AS-061_A69-013_004.tif AS-061_A69-013_005.tif "
                         "AS-061_A69-013_006.tif AS-061_A69-013_007.tif")

    @httpretty.activate
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def testFetch(self, mock_deepharvest, mock_boto):
        '''Test the httpretty mocked fetching of documents'''
        media_json = open(DIR_FIXTURES + '/nuxeo_media_structmap.json').read()
        deepharvest_mocker(mock_deepharvest)
        mock_boto.return_value.get_bucket.return_value.\
            get_key.return_value.\
            get_contents_as_string.return_value = media_json
        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/path-to-asset/here/@children',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_folder.json').read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_folder-1.json').read(),
                    status=200),
            ])
        httpretty.register_uri(
            httpretty.GET,
            re.compile('https://example.edu/api/v1/id/.*'),
            body=open(DIR_FIXTURES + '/nuxeo_doc.json').read())

        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/asset-library/UCI/Cochems'
            '/MS-R016_1092.tif/@children?currentPageIndex=0',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_no_children.json').read(),
                    status=200),
            ])
        h = fetcher.NuxeoFetcher('https://example.edu/api/v1',
                                 'path-to-asset/here')
        mock_deepharvest.assert_called_with(
            'path-to-asset/here',
            '',
            conf_pynux={'api': 'https://example.edu/api/v1'})
        docs = []
        for d in h:
            docs.append(d)
        self.assertEqual(3, len(docs))
        self.assertIn('picture:views', docs[0]['properties'])
        self.assertIn('dc:subjects', docs[0]['properties'])
        self.assertIn('structmap_url', docs[0])
        self.assertIn('structmap_text', docs[0])
        self.assertEqual(docs[0]['structmap_text'],
                         "Angela Davis socializing with students at UC Irvine "
                         "AS-061_A69-013_001.tif AS-061_A69-013_002.tif "
                         "AS-061_A69-013_003.tif AS-061_A69-013_004.tif "
                         "AS-061_A69-013_005.tif AS-061_A69-013_006.tif "
                         "AS-061_A69-013_007.tif")
        self.assertEqual(
            docs[0]['isShownBy'],
            'https://nuxeo.cdlib.org/Nuxeo/nxpicsfile/default/'
            '40677ed1-f7c2-476f-886d-bf79c3fec8c4/Medium:content/')

    @httpretty.activate
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def testFetch_missing_media_json(self, mock_deepharvest, mock_boto):
        '''Test the httpretty mocked fetching of documents'''
        deepharvest_mocker(mock_deepharvest)
        mock_boto.return_value.get_bucket.return_value.\
            get_key.return_value = None
        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/path-to-asset/here/@children',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_folder.json').read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_folder-1.json').read(),
                    status=200),
            ])
        httpretty.register_uri(
            httpretty.GET,
            re.compile('https://example.edu/api/v1/id/.*'),
            body=open(DIR_FIXTURES + '/nuxeo_doc.json').read())
        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/asset-library/UCI/Cochems/'
            'MS-R016_1092.tif/@children?currentPageIndex=0',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_no_children.json').read(),
                    status=200),
            ])
        h = fetcher.NuxeoFetcher('https://example.edu/api/v1',
                                 'path-to-asset/here')
        mock_deepharvest.assert_called_with(
            'path-to-asset/here',
            '',
            conf_pynux={'api': 'https://example.edu/api/v1'})
        docs = []
        for d in h:
            docs.append(d)
        self.assertEqual(docs[0]['structmap_text'], '')
        self.assertEqual(docs[1]['structmap_text'], '')
        self.assertEqual(docs[2]['structmap_text'], '')

    @httpretty.activate
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def test_get_isShownBy_component_image(self, mock_deepharvest, mock_boto):
        ''' test getting correct isShownBy value for Nuxeo doc
            with no image at parent level, but an image at the component level
        '''
        deepharvest_mocker(mock_deepharvest)

        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/@search?query='
            'SELECT+%2A+FROM+Document+WHERE+ecm%3AparentId+%3D+'
            '%27d400bb29-98d4-429c-a0b8-119acdb92006%27+ORDER+BY+'
            'ecm%3Apos&currentPageIndex=0&pageSize=100',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_image_components.json')
                    .read(),
                    status=200),
            ])

        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/id/'
            'e8af2d74-0c8b-4d18-b86c-4067b9e16159',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES +
                              '/nuxeo_first_image_component.json').read(),
                    status=200),
            ])

        h = fetcher.NuxeoFetcher('https://example.edu/api/v1',
                                 'path-to-asset/here')

        nuxeo_metadata = open(DIR_FIXTURES +
                              '/nuxeo_doc_imageless_parent.json').read()
        nuxeo_metadata = json.loads(nuxeo_metadata)
        isShownBy = h._get_isShownBy(nuxeo_metadata)
        self.assertEqual(
            isShownBy, 'https://nuxeo.cdlib.org/Nuxeo/nxpicsfile/default/'
            'e8af2d74-0c8b-4d18-b86c-4067b9e16159/Medium:content/')

    @httpretty.activate
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def test_get_isShownBy_pdf(self, mock_deepharvest, mock_boto):
        ''' test getting correct isShownBy value for Nuxeo doc
            with no images and PDF at parent level
        '''
        deepharvest_mocker(mock_deepharvest)

        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/@search?query=SELECT+%2A+FROM+'
            'Document+WHERE+ecm%3AparentId+%3D+'
            '%2700d55837-01b6-4211-80d8-b966a15c257e%27+ORDER+BY+'
            'ecm%3Apos&currentPageIndex=0&pageSize=100',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_no_children.json').read(),
                    status=200),
            ])

        h = fetcher.NuxeoFetcher('https://example.edu/api/v1',
                                 'path-to-asset/here')

        nuxeo_metadata = open(DIR_FIXTURES +
                              '/nuxeo_doc_pdf_parent.json').read()
        nuxeo_metadata = json.loads(nuxeo_metadata)
        isShownBy = h._get_isShownBy(nuxeo_metadata)
        self.assertEqual(
            isShownBy, 'https://s3.amazonaws.com/static.ucldc.cdlib.org/'
            'ucldc-nuxeo-thumb-media/00d55837-01b6-4211-80d8-b966a15c257e')

    @httpretty.activate
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def test_get_isShownBy_video(self, mock_deepharvest, mock_boto):
        ''' test getting correct isShownBy value for Nuxeo video object
        '''
        deepharvest_mocker(mock_deepharvest)

        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/@search?query=SELECT+%2A+FROM+'
            'Document+WHERE+ecm%3AparentId+%3D+'
            '%274c80e254-6def-4230-9f28-bc48878568d4%27+'
            'AND+ecm%3AcurrentLifeCycleState+%21%3D+%27deleted%27+ORDER+BY+'
            'ecm%3Apos&currentPageIndex=0&pageSize=100',
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/nuxeo_no_children.json').read(),
                    status=200),
            ])

        h = fetcher.NuxeoFetcher('https://example.edu/api/v1',
                                 'path-to-asset/here')

        nuxeo_metadata = open(DIR_FIXTURES + '/nuxeo_doc_video.json').read()
        nuxeo_metadata = json.loads(nuxeo_metadata)
        isShownBy = h._get_isShownBy(nuxeo_metadata)

        self.assertEqual(
            isShownBy, 'https://s3.amazonaws.com/static.ucldc.cdlib.org/'
            'ucldc-nuxeo-thumb-media/4c80e254-6def-4230-9f28-bc48878568d4')


class UCLDCNuxeoFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test that the UCLDC Nuxeo Fetcher errors if necessary
    Nuxeo document schema header property not set.
    '''

    @httpretty.activate
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def testNuxeoPropHeader(self, mock_deepharvest):
        '''Test that the Nuxeo document property header has necessary
        settings. This will test the base UCLDC schemas
        '''
        deepharvest_mocker(mock_deepharvest)
        httpretty.register_uri(
            httpretty.GET,
            'https://example.edu/api/v1/path/path-to-asset/here/@children',
            body=open(DIR_FIXTURES + '/nuxeo_folder.json').read())
        # can test adding a prop, but what if prop needed not there.
        # need to remove ~/.pynuxrc
        self.assertRaises(
            AssertionError,
            fetcher.UCLDCNuxeoFetcher,
            'https://example.edu/api/v1/',
            'path-to-asset/here',
            conf_pynux={'X-NXDocumentProperties': ''})
        self.assertRaises(
            AssertionError,
            fetcher.UCLDCNuxeoFetcher,
            'https://example.edu/api/v1/',
            'path-to-asset/here',
            conf_pynux={'X-NXDocumentProperties': 'dublincore'})
        self.assertRaises(
            AssertionError,
            fetcher.UCLDCNuxeoFetcher,
            'https://example.edu/api/v1/',
            'path-to-asset/here',
            conf_pynux={'X-NXDocumentProperties': 'dublincore,ucldc_schema'})
        h = fetcher.UCLDCNuxeoFetcher(
            'https://example.edu/api/v1/',
            'path-to-asset/here',
            conf_pynux={
                'X-NXDocumentProperties':
                'dublincore,ucldc_schema,picture,file'
            })
        mock_deepharvest.assert_called_with(
            'path-to-asset/here',
            '',
            conf_pynux={
                'X-NXDocumentProperties':
                'dublincore,ucldc_schema,picture,file',
                'api': 'https://example.edu/api/v1/'
            })
        self.assertIn('dublincore', h._nx.conf['X-NXDocumentProperties'])
        self.assertIn('ucldc_schema', h._nx.conf['X-NXDocumentProperties'])
        self.assertIn('picture', h._nx.conf['X-NXDocumentProperties'])


class Harvest_UCLDCNuxeo_ControllerTestCase(ConfigFileOverrideMixin,
                                            LogOverrideMixin, TestCase):
    '''Test the function of an Nuxeo harvest controller'''

    def setUp(self):
        super(Harvest_UCLDCNuxeo_ControllerTestCase, self).setUp()

    def tearDown(self):
        super(Harvest_UCLDCNuxeo_ControllerTestCase, self).tearDown()
        shutil.rmtree(self.controller.dir_save)

    @httpretty.activate
    @patch('boto3.resource', autospec=True)
    @patch('boto.connect_s3', autospec=True)
    @patch('harvester.fetcher.nuxeo_fetcher.DeepHarvestNuxeo', autospec=True)
    def testNuxeoHarvest(self, mock_deepharvest, mock_boto, mock_boto3):
        '''Test the function of the Nuxeo harvest'''
        media_json = open(DIR_FIXTURES + '/nuxeo_media_structmap.json').read()
        mock_boto.return_value.get_bucket.return_value.\
            get_key.return_value.\
            get_contents_as_string.return_value = media_json
        httpretty.register_uri(
            httpretty.GET,
            'http://registry.cdlib.org/api/v1/collection/19/',
            body=open(DIR_FIXTURES + '/collection_api_test_nuxeo.json').read())
        mock_deepharvest.return_value.fetch_objects.return_value = json.load(
            open(DIR_FIXTURES + '/nuxeo_object_list.json'))
        httpretty.register_uri(
            httpretty.GET,
            re.compile('https://example.edu/Nuxeo/site/api/v1/id/.*'),
            body=open(DIR_FIXTURES + '/nuxeo_doc.json').read())

        self.collection = Collection(
            'http://registry.cdlib.org/api/v1/collection/19/')
        with patch(
                'ConfigParser.SafeConfigParser',
                autospec=True) as mock_configparser:
            config_inst = mock_configparser.return_value
            config_inst.get.return_value = 'dublincore,ucldc_schema,picture'
            self.setUp_config(self.collection)
            self.controller = fetcher.HarvestController(
                'email@example.com',
                self.collection,
                config_file=self.config_file,
                profile_path=self.profile_path)
        self.assertTrue(hasattr(self.controller, 'harvest'))
        num = self.controller.harvest()
        self.assertEqual(num, 5)
        self.tearDown_config()
        # verify one record has collection and such filled in
        fname = os.listdir(self.controller.dir_save)[0]
        saved_objset = json.load(
            open(os.path.join(self.controller.dir_save, fname)))
        saved_obj = saved_objset[0]
        self.assertEqual(saved_obj['collection'][0]['@id'],
                         u'http://registry.cdlib.org/api/v1/collection/19/')
        self.assertEqual(saved_obj['collection'][0]['name'],
                         u'Cochems (Edward W.) Photographs')
        self.assertEqual(saved_obj['collection'][0]['title'],
                         u'Cochems (Edward W.) Photographs')
        self.assertEqual(saved_obj['collection'][0]['id'], u'19')
        self.assertEqual(saved_obj['collection'][0]['dcmi_type'], 'I')
        self.assertEqual(saved_obj['collection'][0]['rights_statement'],
                         'a sample rights statement')
        self.assertEqual(saved_obj['collection'][0]['rights_status'], 'PD')
        self.assertEqual(saved_obj['state'], 'project')
        self.assertEqual(
            saved_obj['title'],
            'Adeline Cochems having her portrait taken by her father '
            'Edward W, Cochems in Santa Ana, California: Photograph')


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
