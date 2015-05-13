from unittest import TestCase
from collections import namedtuple
from mock import patch
from mock import MagicMock
import httpretty
from harvester import image_harvest

class ImageHarvestTestCase(TestCase):
    '''Test the md5 s3 image harvesting calls.....
    TODO: Increase test coverage
    '''
    report = namedtuple('Report', 's3_url, md5')
    # StashReport = namedtuple('StashReport', 'url, md5, s3_url, mime_type')

    @patch('couchdb.Server')
    @patch('md5s3stash.md5s3stash', autospec=True, return_value=report('s3 test url', 'md5 test value'))
    @httpretty.activate
    def test_stash_image(self, mock_stash, mock_couch):
        '''Test the stash image calls are correct'''
        doc = {'_id': 'TESTID'}
        self.assertRaises(KeyError, image_harvest.ImageHarvester().stash_image, doc)
        doc['isShownBy'] = None
        self.assertRaises(ValueError, image_harvest.ImageHarvester().stash_image, doc)
        doc['isShownBy'] = 'test_local_url_ark:'
        url_test = 'http://content.cdlib.org/test_local_url_ark:'
        httpretty.register_uri(httpretty.HEAD,
                url_test,
                body='',
                content_length='0',
                content_type='image/jpeg;',
                connection='close',
                )
        ret = image_harvest.ImageHarvester().stash_image(doc)
        mock_stash.assert_called_with('http://content.cdlib.org/test_local_url_ark:', url_auth=None, bucket_base='ucldc')
        self.assertEqual('s3 test url', ret.s3_url)
        ret = image_harvest.ImageHarvester(bucket_base='x').stash_image(doc)
        mock_stash.assert_called_with('http://content.cdlib.org/test_local_url_ark:', url_auth=None, bucket_base='x')
        ret = image_harvest.ImageHarvester(bucket_base='x',
                           object_auth=('tstuser', 'tstpswd')).stash_image(doc)
        mock_stash.assert_called_with(
                'http://content.cdlib.org/test_local_url_ark:',
                url_auth=('tstuser', 'tstpswd'),
                bucket_base='x')

    def test_update_doc_object(self):
        '''Test call to couchdb, right data'''
        doc = {'_id': 'TESTID'}
        r = self.report('s3 test2 url', 'md5 test value')
        db = MagicMock()
        ret = image_harvest.ImageHarvester(cdb=db).update_doc_object(doc, r)
        self.assertEqual('md5 test value', ret)
        self.assertEqual('md5 test value', doc['object'])
        db.save.assert_called_with({'_id': 'TESTID', 'object': 'md5 test value'})
    
    @httpretty.activate
    def test_link_is_to_image(self):
        '''Test the link_is_to_image function'''
        url = 'http://getthisimage/notanimage'
        httpretty.register_uri(httpretty.HEAD,
                url,
                body='',
                content_length='0',
                content_type='text/plain; charset=utf-8',
                connection='close',
                )
        self.assertFalse(image_harvest.link_is_to_image(url))
        url = 'http://getthisimage/isanimage'
        httpretty.register_uri(httpretty.HEAD,
                url,
                body='',
                content_length='0',
                content_type='image/jpeg; charset=utf-8',
                connection='close',
                )
        self.assertTrue(image_harvest.link_is_to_image(url))
        url_redirect = 'http://gethisimage/redirect'
        httpretty.register_uri(httpretty.HEAD,
                url,
                body='',
                status=301,
                location=url_redirect
                )
        httpretty.register_uri(httpretty.HEAD,
                url_redirect,
                body='',
                content_length='0',
                content_type='image/jpeg; charset=utf-8',
                connection='close',
                )
        self.assertTrue(image_harvest.link_is_to_image(url))

    @patch('couchdb.Server')
    @patch('md5s3stash.md5s3stash', autospec=True, return_value=report('s3 test url', 'md5 test value'))
    @httpretty.activate
    def test_check_content_type(self, mock_stash, mock_couch):
        '''Test that the check for content type correctly aborts if the 
        type is not a image
        '''
        url = 'http://getthisimage/notanimage'
        doc = {'_id': 'TESTID', 'isShownBy':url }
        httpretty.register_uri(httpretty.HEAD,
                url,
                body='',
                content_length='0',
                content_type='text/plain; charset=utf-8',
                connection='close',
                )
        ret = image_harvest.ImageHarvester(bucket_base='x').stash_image(doc)
        self.assertEqual(ret, None)
        httpretty.register_uri(httpretty.HEAD,
                url,
                body='',
                content_length='0',
                content_type='image/plain; charset=utf-8',
                connection='close',
                )
        r = self.report('s3 test url', 'md5 test value')
        ret = image_harvest.ImageHarvester(bucket_base='x').stash_image(doc)
        self.assertEqual(ret, r)

