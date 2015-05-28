from unittest import TestCase
from collections import namedtuple
from mock import patch
from mock import MagicMock
import httpretty
from harvester import image_harvest

#TODO: make this importable from md5s3stash
StashReport = namedtuple('StashReport', 'url, md5, s3_url, mime_type, dimensions')
class ImageHarvestTestCase(TestCase):
    '''Test the md5 s3 image harvesting calls.....
    TODO: Increase test coverage
    '''
    @patch('harvester.image_harvest.Redis', autospec=True)
    @patch('couchdb.Server')
    @patch('md5s3stash.md5s3stash', autospec=True,
            return_value=StashReport('test url', 'md5 test value',
                's3 url object', 'mime_type', 'dimensions'))
    @httpretty.activate
    def test_stash_image(self, mock_stash, mock_couch, mock_redis):
        '''Test the stash image calls are correct'''
        doc = {'_id': 'TESTID'}
        self.assertRaises(KeyError, image_harvest.ImageHarvester().stash_image, doc)
        doc['isShownBy'] = None
        self.assertRaises(ValueError, image_harvest.ImageHarvester().stash_image, doc)
        doc['isShownBy'] = 'ark:/test_local_url_ark:'
        url_test = 'http://content.cdlib.org/ark:/test_local_url_ark:'
        httpretty.register_uri(httpretty.HEAD,
                url_test,
                body='',
                content_length='0',
                content_type='image/jpeg;',
                connection='close',
                )
        ret = image_harvest.ImageHarvester().stash_image(doc)
        mock_stash.assert_called_with(
                url_test,
                url_auth=None,
                bucket_base='ucldc',
                hash_cache={},
                url_cache={})
        self.assertEqual('s3 url object', ret.s3_url)
        ret = image_harvest.ImageHarvester(bucket_base='x').stash_image(doc)
        mock_stash.assert_called_with(
                url_test,
                url_auth=None,
                bucket_base='x',
                hash_cache={},
                url_cache={})
        ret = image_harvest.ImageHarvester(bucket_base='x',
                           object_auth=('tstuser', 'tstpswd')).stash_image(doc)
        mock_stash.assert_called_with(
                url_test,
                url_auth=('tstuser', 'tstpswd'),
                bucket_base='x',
                hash_cache={},
                url_cache={})

    def test_update_doc_object(self):
        '''Test call to couchdb, right data'''
        doc = {'_id': 'TESTID'}
        r = StashReport('s3 test2 url', 'md5 test value', 's3 url', 'mime_type',
                'dimensions-x:y')
        db = MagicMock()
        ret = image_harvest.ImageHarvester(cdb=db).update_doc_object(doc, r)
        self.assertEqual('md5 test value', ret)
        self.assertEqual('md5 test value', doc['object'])
        self.assertEqual(doc['object_dimensions'], 'dimensions-x:y')
        db.save.assert_called_with({'_id': 'TESTID',
            'object': 'md5 test value',
            'object_dimensions': 'dimensions-x:y'})
    
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
    @patch('md5s3stash.md5s3stash', autospec=True,
            return_value=StashReport('test url', 'md5 test value',
                's3 url object', 'mime_type', 'dimensions'))
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
        r = StashReport('test url', 'md5 test value',
                's3 url object', 'mime_type', 'dimensions')
        ret = image_harvest.ImageHarvester(bucket_base='x').stash_image(doc)
        self.assertEqual(ret, r)

    def test_url_missing_schema(self):
        '''Test when the url is malformed and doesn't have a proper http
        schema. The LAPL photo feed has URLs like this:

        http:/jpg1.lapl.org/pics47/00043006.jpg
        
        The code was choking complaining about a "MissingSchema" 
        exception.
        '''
        pass

        
