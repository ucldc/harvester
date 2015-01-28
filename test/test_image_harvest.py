from unittest import TestCase
from collections import namedtuple
from mock import patch
from mock import MagicMock
from harvester import image_harvest

class ImageHarvestTestCase(TestCase):
    '''Test the md5 s3 image harvesting calls.....
    TODO: Increase test coverage
    '''
    report = namedtuple('Report', 's3_url, md5')
    # StashReport = namedtuple('StashReport', 'url, md5, s3_url, mime_type')

    @patch('couchdb.Server')
    @patch('md5s3stash.md5s3stash', autospec=True, return_value=report('s3 test url', 'md5 test value'))
    def test_stash_image(self, mock_stash, mock_couch):
        '''Test the stash image calls are correct'''
        doc = {'_id': 'TESTID'}
        self.assertRaises(KeyError, image_harvest.ImageHarvester().stash_image, doc)
        doc['isShownBy'] = None
        self.assertRaises(ValueError, image_harvest.ImageHarvester().stash_image, doc)
        doc['isShownBy'] = 'test local url ark:'
        ret = image_harvest.ImageHarvester().stash_image(doc)
        mock_stash.assert_called_with('http://content.cdlib.org/test local url ark:', url_auth=None, bucket_base='ucldc')
        self.assertEqual('s3 test url', ret.s3_url)
        ret = image_harvest.ImageHarvester(bucket_base='x').stash_image(doc)
        mock_stash.assert_called_with('http://content.cdlib.org/test local url ark:', url_auth=None, bucket_base='x')
        ret = image_harvest.ImageHarvester(bucket_base='x',
                           object_auth=('tstuser', 'tstpswd')).stash_image(doc)
        mock_stash.assert_called_with(
                'http://content.cdlib.org/test local url ark:',
                url_auth=('tstuser', 'tstpswd'),
                bucket_base='x')

    def test_update_doc_object(self):
        '''Test call to couchdb, right data'''
        doc = {}
        r = self.report('s3 test2 url', 'md5 test value')
        db = MagicMock()
        ret = image_harvest.ImageHarvester(cdb=db).update_doc_object(doc, r)
        self.assertEqual('md5 test value', ret)
        self.assertEqual('md5 test value', doc['object'])
        db.save.assert_called_with({'object': 'md5 test value'})



