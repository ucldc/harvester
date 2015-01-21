from unittest import TestCase
import json
from test.utils import DIR_FIXTURES

from harvester.post_processing import dedupe_sourceresource

class DeduperTestCase(TestCase):
    def setUp(self):
        self.path_to_test_doc = DIR_FIXTURES+'/couchdb_doc_with_dups.json'
        # reference to check against, as doc will be modified by func
        self.reference_doc = json.loads(
                open(self.path_to_test_doc).read())

    def test_dedupe(self):
        ''' Test that the de-duplication function returns a dedupe'd
        document
        '''
        doc_with_dups = json.loads(
                open(self.path_to_test_doc).read())
        self.assertEqual(len(doc_with_dups['sourceResource']['relation']), 11)
        self.assertEqual(len(doc_with_dups['sourceResource']['subject']), 7)
        new_doc = dedupe_sourceresource.dedupe_sourceresource(doc_with_dups)
        # make sure other stuff didn't change
        self.assertEqual(self.reference_doc['originalRecord'],
                new_doc['originalRecord'])

        self.assertEqual(self.reference_doc['_id'],
                new_doc['_id'])
        self.assertEqual(self.reference_doc['id'],
                new_doc['id'])
        self.assertEqual(self.reference_doc['object'],
                new_doc['object'])
        self.assertEqual(self.reference_doc['isShownAt'],
                new_doc['isShownAt'])
        self.assertNotEqual(self.reference_doc['sourceResource'],
                new_doc['sourceResource'])
        self.assertEqual(len(new_doc['sourceResource']['relation']), 6)
        self.assertEqual(new_doc['sourceResource']['relation'], 
            [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc',
                u'http://bancroft.berkeley.edu/collections/jarda.html',
                u'hb158005k9',
                u'BANC PIC 1986.059--PIC',
                u'http://calisphere.universityofcalifornia.edu/',
                u'http://bancroft.berkeley.edu/']
            )
        self.assertEqual(len(new_doc['sourceResource']['subject']), 2)
        self.assertEqual(new_doc['sourceResource']['subject'],
            [{u'name': u'Yoshiko Uchida photograph collection'},
                {u'name': u'Japanese American Relocation Digital Archive'}]
            )
