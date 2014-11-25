from unittest import TestCase
import json
import httpretty
from test.utils import DIR_FIXTURES
from harvester.post_processing.enrich_existing_couch_doc import akara_enrich_doc

class EnrichExistingCouchDocTestCase(TestCase):
    '''Test the enrichment of a single couchdb document.
    Want to make sure that the updated document is correct.
    '''
    @httpretty.activate
    def testEnrichDoc(self):
        httpretty.register_uri(httpretty.POST,
                'http://localhost:8889/enrich',
                body=open(DIR_FIXTURES+'/akara_response.json').read(),
                )
        indoc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        doc = akara_enrich_doc(indoc, '/select-oac-id,/dpla_mapper?mapper_type=oac_dc')
        self.assertIn('added-key', doc['sourceResource'])
        self.assertEqual(doc['sourceResource']['title'], 'changed title')
