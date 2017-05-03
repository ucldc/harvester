import os
from unittest import TestCase
import json
import re
from mypretty import httpretty
# import httpretty
from mock import patch
from test.utils import DIR_FIXTURES
from harvester.config import config
from harvester.post_processing.enrich_existing_couch_doc import akara_enrich_doc
from harvester.post_processing.enrich_existing_couch_doc import main

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

    @patch('harvester.post_processing.enrich_existing_couch_doc.akara_enrich_doc')
    @httpretty.activate
    def testMain(self, mock_enrich_doc):
        ''' main in enrich_existing_couchd_doc takes a doc _id and
        the enrichment chain to run. It then downloads the doc, submits it
        for enrichment and then saves the resulting document.
        '''
        conf = config()
        self.url_couch_base = conf['couchdb_url']
        self.cdb = conf['couchdb_dbname']
        url_couchdb = os.path.join(self.url_couch_base, self.cdb)
        httpretty.register_uri(httpretty.HEAD,
                url_couchdb,
                body='',
                content_length='0',
                content_type='text/plain; charset=utf-8',
                connection='close',
                server='CouchDB/1.5.0 (Erlang OTP/R16B03)',
                cache_control='must-revalidate',
                date='Mon, 24 Nov 2014 21:30:38 GMT'
                )
        url_doc = os.path.join(url_couchdb,
                '5112--http%3A%2F%2Fark.cdlib.org%2Fark%3A%2F13030%2Fkt7580382j')
        doc_returned = open(DIR_FIXTURES+'/couchdb_doc.json').read()
        httpretty.register_uri(httpretty.GET,
                url_doc,
                body=doc_returned,
                etag="2U5BW2TDDX9EHZJOO0DNE29D1",
                content_type='application/json',
                connection='close',
                )
        httpretty.register_uri(httpretty.PUT,
                url_doc,
                status=201,
                body='{"ok":true, "id":"5112--http://ark.cdlib.org/ark:/13030/kt7580382j", "rev":"123456789"}',
                content_type='application/json',
                etag="123456789",
                connection='close',
                )
        httpretty.register_uri(httpretty.POST,
                'http://localhost:8889/enrich',
                body=open(DIR_FIXTURES+'/akara_response.json').read(),
                )
        mock_enrich_doc.return_value = json.loads(doc_returned)
        main('5112--http://ark.cdlib.org/ark:/13030/kt7580382j',
            '/select-oac-id,dpla_mapper?mapper_type=oac_dc')
        mock_enrich_doc.assert_called_with(json.loads(doc_returned),
                '/select-oac-id,dpla_mapper?mapper_type=oac_dc', 8889)
