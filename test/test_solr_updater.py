import os
from unittest import TestCase
import json
from mock import patch
from test.utils import DIR_FIXTURES
from harvester.solr_updater import main as solr_updater_main
from harvester.solr_updater import push_doc_to_solr, map_couch_to_solr_doc
from harvester.solr_updater import OldCollectionException
from harvester.solr_updater import CouchdbLastSeq_S3
from harvester.solr_updater import get_key_for_env
from harvester import grab_solr_index

class SolrUpdaterTestCase(TestCase):
    '''Test the solr update from couchdb changes feed'''
    def setUp(self):
        self.old_data_branch = os.environ.get('DATA_BRANCH', None)
        os.environ['DATA_BRANCH'] = 'test_branch'

    def tearDown(self):
        if self.old_data_branch:
            os.environ['DATA_BRANCH'] = self.old_data_branch
        
    @patch('solr.Solr', autospec=True)
    def test_push_doc_to_solr(self, mock_solr):
        '''Unit test calls to solr'''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        push_doc_to_solr(sdoc, mock_solr)
#        mock_solr.add.assert_called_with({'repository_name': [u'Bancroft Library'], 'url_item': u'http://ark.cdlib.org/ark:/13030/ft009nb05r', 'repository': [u'https://registry.cdlib.org/api/v1/repository/4/'], 'publisher': u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/', 'collection_name': [u'Uchida (Yoshiko) photograph collection'], 'format': u'mods', 'rights': [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'], 'collection': [u'https://registry.cdlib.org/api/v1/collection/23066/'], 'id': u'23066--http://ark.cdlib.org/ark:/13030/ft009nb05r', 'campus_name': [u'UC Berkeley'], 'reference_image_md5': u'f2610262f487f013fb96149f98990fb0', 'relation': [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://bancroft.berkeley.edu/collections/jarda.html', u'hb158005k9', u'BANC PIC 1986.059--PIC', u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://calisphere.universityofcalifornia.edu/', u'http://bancroft.berkeley.edu/'], 'title': u'Neighbor', 'identifier': [u'http://ark.cdlib.org/ark:/13030/ft009nb05r', u'Banc Pic 1986.059:124--PIC'], 'type': u'image', 'campus': [u'https://registry.cdlib.org/api/v1/campus/1/'], 'subject': [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive']})

    def test_map_couch_to_solr_no_campus(self):
        doc = json.load(
                open(DIR_FIXTURES+'/couchdb_nocampus.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertNotIn('campus', sdoc)
        self.assertNotIn('campus_url', sdoc)
        self.assertNotIn('campus_name', sdoc)
        self.assertNotIn('campus_data', sdoc)
        repo_data = ['https://registry.cdlib.org/api/v1/repository/4/::'
                     'Bancroft Library']
        self.assertEqual(sdoc['repository_data'], repo_data)
        self.assertEqual(sdoc['sort_title'], u'Neighbor')

    def test_map_couch_to_solr_nuxeo_doc(self):
        '''Test the mapping of a couch db source json doc from Nuxeo
        to a solr schema compatible doc
        '''
        doc = json.load(open(DIR_FIXTURES+'/nuxeo_couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], doc['_id'])
        self.assertEqual(sdoc['id'], '2--01db4725-3676-4c47-9bef-d93bc084827a')
        self.assertEqual(sdoc['title'], 'Gold Coast.  Chicago, Illinois, 1940')
        self.assertEqual(sdoc['alternative_title'], ['test alt title'])
        self.assertEqual(sdoc['contributor'], ['contributor1', 'contributor2'])
        self.assertEqual(sdoc['coverage'], ['place1', 'place2'])
        self.assertEqual(sdoc['creator'], ['Halberstadt, Milton'])
        self.assertEqual(sdoc['date'], ['1940'])
        self.assertEqual(sdoc['description'], ['test description'])
        # dimensions - not sure where to get this from
        self.assertNotIn('extent', sdoc)
        self.assertEqual(sdoc['format'], '1 slide :  b&w')
        self.assertEqual(sdoc['genre'], ['graphic'])
        self.assertNotIn('identifier', sdoc)        
        self.assertEqual(sdoc['language'], ['testlang'])
        self.assertEqual(sdoc['location'], 'test location')
        self.assertEqual(sdoc['provenance'], ['test provenance'])
        self.assertNotIn('publisher', sdoc)
        self.assertNotIn('relation', sdoc)
        self.assertEqual(sdoc['rights'], ['copyrighted', 'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.'])
        self.assertEqual(sdoc['rights_date'], 'test copyright date')
        self.assertEqual(sdoc['rights_holder'], ["University of California Regents", "Department of Special Collections, Shields Library, University of California, 100 N.W. Quad, Davis, CA, 95616-5292. (530) 752-1621; Fax (530) 754-5758. speccoll@ucdavis.edu"])
        self.assertEqual(sdoc['rights_note'], ['From the Milton Halberstadt Papers and Photographs , Dept. of Special Collections, General Library, University of California, Davis. The collection is the property of the Regents of the University of California; no part may be reproduced or used without permission of the Dept. of Special Collections. Permissions for use must be submitted in writing to: The Head of the Dept. of Special Collections, General Library, University of California, Davis 95616-5292'])
        self.assertEqual(sdoc['source'], 'test source')
        self.assertEqual(sdoc['structmap_text'], 'Gold Coast.  Chicago, Illinois, 1940')
        self.assertEqual(sdoc['structmap_url'], 's3://static.ucldc.cdlib.org/media_json/01db4725-3676-4c47-9bef-d93bc084827a-media.json')
        self.assertEqual(sdoc['subject'], ['subject1', 'subject2', 'subject3'])
        self.assertEqual(sdoc['temporal'], ['test temporal coverage'])
        self.assertEqual(sdoc['transcription'], 'this is a test transcription for this here object')
        self.assertEqual(sdoc['type'], 'image')
 
    def test_map_couch_to_solr_doc(self):
        '''Test the mapping of a couch db source json doc to a solr schema
        compatible doc.
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], doc['_id'])
        self.assertEqual(sdoc['id'], '23066--http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertNotIn('campus', sdoc)
        self.assertEqual(sdoc['campus_url'], [u'https://registry.cdlib.org/api/v1/campus/1/'])
        self.assertEqual(sdoc['campus_name'], [u'UC Berkeley'])
        self.assertEqual(sdoc['campus_data'],
                [u'https://registry.cdlib.org/api/v1/campus/1/::UC Berkeley'])
        self.assertNotIn('repository', sdoc)
        self.assertEqual(sdoc['repository_url'],
                         [u'https://registry.cdlib.org/api/v1/repository/4/'])
        self.assertEqual(sdoc['repository_name'], [u'Bancroft Library'])
        repo_data = ['https://registry.cdlib.org/api/v1/repository/4/::'
                     'Bancroft Library::UC Berkeley']
        self.assertEqual(sdoc['repository_data'], repo_data)
        self.assertNotIn('collection', sdoc)
        self.assertEqual(sdoc['collection_url'],
                         ['https://registry.cdlib.org/api/v1/collection/23066/'])
        self.assertEqual(sdoc['collection_name'],
                         ['Uchida (Yoshiko) photograph collection'])
        c_data = ['https://registry.cdlib.org/api/v1/collection/23066/::'
                  'Uchida (Yoshiko) photograph collection']
        self.assertEqual(sdoc['collection_data'], c_data)
        self.assertEqual(sdoc['url_item'], u'http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertEqual(sdoc['contributor'], ['contrib 1', 'contrib 2'])
        self.assertEqual(sdoc['coverage'], ['Palm Springs (Calif.)', 'San Jacinto Mountains (Calif.)', 'Tahquitz Stream', 'Tahquitz Canyon'])
        self.assertEqual(sdoc['creator'], [u'creator 1', u'creator 2'])
        self.assertEqual(sdoc['description'], [u'description 1',
                                        u'description 2', u'description 3'])
        self.assertEqual(sdoc['date'], ['between 1885-1890'])
        self.assertEqual(sdoc['language'], ['en'])
        self.assertEqual(sdoc['publisher'], [u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/']),
        self.assertEqual(sdoc['relation'], [
            u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc',
            u'http://bancroft.berkeley.edu/collections/jarda.html',
            u'hb158005k9',
            u'BANC PIC 1986.059--PIC',
            u'http://calisphere.universityofcalifornia.edu/',
            u'http://bancroft.berkeley.edu/'])
        self.assertEqual(sdoc['rights'], [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'])
        self.assertEqual(sdoc['subject'], [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive'])
        self.assertEqual(sdoc['title'], [u'Neighbor'])
        self.assertEqual(sdoc['type'], u'image')
        self.assertEqual(sdoc['format'], 'mods')
        self.assertTrue('extent' not in sdoc)
        self.assertEqual(sdoc['sort_title'], u'neighbor')

    def test_decade_facet(self):
        '''Test generation of decade facet
        Currently generated from sourceResource.date.displayDate
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['facet_decade'], set(['1880s','1890s']))
        # no "date" in sourceResource
        doc = json.load(open(DIR_FIXTURES+'/couchdb_nocampus.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['facet_decade'], set([]))

    @patch('boto.s3.connect_to_region', autospec=True)
    def test_set_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq setting'''
        self.seq_obj = CouchdbLastSeq_S3()
        self.seq_obj.last_seq = 5
        mock_boto.assert_called_with('us-west-2')
        mock_boto('us-west-2').get_bucket.assert_called_with('solr.ucldc')
        mock_boto('us-west-2').get_bucket().get_key.assert_called_with('couchdb_since/test_branch')
        mock_boto('us-west-2').get_bucket().get_key().set_contents_from_string.assert_called_with(5)

    @patch('boto.s3.connect_to_region', autospec=True)
    def test_get_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq getting'''
        self.seq_obj = CouchdbLastSeq_S3()
        x = self.seq_obj.last_seq
        mock_boto.assert_called_with('us-west-2')
        mock_boto('us-west-2').get_bucket.assert_called_with('solr.ucldc')
        mock_boto('us-west-2').get_bucket().get_key.assert_called_with('couchdb_since/test_branch')
        mock_boto('us-west-2').get_bucket().get_key().get_contents_as_string.assert_called_with()

    def test_old_collection(self):
        doc = json.load(open(DIR_FIXTURES+'/couchdb_norepo.json'))
        self.assertRaises(OldCollectionException, map_couch_to_solr_doc, doc )

    def test_get_key_for_env(self):
        '''test correct key for env'''
        self.assertEqual(get_key_for_env(), 'couchdb_since/test_branch')


class GrabSolrIndexTestCase(TestCase):
    '''Basic test for grabbing solr index. Like others, heavily mocked
    '''
    def setUp(self):
        os.environ['DIR_CODE'] = os.path.abspath('../harvester')

    def tearDown(self):
        del os.environ['DIR_CODE']

    @patch('ansible.callbacks.PlaybookRunnerCallbacks', autospec=True)
    @patch('ansible.callbacks.PlaybookCallbacks', autospec=True)
    @patch('ansible.callbacks.AggregateStats', autospec=True)
    @patch('ansible.inventory.Inventory', autospec=True)
    @patch('ansible.playbook.PlayBook', autospec=True)
    def test_grab_solr_main(self, mock_pb, mock_inv, mock_stats, mock_cb, mock_cbr):
        inventory = mock_inv.return_value
        inventory.list_hosts.return_value = ['test-host']
        grab_solr_index.main()
        mock_pb.assert_called_with(playbook=os.path.join(os.environ['DIR_CODE'], 'harvester/grab-solr-index-playbook.yml'),
                                   inventory=inventory,
                                   callbacks=mock_cb.return_value,
                                   runner_callbacks=mock_cbr.return_value,
                                   stats=mock_stats.return_value
                                   )
        self.assertEqual(mock_pb.return_value.run.called, True)
        self.assertEqual(mock_pb.return_value.run.call_count, 1)
