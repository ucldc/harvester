import os
from unittest import TestCase
import json
from mock import patch
from test.utils import DIR_FIXTURES
from harvester.solr_updater import main as solr_updater_main
from harvester.solr_updater import push_doc_to_solr, map_couch_to_solr_doc
from harvester.solr_updater import set_couchdb_last_seq, get_couchdb_last_seq
from harvester import grab_solr_index

class SolrUpdaterTestCase(TestCase):
    '''Test the solr update from couchdb changes feed'''
    @patch('solr.Solr', autospec=True)
    def test_push_doc_to_solr(self, mock_solr):
        '''Unit test calls to solr'''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        push_doc_to_solr(sdoc, mock_solr)
        mock_solr.add.assert_called_with({'repository_name': [u'Bancroft Library'], 'url_item': u'http://ark.cdlib.org/ark:/13030/ft009nb05r', 'repository': [u'https://registry.cdlib.org/api/v1/repository/4/'], 'publisher': u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/', 'collection_name': [u'Uchida (Yoshiko) photograph collection'], 'format': u'mods', 'rights': [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'], 'collection': [u'https://registry.cdlib.org/api/v1/collection/23066/'], 'id': u'23066--http://ark.cdlib.org/ark:/13030/ft009nb05r', 'campus_name': [u'UC Berkeley'], 'reference_image_md5': u'f2610262f487f013fb96149f98990fb0', 'relation': [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://bancroft.berkeley.edu/collections/jarda.html', u'hb158005k9', u'BANC PIC 1986.059--PIC', u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://calisphere.universityofcalifornia.edu/', u'http://bancroft.berkeley.edu/'], 'title': u'Neighbor', 'identifier': [u'http://ark.cdlib.org/ark:/13030/ft009nb05r', u'Banc Pic 1986.059:124--PIC'], 'type': u'image', 'campus': [u'https://registry.cdlib.org/api/v1/campus/1/'], 'subject': [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive']})

    def test_map_couch_to_solr_doc(self):
        '''Test the mapping of a couch db source json doc to a solr schema
        compatible doc.
        '''
        doc = json.load(open(DIR_FIXTURES+'/couchdb_doc.json'))
        sdoc = map_couch_to_solr_doc(doc)
        self.assertEqual(sdoc['id'], doc['_id'])
        self.assertEqual(sdoc['id'], '23066--http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertEqual(sdoc['campus'], [u'https://registry.cdlib.org/api/v1/campus/1/'])
        self.assertEqual(sdoc['campus_name'], [u'UC Berkeley'])
        self.assertEqual(sdoc['repository'],
                         [u'https://registry.cdlib.org/api/v1/repository/4/'])
        self.assertEqual(sdoc['repository_name'], [u'Bancroft Library'])
        self.assertEqual(sdoc['collection'],
                         ['https://registry.cdlib.org/api/v1/collection/23066/'])
        self.assertEqual(sdoc['collection_name'],
                         ['Uchida (Yoshiko) photograph collection'])
        self.assertEqual(sdoc['url_item'], u'http://ark.cdlib.org/ark:/13030/ft009nb05r')
        self.assertTrue('contributor' not in sdoc)
        self.assertTrue('coverage' not in sdoc)
        self.assertTrue('creator' not in sdoc)
        self.assertTrue('description' not in sdoc)
        self.assertTrue('date' not in sdoc)
        self.assertTrue('language' not in sdoc)
        self.assertEqual(sdoc['publisher'], u'The Bancroft Library, University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu, URL: http://bancroft.berkeley.edu/'),
        self.assertEqual(sdoc['relation'], [u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://bancroft.berkeley.edu/collections/jarda.html', u'hb158005k9', u'BANC PIC 1986.059--PIC', u'http://www.oac.cdlib.org/findaid/ark:/13030/ft6k4007pc', u'http://calisphere.universityofcalifornia.edu/', u'http://bancroft.berkeley.edu/'])
        self.assertEqual(sdoc['rights'], [u'Transmission or reproduction of materials protected by copyright beyond that allowed by fair use requires the written permission of the copyright owners. Works not in the public domain cannot be commercially exploited without permission of the copyright owner. Responsibility for any use rests exclusively with the user.', u'The Bancroft Library--assigned', u'All requests to reproduce, publish, quote from, or otherwise use collection materials must be submitted in writing to the Head of Public Services, The Bancroft Library, University of California, Berkeley 94720-6000. See: http://bancroft.berkeley.edu/reference/permissions.html', u'University of California, Berkeley, Berkeley, CA 94720-6000, Phone: (510) 642-6481, Fax: (510) 642-7589, Email: bancref@library.berkeley.edu'])
        self.assertEqual(sdoc['subject'], [u'Yoshiko Uchida photograph collection', u'Japanese American Relocation Digital Archive'])
        self.assertEqual(sdoc['title'], u'Neighbor')
        self.assertEqual(sdoc['type'], u'image')
        self.assertEqual(sdoc['format'], 'mods')
        self.assertTrue('extent' not in sdoc)

    @patch('boto.connect_s3', autospec=True)
    def test_set_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq setting'''
        set_couchdb_last_seq(5)
        mock_boto.assert_called_with()
        mock_boto().get_bucket.assert_called_with('ucldc')
        mock_boto().get_bucket().get_key.assert_called_with('couchdb_last_seq')
        mock_boto().get_bucket().get_key().set_contents_from_string.assert_called_with(5)

    @patch('boto.connect_s3', autospec=True)
    def test_get_couchdb_last_seq(self, mock_boto):
        '''Mock test s3 last_seq getting'''
        get_couchdb_last_seq()
        mock_boto.assert_called_with()
        mock_boto().get_bucket.assert_called_with('ucldc')
        mock_boto().get_bucket().get_key.assert_called_with('couchdb_last_seq')
        mock_boto().get_bucket().get_key().get_contents_as_string.assert_called_with()


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



