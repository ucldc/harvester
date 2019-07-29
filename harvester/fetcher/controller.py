# -*- coding: utf-8 -*-
import os
import tempfile
import urlparse
import datetime
import uuid
import json
import codecs
import boto3
from email.mime.text import MIMEText
import logbook
from logbook import FileHandler
import dplaingestion.couch
from ..collection_registry_client import Collection
from .. import config
from .fetcher import Fetcher
from .fetcher import NoRecordsFetchedException
from .oai_fetcher import OAIFetcher
from .solr_fetcher import SolrFetcher
from .solr_fetcher import PySolrQueryFetcher
from .solr_fetcher import RequestsSolrFetcher
from .marc_fetcher import MARCFetcher
from .marc_fetcher import AlephMARCXMLFetcher
from .nuxeo_fetcher import UCLDCNuxeoFetcher
from .oac_fetcher import OAC_XML_Fetcher
from .oac_fetcher import OAC_JSON_Fetcher
from .cmis_atom_feed_fetcher import CMISAtomFeedFetcher
from .flickr_fetcher import Flickr_Fetcher
from .youtube_fetcher import YouTube_Fetcher
from .xml_fetcher import XML_Fetcher
from .emuseum_fetcher import eMuseum_Fetcher
from .ucd_json_fetcher import UCD_JSON_Fetcher
from .ia_fetcher import IA_Fetcher

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
HARVEST_TYPES = {
    'OAI': OAIFetcher,
    'OAJ': OAC_JSON_Fetcher,
    'OAC': OAC_XML_Fetcher,
    'SLR': SolrFetcher,
    'MRC': MARCFetcher,
    'NUX': UCLDCNuxeoFetcher,
    'ALX': AlephMARCXMLFetcher,
    'SFX': PySolrQueryFetcher,
    'UCB': RequestsSolrFetcher,  # Now points to more generic
    # class, accepts parameters
    # from extra data field
    'PRE': CMISAtomFeedFetcher,  # 'Preservica CMIS Atom Feed'),
    'FLK': Flickr_Fetcher,  # All public photos fetcher
    'YTB': YouTube_Fetcher,  # by playlist id, use "uploads" list
    'XML': XML_Fetcher,
    'EMS': eMuseum_Fetcher,
    'UCD': UCD_JSON_Fetcher,
    'IAR': IA_Fetcher
}


class HarvestController(object):
    '''Controller for the harvesting. Selects correct Fetcher for the given
    collection, then retrieves records for the given collection and saves to
    disk.
    TODO: produce profile file
    '''
    campus_valid = [
        'UCB', 'UCD', 'UCI', 'UCLA', 'UCM', 'UCR', 'UCSB', 'UCSC', 'UCSD',
        'UCSF', 'UCDL'
    ]
    dc_elements = [
        'title', 'creator', 'subject', 'description', 'publisher',
        'contributor', 'date', 'type', 'format', 'identifier', 'source',
        'language', 'relation', 'coverage', 'rights'
    ]
    bucket = 'ucldc-ingest'
    tmpl_s3path = 'data-fetched/{cid}/{datetime_start}/'

    def __init__(self,
                 user_email,
                 collection,
                 profile_path=None,
                 config_file=None,
                 **kwargs):
        self.user_email = user_email  # single or list
        self.collection = collection
        self.profile_path = profile_path
        self.config_file = config_file
        self._config = config.config(config_file)
        self.couch_db_name = self._config.get('couchdb_dbname', None)
        if not self.couch_db_name:
            self.couch_db_name = 'ucldc'
        self.couch_dashboard_name = self._config.get('couchdb_dashboard')
        if not self.couch_dashboard_name:
            self.couch_dashboard_name = 'dashboard'

        cls_fetcher = HARVEST_TYPES.get(self.collection.harvest_type, None)
        self.fetcher = cls_fetcher(self.collection.url_harvest,
                                   self.collection.harvest_extra_data,
                                   **kwargs)
        self.logger = logbook.Logger('HarvestController')
        self.dir_save = tempfile.mkdtemp('_' + self.collection.slug)
        self.ingest_doc_id = None
        self.ingestion_doc = None
        self.couch = None
        self.num_records = 0
        self.datetime_start = datetime.datetime.now()
        self.objset_page = 0

    @property
    def s3path(self):
        return self.tmpl_s3path.format(
            cid=self.collection.id,
            datetime_start=self.datetime_start.strftime('%Y-%m-%d-%H%M'))

    @staticmethod
    def dt_json_handler(obj):
        '''The json package cannot deal with datetimes.
        Provide this serializer for datetimes.
        '''
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(obj)

    @staticmethod
    def jsonl(objset):
        '''Return a JSONL string for a given set of python objects
        '''
        jsonl = ''
        if isinstance(objset, dict):
            objset = [objset]
        for obj in objset:
            jsonl += ''.join((json.dumps(
                obj, default=HarvestController.dt_json_handler), '\n'))
        return jsonl

    def save_objset_s3(self, objset):
        '''Save the objset to a bucket'''
        if not hasattr(self, 's3'):
            self.s3 = boto3.resource('s3')
        body = HarvestController.jsonl(objset)
        bucket = self.s3.Bucket('ucldc-ingest')
        key = ''.join((self.s3path, 'page-{}.jsonl'.format(self.objset_page)))
        self.objset_page += 1
        bucket.put_object(Body=body, Key=key)

    def save_objset(self, objset):
        '''Save an object set to disk. If it is a single object, wrap in a
        list to be uniform'''
        filename = os.path.join(self.dir_save, str(uuid.uuid4()))
        if not type(objset) == list:
            objset = [objset]
        with open(filename, 'w') as foo:
            foo.write(
                json.dumps(
                    objset, default=HarvestController.dt_json_handler))

    def create_ingest_doc(self):
        '''Create the DPLA style ingest doc in couch for this harvest session.
        Update with the current information. Status is running'''
        self.couch = dplaingestion.couch.Couch(
            config_file=self.config_file,
            dpla_db_name=self.couch_db_name,
            dashboard_db_name=self.couch_dashboard_name)
        uri_base = "http://localhost:" + self._config['akara_port']
        self.ingest_doc_id = self.couch._create_ingestion_document(
            self.collection.provider, uri_base, self.profile_path,
            self.collection.dpla_profile_obj['thresholds'])
        self.ingestion_doc = self.couch.dashboard_db[self.ingest_doc_id]
        kwargs = {
            "fetch_process/status": "running",
            "fetch_process/data_dir": self.dir_save,
            "fetch_process/start_time": datetime.datetime.now().isoformat(),
            "fetch_process/end_time": None,
            "fetch_process/error": None,
            "fetch_process/total_items": None,
            "fetch_process/total_collections": None
        }
        try:
            self.couch.update_ingestion_doc(self.ingestion_doc, **kwargs)
        except Exception as e:
            self.logger.error("Error updating ingestion doc %s in %s" %
                              (self.ingestion_doc["_id"], __name__))
            raise e
        return self.ingest_doc_id

    def update_ingest_doc(self,
                          status,
                          error_msg=None,
                          items=None,
                          num_coll=None):
        '''Update the ingest doc with status'''
        if not items:
            items = self.num_records
        if status == 'error' and not error_msg:
            raise ValueError('If status is error please add an error_msg')
        kwargs = {
            "fetch_process/status": status,
            "fetch_process/error": error_msg,
            "fetch_process/end_time": datetime.datetime.now().isoformat(),
            "fetch_process/total_items": items,
            "fetch_process/total_collections": num_coll
        }
        if not self.ingestion_doc:
            self.create_ingest_doc()
        try:
            self.couch.update_ingestion_doc(self.ingestion_doc, **kwargs)
        except Exception as e:
            self.logger.error("Error updating ingestion doc %s in %s" %
                              (self.ingestion_doc["_id"], __name__))
            raise e

    def _add_registry_data(self, obj):
        '''Add the registry based data to the harvested object.
        '''
        # get base registry URL
        url_tuple = urlparse.urlparse(self.collection.url)
        base_url = ''.join((url_tuple.scheme, '://', url_tuple.netloc))
        self.collection['@id'] = self.collection.url
        self.collection['id'] = self.collection.url.strip('/').rsplit('/',
                                                                      1)[1]
        self.collection['ingestType'] = 'collection'
        self.collection['title'] = self.collection.name
        if 'collection' in obj:
            # save before hammering
            obj['source_collection_name'] = obj['collection']
        obj['collection'] = dict(self.collection)
        campus = []
        for c in self.collection.get('campus', []):
            c.update({'@id': ''.join((base_url, c['resource_uri']))})
            campus.append(c)
        obj['collection']['campus'] = campus
        repository = []
        for r in self.collection['repository']:
            r.update({'@id': ''.join((base_url, r['resource_uri']))})
            repository.append(r)
        obj['collection']['repository'] = repository
        # in future may be more than one
        obj['collection'] = [obj['collection']]
        return obj

    def harvest(self):
        '''Harvest the collection'''
        self.logger.info(' '.join((
            'Starting harvest for:',
            str(self.user_email),
            self.collection.url,
            str(self.collection['campus']),
            str(self.collection['repository']))))
        self.num_records = 0
        next_log_n = interval = 100
        for objset in self.fetcher:
            if isinstance(objset, list):
                self.num_records += len(objset)
                # TODO: use map here
                for obj in objset:
                    self._add_registry_data(obj)
            else:
                self.num_records += 1
                self._add_registry_data(objset)
            self.save_objset(objset)
            self.save_objset_s3(objset)
            if self.num_records >= next_log_n:
                self.logger.info(' '.join((str(self.num_records),
                                           'records harvested')))
                if self.num_records < 10000 and \
                        self.num_records >= 10 * interval:
                    interval = 10 * interval
                next_log_n += interval

        if self.num_records == 0:
            raise NoRecordsFetchedException
        msg = ' '.join((str(self.num_records), 'records harvested'))
        self.logger.info(msg)
        return self.num_records


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, nargs='?', help='user email')
    parser.add_argument(
        'url_api_collection',
        type=str,
        nargs='?',
        help='URL for the collection Django tastypie api resource')
    return parser.parse_args()


def get_log_file_path(collection_slug):
    '''Get the log file name for the given collection, start time and
    environment
    '''
    log_file_dir = os.environ.get('DIR_HARVESTER_LOG',
                                  os.path.join(
                                      os.environ.get('HOME', '.'), 'log'))
    log_file_name = '-'.join(
        ('harvester', collection_slug,
         datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), '.log'))
    return os.path.join(log_file_dir, log_file_name)


def create_mimetext_msg(mail_from, mail_to, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = str(subject)
    msg['From'] = mail_from
    msg['To'] = mail_to if type(mail_to) == str else ', '.join(mail_to)
    return msg


def main(user_email,
         url_api_collection,
         log_handler=None,
         mail_handler=None,
         dir_profile='profiles',
         profile_path=None,
         config_file=None,
         **kwargs):
    '''Executes a harvest with given parameters.
    Returns the ingest_doc_id, directory harvest saved to and number of
    records.
    '''
    if not config_file:
        config_file = os.environ.get('DPLA_CONFIG_FILE', 'akara.ini')
    num_recs = -1
    my_mail_handler = None
    if not mail_handler:
        my_mail_handler = logbook.MailHandler(
            EMAIL_RETURN_ADDRESS, user_email, level='ERROR', bubble=True)
        my_mail_handler.push_application()
        mail_handler = my_mail_handler
    try:
        collection = Collection(url_api_collection)
    except Exception as e:
        msg = 'Exception in Collection {}, init {}'.format(url_api_collection,
                                                           str(e))
        logbook.error(msg)
        raise e
    if not (collection['harvest_type'] in HARVEST_TYPES):
        msg = 'Collection {} wrong type {} for harvesting. Harvest type {} \
                is not in {}'.format(url_api_collection,
                                     collection['harvest_type'],
                                     collection['harvest_type'],
                                     HARVEST_TYPES.keys())
        logbook.error(msg)
        raise ValueError(msg)
    mail_handler.subject = "Error during harvest of " + collection.url
    my_log_handler = None
    if not log_handler:  # can't init until have collection
        my_log_handler = FileHandler(get_log_file_path(collection.slug))
        my_log_handler.push_application()
    logger = logbook.Logger('HarvestMain')
    msg = 'Init harvester next. Collection:{}'.format(collection.url)
    logger.info(msg)
    # email directly
    mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email, ' '.join(
        ('Starting harvest for ', collection.slug)), msg)
    try:  # TODO: request more emails from AWS
        mail_handler.deliver(mimetext, 'mredar@gmail.com')
    except:
        pass
    logger.info('Create DPLA profile document')
    if not profile_path:
        profile_path = os.path.abspath(
            os.path.join(dir_profile, collection.id + '.pjs'))
    with codecs.open(profile_path, 'w', 'utf8') as pfoo:
        pfoo.write(collection.dpla_profile)
    logger.info('DPLA profile document : ' + profile_path)
    harvester = None
    try:
        harvester = HarvestController(
            user_email,
            collection,
            profile_path=profile_path,
            config_file=config_file,
            **kwargs)
    except Exception as e:
        import traceback
        msg = 'Exception in harvester init: type: {} TRACE:\n{}'.format(
            type(e), traceback.format_exc())
        logger.error(msg)
        raise e
    logger.info('Create ingest doc in couch')
    ingest_doc_id = harvester.create_ingest_doc()
    logger.info('Ingest DOC ID: ' + ingest_doc_id)
    logger.info('Start harvesting next')
    num_recs = harvester.harvest()
    msg = ''.join(('Finished harvest of ', collection.slug, '. ',
                   str(num_recs), ' records harvested.'))
    logger.info(msg)
    logger.debug('-- get a new harvester --')
    harvester = HarvestController(
         user_email,
         collection,
         profile_path=profile_path,
         config_file=config_file,
         **kwargs)
    harvester.ingest_doc_id = ingest_doc_id
    harvester.couch = dplaingestion.couch.Couch(
            config_file=harvester.config_file,
            dpla_db_name=harvester.couch_db_name,
            dashboard_db_name=harvester.couch_dashboard_name)
    harvester.ingestion_doc = harvester.couch.dashboard_db[ingest_doc_id]
    try:
        harvester.update_ingest_doc('complete', items=num_recs, num_coll=1)
        logger.debug('updated ingest doc!')
    except Exception as e:
        import traceback
        error_msg = ''.join(("Error while harvesting: type-> ", str(type(e)),
                             " TRACE:\n" + str(traceback.format_exc())))
        logger.error(error_msg)
        harvester.update_ingest_doc(
            'error', error_msg=error_msg, items=num_recs)
        raise e
    if my_log_handler:
        my_log_handler.pop_application()
    if my_mail_handler:
        my_mail_handler.pop_application()
    return ingest_doc_id, num_recs, harvester.dir_save, harvester


__all__ = (Fetcher, NoRecordsFetchedException, HARVEST_TYPES)

if __name__ == '__main__':
    args = parse_args()
    main(args.user_email, args.url_api_collection)

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
