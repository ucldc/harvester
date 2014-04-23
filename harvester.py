import os
import sys
import datetime
import codecs
from email.mime.text import MIMEText
import tempfile
import uuid
import json
import ConfigParser
from sickle import Sickle
import requests
import logbook
from logbook import FileHandler
import dplaingestion.couch 

EMAIL_RETURN_ADDRESS = 'mark.redar@ucop.edu'

class Collection(dict):
    '''A representation of the avram collection, as presented by the 
    tastypie api
    '''
    def __init__(self, url_api):
        self.url = url_api
        resp = requests.get(url_api)
        api_json = json.loads(resp.text)
        if not(api_json['harvest_type'] in HARVEST_TYPES):
            raise ValueError('Collection is not an OAC or OAI harvest collection')
        self.update(api_json)
        self.__dict__.update(api_json)

    def _build_contributor_list(self):
        '''Build the dpla style contributor list from the campus and
        repositories
        This will need review
        '''
        clist = []
        for campus in self.campus:
            campus_dict = dict(name=campus['slug'])
            campus_dict['@id'] = campus['resource_uri'] 
            clist.append(campus_dict)
        for repository in self.repository:
            repository_dict = dict(name=repository['slug'])
            repository_dict['@id'] = repository['resource_uri'] 
            clist.append(repository_dict)
        return clist

    @property
    def dpla_profile_obj(self):
        '''Return a json string appropriate for creating a dpla ingest profile.
        First create dictionary that is correct and then serialize'''
        profile = {}
        profile['name'] = self.slug
        profile['contributor'] = self._build_contributor_list()
        profile['enrichments_coll'] = [ '/compare_with_schema' ] 
        #TODO: add to avram
        profile['thresholds'] = {
                "added": 5000,
                "changed": 1000,
                "deleted": 1000
                },
        profile['enrichments_item'] = [
        '/select-id', 
        '/oai-to-dpla', 
        '/shred?prop=sourceResource%2Fcontributor%2CsourceResource%2Fcreator%2CsourceResource%2Fdate', 
        '/shred?prop=sourceResource%2Flanguage%2CsourceResource%2Fpublisher%2CsourceResource%2Frelation', 
        '/shred?prop=sourceResource%2Fsubject%2CsourceResource%2Ftype%2CsourceResource%2Fformat', 
        '/shred?prop=sourceResource%2Fsubject&delim=%3Cbr%3E',
        '/cleanup_value',
        '/move_date_values?prop=sourceResource%2Fsubject',
        '/move_date_values?prop=sourceResource%2Fspatial',
        '/shred?prop=sourceResource%2Fspatial&delim=--',
        '/capitalize_value',
        '/enrich_earliest_date', 
        '/enrich-subject', 
        '/enrich_date',
        '/enrich-type', 
        '/enrich-format', 
        '/contentdm_identify_object', 
        '/enrich_location', 
        '/scdl_enrich_location', 
        '/geocode', 
        '/scdl_geocode_regions',
        '/copy_prop?prop=sourceResource%2Fpublisher&to_prop=dataProvider&create=True&remove=True',
        '/cleanup_language',
        '/enrich_language',
        '/lookup?prop=sourceResource%2Flanguage%2Fname&target=sourceResource%2Flanguage%2Fname&substitution=iso639_3',
        '/lookup?prop=sourceResource%2Flanguage%2Fname&target=sourceResource%2Flanguage%2Fiso639_3&substitution=iso639_3&inverse=True',
        '/copy_prop?prop=provider%2Fname&to_prop=dataProvider&create=True&no_overwrite=True',
        '/lookup?prop=sourceResource%2Fformat&target=sourceResource%2Fformat&substitution=scdl_fix_format',
        '/set_prop?prop=sourceResource%2FstateLocatedIn&value=California',
        '/enrich_location?prop=sourceResource%2FstateLocatedIn',
        '/compare_with_schema'
    ]
        return profile

    @property
    def dpla_profile(self):
        return json.dumps(self.dpla_profile_obj)

#TODO: Each harvester must pick correct field for creating a "handle"
class Harvester(object):
    '''Base class for harvest objects.'''
    def __init__(self, url_harvest, extra_data):
        self.url = url_harvest
        self.extra_data = extra_data

    def __iter__(self):
        return self

    def next(self):
        raise NotImplementedError


class OAIHarvester(Harvester):
    '''Harvester for oai'''
    def __init__(self, url_harvest, extra_data):
        super(OAIHarvester, self).__init__(url_harvest, extra_data)
        #TODO: check extra_data?
        self.oai_client = Sickle(self.url)
        self.records = self.oai_client.ListRecords(set=extra_data, metadataPrefix='oai_dc')

    def next(self):
        '''return a record iterator? then outside layer is a controller, same for all. Records are dicts that include:
        any metadata
        campus list
        repo list
        collection name
        '''
        sickle_rec = self.records.next()
        rec = sickle_rec.metadata
        rec['handle'] = sickle_rec.header.identifier
        return rec

#TODO: handle is qdc['identifier']
class OAC_JSON_Harvester(Harvester):
    '''Harvester for oac'''
    def __init__(self, url_harvest, extra_data):
        super(OAC_JSON_Harvester, self).__init__(url_harvest, extra_data)
        self.oac_findaid_ark = self._parse_oac_findaid_ark(self.url)
        self.headers = {'content-type': 'application/json'}
        self.objset_last = False
        self.resp = requests.get(self.url, headers=self.headers)
        api_resp = self.resp.json()
        #for key in api_resp.keys():
        #    self.__dict__[key] = api_resp[key]
        self.objset_total = api_resp[u'objset_total']
        self.objset_start = api_resp['objset_start']
        self.objset_end = api_resp['objset_end']
        self.objset = api_resp['objset']
        n_objset = []
        for rec in self.objset:
            rec_orig = rec
            rec = rec['qdc']
            rec['handle'] = rec['identifier']
            rec['files'] = rec_orig['files']
            n_objset.append(rec)
        self.objset = n_objset


    def _parse_oac_findaid_ark(self, url_findaid):
        return ''.join(('ark:', url_findaid.split('ark:')[1]))

    def next(self):
        '''Point to which function we want as main'''
        return self.next_objset()

    def next_record(self):
        '''Return the next record'''
        while self.resp:
            try:
                rec = self.objset.pop()
                rec['handle'] = rec['identifier']
                return rec
            except IndexError, e:
                if self.objset_end == self.objset_total:
                    self.resp = None
                    raise StopIteration
            url_next = ''.join((self.url, '&startDoc=', unicode(self.objset_end+1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            #self.objset_total = api_resp['objset_total']
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
            n_objset = []
            for rec in self.objset:
                rec_orig = rec
                rec = rec['qdc']
                rec['handle'] = rec['identifier']
                rec['files'] = rec_orig['files']
                n_objset.append(rec)
            self.objset = n_objset

    def next_objset(self):
        '''Return records in objset batches. More efficient and makes
        sense when storing to file in DPLA type ingest'''
        if self.objset_last:
            raise StopIteration
        cur_objset = self.objset
        if self.objset_end == self.objset_total:
            self.objset_last = True
        else:
            url_next = ''.join((self.url, '&startDoc=', unicode(self.objset_end+1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
            n_objset = []
            for rec in self.objset:
                rec_orig = rec
                rec = rec['qdc']
                rec['handle'] = rec['identifier']
                rec['files'] = rec_orig['files']
                n_objset.append(rec)
            self.objset = n_objset
        return cur_objset


HARVEST_TYPES = { 'OAI': OAIHarvester,
            'OAJ': OAC_JSON_Harvester,
        }

class HarvestController(object):
    '''Controller for the harvesting. Selects correct harvester for the given 
    collection, then retrieves records for the given collection and saves to 
    disk.
    TODO: produce profile file
    '''
    campus_valid = ['UCB', 'UCD', 'UCI', 'UCLA', 'UCM', 'UCR', 'UCSB', 'UCSC', 'UCSD', 'UCSF', 'UCDL']
    dc_elements = ['title', 'creator', 'subject', 'description', 'publisher', 'contributor', 'date', 'type', 'format', 'identifier', 'source', 'language', 'relation', 'coverage', 'rights']

    def __init__(self, user_email, collection, profile_path=None, config_file='akara.ini'):
        self.user_email = user_email
        self.collection = collection
        self.profile_path = profile_path
        self.config_file = config_file
        self.config_dpla = ConfigParser.ConfigParser()
        self.config_dpla.readfp(open(config_file))
        self.couch_db_name = self.config_dpla.get("CouchDb", "ItemDatabase")
        if not self.couch_db_name:
            self.couch_db_name = 'ucldc'
        self.couch_dashboard_name = self.config_dpla.get("CouchDb", "DashboardDatabase")
        if not self.couch_dashboard_name:
            self.couch_dashboard_name = 'dashboard'

        self.harvester = HARVEST_TYPES.get(self.collection.harvest_type, None)(self.collection.url_harvest, self.collection.harvest_extra_data)
        self.logger = logbook.Logger('HarvestController')
        self.dir_save = tempfile.mkdtemp('_' + self.collection.slug)
        self.ingest_doc_id = None
        self.ingestion_doc = None
        self.couch = None
        self.num_records = 0

    def create_id(self, identifier):
        '''Create an id that is good for items. Take campus, repo and collection
        name to form prefix to individual item id. Ensures unique ids in db,
        in case any local ids are identical.
        May do something smarter when known GUIDs (arks, doi, etc) are in use.
        Takes a list of possible identifiers and creates a string id.
        '''
        if not isinstance(identifier, list):
            raise TypeError('Identifier field should be a list')
        campusStr = '-'.join([x['slug'] for x in self.collection.campus])
        repoStr = '-'.join([x['slug'] for x in self.collection.repository])
        sID = '-'.join((campusStr, repoStr, self.collection.slug, identifier[0].replace(' ', '-')))
        return sID

    def save_objset(self, objset):
        '''Save an object set to disk'''
        filename = os.path.join(self.dir_save, str(uuid.uuid4()))
        with open(filename, 'w') as foo:
            foo.write(json.dumps(objset))

    def create_ingest_doc(self):
        '''Create the DPLA style ingest doc in couch for this harvest session.
        Update with the current information. Status is running'''
        self.couch = dplaingestion.couch.Couch(config_file=self.config_file,
                dpla_db_name = self.couch_db_name,
                dashboard_db_name = self.couch_dashboard_name
            )
        uri_base = "http://localhost:" + self.config_dpla.get("Akara", "Port")
        self.ingest_doc_id = self.couch._create_ingestion_document(self.collection.slug, uri_base, self.profile_path, self.collection.dpla_profile_obj['thresholds'])
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
        except Exception, e:
            self.logger.error("Error updating ingestion doc %s in %s" %
                         (self.ingestion_doc["_id"], __name__))
            raise e
        return self.ingest_doc_id

    def update_ingest_doc(self, status, error_msg=None, items=None, num_coll=None):
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
        except Exception, e:
            self.logger.error("Error updating ingestion doc %s in %s" %
                     (self.ingestion_doc["_id"], __name__))
            raise e

    def harvest(self):
        '''Harvest the collection'''
        self.logger.info(' '.join(('Starting harvest for:', self.user_email, self.collection.url, str(self.collection['campus']), str(self.collection['repository']))))
        self.num_records = 0
        next_log_n = interval = 100
        for objset in self.harvester:
            if isinstance(objset, list):
                self.num_records += len(objset)
            else:
                self.num_records += 1
            self.save_objset(objset)
            if self.num_records >= next_log_n:
                self.logger.info(' '.join((str(self.num_records), 'records harvested')))
                if self.num_records  < 10000 and self.num_records  >= 10*interval:
                    interval = 10*interval
                next_log_n += interval
        self.logger.info(' '.join((str(self.num_records), 'records harvested')))
        return self.num_records 

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, nargs='?', help='user email')
    parser.add_argument('url_api_collection', type=str, nargs='?',
            help='URL for the collection Django tastypie api resource')
    return parser.parse_args()

def get_log_file_path(collection_slug):
    '''Get the log file name for the given collection, start time and environment
    '''
    log_file_dir = os.environ.get('DIR_HARVESTER_LOG', os.path.join(os.environ.get('HOME', '.'), 'log'))
    log_file_name = 'harvester-' + collection_slug + '-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '.log'
    return os.path.join(log_file_dir, log_file_name)

def create_mimetext_msg(mail_from, mail_to, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = str(subject)
    msg['From'] = mail_from
    msg['To'] = mail_to
    return msg

def main(user_email, url_api_collection, log_handler=None, mail_handler=None, dir_profile='profiles', profile_path=None, config_file='akara.ini'):
    '''Executes a harvest with given parameters.
    Returns the ingest_doc_id, directory harvest saved to and number of records.
    '''
    num_recs = -1
    if not mail_handler:
        mail_handler = logbook.MailHandler(EMAIL_RETURN_ADDRESS, user_email, level=logbook.ERROR) 
    try:
        collection = Collection(url_api_collection)
    except Exception, e:
        mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email, 'Collection init failed for '+url_api_collection, ' '.join(("Exception in Collection", url_api_collection, " init", str(e))))
        mail_handler.deliver(mimetext, user_email)
        raise e
    mail_handler.subject = "Error during harvest of " + collection.url
    if not log_handler:
        log_handler = FileHandler(get_log_file_path(collection.slug))
    with log_handler.applicationbound():
        with mail_handler.applicationbound():
            logger = logbook.Logger('HarvestMain')
            logger.info('Init harvester next')
            msg = ' '.join(('ARGS:', user_email, collection.url))
            logger.info(msg)
            #email directly
            mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email, ' '.join(('Starting harvest for ', collection.slug)), msg)
            mail_handler.deliver(mimetext, user_email)
            logger.info('Create DPLA profile document')
            if not profile_path:
                profile_path = os.path.abspath(os.path.join(dir_profile, collection.slug+'.pjs'))
            with codecs.open(profile_path, 'w', 'utf8') as pfoo:
                pfoo.write(collection.dpla_profile)
            logger.info('DPLA profile document : '+profile_path)
            harvester = None
            try:
                harvester = HarvestController(user_email, collection, profile_path=profile_path, config_file=config_file)
            except Exception, e:
                import traceback
                error_msg = "Exception in harvester init: type-> "+str(type(e))+ " TRACE:\n"+str(traceback.format_exc())
                #logger.error(' '.join(("Exception in harvester init", unicode(e))))
                logger.error(error_msg)
                raise e
            logger.info('Create ingest doc in couch')
            ingest_doc_id = harvester.create_ingest_doc()
            logger.info('Ingest DOC ID: '+ ingest_doc_id)
            logger.info('Start harvesting next')
            try:
                num_recs = harvester.harvest()
                msg = ''.join(('Finished harvest of ', collection.slug, '. ', str(num_recs), ' records harvested.'))
                harvester.update_ingest_doc('complete', items=num_recs, num_coll=1)
                logger.info(msg)
                #email directly
                mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email, ' '.join(('Finished harvest for ', collection.slug)), msg)
                mail_handler.deliver(mimetext, user_email)
            except Exception, e:
                import traceback
                error_msg = "Error while harvesting: type-> "+str(type(e))+ " TRACE:\n"+str(traceback.format_exc())
                logger.error(error_msg)
                harvester.update_ingest_doc('error', error_msg=error_msg, items=num_recs)
    return ingest_doc_id, num_recs, harvester.dir_save

if __name__=='__main__':
    args = parse_args()
    main(args.user_email, args.url_api_collection)
