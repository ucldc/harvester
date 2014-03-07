import os
import sys
import datetime
import codecs
from email.mime.text import MIMEText
import tempfile
import uuid
import json
from sickle import Sickle
import requests
import logbook
from logbook import FileHandler
from dplaingestion.couch import Couch

EMAIL_RETURN_ADDRESS = 'mark.redar@ucop.edu'

class Collection(dict):
    '''A representation of the avram collection, as presented by the 
    tastypie api
    '''
    def __init__(self, url_api):
        self.url = url_api
        resp = requests.get(url_api)
        api_json = json.loads(resp.text)
        if api_json['url_oac']:
            api_json['harvest_type'] = 'OAC'
            api_json['url_harvest'] = api_json['url_oac']
            api_json['extra_data'] = ''
        elif api_json['url_oai']:
            api_json['harvest_type'] = 'OAI'
            api_json['url_harvest'] = api_json['url_oai']
            api_json['extra_data'] = api_json['oai_set_spec']
        else:
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
        self.oai_client = Sickle(url_harvest)
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
        return rec

class OACHarvester(Harvester):
    '''Harvester for oac'''
    def __init__(self, url_harvest, extra_data):
        super(OACHarvester, self).__init__(url_harvest, extra_data)
        self.url_harvest = url_harvest
        self.oac_findaid_ark = self._parse_oac_findaid_ark(self.url_harvest)
        self.headers = {'content-type': 'application/json'}
        self.objset_last = False
        self.resp = requests.get(self.url_harvest, headers=self.headers)
        api_resp = self.resp.json()
        #for key in api_resp.keys():
        #    self.__dict__[key] = api_resp[key]
        self.objset_total = api_resp[u'objset_total']
        self.objset_start = api_resp['objset_start']
        self.objset_end = api_resp['objset_end']
        self.objset = api_resp['objset']

    def _parse_oac_findaid_ark(self, url_findaid):
        return ''.join(('ark:', url_findaid.split('ark:')[1]))

    def next(self):
        '''Point to which function we want as main'''
        return self.next_objset()

    def next_record(self):
        '''Return the next record'''
        while self.resp:
            try:
                obj = self.objset.pop()
                return obj['qdc'] #self.objset.pop()
            except IndexError, e:
                if self.objset_end == self.objset_total:
                    self.resp = None
                    raise StopIteration
            url_next = ''.join((self.url_harvest, '&startDoc=', unicode(self.objset_end+1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            #self.objset_total = api_resp['objset_total']
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']

    def next_objset(self):
        '''Return records in objset batches. More efficient and makes
        sense when storing to file in DPLA type ingest'''
        if self.objset_last:
            raise StopIteration
        cur_objset = self.objset
        if self.objset_end == self.objset_total:
            self.objset_last = True
        else:
            url_next = ''.join((self.url_harvest, '&startDoc=', unicode(self.objset_end+1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
        return cur_objset


class HarvestController(object):
    '''Controller for the harvesting. Selects correct harvester for the given 
    collection, then retrieves records for the given collection and saves to 
    disk.
    TODO: produce profile file
    '''
    campus_valid = ['UCB', 'UCD', 'UCI', 'UCLA', 'UCM', 'UCR', 'UCSB', 'UCSC', 'UCSD', 'UCSF', 'UCDL']
    harvest_types = { 'OAI': OAIHarvester,
            'OAC': OACHarvester,
        }
    dc_elements = ['title', 'creator', 'subject', 'description', 'publisher', 'contributor', 'date', 'type', 'format', 'identifier', 'source', 'language', 'relation', 'coverage', 'rights']

    def __init__(self, user_email, collection):
        self.user_email = user_email
        self.collection = collection
        self.harvester = self.harvest_types.get(self.collection.harvest_type, None)(self.collection.url_harvest, self.collection.extra_data)
        self.logger = logbook.Logger('HarvestController')
        self.dir_save = tempfile.mkdtemp('_' + self.collection.name)

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

    def harvest(self):
        '''Harvest the collection'''
        self.logger.info(' '.join(('Starting harvest for:', self.user_email, self.collection.url, str(self.collection['campus']), str(self.collection['repository']))))
        n = 0
        next_log_n = interval = 100
        for objset in self.harvester:
            if isinstance(objset, list):
                n += len(objset)
            else:
                n += 1
            self.save_objset(objset)
            if n >= next_log_n:
                self.logger.info(' '.join((str(n), 'records harvested')))
                if n < 10000 and n >= 10*interval:
                    interval = 10*interval
                next_log_n += interval
        self.logger.info(' '.join((str(n), 'records harvested')))
        return n

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

def main(log_handler=None, mail_handler=None, dir_profile='profiles'):
    args = parse_args()
    if not mail_handler:
        mail_handler = logbook.MailHandler(EMAIL_RETURN_ADDRESS, args.user_email, level=logbook.ERROR) 
    try:
        collection = Collection(args.url_api_collection)
    except Exception, e:
        mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, args.user_email, 'Collection init failed for '+args.url_api_collection, ' '.join(("Exception in Collection", args.url_api_collection, " init", str(e))))
        mail_handler.deliver(mimetext, args.user_email)
        raise e
    mail_handler.subject = "Error during harvest of " + collection.url
    if not log_handler:
        log_handler = FileHandler(get_log_file_path(collection.slug))
    with log_handler.applicationbound():
        with mail_handler.applicationbound():
            logger = logbook.Logger('HarvestMain')
            logger.info('Init harvester next')
            msg = ' '.join(('ARGS:', args.user_email, collection.url))
            logger.info(msg)
            #email directly
            mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, args.user_email, ' '.join(('Starting harvest for ', collection.slug)), msg)
            mail_handler.deliver(mimetext, args.user_email)
            harvester = None
            try:
                harvester = HarvestController(args.user_email, collection)
            except Exception, e:
                logger.error(' '.join(("Exception in harvester init", str(e))))
                raise e
            logger.info('Create DPLA profile document')

            profile_path = os.path.abspath(os.path.join(dir_profile, collection.slug+'.pjs'))
            with codecs.open(profile_path, 'w', 'utf8') as pfoo:
                pfoo.write(collection.dpla_profile)


            logger.info('Start harvesting next')
            try:
                num_recs = harvester.harvest()
                msg = ''.join(('Finished harvest of ', collection.slug, '. ', str(num_recs), ' records harvested.'))
                logger.info(msg)
                #email directly
                mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, args.user_email, ' '.join(('Finished harvest for ', collection.slug)), msg)
                mail_handler.deliver(mimetext, args.user_email)
            except Exception, e:
                import traceback
                logger.error("Error while harvesting: type-> "+str(type(e))+ " TRACE:\n"+str(traceback.format_exc()))

if __name__=='__main__':
    main()
