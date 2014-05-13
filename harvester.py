import os
import sys
import datetime
import codecs
from email.mime.text import MIMEText
import tempfile
import uuid
import json
import ConfigParser
from collections import defaultdict
from xml.etree import ElementTree as ET
from sickle import Sickle
import requests
import logbook
from logbook import FileHandler
import dplaingestion.couch 

EMAIL_RETURN_ADDRESS = 'mark.redar@ucop.edu'
CONTENT_SERVER = 'http://content.cdlib.org/'

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
        if not self.enrichments_item:
            raise Exception("NO ITEM ENRICHMENTS FOR COLLECTION. ENRICHING WILL FAIL!")
        profile = {}
        profile['name'] = self.slug
        profile['contributor'] = self._build_contributor_list()
        profile['enrichments_coll'] = [ '/compare_with_schema' ] 
        profile['thresholds'] = {
                "added": 5000,
                "changed": 1000,
                "deleted": 1000
                }
        profile['enrichments_item'] = [ s.strip() for s in self.enrichments_item.split(',')]
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
        self.logger = logbook.Logger('HarvesterBaseClass')

    def __iter__(self):
        return self

    def next(self):
        '''This returns a set of json objects.
        Set can be only one json object.
        '''
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

class BunchDict(dict):
    def __init__(self, **kwds):
        dict.__init__(self, kwds)
        self.__dict__ = self

class OAC_XML_Harvester(Harvester):
    '''Harvester for the OAC
    The results are returned in 3 groups, image, text and website.
    Image and text are the ones we care about.
    '''
    def __init__(self, url_harvest, extra_data, docsPerPage=100):
        super(OAC_XML_Harvester, self).__init__(url_harvest, extra_data)
        self.logger = logbook.Logger('HarvesterOACXML')
        self.docsPerPage = docsPerPage
        self.url = self.url + '&docsPerPage=' + str(self.docsPerPage)
        self.currentDoc = 0
        self.currentGroup = ('image', 0)
        #this will be used to track counts for the 3 groups
        self.groups = dict(
                image = BunchDict(),
                text = BunchDict(),
                website = BunchDict()
            )
        resp = requests.get(self.url)
        crossQueryResult = ET.fromstring(resp.text)
        facet_type_tab = crossQueryResult.find('facet')
        #set total number of hits across the 3 groups
        self.totalDocs = int(facet_type_tab.attrib['totalDocs'])
        if self.totalDocs <= 0:
            raise ValueError(self.url + ' yields no results')
        self.totalGroups = int(facet_type_tab.attrib['totalGroups'])
        self._update_groups(facet_type_tab.findall('group'))
        #set initial group to image, if there are any
        for key, hitgroup in self.groups.items():
            hitgroup.end = 0
            hitgroup.currentDoc = 1
        if self.groups['image'].start != 0:
            self.currentGroup = 'image'
        elif self.groups['text'].start != 0:
            self.currentGroup = 'text'
        else:
            self.currentGroup = None

    def _get_doc_ark(self, docHit):
        '''Return the object's ark from the xml etree docHit'''
        ids = docHit.find('meta').findall('identifier')
        ark = None
        for i in ids:
            split = i.text.split('ark:')
            if len(split) > 1:
                ark = ''.join(('ark:', split[1]))
        return ark

    def _docHits_to_objset(self, docHits):
        '''Transform the ElementTree docHits into a python object list
        ready to be jsonfied

        Any elements with sub-elements (just seem to be the snippets in the 
        relation field for the findaid ark) the innertext of the element + 
        subelements becomes the value of the output.

        '''
        objset = []
        for d in docHits:
            obj = defaultdict(list)
            meta = d.find('meta')
            ark = self._get_doc_ark(d)
            for t in meta:
                if t.tag == 'google_analytics_tracking_code':
                    continue
                data=''
                if t.tag == 'reference-image':
                    #ref image & thumbnail have data in attribs
                    # return as dicts
                    try:
                        x = int(t.attrib['X'])
                    except ValueError:
                        x = 0
                    try:
                        y = int(t.attrib['Y'])
                    except ValueError:
                        y = 0
                    data = { 'X': x,
                            'Y': y,
                            'src': ''.join((CONTENT_SERVER, t.attrib['src'])).replace('//','/').replace('/', '//', 1)
                            }
                elif t.tag == 'thumbnail':
                    try:
                        x = int(t.attrib['X'])
                    except ValueError:
                        x = 0
                    try:
                        y = int(t.attrib['Y'])
                    except ValueError:
                        y = 0
                    data = {'X': x,
                            'Y': y,
                            'src': ''.join((CONTENT_SERVER, '/', ark, '/thumbnail')).replace('//','/').replace('/', '//', 1)
                            }
                elif len(list(t)) > 0:
                    #<snippet> tag breaks up text for findaid <relation>
                    for innertext in t.itertext():
                        data = ''.join((data, innertext.strip()))
                else:
                    data = t.text
                obj[t.tag].append(data)
                if t.tag == 'identifier':
                    obj['handle'].append(t.text)
            for key, value in obj.items():
                if len(value) == 1:
                    obj[key] = value[0] # de list non-duplicate  tags
            objset.append(obj)
        return objset

    def _update_groups(self, group_elements):
        '''Update the internal data structure with the count from
        the current ElementTree results for the search groups
        '''
        for g in group_elements:
            v = g.attrib['value']
            self.groups[v].total = int(g.attrib['totalDocs'])
            self.groups[v].start = int(g.attrib['startDoc'])
            self.groups[v].end = int(g.attrib['endDoc'])

    def next(self):
        '''Get the next page of search results
        '''
        if self.currentDoc >= self.totalDocs:
            raise StopIteration
        if self.currentGroup == 'image':
            if self.groups['image']['end'] == self.groups['image']['total']:
                self.currentGroup = 'text'
                if self.groups['text']['total'] == 0:
                    raise StopIteration
        self._url_current =  ''.join((self.url, '&startDoc=', str(self.groups[self.currentGroup]['currentDoc']), '&group=', self.currentGroup))
        self.logger.debug(''.join(('===== Current URL-->', self._url_current)))
        resp = requests.get(self._url_current)
        crossQueryResult = ET.fromstring(resp.text)
        facet_type_tab = crossQueryResult.find('facet')
        self._update_groups(facet_type_tab.findall('group'))
        objset = self._docHits_to_objset(facet_type_tab.findall('./group/docHit'))
        self.currentDoc += len(objset)
        self.groups[self.currentGroup]['currentDoc'] += len(objset)
        self.logger.debug('++++++++++++++ curDoc'+str(self.currentDoc))
        return objset

#TODO: handle is qdc['identifier']
class OAC_JSON_Harvester(Harvester):
    '''Harvester for oac, using the JSON objset interface
    This is being deprecated in favor of the xml interface'''
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
            'OAC': OAC_XML_Harvester,
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
        n = 0
        for objset in self.harvester:
            n += 1
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

        msg = ' '.join((str(self.num_records), 'records harvested'))
        self.logger.info(msg)
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
