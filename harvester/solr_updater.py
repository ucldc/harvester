#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import urllib
import argparse
import re
import hashlib
import json
from collections import defaultdict
import requests
import boto
from solr import Solr, SolrException
from harvester.couchdb_init import get_couchdb
from facet_decade import facet_decade
import datetime

reload(sys)
sys.setdefaultencoding('utf8')
 
S3_BUCKET = 'solr.ucldc'

RE_ARK_FINDER = re.compile('(ark:/\d\d\d\d\d/[^/|\s]*)')
RE_ALPHANUMSPACE = re.compile(r'[^0-9A-Za-z\s]*') #\W include "_" as does A-z

def dict_for_data_field(field_src, data, field_dest):
    '''For a given field_src  in the data, create a dictionary to
    update the field_dest with.
    If no values, make the dict {}, this will avoid empty data values
    '''
    ddict = {}
    items_not_blank = []
    items = data.get(field_src)
    if items:
        items = dejson(field_src, items) #this handles list, scalar
        #remove blanks
        if isinstance(items, basestring):
            if items:
                items_not_blank = items
        else:
            for i in items:
                if i:
                    items_not_blank.append(i)
    if items_not_blank:
        ddict = {field_dest: items_not_blank }
    return ddict

COUCHDOC_TO_SOLR_MAPPING = {
    '_id'       : lambda d: {'harvest_id_s': d['_id']},
    'object'   : lambda d: {'reference_image_md5': d['object']},
    'isShownAt': lambda d: {'url_item': d['isShownAt']},
}

COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING = {
    'alternativeTitle'   : lambda d: dict_for_data_field('alternativeTitle', d,
                                'alternative_title'),
    'contributor' : lambda d: dict_for_data_field('contributor', d, 'contributor'),
    'coverage'    : lambda d: dict_for_data_field('coverage', d, 'coverage'),
    'spatial'     : lambda d: dict_for_data_field('spatial', d, 'coverage'),
    'creator'     : lambda d: dict_for_data_field('creator', d, 'creator'),
    'date'        : lambda d:  map_date(d),
    'description' : lambda d: dict_for_data_field('description', d,
                              'description'),
    'extent'      : lambda d: dict_for_data_field('extent', d, 'extent'),
    'format'      : lambda d: dict_for_data_field('format', d, 'format'),
    'genre'       : lambda d: dict_for_data_field('genre', d, 'genre'),
    'identifier'  : lambda d: dict_for_data_field('identifier', d, 'identifier'),
    'language'    : lambda d: {'language': [l.get('name', l.get('iso639_3', None)) if isinstance(l, dict) else l for l in d['language']]},
    'publisher'   : lambda d: dict_for_data_field('publisher', d, 'publisher'),
    'relation'    : lambda d: dict_for_data_field('relation', d, 'relation'),
    'rights'      : lambda d: dict_for_data_field('rights', d, 'rights'),
    'subject'     : lambda d: {'subject': [s['name'] if isinstance(s, dict) else dejson('subject', s) for s in d['subject']]},
    'temporalCoverage'    : lambda d: dict_for_data_field('temporalCoverage', d,
        'temporal'),
    'title'       : lambda d: dict_for_data_field('title', d, 'title'),
    'type'        : lambda d: dict_for_data_field('type', d, 'type'),
}

COUCHDOC_ORIGINAL_RECORD_TO_SOLR_MAPPING = {
#    'location'        : lambda d: {'location': d.get('location', None)},
    'provenance'      : lambda d: dict_for_data_field('provenance', d,
                        'provenance'),
    'dateCopyrighted' : lambda d: dict_for_data_field('dateCopyrighted', d,
                        'rights_date'),
    'rightsHolder'    : lambda d: dict_for_data_field('rightsHolder', d,
                        'rights_holder'),
    'rightsNote'      : lambda d: dict_for_data_field('rightsNote', d,
                        'rights_note'),
    'source'          : lambda d: dict_for_data_field('source', d, 'source'),
    'structmap_text'  : lambda d: dict_for_data_field('structmap_text', d,
                        'structmap_text'),
    'structmap_url'   : lambda d: dict_for_data_field('structmap_url', d,
                        'structmap_url'),
    'transcription'   : lambda d: dict_for_data_field('transcription', d,
                        'transcription'),
}

def getjobj(data):
    jobj = None
    try:
        jobj = json.loads(data)
    except ValueError:
        pass
    return jobj

def unpack_if_json(field, data):
    '''If data is a valid json object, attempt to flatten data to a string.
    All the json data at this point should be a scalar or a dict
    In general if there is a field 'name' that is the data
    '''
    flatdata = data
    j = getjobj(data)
    if j:
        try:
            flatdata = j.get('name', flatdata)
        except AttributeError: #not a dict
            pass
    return flatdata

def dejson(field, data):
    '''de-jsonfy the data.
    For valid json strings, unpack in sensible way?
    '''
    dejson_data = data
    if not dejson_data:
        return dejson_data
    if isinstance(data, list):
        dejson_data = []
        for d in data:
            flatdata = dejson(field, d)
            dejson_data.append(flatdata)
    elif isinstance(data, dict):
        #already parsed json?
        flatdata = data.get('item', data.get('name', data.get('text', None)))
        if flatdata:
            dejson_data = flatdata
    else:
        dejson_data = unpack_if_json(field, data)
    return dejson_data

class UTCtz(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=0)
    def tzname(self, dt):
        return 'GMT'
    def dst(self, dt):
        return timedelta(0)
UTC = UTCtz()

def make_datetime(dstring):
    '''make a datetime from a valid date string
    Right now formats are YYYY or YYYY-MM-DD
    '''
    dt = None
    if not dstring:
        return dt
    try:
        dint = int(dstring)
        dt = datetime.datetime(dint, 1, 1)
    except ValueError:
        pass
    except TypeError, e:
        print('Date type err DATA:{} ERROR:{}'.format(dstring, e))
    try:
        strfmt = '%Y-%m-%d'
        dt = datetime.datetime.strptime(dstring, strfmt)
    except ValueError:
        pass
    except TypeError, e:
        print('Date type err in strptime:{} {}'.format(dstring, e))
    # add UTC as timezone, solrpy looks for tzinfo
    if dt:
        dt = datetime.datetime(dt.year, dt.month, dt.day, tzinfo=UTC)
    return dt

def get_dates_from_date_obj(date_obj):
    '''Return list of display date, start dates, end dates'''
    if isinstance(date_obj, dict):
        date_start = make_datetime(date_obj.get('begin', None))
        date_end = make_datetime(date_obj.get('end', None))
        dates = (date_obj.get('displayDate', None),
                date_start,
                date_end
                )
        return dates
    elif isinstance(date_obj, basestring):
        return date_obj, None, None
    else:
        return None, None, None

def map_date(d):
    date_map = {}
    date_source = d.get('date', None)
    dates = []
    start_date = end_date = None
    dates_start = []
    dates_end = []
    if date_source:
        if isinstance(date_source, dict):
            try:
                displayDate, dt_start, dt_end = get_dates_from_date_obj(date_source)
                dates.append(displayDate)
                if dt_start:
                     dates_start.append(dt_start)
                if dt_end:
                    dates_end.append(dt_end)
            except KeyError:
                pass
        else: #should be list
            for dt in date_source:
                displayDate, dt_start, dt_end = get_dates_from_date_obj(dt)
                dates.append(displayDate)
                if dt_start:
                     dates_start.append(dt_start)
                if dt_end:
                    dates_end.append(dt_end)

    date_map['date'] = dates

    dates_start = sorted(dates_start)
    dates_end = sorted(dates_end)
    if len(dates_start):
    	start_date = dates_start[0]
    if len(dates_end):
    	end_date = dates_end[0]
    # fill in start_date == end_date if only one exists
    start_date = end_date if not start_date else start_date
    end_date = start_date if not end_date else end_date
    if start_date:
        #add timezone as UTC, for solrpy (uses astimzone())
        date_map['sort_date_start'] = start_date
        date_map['sort_date_end'] = end_date

    return date_map

def find_ark_in_identifiers(doc):
    identifiers = doc['sourceResource'].get('identifier', None)
    if identifiers:
        for identifier in identifiers:
            match = RE_ARK_FINDER.search(identifier)
            if match:
                return match.group(0)
    return None

def uuid_if_nuxeo(doc):
    collection = doc['originalRecord']['collection'][0]
    harvest_type = collection['harvest_type']
    if harvest_type == 'NUX':
        return doc['originalRecord'].get('uid', None)
    return None

def ucsd_ark(doc):
    #is this UCSD?
    campus = None
    ark = None
    collection = doc['originalRecord']['collection'][0]
    campus_list = collection.get('campus', None)
    if campus_list:
        campus = campus_list [0]['@id']
    if campus == "https://registry.cdlib.org/api/v1/campus/6/":
        #UCSD get ark id
        ark_frag = doc['originalRecord'].get('id', None)
        if ark_frag:
            ark = 'ark:/20775/' + ark_frag
    return ark

def ucla_ark(doc):
    '''UCLA ARKs are buried in a mods field in originalRecord:

    "mods_recordInfo_recordIdentifier_mlt": "21198-zz002b1833",
    "mods_recordInfo_recordIdentifier_s": "21198-zz002b1833",
    "mods_recordInfo_recordIdentifier_t": "21198-zz002b1833",
    
    If one is found, safe to assume UCLA & make the ARK
    '''
    ark = None
    id_fields =("mods_recordInfo_recordIdentifier_mlt",
                 "mods_recordInfo_recordIdentifier_s",
                 "mods_recordInfo_recordIdentifier_t")
    for f in id_fields:
        try:
            mangled_ark = doc['originalRecord'][f]
            naan, arkid = mangled_ark.split('-') #could fail?
            ark = '/'.join(('ark:', naan, arkid))
            break
        except KeyError:
            pass
    return ark

def get_solr_id(couch_doc):
    ''' Extract a good ID to use in the solr index.
    see : https://github.com/ucldc/ucldc-docs/wiki/pretty_id
    arks are always pulled if found, gets first.
    Some institutions have known ark framents, arks are constructed
    for these.
    Nuxeo objects retain their UUID
    All other objects the couchdb _id is md5 sum
    '''
    # look in sourceResoure.identifier for an ARK if found return it
    solr_id = find_ark_in_identifiers(couch_doc)
    # no ARK in identifiers. See if is a nuxeo object
    if not solr_id:
        solr_id = uuid_if_nuxeo(couch_doc)
    if not solr_id:
        solr_id = ucsd_ark(couch_doc)
    if not solr_id:
        solr_id = ucla_ark(couch_doc)
    if not solr_id:
        # no recognized special id, just has couchdb id
        hash_id = hashlib.md5()
        hash_id.update(couch_doc['_id'])
        solr_id = hash_id.hexdigest()
    return solr_id

def has_required_fields(doc):
    '''Check the couchdb doc has required fields'''
    if 'sourceResource' not in doc:
        raise KeyError(
            '+++++OMITTED: Doc:{0} has no sourceResource.'.format(doc['_id']))
    if 'title' not in doc['sourceResource']:
        raise KeyError('+++++OMITTED: Doc:{0} has no title.'.format(doc['_id']))
    if 'image' == doc['sourceResource'].get('type', '').lower():
        collection = doc.get('originalRecord', {}).get(
                'collection', [{'harvest_type':'NONE'}])[0]
        if collection['harvest_type'] != 'NUX':
            #if doesnt have a reference_image_md5, reject
            if 'object' not in doc:
                raise KeyError(
                '+++++OMITTED: Doc:{0} is image type with no harvested image.'.format(doc['_id']))
    return True

def add_slash(url):
    '''Add slash to url is it is not there.'''
    return os.path.join(url, '')

class OldCollectionException(Exception):
    pass

def map_registry_data(collections):
    '''Map the collections data to corresponding data fields in the solr doc
    '''
    collection_urls = []
    collection_names = []
    collection_datas = []
    collection_sort_datas = []
    repository_urls = []
    repository_names = []
    repository_datas = []
    campus_urls = campus_names = campus_datas = None
    for collection in collections: #can have multiple collections
        collection_urls.append(add_slash(collection['@id']))
        collection_names.append(collection['name'])
        collection_datas.append('::'.join((add_slash(collection['@id']),
            collection['name'])))
        scd = get_sort_collection_data_string(collection)
        collection_sort_datas.append(scd)
        if 'campus' in collection:
            campus_urls = []
            campus_names = []
            campus_datas = []
            campuses = collection['campus']
            campus_urls.extend([add_slash(campus['@id']) for campus in campuses])
            campus_names.extend([campus['name'] for c in campuses])
            campus_datas.extend(['::'.join((add_slash(campus['@id']),
                                            campus['name']))
                for campus in campuses])
        try:
            repositories = collection['repository']
        except KeyError:
            raise OldCollectionException
        repository_urls.extend([add_slash(repo['@id']) for repo in repositories])
        repository_names.extend([repo['name'] for repo in repositories])
        repo_datas = []
        for repo in repositories:
            repo_data = '::'.join((add_slash(repo['@id']), repo['name']))
            if 'campus' in repo and len(repo['campus']):
                repo_data = '::'.join((add_slash(repo['@id']), repo['name'],
                            repo['campus'][0]['name']))
            repo_datas.append(repo_data)
        repository_datas.extend(repo_datas)
    registry_dict = dict(collection_url = collection_urls,
                collection_name = collection_names,
                collection_data = collection_datas,
                sort_collection_data = collection_sort_datas,
                repository_url = repository_urls,
                repository_name = repository_names,
                repository_data = repository_datas,
                )
    if campus_urls:
        registry_dict.update({'campus_url': campus_urls,
                'campus_name': campus_names,
                'campus_data': campus_datas
            }
        )
    return registry_dict
                                    

def get_facet_decades(date):
    '''Return set of decade string for given date structure.
    date is a dict with a "displayDate" key.
    '''
    if isinstance(date, dict):
        facet_decades = facet_decade(date.get('displayDate', ''))
    facet_decade_set = set() #don't repeat values
    for decade in facet_decades:
        facet_decade_set.add(decade)
    return facet_decade_set

def normalize_sort_field(sort_field, default_missing='~title unknown',
        missing_equivalents=['title unknown']):
    sort_field = sort_field.lower()
    #remove punctuation
    sort_field = RE_ALPHANUMSPACE.sub('', sort_field)
    words = sort_field.split()
    if words:
        if words[0] in ('the', 'a', 'an'):
            sort_field = ' '.join(words[1:])
    if not sort_field or sort_field in missing_equivalents:
        sort_field = default_missing
    return sort_field

def get_sort_collection_data_string(collection):
    '''Return the string form of the collection data.
    sort_collection_data ->
    [sort_collection_name::collection_name::collection_url, <>,<>]
    '''
    sort_name = normalize_sort_field(collection['name'], 
            missing_equivalents=[])
    sort_string = ':'.join((sort_name,
                            collection['name'],
                            collection['@id']))
    return sort_string

def add_sort_title(couch_doc, solr_doc):
    '''Add a sort title to the solr doc'''
    sort_title = couch_doc['sourceResource']['title'][0]
    if couch_doc['originalRecord'].has_key('sort-title'): #OAC mostly
        sort_obj = couch_doc['originalRecord']['sort-title']
        if isinstance(sort_obj, list):
            sort_obj = sort_obj[0]
            if isinstance(sort_obj, dict):
                sort_title = sort_obj.get('text',
                        couch_doc['sourceResource']['title'][0])
            else:
                sort_title = sort_obj
        else: #assume flat string
            sort_title = sort_obj
    sort_title = normalize_sort_field(sort_title)
    solr_doc['sort_title'] = sort_title

def fill_in_title(couch_doc):
    '''if title has no entries, set to ['Title unknown']
    '''
    if not 'sourceResource' in couch_doc:
        raise KeyError("ERROR: KeyError - NO SOURCE RESOURCE in DOC:{}".format(couch_doc['_id']))
    if not couch_doc['sourceResource'].get('title', None):
        couch_doc['sourceResource']['title'] = ['Title unknown']
    elif not couch_doc['sourceResource'].get('title'): # empty string?
        couch_doc['sourceResource']['title'] = ['Title unknown']
    return couch_doc

def add_facet_decade(couch_doc, solr_doc):
    '''Add the facet_decade field to the solr_doc dictionary'''
    solr_doc['facet_decade'] = set()
    if 'date' in couch_doc['sourceResource']:
        date_field = couch_doc['sourceResource']['date']
        if isinstance(date_field, list):
            for date in date_field:
                try:
                    facet_decades = get_facet_decades(date)
                    solr_doc['facet_decade'] = facet_decades
                except AttributeError, e:
                    print('Attr Error for facet_decades in doc:{} ERROR:{}'.format(
                            couch_doc['_id'],e))
        else:
            try:
                facet_decades = get_facet_decades(date_field)
                solr_doc['facet_decade'] = facet_decades
            except AttributeError, e:
                print('Attr Error for doc:{} ERROR:{}'.format(couch_doc['_id'],e))

def map_couch_to_solr_doc(doc):
    '''Return a json document suitable for updating the solr index
    how to make schema aware mapping?'''
    solr_doc = {}
    for p in doc.keys():
        if p in COUCHDOC_TO_SOLR_MAPPING:
            solr_doc.update(COUCHDOC_TO_SOLR_MAPPING[p](doc))
    reg_data_dict = map_registry_data(doc['originalRecord']['collection'])
    solr_doc.update(reg_data_dict)
    sourceResource = doc['sourceResource']
    for p in sourceResource.keys():
        if p in COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING:
            try:
                solr_doc.update(COUCHDOC_SRC_RESOURCE_TO_SOLR_MAPPING[p](sourceResource))
            except TypeError, e:
                print('TypeError for doc {} on sourceResource {}'.format(doc['_id'], p))
                raise e
    originalRecord = doc['originalRecord']
    for p in originalRecord.keys():
        if p in COUCHDOC_ORIGINAL_RECORD_TO_SOLR_MAPPING:
            try:
                solr_doc.update(COUCHDOC_ORIGINAL_RECORD_TO_SOLR_MAPPING[p](originalRecord))
            except TypeError, e:
                print('TypeError for doc {} on originalRecord {}'.format(doc['_id'], p))
                raise e
    add_sort_title(doc, solr_doc)
    add_facet_decade(doc, solr_doc)
    solr_doc['id'] = get_solr_id(doc)
    return solr_doc


def push_doc_to_solr(solr_doc, solr_db):
    '''Push one couch doc to solr'''
    try:
        solr_db.add(solr_doc)
        print "+++ ADDED: {} +++ {}".format(solr_doc['id'], solr_doc['harvest_id_s'])
    except SolrException, e:
        print "ERROR for {} : {} {} {}".format(solr_doc['id'], e, solr_doc['collection_url'], solr_doc['harvest_id_s'])
        if not e.httpcode == 400:
            raise e
    return solr_doc

def get_key_for_env():
    '''Get key based on DATA_BRANCH env var'''
    if 'DATA_BRANCH' not in os.environ:
        raise ValueError('Please set DATA_BRANCH environment variable')
    return ''.join(('couchdb_since/', os.environ['DATA_BRANCH']))

class CouchdbLastSeq_S3(object):
    '''store the last seq for only delta updates.
    '''
    def __init__(self):
        #self.conn = boto.connect_s3()
        self.conn = boto.s3.connect_to_region('us-west-2')
        self.bucket =  self.conn.get_bucket(S3_BUCKET)
        self.key =  self.bucket.get_key(get_key_for_env())
        if not self.key:
            self.key = boto.s3.key.Key(self.bucket)
            self.key.key = get_key_for_env()

    @property
    def last_seq(self):
        return int(self.key.get_contents_as_string())

    @last_seq.setter
    def last_seq(self, value):
        '''value should be last_seq from couchdb _changes'''
        self.key.set_contents_from_string(value) 

def main(url_couchdb=None, dbname=None, url_solr=None, all_docs=False, since=None):
    '''Use the _changes feed with a "since" parameter to only catch new 
    changes to docs. The _changes feed will only have the *last* event on
    a document and does not retain intermediate changes.
    Setting the "since" to 0 will result in getting a _changes record for 
    each document, essentially dumping the db to solr
    '''
    print('Solr update PID: {}'.format(os.getpid()))
    sys.stdout.flush() # put pd
    db = get_couchdb(url=url_couchdb, dbname=dbname)
    s3_seq_cache = CouchdbLastSeq_S3()
    if all_docs:
        since = '0'
    if not since:
        since = s3_seq_cache.last_seq
    print('Attempt to connect to {0} - db:{1}'.format(url_couchdb, dbname))
    print('Getting changes since:{}'.format(since))
    sys.stdout.flush() # put pd
    db = get_couchdb(url=url_couchdb, dbname=dbname)
    changes = db.changes(since=since)
    previous_since = since
    last_since = int(changes['last_seq']) #get new last_since for changes feed
    results = changes['results']
    n_up = n_design = n_delete = 0
    solr_db = Solr(url_solr)
    start_time = datetime.datetime.now()
    for row in results:
        cur_id = row['id']
        if '_design' in cur_id:
            n_design += 1
            print("Skip {0}".format(cur_id))
            continue
        if row.get('deleted', False):
            #need to get the solr doc for this couch
            #TODO: test for this?
            resp = solr_db.select(q=''.join(('harvest_id_s:"', cur_id, '"')))
            if resp.numFound == 1:
                sdoc = resp.results[0]
            	print('====DELETING: {0} -- {1}'.format(cur_id, sdoc['id']))
            	solr_db.delete(id=sdoc['id'])
            	n_delete += 1
            else:
                print("-----DELETION of {} - FOUND {} docs".format(cur_id, resp.numFound))
        else:
            doc = db.get(cur_id)
            try:
                doc = fill_in_title(doc)
                has_required_fields(doc)
            except KeyError, e:
                print(e.message)
                continue
            try:
                try:
                    solr_doc = map_couch_to_solr_doc(doc)
                except OldCollectionException:
                    print('---- ERROR: OLD COLLECTION FOR:{}'.format(cur_id))
                    continue
                solr_doc = push_doc_to_solr(solr_doc, solr_db=solr_db)
            except TypeError, e:
                print('TypeError for {0} : {1}'.format(cur_id, e))
                continue
        n_up += 1
        if n_up % 1000 == 0:
            elapsed_time = datetime.datetime.now() - start_time
            print "Updated {} so far in {}".format(n_up, elapsed_time)
    solr_db.commit() #commit updates
    if not all_docs:
        s3_seq_cache.last_seq = last_since
    print("UPDATED {0} DOCUMENTS. DELETED:{1}".format(n_up, n_delete))
    print("PREVIOUS SINCE:{0}".format(previous_since))
    print("LAST SINCE:{0}".format(last_since))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='update a solr instance from the couchdb doc store')
    parser.add_argument('url_couchdb',
                        help='URL to couchdb (http://127.0.0.1:5984)')
    parser.add_argument('dbname', help='Couchdb database name')
    parser.add_argument('url_solr', help='URL to writeable solr instance')
    parser.add_argument('--since',
            help='Since parameter for update. Defaults to value stored in S3')
    parser.add_argument('--all_docs', action='store_true',
            help=''.join(('Harvest all couchdb docs. Safest bet. ',
                          'Will not set last sequence in s3')))

    args = parser.parse_args()
    print('Warning: this may take some time')
    main(url_couchdb=args.url_couchdb, dbname=args.dbname,
         url_solr=args.url_solr,
         all_docs=args.all_docs,
         since=args.since)
