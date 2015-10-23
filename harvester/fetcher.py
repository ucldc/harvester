import os
import sys
import datetime
import time
import codecs
from email.mime.text import MIMEText
import tempfile
import uuid
import json
from collections import defaultdict
from xml.etree import ElementTree as ET
import urlparse
import urllib
import tempfile
import re
from sickle import Sickle
from sickle.models import Record as SickleDCRecord
from sickle.utils import xml_to_dict
import urllib
import requests
import logbook
from logbook import FileHandler
import solr
import pysolr
from collection_registry_client import Collection
import config
from urlparse import parse_qs
from pymarc import MARCReader
import pymarc
import dplaingestion.couch
import pynux.utils
from requests.packages.urllib3.exceptions import DecodeError
import boto
from deepharvest.deepharvest_nuxeo import DeepHarvestNuxeo

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
CONTENT_SERVER = 'http://content.cdlib.org/'
STRUCTMAP_S3_BUCKET = 'static.ucldc.cdlib.org/media_json'
NUXEO_MEDIUM_IMAGE_URL_FORMAT = "https://nuxeo.cdlib.org/Nuxeo/nxpicsfile/default/{}/Medium:content/"
NUXEO_S3_THUMB_URL_FORMAT = "https://s3.amazonaws.com/static.ucldc.cdlib.org/ucldc-nuxeo-thumb-media/{}"

class NoRecordsFetchedException(Exception):
    pass

class Fetcher(object):
    '''Base class for harvest objects.'''
    def __init__(self, url_harvest, extra_data):
        self.url = url_harvest
        self.extra_data = extra_data
        self.logger = logbook.Logger('FetcherBaseClass')

    def __iter__(self):
        return self

    def next(self):
        '''This returns a set of json objects.
        Set can be only one json object.
        '''
        raise NotImplementedError

def etree_to_dict(t):
    d = {t.tag : map(etree_to_dict, t.iterchildren())}
    d.update(('@' + k, v) for k, v in t.attrib.iteritems())
    d['text'] = t.text
    return d

class SickleDIDLRecord(SickleDCRecord):
    '''Extend the Sickle Record to handle oai didl xml.
    Fills in data for the didl specific values

    After Record's __init__ runs, the self.metadata contains keys for the 
    following DIDL data: DIDLInfo, Resource, Item, Component, Statement, 
    Descriptor
    DIDLInfo contains created date for the data feed - drop
    Statement wraps the dc metadata

    Only the Resource & Component have unique data in them

    '''
    def __init__(self, record_element, strip_ns=True):
        super(SickleDIDLRecord, self).__init__(record_element,
                                                strip_ns=strip_ns)
        #need to grab the didl components here
        if not self.deleted:
            didl = self.xml.find('.//{urn:mpeg:mpeg21:2002:02-DIDL-NS}DIDL')
            didls = didl.findall('.//{urn:mpeg:mpeg21:2002:02-DIDL-NS}*')
            for element in didls:
                tag = re.sub(r'\{.*\}', '', element.tag)
                self.metadata[tag] = etree_to_dict(element)


class OAIFetcher(Fetcher):
    '''Fetcher for oai'''
    def __init__(self, url_harvest, extra_data):
        super(OAIFetcher, self).__init__(url_harvest, extra_data)
        # TODO: check extra_data?
        self.oai_client = Sickle(self.url)
        self._metadataPrefix = 'oai_dc'
        # ensure not cached in module?
        self.oai_client.class_mapping['ListRecords'] = SickleDCRecord
        self.oai_client.class_mapping['GetRecord'] = SickleDCRecord
        if extra_data: # extra data is set spec
            if 'set' in extra_data:
                params = parse_qs(extra_data)
                self._set = params['set'][0]
                self._metadataPrefix = params.get('metadataPrefix', ['oai_dc'])[0]
            else:
                self._set = extra_data
            #if metadataPrefix=didl, use didlRecord for parsing
            if self._metadataPrefix.lower() == 'didl':
                self.oai_client.class_mapping['ListRecords'] = SickleDIDLRecord
                self.oai_client.class_mapping['GetRecord'] = SickleDIDLRecord
            self.records = self.oai_client.ListRecords(
                                    metadataPrefix=self._metadataPrefix,
                                                    set=self._set)
        else:
            self.records = self.oai_client.ListRecords(
                                    metadataPrefix=self._metadataPrefix)


    def next(self):
        '''return a record iterator? then outside layer is a controller,
        same for all. Records are dicts that include:
        any metadata
        campus list
        repo list
        collection name
        '''
        while True:
            sickle_rec = self.records.next()
            if not sickle_rec.deleted:
                break #good record to harvest, don't do deleted
                      # update process looks for deletions
        rec = sickle_rec.metadata
        rec['datestamp'] = sickle_rec.header.datestamp
        rec['id'] = sickle_rec.header.identifier
        return rec


class SolrFetcher(Fetcher):
    def __init__(self, url_harvest, query, **query_params):
        super(SolrFetcher, self).__init__(url_harvest, query)
        self.solr = solr.Solr(url_harvest)  # , debug=True)
        self.query = query
        self.resp = self.solr.select(self.query)
        self.numFound = self.resp.numFound
        self.index = 0

    def next(self):
        if self.index < len(self.resp.results):
            self.index += 1
            return self.resp.results[self.index-1]
        self.index = 1
        self.resp = self.resp.next_batch()
        if not len(self.resp.results):
            raise StopIteration
        return self.resp.results[self.index-1]


class PySolrFetcher(Fetcher):
    def __init__(self, url_harvest, query, **query_params):
        super(PySolrFetcher, self).__init__(url_harvest, query)
        print "URL: {}".format(url_harvest)
        self.solr = pysolr.Solr(url_harvest, timeout=1)
        self.queryParams = {'q':query, 'sort':'id asc', 'wt':'json',
                'cursorMark':'*'}
        print "self.queryParams = {}".format(self.queryParams)
        self.get_next_results()
        print "self.results dir:{}".format(dir(self.results))
        print "self.results keys:{}".format(self.results.keys())
        self.numFound = self.results['response'].get('numFound')
        self.index = 0
        self.iter = self.results.__iter__()

    def set_select_path(self):
        '''Set the encoded path to send to Solr'''
        queryParams_encoded = pysolr.safe_urlencode(self.queryParams)
        self.selectPath = 'select?{}'.format(queryParams_encoded)

    def get_next_results(self):
        self.set_select_path()
        resp = self.solr._send_request('get', path=self.selectPath)
        self.results = self.solr.decoder.decode(resp)
        self.nextCursorMark = self.results.get('nextCursorMark')

    def next(self):
        try:
            next_result = self.iter.next()
            self.index += 1
            return next_result
        except StopIteration:
            if self.index >= self.numFound:
                raise StopIteration
        self.queryParams['cursorMark'] = self.nextCursorMark
        self.get_next_results()
        if self.nextCursorMark == self.queryParams['cursorMark']:
            print "CURSOR MARKS THE SAME:::{}".format(self.nextCursorMark)
            if self.index >= self.numFound:
                raise StopIteration
        self.iter = self.results.__iter__()


class MARCFetcher(Fetcher):
    '''Harvest a MARC FILE. Can be local or at a URL'''
    def __init__(self, url_harvest, extra_data):
        '''Grab file and copy to local temp file'''
        super(MARCFetcher, self).__init__(url_harvest, extra_data)
        self.url_marc_file = url_harvest
        self.marc_file = tempfile.TemporaryFile()
        self.marc_file.write(urllib.urlopen(self.url_marc_file).read())
        self.marc_file.seek(0)
        self.marc_reader = MARCReader(self.marc_file,
                                      to_unicode=True,
                                      utf8_handling='replace')

    def next(self):
        '''Return MARC record by record to the controller'''
        return self.marc_reader.next().as_dict()


class NuxeoFetcher(Fetcher):
    '''Harvest a Nuxeo FILE. Can be local or at a URL'''
    def __init__(self, url_harvest, extra_data, conf_pynux={}):
        '''
        uses pynux (https://github.com/ucldc/pynux) to grab objects from
        the Nuxeo API

        api url is set from url_harvest, overriding pynuxrc config and
        passed in conf.

        the pynux config file should have user & password
        and X-NXDocumemtProperties values filled in.
        '''
        super(NuxeoFetcher, self).__init__(url_harvest, extra_data)
        self._url = url_harvest
        self._path = extra_data
        self._nx = pynux.utils.Nuxeo(conf=conf_pynux)
        self._nx.conf['api'] = self._url
        self._structmap_bucket = STRUCTMAP_S3_BUCKET

        # get harvestable child objects
        self._dh = DeepHarvestNuxeo(self._path, '', conf_pynux=conf_pynux)
        self._dh.nx.conf['api'] = self._url

        self._children = iter(self._dh.fetch_objects())

    def _get_structmap_url(self, bucket, obj_key):
        '''Get structmap_url property for object'''
        structmap_url = "s3://{0}/{1}{2}".format(bucket, obj_key, '-media.json') # get this from somewhere else?
        return structmap_url

    def _get_structmap_text(self, structmap_url):
        '''
           Get structmap_text for object. This is all the words from 'label' in the json.
           See https://github.com/ucldc/ucldc-docs/wiki/media.json 
        '''
        structmap_text = ""
        
        bucketpath = self._structmap_bucket.strip("/")
        bucketbase = bucketpath.split("/")[0]
        parts = urlparse.urlsplit(structmap_url)

        # get contents of <nuxeo_id>-media.json file
        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucketbase)
        key = bucket.get_key(parts.path)
        if not key: # media_json hasn't been harvested yet for this record
            self.logger.error(
                'Media json at: {} missing.'.format(parts.path))
            return structmap_text
        mediajson = key.get_contents_as_string()            
        mediajson_dict = json.loads(mediajson)

        # concatenate all of the words from 'label' in the json
        labels = []
        labels.append(mediajson_dict['label'])
        if 'structMap' in mediajson_dict:
            labels.extend([sm['label'] for sm in mediajson_dict['structMap']])
        structmap_text = ' '.join(labels)
        return structmap_text

    def _get_isShownBy(self, nuxeo_metadata):
        '''
            Get isShownBy value for object
            1) if object has image at parent level, use this
            2) if component(s) have image, use first one we can find 
            3) if object has PDF at parent level, use image stashed on S3
            4) return None 
        '''
        is_shown_by = None 
        uid = nuxeo_metadata['uid']
        self.logger.info("About to get isShownBy for uid {}".format(uid))

        # 1) if object has image at parent level, use this
        if self._has_image(nuxeo_metadata):
            self.logger.info("Nuxeo doc with uid {} has an image at the parent level".format(uid))
            is_shown_by = NUXEO_MEDIUM_IMAGE_URL_FORMAT.format(nuxeo_metadata['uid'])
            self.logger.info("is_shown_by: {}".format(is_shown_by))
            return is_shown_by

        # 2) if component(s) have image, use first one we can find
        first_image_component_uid = self._get_first_image_component(nuxeo_metadata)
        self.logger.info("first_image_component_uid: {}".format(first_image_component_uid))
        if first_image_component_uid:
            self.logger.info("Nuxeo doc with uid {} has an image at the component level".format(uid))
            is_shown_by = NUXEO_MEDIUM_IMAGE_URL_FORMAT.format(first_image_component_uid) 
            self.logger.info("is_shown_by: {}".format(is_shown_by))
            return is_shown_by

        # 3) if object has PDF at parent level, use image stashed on S3
        if self._has_s3_thumbnail(nuxeo_metadata):
            self.logger.info("Nuxeo doc with uid {} has a thumbnail for parent file (probably PDF) stashed on S3".format(uid))
            is_shown_by = NUXEO_S3_THUMB_URL_FORMAT.format(nuxeo_metadata['uid'])
            self.logger.info("is_shown_by: {}".format(is_shown_by))
            return is_shown_by
 
        # 4) return None
        self.logger.info("Could not find any image for Nuxeo doc with uid {}! Returning None".format(uid))
        return is_shown_by
           
    def _has_image(self, metadata):
        ''' based on json metadata, determine whether or not this Nuxeo doc has an image file associated '''

        if metadata['type'] != "SampleCustomPicture":
            return False

        properties = metadata['properties']
        file_content = properties.get('file:content')
        if file_content and 'data' in file_content:
            return True
        else:
            return False 

    def _has_s3_thumbnail(self, metadata):
        ''' based on json metadata, determine whether or not this Nuxeo doc is PDF (or other non-image)
            that will have thumb image stashed on S3 for it '''
        if metadata['type'] != "CustomFile":
            return False

        properties = metadata['properties']
        file_content = properties.get('file:content')
        if file_content and 'data' in file_content:
            return True
        else:
            return False

    def _get_first_image_component(self, parent_metadata):
        ''' get first image component we can find '''
        component_uid = None

        path = urllib.quote(parent_metadata['path'])
        children = self._nx.children(path)
        for child in children:
            child_metadata = self._nx.get_metadata(uid=child['uid'])
            if self._has_image(child_metadata):
                component_uid = child_metadata['uid']
                break

        return component_uid

    def next(self):
        '''Return Nuxeo record by record to the controller'''
        doc = self._children.next()
        self.metadata = self._nx.get_metadata(uid=doc['uid'])
        self.structmap_url = self._get_structmap_url(self._structmap_bucket,
                                            doc['uid'])
        self.metadata['structmap_url'] = self.structmap_url 
        self.metadata['structmap_text'] = self._get_structmap_text(self.structmap_url) 
        self.metadata['isShownBy'] = self._get_isShownBy(self.metadata)

        return self.metadata

class UCLDCNuxeoFetcher(NuxeoFetcher):
    '''A nuxeo fetcher that verifies headers required for UCLDC metadata
    from the UCLDC Nuxeo instance.
    Essentially, this checks that the X-NXDocumentProperties is correct
    for the UCLDC
    '''
    def __init__(self, url_harvest, extra_data, conf_pynux={}):
        '''Check that required UCLDC properties in conf setting'''
        super(UCLDCNuxeoFetcher, self).__init__(url_harvest,
                                                extra_data, conf_pynux)
        assert('dublincore' in self._nx.conf['X-NXDocumentProperties'])
        assert('ucldc_schema' in self._nx.conf['X-NXDocumentProperties'])
        assert('picture' in self._nx.conf['X-NXDocumentProperties'])


class BunchDict(dict):
    def __init__(self, **kwds):
        dict.__init__(self, kwds)
        self.__dict__ = self


class OAC_XML_Fetcher(Fetcher):
    '''Fetcher for the OAC
    The results are returned in 3 groups, image, text and website.
    Image and text are the ones we care about.
    '''
    def __init__(self, url_harvest, extra_data, docsPerPage=100):
        super(OAC_XML_Fetcher, self).__init__(url_harvest, extra_data)
        self.logger = logbook.Logger('FetcherOACXML')
        self.docsPerPage = docsPerPage
        self.url = self.url + '&docsPerPage=' + str(self.docsPerPage)
        self._url_current = self.url
        self.currentDoc = 0
        self.currentGroup = ('image', 0)
        # this will be used to track counts for the 3 groups
        self.groups = dict(
            image=BunchDict(),
            text=BunchDict(),
            website=BunchDict()
        )
        facet_type_tab = self._get_next_result_set()
        # set total number of hits across the 3 groups
        self.totalDocs = int(facet_type_tab.attrib['totalDocs'])
        if self.totalDocs <= 0:
            raise ValueError(self.url + ' yields no results')
        self.totalGroups = int(facet_type_tab.attrib['totalGroups'])
        self._update_groups(facet_type_tab.findall('group'))
        # set initial group to image, if there are any
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
            if i.attrib.get('q', None) != 'local':
                try:
                    split = i.text.split('ark:')
                except AttributeError:
                    continue
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
                data = ''
                if t.tag == 'reference-image':
                    # ref image & thumbnail have data in attribs
                    # return as dicts
                    try:
                        x = int(t.attrib['X'])
                    except ValueError:
                        x = 0
                    try:
                        y = int(t.attrib['Y'])
                    except ValueError:
                        y = 0
                    src = ''.join((CONTENT_SERVER, t.attrib['src']))
                    src = src.replace('//', '/').replace('/', '//', 1)
                    data = {'X': x,
                            'Y': y,
                            'src': src,
                            }
                    obj[t.tag].append(data)
                elif t.tag == 'thumbnail':
                    try:
                        x = int(t.attrib['X'])
                    except ValueError:
                        x = 0
                    try:
                        y = int(t.attrib['Y'])
                    except ValueError:
                        y = 0
                    src = ''.join((CONTENT_SERVER, '/', ark, '/thumbnail'))
                    src = src.replace('//', '/').replace('/', '//', 1)
                    data = {'X': x,
                            'Y': y,
                            'src': src,
                            }
                    obj[t.tag] = data
                elif len(list(t)) > 0:
                    # <snippet> tag breaks up text for findaid <relation>
                    for innertext in t.itertext():
                        data = ''.join((data, innertext.strip()))
                    if data: 
                        obj[t.tag].append({'attrib':t.attrib, 'text':data})
                else:
                    if t.text: #don't add blank ones
                        obj[t.tag].append({'attrib':t.attrib, 'text':t.text})
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

    def _get_next_result_set(self):
        '''get the next result set
        Return the facet element, only one were interested in'''
        n_tries = 0
        pause = 5 
        while True:
            try:
                #resp = requests.get(self._url_current)
                resp = urllib.urlopen(self._url_current)
                break
            except DecodeError, e:
                n_tries += 1
                if n_tries > 5:
                    raise e
                #backoff
                time.sleep(pause)
                pause = pause*2
        #resp.encoding = 'utf-8'  # thinks it's ISO-8859-1
        crossQueryResult = ET.fromstring(resp.read())
        #crossQueryResult = ET.fromstring(resp.text.encode('utf-8'))
        return crossQueryResult.find('facet')

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
        self._url_current = ''.join((self.url, '&startDoc=',
                            str(self.groups[self.currentGroup]['currentDoc']),
                            '&group=', self.currentGroup))
        facet_type_tab = self._get_next_result_set()
        self._update_groups(facet_type_tab.findall('group'))
        objset = self._docHits_to_objset(
            facet_type_tab.findall('./group/docHit')
        )
        self.currentDoc += len(objset)
        self.groups[self.currentGroup]['currentDoc'] += len(objset)
        return objset


class UCSF_XML_Fetcher(Fetcher):
    def __init__(self, url_harvest, extra_data, page_size=100):
        self.url_base = url_harvest
        self.page_size = page_size
        self.page_current = 1
        self.doc_current = 1
        self.docs_fetched = 0
        xml = urllib.urlopen(self.url_current).read()
        total = re.search('search-hits pages="(?P<pages>\d+)" page="(?P<page>\d+)" total="(?P<total>\d+)"', xml)
        self.docs_total = int(total.group('total'))
        self.re_ns_strip = re.compile('{.*}(?P<tag>.*)$')

    @property
    def url_current(self):
        return '{0}&ps={1}&p={2}'.format(self.url_base, self.page_size,
                                         self.page_current)

    def _dochits_to_objset(self, docHits):
        '''Returns list of objecs.
        Objects are dictionary with tid, uri & metadata keys.
        tid & uri are strings
        metadata is a dict with keys matching UCSF xml metadata tags.
        '''
        objset = []
        for d in docHits:
            doc = d.find('./{http://legacy.library.ucsf.edu/document/1.1}document')
            obj = {}
            obj['tid'] = doc.attrib['tid']
            obj['uri'] = doc.find('./{http://legacy.library.ucsf.edu/document/1.1}uri').text
            mdata = doc.find('./{http://legacy.library.ucsf.edu/document/1.1}metadata')
            obj_mdata = defaultdict(list)
            for md in mdata:
                # strip namespace
                key = self.re_ns_strip.match(md.tag).group('tag')
                obj_mdata[key].append(md.text)
            obj['metadata'] = obj_mdata
            self.docs_fetched += 1
            objset.append(obj)
        return objset

    def next(self):
        '''get next objset, use etree to pythonize'''
        if self.doc_current == self.docs_total:
            if self.docs_fetched != self.docs_total:
                raise ValueError(
                   "Number of documents fetched ({0}) doesn't match \
                    total reported by server ({1})".format(
                        self.docs_fetched,
                        self.docs_total)
                    )
            else:
                raise StopIteration
        tree = ET.fromstring(urllib.urlopen(self.url_current).read())
        hits=tree.findall(".//{http://legacy.library.ucsf.edu/search/1.0}search-hit")
        self.page_current += 1
        self.doc_current = int(hits[-1].attrib['number'])
        return self._dochits_to_objset(hits)



class OAC_JSON_Fetcher(Fetcher):
    '''Fetcher for oac, using the JSON objset interface
    This is being deprecated in favor of the xml interface'''
    def __init__(self, url_harvest, extra_data):
        super(OAC_JSON_Fetcher, self).__init__(url_harvest, extra_data)
        self.oac_findaid_ark = self._parse_oac_findaid_ark(self.url)
        self.headers = {'content-type': 'application/json'}
        self.objset_last = False
        self.resp = requests.get(self.url, headers=self.headers)
        api_resp = self.resp.json()
        # for key in api_resp.keys():
        #    self.__dict__[key] = api_resp[key]
        self.objset_total = api_resp[u'objset_total']
        self.objset_start = api_resp['objset_start']
        self.objset_end = api_resp['objset_end']
        self.objset = api_resp['objset']
        n_objset = []
        for rec in self.objset:
            rec_orig = rec
            rec = rec['qdc']
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
                return rec
            except IndexError, e:
                if self.objset_end == self.objset_total:
                    self.resp = None
                    raise StopIteration
            url_next = ''.join((self.url, '&startDoc=',
                                unicode(self.objset_end+1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            # self.objset_total = api_resp['objset_total']
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
            n_objset = []
            for rec in self.objset:
                rec_orig = rec
                rec = rec['qdc']
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
            url_next = ''.join((self.url, '&startDoc=',
                                unicode(self.objset_end+1)))
            self.resp = requests.get(url_next, headers=self.headers)
            self.api_resp = self.resp.json()
            self.objset_start = self.api_resp['objset_start']
            self.objset_end = self.api_resp['objset_end']
            self.objset = self.api_resp['objset']
            n_objset = []
            for rec in self.objset:
                rec_orig = rec
                rec = rec['qdc']
                rec['files'] = rec_orig['files']
                n_objset.append(rec)
            self.objset = n_objset
        return cur_objset



class AlephMARCXMLFetcher(Fetcher):
    '''Harvest a MARC XML feed from Aleph. Currently used for the 
    UCSB cylinders project'''
    def __init__(self, url_harvest, extra_data, page_size=500):
        '''Grab file and copy to local temp file'''
        super(AlephMARCXMLFetcher, self).__init__(url_harvest, extra_data)
        self.ns={'zs':"http://www.loc.gov/zing/srw/"}
        self.page_size = page_size
        self.url_base = url_harvest + '&maximumRecords=' + str(self.page_size)
        self.current_record = 1
        url_current = ''.join((self.url_base, '&startRecord=',
                                   str(self.current_record)))
        
        tree_current = self.get_current_xml_tree()
        self.num_records = self.get_total_records(tree_current)

    def get_url_current_chunk(self):
        '''Set the next URL to retrieve according to page size and current
        record'''
        return ''.join((self.url_base, '&startRecord=',
                                   str(self.current_record)))

    def get_current_xml_tree(self):
        '''Return an ElementTree for the next xml_page'''
        url = self.get_url_current_chunk()
        return ET.fromstring(urllib.urlopen(url).read())

    def get_total_records(self, tree):
        '''Return the total number of records from the etree passed in'''
        return int(tree.find('.//zs:numberOfRecords', self.ns).text)

    def next(self):
        '''Return MARC records in sets to controller.
        Break when last record position == num_records
        '''
        if self.current_record >= self.num_records:
            raise StopIteration
        #get chunk from self.current_record to self.current_record + page_size
        tree = self.get_current_xml_tree()
        recs_xml=tree.findall('.//zs:record', self.ns)
        #advance current record to end of set
        self.current_record = int(recs_xml[-1].find(
                                './/zs:recordPosition', self.ns).text)
        self.current_record += 1
        #translate to pymarc records & return
        marc_xml_file = tempfile.TemporaryFile()
        marc_xml_file.write(ET.tostring(tree))
        marc_xml_file.seek(0)
        recs=[rec.as_dict() for rec in pymarc.parse_xml_to_array(marc_xml_file) if rec is not None]
        return recs



HARVEST_TYPES = {'OAI': OAIFetcher,
                 'OAJ': OAC_JSON_Fetcher,
                 'OAC': OAC_XML_Fetcher,
                 'SLR': SolrFetcher,
                 'MRC': MARCFetcher,
                 'NUX': UCLDCNuxeoFetcher,
                 'ALX': AlephMARCXMLFetcher,
                 'SFX': UCSF_XML_Fetcher,
}


class HarvestController(object):
    '''Controller for the harvesting. Selects correct Fetcher for the given
    collection, then retrieves records for the given collection and saves to
    disk.
    TODO: produce profile file
    '''
    campus_valid = ['UCB', 'UCD', 'UCI', 'UCLA', 'UCM', 'UCR', 'UCSB', 'UCSC',
                    'UCSD', 'UCSF', 'UCDL']
    dc_elements = ['title', 'creator', 'subject', 'description', 'publisher',
                   'contributor', 'date', 'type', 'format', 'identifier',
                   'source', 'language', 'relation', 'coverage', 'rights']

    def __init__(self, user_email, collection, profile_path=None,
                 config_file=None):
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
                                   self.collection.harvest_extra_data)
        self.logger = logbook.Logger('HarvestController')
        self.dir_save = tempfile.mkdtemp('_' + self.collection.slug)
        self.ingest_doc_id = None
        self.ingestion_doc = None
        self.couch = None
        self.num_records = 0

    @staticmethod
    def dt_json_handler(obj):
        '''The json package cannot deal with datetimes.
        Provide this serializer for datetimes.
        '''
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(obj)

    def save_objset(self, objset):
        '''Save an object set to disk. If it is a single object, wrap in a
        list to be uniform'''
        filename = os.path.join(self.dir_save, str(uuid.uuid4()))
        if not type(objset) == list:
            objset = [objset]
        with open(filename, 'w') as foo:
            foo.write(json.dumps(objset,
                      default=HarvestController.dt_json_handler))

    def create_ingest_doc(self):
        '''Create the DPLA style ingest doc in couch for this harvest session.
        Update with the current information. Status is running'''
        self.couch = dplaingestion.couch.Couch(config_file=self.config_file,
                                dpla_db_name=self.couch_db_name,
                                dashboard_db_name=self.couch_dashboard_name
                                )
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
        except Exception, e:
            self.logger.error("Error updating ingestion doc %s in %s" %
                              (self.ingestion_doc["_id"], __name__))
            raise e
        return self.ingest_doc_id

    def update_ingest_doc(self, status, error_msg=None, items=None,
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
        except Exception, e:
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
        self.collection['id'] = self.collection.url.strip('/').rsplit('/', 1)[1]
        self.collection['ingestType'] = 'collection'
        self.collection['title'] = self.collection.name
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
        obj['collection'] = [obj['collection']]  # in future may be more than one
        return obj

    def harvest(self):
        '''Harvest the collection'''
        self.logger.info(' '.join(('Starting harvest for:',
                                   str(self.user_email), self.collection.url,
                                   str(self.collection['campus']),
                                   str(self.collection['repository']))))
        self.num_records = 0
        next_log_n = interval = 100
        for objset in self.fetcher:
            if isinstance(objset, list):
                self.num_records += len(objset)
                #TODO: use map here
                for obj in objset:
                    self._add_registry_data(obj)
            else:
                self.num_records += 1
                self._add_registry_data(objset)
            self.save_objset(objset)
            if self.num_records >= next_log_n:
                self.logger.info(' '.join((str(self.num_records),
                                 'records harvested')))
                if self.num_records < 10000 and self.num_records >= 10*interval:
                    interval = 10*interval
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
    parser.add_argument('url_api_collection', type=str, nargs='?',
            help='URL for the collection Django tastypie api resource')
    return parser.parse_args()


def get_log_file_path(collection_slug):
    '''Get the log file name for the given collection, start time and environment
    '''
    log_file_dir = os.environ.get('DIR_HARVESTER_LOG',
                                  os.path.join(os.environ.get('HOME', '.'),
                                               'log')
                                  )
    log_file_name = '-'.join(('harvester', collection_slug,
                             datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),
                             '.log'))
    return os.path.join(log_file_dir, log_file_name)


def create_mimetext_msg(mail_from, mail_to, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = str(subject)
    msg['From'] = mail_from
    msg['To'] = mail_to if type(mail_to) == str else ', '.join(mail_to)
    return msg


def main(user_email, url_api_collection, log_handler=None, mail_handler=None,
         dir_profile='profiles', profile_path=None,
         config_file=None):
    '''Executes a harvest with given parameters.
    Returns the ingest_doc_id, directory harvest saved to and number of
    records.
    '''
    if not config_file:
         config_file=os.environ.get('DPLA_CONFIG_FILE', 'akara.ini')
    num_recs = -1
    my_mail_handler = None
    if not mail_handler:
        my_mail_handler = logbook.MailHandler(EMAIL_RETURN_ADDRESS,
                                              user_email,
                                              level='ERROR',
                                              bubble=True)
        my_mail_handler.push_application()
        mail_handler = my_mail_handler
    try:
        collection = Collection(url_api_collection)
    except Exception, e:
        msg = 'Exception in Collection {}, init {}'.format(url_api_collection,
                                                           str(e))
        logbook.error(msg)
        raise e
    if not(collection['harvest_type'] in HARVEST_TYPES):
        msg = 'Collection {} wrong type {} for harvesting. Harvest type {} \
                is not in {}'.format(url_api_collection,
                                     collection['harvest_type'],
                                     collection['harvest_type'],
                                     HARVEST_TYPES.keys()
                                     )
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
    mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email,
                                   ' '.join(('Starting harvest for ',
                                            collection.slug)),
                                   msg)
    try:  # TODO: request more emails from AWS
        mail_handler.deliver(mimetext, 'mredar@gmail.com')
    except:
        pass
    logger.info('Create DPLA profile document')
    if not profile_path:
        profile_path = os.path.abspath(
            os.path.join(dir_profile, collection.id+'.pjs'))
            #os.path.join(dir_profile, collection.slug+'.pjs'))
    with codecs.open(profile_path, 'w', 'utf8') as pfoo:
        pfoo.write(collection.dpla_profile)
    logger.info('DPLA profile document : '+profile_path)
    harvester = None
    try:
        harvester = HarvestController(user_email, collection,
                                      profile_path=profile_path,
                                      config_file=config_file)
    except Exception, e:
        import traceback
        msg = 'Exception in harvester init: type: {} TRACE:\n{}'.format(
              type(e), traceback.format_exc())
        logger.error(msg)
        raise e
    logger.info('Create ingest doc in couch')
    ingest_doc_id = harvester.create_ingest_doc()
    logger.info('Ingest DOC ID: ' + ingest_doc_id)
    logger.info('Start harvesting next')
    try:
        num_recs = harvester.harvest()
        msg = ''.join(('Finished harvest of ', collection.slug,
                       '. ', str(num_recs), ' records harvested.'))
        harvester.update_ingest_doc('complete', items=num_recs, num_coll=1)
        logger.info(msg)
        # email directly
        mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email,
                                       ' '.join(('Finished harvest of raw records for ',
                                                 collection.slug,
                                                 ' enriching next')),
                                       msg)
        try:
            mail_handler.deliver(mimetext, 'mredar@gmail.com')
        except:
            pass
    except Exception, e:
        import traceback
        error_msg = ''.join(("Error while harvesting: type-> ", str(type(e)),
                             " TRACE:\n"+str(traceback.format_exc())))
        logger.error(error_msg)
        harvester.update_ingest_doc('error', error_msg=error_msg,
                                    items=num_recs)
        raise e 
    if my_log_handler:
        my_log_handler.pop_application()
    if my_mail_handler:
        my_mail_handler.pop_application()
    return ingest_doc_id, num_recs, harvester.dir_save, harvester

if __name__ == '__main__':
    args = parse_args()
    main(args.user_email, args.url_api_collection)
