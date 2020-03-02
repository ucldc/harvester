# harvest images for the given collection
# by this point the isShownBy value for the doc should be filled in.
# this should point at the highest fidelity object file available
# from the source.
# use brian's content md5s3stash to store the resulting image.

import os
import sys
import datetime
import time
import urlparse
import urllib
from couchdb import ResourceConflict
import requests
import md5s3stash
import boto.s3
import logging
from collections import namedtuple
from collections import defaultdict
from harvester.couchdb_init import get_couchdb
from harvester.config import config
from redis import Redis
import redis_collections
from harvester.couchdb_pager import couchdb_pager
from harvester.cleanup_dir import cleanup_work_dir
from harvester.sns_message import publish_to_harvesting
from harvester.sns_message import format_results_subject

BUCKET_BASES = os.environ.get(
    'S3_BUCKET_IMAGE_BASE',
    'us-west-2:static-ucldc-cdlib-org/harvested_images;us-east-1:'
    'static.ucldc.cdlib.org/harvested_images').split(';')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
URL_OAC_CONTENT_BASE = os.environ.get('URL_OAC_CONTENT_BASE',
                                      'http://content.cdlib.org')

logging.basicConfig(level=logging.DEBUG, )


class ImageHarvestError(Exception):
    def __init__(self, message, doc_id=None):
        super(ImageHarvestError, self).__init__(message)
        self.doc_id = doc_id


class ImageHTTPError(ImageHarvestError):
    # will waap exceptions from the HTTP request
    dict_key = 'HTTP Error'


class HasObject(ImageHarvestError):
    dict_key = 'Has Object already'


class RestoreFromObjectCache(ImageHarvestError):
    dict_key = 'Restored From Object Cache'


class IsShownByError(ImageHarvestError):
    dict_key = 'isShownBy Error'


class FailsImageTest(ImageHarvestError):
    dict_key = 'Fails the link is to image test'


def link_is_to_image(doc_id, url, auth=None):
    '''Check if the link points to an image content type.
    Return True or False accordingly.
    '''
    if md5s3stash.is_s3_url(url):
        response = requests.head(url, allow_redirects=True)
    else:
        response = requests.head(url, allow_redirects=True, auth=auth)
    # have a server that returns a 403 here, does have content-type of
    # text/html. Dropping this test here. requests throws if can't connect
    if response.status_code != 200:
        # many servers do not support HEAD requests, try get
        if md5s3stash.is_s3_url(url):
            response = requests.get(url, allow_redirects=True)
        else:
            response = requests.get(url, allow_redirects=True, auth=auth)
        if response.status_code != 200:
            raise ImageHTTPError(
                'HTTP ERROR: {}'.format(response.status_code), doc_id=doc_id)
    content_type = response.headers.get('content-type', None)
    if not content_type:
        return False
    reg_type = content_type.split('/', 1)[0].lower()
    # situation where a server returned 'text/html' to HEAD requests
    # but returned 'image/jpeg' for GET.
    # try a slower GET if not image type
    if reg_type != 'image':
        response = requests.get(url, allow_redirects=True, auth=auth)
        if response.status_code != 200:
            raise ImageHTTPError(
                'HTTP ERROR: {}'.format(response.status_code), doc_id=doc_id)
        content_type = response.headers.get('content-type', None)
        if not content_type:
            return False
        reg_type = content_type.split('/', 1)[0].lower()
    return reg_type == 'image'


# Need to make each download a separate job.
def stash_image_for_doc(doc,
                        url_cache,
                        hash_cache,
                        ignore_content_type,
                        bucket_bases=BUCKET_BASES,
                        auth=None):
    '''Stash the images in s3, using md5s3stash
    Duplicate it among the "BUCKET_BASES" list. This will give redundancy
    in case some idiot (me) deletes one of the copies. Not tons of data so
    cheap to replicate them.
    Return md5s3stash report if image found
    If link is not an image type, don't stash & raise
    '''
    try:
        url_image = doc['isShownBy']
        if not url_image:
            raise IsShownByError(
                "isShownBy empty for {0}".format(doc['_id']),
                doc_id=doc['_id'])
    except KeyError as e:
        raise IsShownByError(
            "isShownBy missing for {0}".format(doc['_id']), doc_id=doc['_id'])
    if isinstance(url_image, list):  # need to fix marc map_is_shown_at
        url_image = url_image[0]
    # try to parse url, check values of scheme & netloc at least
    url_parsed = urlparse.urlsplit(url_image)
    if url_parsed.scheme == 'ark':
        # for some OAC objects, the reference image is not a url but a path.
        url_image = '/'.join((URL_OAC_CONTENT_BASE, url_image))
    elif not url_parsed.scheme or not url_parsed.netloc:
        msg = 'Link not http URL for {} - {}'.format(doc['_id'], url_image)
        print >> sys.stderr, msg
        raise FailsImageTest(msg, doc_id=doc['_id'])
    reports = []
    # If '--ignore_content_type' set, don't check link_is_to_image
    if link_is_to_image(doc['_id'], url_image, auth) or ignore_content_type:
        for bucket_base in bucket_bases:
            try:
                logging.getLogger('image_harvest.stash_image').info(
                    'bucket_base:{0} url_image:{1}'.format(bucket_base,
                                                           url_image))
                region, bucket_base = bucket_base.split(':')
                conn = boto.s3.connect_to_region(region)
                report = md5s3stash.md5s3stash(
                    url_image,
                    bucket_base=bucket_base,
                    conn=conn,
                    url_auth=auth,
                    url_cache=url_cache,
                    hash_cache=hash_cache)
                reports.append(report)
            except TypeError as e:
                print >> sys.stderr, 'TypeError for doc:{} {} Msg: {} Args:' \
                    ' {}'.format(
                        doc['_id'], url_image, e.message, e.args)
        return reports
    else:
        msg = 'Not an image for {} - {}'.format(doc['_id'], url_image)
        print >> sys.stderr, msg
        raise FailsImageTest(msg, doc_id=doc['_id'])


class ImageHarvester(object):
    '''Useful to cache couchdb, authentication info and such'''

    def __init__(self,
                 cdb=None,
                 url_couchdb=None,
                 couchdb_name=None,
                 couch_view=COUCHDB_VIEW,
                 bucket_bases=BUCKET_BASES,
                 object_auth=None,
                 get_if_object=False,
                 ignore_content_type=False,
                 url_cache=None,
                 hash_cache=None,
                 harvested_object_cache=None):
        self._config = config()
        if cdb:
            self._couchdb = cdb
        else:
            if not url_couchdb:
                url_couchdb = self._config['couchdb_url']
            self._couchdb = get_couchdb(url=url_couchdb, dbname=couchdb_name)
        self._bucket_bases = bucket_bases
        self._view = couch_view
        # auth is a tuple of username, password
        self._auth = object_auth
        self.get_if_object = get_if_object  # if object field exists, get
        self.ignore_content_type = ignore_content_type # Don't check content-type in headers
        self._redis = Redis(
            host=self._config['redis_host'],
            port=self._config['redis_port'],
            password=self._config['redis_password'],
            socket_connect_timeout=self._config['redis_connect_timeout'])
        self._url_cache = url_cache if url_cache is not None else \
            redis_collections.Dict(key='ucldc-image-url-cache',
                                   redis=self._redis)
        self._hash_cache = hash_cache if hash_cache is not None else \
            redis_collections.Dict(key='ucldc-image-hash-cache',
                                   redis=self._redis)
        self._object_cache = harvested_object_cache if harvested_object_cache \
            else \
            redis_collections.Dict(
                key='ucldc:harvester:harvested-images',
                redis=self._redis)

    def stash_image(self, doc):
        return stash_image_for_doc(
            doc,
            self._url_cache,
            self._hash_cache,
            self.ignore_content_type,
            bucket_bases=self._bucket_bases,
            auth=self._auth)

    def update_doc_object(self, doc, report):
        '''Update the object field to point to an s3 bucket'''
        doc['object'] = report.md5
        doc['object_dimensions'] = report.dimensions
        try:
            self._couchdb.save(doc)
        except ResourceConflict as e:
            msg = 'ResourceConflictfor doc: {} - {}'.format(doc[
                '_id'], e.message)
            print >> sys.stderr, msg
        return doc['object']

    def harvest_image_for_doc(self, doc, force=False):
        '''Try to harvest an image for a couchdb doc'''
        reports = None
        did = doc['_id']
        object_cached = self._object_cache.get(did, False)
        if not self.get_if_object and doc.get('object', False) and not force:
            msg = 'Skipping {}, has object field'.format(did)
            print >> sys.stderr, msg
            if not object_cached:
                msg = 'Save to object cache {}'.format(did)
                print >> sys.stderr, msg
                self._object_cache[did] = [
                    doc['object'], doc['object_dimensions']
                ]
            raise HasObject(msg, doc_id=doc['_id'])
        if not self.get_if_object and object_cached and not force:
            # have already downloaded an image for this, just fill in data
            ImageReport = namedtuple('ImageReport', 'md5, dimensions')
            msg = 'Restore from object_cache: {}'.format(did)
            print >> sys.stderr, msg
            self.update_doc_object(doc,
                                   ImageReport(object_cached[0],
                                               object_cached[1]))
            raise RestoreFromObjectCache(msg, doc_id=doc['_id'])
        try:
            reports = self.stash_image(doc)
            if reports is not None and len(reports) > 0:
                self._object_cache[did] = [
                    reports[0].md5, reports[0].dimensions
                ]
                self.update_doc_object(doc, reports[0])
        except IOError as e:
            print >> sys.stderr, e
        return reports

    def by_doc_id(self, doc_id):
        '''For a list of ids, harvest images'''
        doc = self._couchdb[doc_id]
        dt_start = dt_end = datetime.datetime.now()
        report_errors = defaultdict(list)
        try:
            reports = self.harvest_image_for_doc(doc, force=True)
        except ImageHarvestError as e:
            report_errors[e.dict_key].append((e.doc_id, str(e)))
        dt_end = datetime.datetime.now()
        time.sleep((dt_end - dt_start).total_seconds())
        return report_errors

    def by_collection(self, collection_key=None):
        '''If collection_key is none, trying to grab all of the images. (Not
        recommended)
        '''
        if collection_key:
            v = couchdb_pager(
                self._couchdb,
                view_name=self._view,
                startkey='"{0}"'.format(collection_key),
                endkey='"{0}"'.format(collection_key),
                include_docs='true')
        else:
            # use _all_docs view
            v = couchdb_pager(self._couchdb, include_docs='true')
        doc_ids = []
        report_errors = defaultdict(list)
        for r in v:
            dt_start = dt_end = datetime.datetime.now()
            try:
                reports = self.harvest_image_for_doc(r.doc)
            except ImageHarvestError as e:
                report_errors[e.dict_key].append((e.doc_id, str(e)))
            doc_ids.append(r.doc['_id'])
            dt_end = datetime.datetime.now()
            time.sleep((dt_end - dt_start).total_seconds())
        report_list = [
            ' : '.join((key, str(val))) for key, val in report_errors.items()
        ]
        report_msg = '\n'.join(report_list)
        subject = format_results_subject(collection_key,
                                         'Image harvest to CouchDB {env}')
        publish_to_harvesting(subject, ''.join(
            ('Processed {} documents\n'.format(len(doc_ids)), report_msg)))
        return doc_ids, report_errors


def harvest_image_for_doc(doc_id,
                          url_couchdb=None,
                          object_auth=None,
                          get_if_object=False,
                          force=False):
    '''Wrapper to call from rqworker.
    Creates ImageHarvester object & then calls harvest_image_for_doc
    '''
    harvester = ImageHarvester(
        url_couchdb=url_couchdb,
        object_auth=object_auth,
        get_if_object=get_if_object,
        ignore_content_type=ignore_content_type)
    # get doc from couchdb
    couchdb = get_couchdb(url=url_couchdb)
    doc = couchdb[doc_id]
    if not get_if_object and 'object' in doc and not force:
        print >> sys.stderr, 'Skipping {}, has object field'.format(doc['_id'])
    else:
        harvester.harvest_image_for_doc(doc, force=force)


def main(collection_key=None,
         url_couchdb=None,
         object_auth=None,
         get_if_object=False,
         ignore_content_type=False):
    cleanup_work_dir()  # remove files from /tmp
    doc_ids, report_errors = ImageHarvester(
        url_couchdb=url_couchdb,
        object_auth=object_auth,
        get_if_object=get_if_object,
        ignore_content_type=ignore_content_type).by_collection(collection_key)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Run the image harvesting on a collection')
    parser.add_argument('collection_key', help='Registry collection id')
    parser.add_argument(
        '--object_auth',
        nargs='?',
        help='HTTP Auth needed to download images - username:password')
    parser.add_argument(
        '--url_couchdb', nargs='?', help='Override url to couchdb')
    parser.add_argument(
        '--get_if_object',
        action='store_true',
        default=False,
        help='Should image harvester not get image if the object field exists '
        'for the doc (default: False, always get)')
    args = parser.parse_args()
    print(args)
    object_auth = None
    if args.object_auth:
        object_auth = (args.object_auth.split(':')[0],
                       args.object_auth.split(':')[1])
    main(
        args.collection_key,
        object_auth=object_auth,
        url_couchdb=args.url_couchdb,
        get_if_object=args.get_if_object)
