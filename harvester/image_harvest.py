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
from couchdb import ResourceConflict
import requests
import md5s3stash
import boto.s3
import logging
from collections import namedtuple
from harvester.couchdb_init import get_couchdb
from harvester.config import config
from redis import Redis
import redis_collections
from harvester.couchdb_pager import couchdb_pager
from harvester.cleanup_dir import cleanup_work_dir
from harvester.sns_message import publish_to_harvesting

BUCKET_BASES = os.environ.get(
    'S3_BUCKET_IMAGE_BASE',
    'us-west-2:static-ucldc-cdlib-org/harvested_images;us-east-1:'
    'static.ucldc.cdlib.org/harvested_images'
).split(';')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
URL_OAC_CONTENT_BASE = os.environ.get('URL_OAC_CONTENT_BASE',
                                      'http://content.cdlib.org')

logging.basicConfig(level=logging.DEBUG, )


def link_is_to_image(url, auth=None):
    '''Check if the link points to an image content type.
    Return True or False accordingly
    '''
    if md5s3stash.is_s3_url(url):
        response = requests.head(url, allow_redirects=True)
    else:
        response = requests.head(url, allow_redirects=True, auth=auth)
    if response.status_code != 200:
        return False
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
            return False
        content_type = response.headers.get('content-type', None)
        if not content_type:
            return False
        reg_type = content_type.split('/', 1)[0].lower()
    return reg_type == 'image'


# Need to make each download a separate job.
def stash_image_for_doc(doc,
                        url_cache,
                        hash_cache,
                        bucket_bases=BUCKET_BASES,
                        auth=None):
    '''Stash the images in s3, using md5s3stash
    Duplicate it among the "BUCKET_BASES" list. This will give redundancy
    in case some idiot (me) deletes one of the copies. Not tons of data so
    cheap to replicate them.
    Return md5s3stash report if image found
    If link is not an image type, don't stash & return None
    '''
    try:
        url_image = doc['isShownBy']
        if not url_image:
            raise ValueError("isShownBy empty for {0}".format(doc['_id']))
    except KeyError, e:
        raise KeyError("isShownBy missing for {0}".format(doc['_id']))
    if isinstance(url_image, list):  # need to fix marc map_is_shown_at
        url_image = url_image[0]
    # try to parse url, check values of scheme & netloc at least
    url_parsed = urlparse.urlsplit(url_image)
    if url_parsed.scheme == 'ark':
        # for some OAC objects, the reference image is not a url but a path.
        url_image = '/'.join((URL_OAC_CONTENT_BASE, url_image))
    elif not url_parsed.scheme or not url_parsed.netloc:
        print >> sys.stderr, 'Link not http URL for {} - {}'.format(doc['_id'],
                                                                    url_image)
        return None
    reports = []
    if link_is_to_image(url_image, auth):
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
            except TypeError, e:
                print >> sys.stderr, 'TypeError for doc:{} {} Msg: {} Args:' \
                        ' {}'.format(
                                doc['_id'], url_image, e.message, e.args)
        return reports
    else:
        print >> sys.stderr, 'Not an image for {} - {}'.format(doc['_id'],
                                                               url_image)
        return None


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
            else redis_collections.Dict(
                        key='ucldc:harvester:harvested-images',
                        redis=self._redis)

    def stash_image(self, doc):
        return stash_image_for_doc(
            doc,
            self._url_cache,
            self._hash_cache,
            bucket_bases=self._bucket_bases,
            auth=self._auth)

    def update_doc_object(self, doc, report):
        '''Update the object field to point to an s3 bucket'''
        doc['object'] = report.md5
        doc['object_dimensions'] = report.dimensions
        try:
            self._couchdb.save(doc)
        except ResourceConflict, e:
            print >> sys.stderr, 'ResourceConflictfor doc: {}'.format(doc[
                'id'])
        return doc['object']

    def harvest_image_for_doc(self, doc):
        '''Try to harvest an image for a couchdb doc'''
        reports = None
        did = doc['_id']
        object_cached = self._object_cache.get(did, False)
        if not self.get_if_object and doc.get('object', False):
            print >> sys.stderr, 'Skipping {}, has object field'.format(did)
            if not object_cached:
                print >> sys.stderr, 'Save to object cache {}'.format(did)
                self._object_cache[did] = [
                    doc['object'], doc['object_dimensions']
                ]
            return
        if not self.get_if_object and object_cached:
            # have already downloaded an image for this, just fill in data
            ImageReport = namedtuple('ImageReport', 'md5, dimensions')
            print >> sys.stderr, 'Restore from object_cache: {}'.format(did)
            self.update_doc_object(doc,
                                   ImageReport(object_cached[0],
                                               object_cached[1]))
            return None
        try:
            reports = self.stash_image(doc)
            if reports is not None and len(reports) > 0:
                self._object_cache[did] = [
                    reports[0].md5, reports[0].dimensions
                ]
                obj_val = self.update_doc_object(doc, reports[0])
        except KeyError, e:
            if 'isShownBy' in e.message:
                print >> sys.stderr, e
            else:
                raise e
        except ValueError, e:
            if 'isShownBy' in e.message:
                print >> sys.stderr, e
            else:
                raise e
        except IOError, e:
            print >> sys.stderr, e
        return reports

    def by_list_of_doc_ids(self, doc_ids):
        '''For a list of ids, harvest images'''
        for doc_id in doc_ids:
            doc = self._couchdb[doc_id]
            dt_start = dt_end = datetime.datetime.now()
            reports = self.harvest_image_for_doc(doc)
            dt_end = datetime.datetime.now()
            time.sleep((dt_end - dt_start).total_seconds())

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
        for r in v:
            dt_start = dt_end = datetime.datetime.now()
            reports = self.harvest_image_for_doc(r.doc)
            doc_ids.append(r.doc['_id'])
            dt_end = datetime.datetime.now()
            time.sleep((dt_end - dt_start).total_seconds())
        publish_to_harvesting('Image harvested {}'.format(collection_key),
                              'Processed {} documents'.format(len(doc_ids)))
        return doc_ids


def harvest_image_for_doc(doc_id,
                          url_couchdb=None,
                          object_auth=None,
                          get_if_object=False):
    '''Wrapper to call from rqworker.
    Creates ImageHarvester object & then calls harvest_image_for_doc
    '''
    harvester = ImageHarvester(
        url_couchdb=url_couchdb,
        object_auth=object_auth,
        get_if_object=get_if_object)
    # get doc from couchdb
    couchdb = get_couchdb(url=url_couchdb)
    doc = couchdb[doc_id]
    if not get_if_object and 'object' in doc:
        print >> sys.stderr, 'Skipping {}, has object field'.format(doc['_id'])
    else:
        harvester.harvest_image_for_doc(doc)


def main(collection_key=None,
         url_couchdb=None,
         object_auth=None,
         get_if_object=False):
    cleanup_work_dir()  # remove files from /tmp
    ImageHarvester(
        url_couchdb=url_couchdb,
        object_auth=object_auth,
        get_if_object=get_if_object).by_collection(collection_key)


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
        'for the doc (default: False, always get)'
    )
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
