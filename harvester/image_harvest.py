# harvest images for the given collection
# by this point the isShownBy value for the doc should be filled in.
# this should point at the highest fidelity object file available
# from the source.
# use brian's content md5s3stash to store the resulting image.

import os
import sys
import datetime
import time
import couchdb
import requests
import md5s3stash
from harvester.couchdb_init import get_couchdb
from harvester.config import config
from redis import Redis
import redis_collections
from harvester.couchdb_pager import couchdb_pager

BUCKET_BASE = os.environ.get('S3_BUCKET_IMAGE_BASE', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
URL_OAC_CONTENT_BASE = os.environ.get('URL_OAC_CONTENT_BASE',
                                      'http://content.cdlib.org')

def link_is_to_image(url):
    '''Check if the link points to an image content type.
    Return True or False accordingly
    '''
    response = requests.head(url, allow_redirects=True)
    if response.status_code != 200:
        return False
    content_type = response.headers.get('content-type', None)
    if not content_type:
        return False
    reg_type = content_type.split('/', 1)[0].lower()
    return reg_type == 'image'

class ImageHarvester(object):
    '''Useful to cache couchdb, authentication info and such'''
    def __init__(self, cdb=None,
                 url_couchdb=None,
                 couchdb_name=None,
                 couch_view=COUCHDB_VIEW,
                 bucket_base=BUCKET_BASE,
                 object_auth=None,
                 no_get_if_object=False):
        if cdb:
            self._couchdb = cdb
        else:
            self._couchdb = get_couchdb(url=url_couchdb,
                                        dbname=couchdb_name)
        self._bucket_base = bucket_base
        self._view = couch_view
        # auth is a tuple of username, password
        self._auth = object_auth
        self.no_get_if_object = no_get_if_object # if object field exists, try to get
        
        self._config = config()
        self._redis = Redis(host=self._config['redis_host'],
                            port=self._config['redis_port'],
                            password=self._config['redis_password'],
                            socket_connect_timeout=self._config['redis_connect_timeout'])
        self._url_cache = redis_collections.Dict(key='ucldc-image-url-cache',
                redis=self._redis)
        self._hash_cache = redis_collections.Dict(key='ucldc-image-hash-cache',
                redis=self._redis)

    # Need to make each download a separate job.
    def stash_image(self, doc):
        '''Stash the images in s3, using md5s3stash
        Return md5s3stash report if image found
        If link is not an image type, don't stash & return None
        '''
        try:
            url_image = doc['isShownBy']
            if not url_image:
                raise ValueError("isShownBy empty for {0}".format(doc['_id']))
        except KeyError, e:
            raise KeyError("isShownBy missing for {0}".format(doc['_id']))
        if isinstance(url_image, list): # need to fix marc map_is_shown_at
            url_image = url_image[0]
        # for some OAC objects, the reference image is not a url but a path.
        if 'http' != url_image[:4] and 'https' != url_image[:5]:
            # not a URL....
            if 'ark:' in url_image:
                url_image = '/'.join((URL_OAC_CONTENT_BASE, url_image))
            else:
                print >> sys.stderr, 'Link not http URL for {} - {}'.format(
                                      doc['_id'], url_image)
                return None
        if link_is_to_image(url_image):
            return md5s3stash.md5s3stash(url_image,
                                         bucket_base=self._bucket_base,
                                         url_auth=self._auth,
                                         url_cache=self._url_cache,
                                         hash_cache=self._hash_cache)
        else:
            print >> sys.stderr, 'Not an image for {} - {}'.format(
                                      doc['_id'], url_image)
            return None

    def update_doc_object(self, doc, report):
        '''Update the object field to point to an s3 bucket'''
        doc['object'] = report.md5
        doc['object_dimensions'] = report.dimensions
        self._couchdb.save(doc)
        return doc['object']

    def harvest_image_for_doc(self, doc):
        '''Try to harvest an image for a couchdb doc'''
        report = None
        if self.no_get_if_object:
            if doc.get('object', None):
                print >> sys.stderr, 'Skipping {}, has object field'.format(doc['_id'])
                return
        try:
            report = self.stash_image(doc)
            if report != None:
                obj_val = self.update_doc_object(doc, report)
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
        return report

    def by_list_of_doc_ids(self, doc_ids):
        '''For a list of ids, harvest images'''
        for doc_id in doc_ids:
            doc = self._couchdb[doc_id]
            dt_start = dt_end = datetime.datetime.now()
            report = self.harvest_image_for_doc(doc)
            dt_end = datetime.datetime.now()
            time.sleep((dt_end-dt_start).total_seconds())

    def by_collection(self, collection_key=None):
        '''If collection_key is none, trying to grab all of the images. (Not
        recommended)
        '''
        if collection_key:
            v = couchdb_pager(self._couchdb, view_name=self._view,
                    startkey=[collection_key],
                    endkey=[collection_key, {}],
                    include_docs='true')
        else:
            #use _all_docs view
            v = couchdb_pager(self._couchdb, include_docs='true')
        doc_ids = []
        for r in v:
            dt_start = dt_end = datetime.datetime.now()
            report = self.harvest_image_for_doc(r.doc)
            doc_ids.append(r.doc['_id'])
            dt_end = datetime.datetime.now()
            time.sleep((dt_end-dt_start).total_seconds())
        return doc_ids

def harvest_image_for_doc(doc_id,
        url_couchdb=None,
        object_auth=None,
        no_get_if_object=False):
    '''Wrapper to call from rqworker.
    Creates ImageHarvester object & then calls harvest_image_for_doc
    '''
    harvester = ImageHarvester(url_couchdb=url_couchdb,
                               object_auth=object_auth, 
                               no_get_if_object=no_get_if_object)
    #get doc from couchdb
    couchdb = get_couchdb(url=url_couchdb)
    doc = couchdb[doc_id]
    harvester.harvest_image_for_doc(doc)


def main(collection_key=None,
         url_couchdb=None,
         object_auth=None,
         no_get_if_object=False):
    print(ImageHarvester(url_couchdb=url_couchdb,
                         object_auth=object_auth,
                         no_get_if_object=no_get_if_object).by_collection(collection_key))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
            description='Run the image harvesting on a collection')
    parser.add_argument('collection_key', help='Registry collection id')
    parser.add_argument('--object_auth', nargs='?',
            help='HTTP Auth needed to download images - username:password')
    parser.add_argument('--url_couchdb', nargs='?',
            help='Override url to couchdb')
    parser.add_argument('--no_get_if_object', action='store_true',
                        default=False,
            help='Should image harvester not get image if the object field exists for the doc (default: False, always get)')
    args = parser.parse_args()
    print(args)
    object_auth=None
    if args.object_auth:
        object_auth = (args.object_auth.split(':')[0],
                args.object_auth.split(':')[1])
    main(args.collection_key,
         object_auth=object_auth,
         url_couchdb=args.url_couchdb,
         no_get_if_object=args.no_get_if_object)

