# harvest images for the given collection
# by this point the isShownBy value for the doc should be filled in.
# this should point at the highest fidelity object file available
# from the source.
# use brian's content md5s3stash to store the resulting image.

import os
import sys
import datetime
import time
import md5s3stash
import couchdb
from harvester.config import config

BUCKET_BASE = os.environ.get('S3_BUCKET_IMAGE_BASE', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
URL_OAC_CONTENT_BASE = os.environ.get('URL_OAC_CONTENT_BASE',
                                      'http://content.cdlib.org')


class ImageHarvester(object):
    '''Useful to cache couchdb, authentication info and such'''
    def __init__(self, cdb=None,
                 url_couchdb=None,
                 couchdb_name=None,
                 couch_view=COUCHDB_VIEW,
                 bucket_base=BUCKET_BASE,
                 object_auth=None,
                 get_if_object=True):
        if cdb:
            self._couchdb = cdb
        else:
            cfg = config()
            if not url_couchdb:
                url_couchdb = cfg.DPLA.get("CouchDb", "URL")
            if not couchdb_name:
                couchdb_name = cfg.DPLA.get("CouchDb", "ItemDatabase")
            username = cfg.DPLA.get("CouchDb", "Username")
            password = cfg.DPLA.get("CouchDb", "Password")
            url = url_couchdb.split("//")
            url_server = "{0}//{1}:{2}@{3}".format(url[0], username, password, url[1])
            self._couchdb = couchdb.Server(url=url_server)[couchdb_name]
        self._bucket_base = bucket_base
        self._view = couch_view
        # auth is a tuple of username, password
        self._auth = object_auth
        self.get_if_object = get_if_object # if object field exists, try to get

    # Need to make each download a separate job.
    def stash_image(self, doc):
        '''Stash the images in s3, using md5s3stash'''
        try:
            url_image = doc['isShownBy']
            if not url_image:
                raise ValueError("isShownBy empty for {0}".format(doc['_id']))
        except KeyError, e:
            raise KeyError("isShownBy missing for {0}".format(doc['_id']))
        if isinstance(url_image, list): # need to fix marc map_is_shown_at
            url_image = url_image[0]
        # for some OAC objects, the reference image is not a url but a path.
        if 'http' != url_image[:4]:
            # not a URL....
            if 'ark:' in url_image:
                url_image = '/'.join((URL_OAC_CONTENT_BASE, url_image))
        # print >> sys.stderr,("For {} url image is:{}".format(doc['_id'], url_image))
        return md5s3stash.md5s3stash(url_image, bucket_base=self._bucket_base,
                                     url_auth=self._auth)

    def update_doc_object(self, doc, report):
        '''Update the object field to point to an s3 bucket'''
        doc['object'] = report.md5
        self._couchdb.save(doc)
        return doc['object']

    def harvest_image_for_doc(self, doc):
        '''Try to harvest an image for a couchdb doc'''
        report = None
        if not self.get_if_object:
            if doc.get('object', None):
                print >> sys.stderr, 'Skipping {}, has object field'.format(doc['_id'])
        try:
            report = self.stash_image(doc)
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
            v = self._couchdb.view(self._view, include_docs='true',
                                   key=collection_key)
        else:
            v = self._couchdb.view(self._view, include_docs='true')
        doc_ids = []
        for r in v:
            dt_start = dt_end = datetime.datetime.now()
            report = self.harvest_image_for_doc(r.doc)
            doc_ids.append(r.doc['_id'])
            dt_end = datetime.datetime.now()
            time.sleep((dt_end-dt_start).total_seconds())
        return doc_ids


def main(collection_key=None,
         url_couchdb=None,
         object_auth=None,
         get_if_object=True):
    print(ImageHarvester(url_couchdb=url_couchdb,
                         object_auth=object_auth,
                         get_if_object=get_if_object).by_collection(collection_key))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
            description='Run the image harvesting on a collection')
    parser.add_argument('collection_key', help='Registry collection id')
    parser.add_argument('--object_auth', nargs='?',
            help='HTTP Auth needed to download images - username:password')
    parser.add_argument('--url_couchdb', nargs='?',
            help='Override url to couchdb')
    parser.add_argument('--get_if_object', nargs='?', default=True,
            help='Should image harvester try to get image if the object field exists for the doc (default: True)')
    args = parser.parse_args()
    print(args)
    object_auth=None
    if args.object_auth:
        object_auth = (args.object_auth.split(':')[0],
                args.object_auth.split(':')[1])
    main(args.collection_key, object_auth=object_auth, url_couchdb=args.url_couchdb)
