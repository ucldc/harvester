# harvest images for the given collection
# by this point the isShownBy value for the doc should be filled in.
# this should point at the highest fidelity object file available
# from the source.
# use brian's content md5s3stash to store the resulting image.

import os
import datetime
import time
import md5s3stash
import couchdb

BUCKET_BASE = os.environ.get('S3_BUCKET_IMAGE_BASE', 'ucldc')
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://127.0.0.1:5984')
COUCHDB_DB = os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
URL_OAC_CONTENT_BASE = os.environ.get('URL_OAC_CONTENT_BASE',
                                      'http://content.cdlib.org')


class ImageHarvester(object):
    '''Useful to cache couchdb, authentication info and such'''
    def __init__(self, cdb=None,
                 url_couchdb=COUCHDB_URL, couchdb_name=COUCHDB_DB,
                 couch_view=COUCHDB_VIEW,
                 bucket_base=BUCKET_BASE,
                 object_auth=None):
        if cdb:
            self._couchdb = cdb
        else:
            self._couchdb = couchdb.Server(url=url_couchdb)[couchdb_name]
        self._bucket_base = bucket_base
        self._view = couch_view
        # auth is a tuple of username, password
        self._auth = object_auth

    # Need to make each download a separate job.
    def stash_image(self, doc):
        '''Stash the images in s3, using md5s3stash'''
        try:
            url_image = doc['isShownBy']
            if not url_image:
                raise ValueError("isShownBy is blank for {0}".format(doc['_id']))
        except KeyError, e:
            raise KeyError("isShownBy missing for {0}".format(doc['_id']))
        if isinstance(url_image, list): # need to fix marc map_is_shown_at
            url_image = url_image[0]
        # for some OAC objects, the reference image is not a url but a path.
        if 'http' != url_image[:4]:
            # not a URL....
            if 'ark:' in url_image:
                url_image = '/'.join((URL_OAC_CONTENT_BASE, url_image))
        #print("For {} url image is:{}".format(doc['_id'], url_image))
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
        try:
            report = self.stash_image(doc)
            obj_val = self.update_doc_object(doc, report)
        except KeyError, e:
            if 'isShownBy' in e.message:
                print e
            else:
                raise e
        except ValueError, e:
            if 'isShownBy' in e.message:
                print e
            else:
                raise e
        except IOError, e:
            print e
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
        v = self._couchdb.view(self._view, include_docs='true',
                               key=collection_key) if collection_key else \
                               self._couchdb.view(self._view, include_docs='true')
        doc_ids = []
        for r in v:
            dt_start = dt_end = datetime.datetime.now()
            report = self.harvest_image_for_doc(r.doc)
            doc_ids.append(r.doc['_id'])
            dt_end = datetime.datetime.now()
            time.sleep((dt_end-dt_start).total_seconds())
        return doc_ids


def main(collection_key=None, url_couchdb=COUCHDB_URL, object_auth=None):
    print(ImageHarvester(url_couchdb=url_couchdb,
                         object_auth=object_auth).by_collection(collection_key))

if __name__ == '__main__':
    main(collection_key='lapl-photos-marc')
    # main(collection_key='uchida-yoshiko-photograph-collection')
    # main(collection_key='1934-international-longshoremens-association-and-g')
