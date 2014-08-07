# harvest images for the given collection
# by this point the isShownBy value for the doc should be filled in. 
# this should point at the highest fidelity object file available 
# from the source.
# use brian's content md5s3stash to store the resulting image.

import os
from md5s3stash import md5s3stash
import couchdb

BUCKET_BASE = os.environ.get('S3_BUCKET_IMAGE_BASE', 'ucldc')
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://localhost:5984')
COUCHDB_DB = os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'

#Need to make each download a separate job.
def stash_image(doc, bucket_base=BUCKET_BASE):
    '''Stash the images in s3, using md5s3stash'''
    try:
        url_image = doc['isShownBy']['src']
    except KeyError, e:
        raise KeyError("NO isShownBy field for {}".format(r))
    report = md5s3stash(url_image, bucket_base=BUCKET_BASE)

def update_doc_object(doc, report):
    '''Update the object field to point to an s3 bucket'''
    doc['object'] = report.s3_url
    db.save(doc)
    return doc['object']

def main(collection_key=None, url_couchdb=COUCHDB_URL):
    '''If collection_key is none, trying to grab all of the images. (Not 
    recommended)
    '''
    s = couchdb.Server(url=url_couchdb)
    db = s[COUCHDB_DB]
    v = db.view(COUCHDB_VIEW, include_docs='true', key=collection_key) if collection_key else db.view(COUCH_VIEW, include_docs='true')
    for r in v:
        try:
            doc = r.doc
            report = stash_image(doc)
            obj_val = update_doc_object(doc, report)
            print obj_val
        except KeyError, e:
            print e

if __name__=='__main__':
    main(collection_key='uchida-yoshiko-papers')
    #main(collection_key='uchida-yoshiko-photograph-collection')
    #main(collection_key='1934-international-longshoremens-association-and-g')
