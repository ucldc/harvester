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
COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://localhost:5984')
COUCHDB_DB = os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'

def get_couchdb(url_couchdb=COUCHDB_URL, db_name=COUCHDB_DB):
    '''Return a python couchdb DB'''
    return couchdb.Server(url=url_couchdb)[db_name]

#Need to make each download a separate job.
def stash_image(doc, bucket_base=BUCKET_BASE):
    '''Stash the images in s3, using md5s3stash'''
    try:
        url_image = doc['isShownBy']['src']
    except KeyError, e:
        raise KeyError("isShownBy field missing for {}".format(doc['_id']))
    return md5s3stash.md5s3stash(url_image, bucket_base=bucket_base)

def update_doc_object(doc, report, db_couchdb):
    '''Update the object field to point to an s3 bucket'''
    doc['object'] = report.s3_url
    db_couchdb.save(doc)
    return doc['object']

def harvest_image_for_doc(doc, db_couchdb=None):
    '''Try to harvest an image for a couchdb doc'''
    if not db_couchdb:
        db_couchdb = get_couchdb()
    try:
        report = stash_image(doc)
        obj_val = update_doc_object(doc, report, db_couchdb)
    except KeyError, e:
        if 'isShownBy' in e.message:
            print e
        else:
            raise e
    return report

def by_list_of_doc_ids(doc_ids, url_couchdb=COUCHDB_URL):
    '''For a list of ids, harvest images'''
    db = get_couchdb(url_couchdb=url_couchdb)
    for doc_id in doc_ids:
        dt_start = dt_end = datetime.datetime.now()
        doc = db[doc_id]
        report = harvest_image_for_doc(doc, db_couchdb=db)
        dt_end = datetime.datetime.now()
        time.sleep((dt_end-dt_start).total_seconds()) #delay to not hammer
    print("IMAGES FOR {}".format(doc_ids))

def by_collection(collection_key=None, url_couchdb=COUCHDB_URL):
    '''If collection_key is none, trying to grab all of the images. (Not 
    recommended)
    '''
    db = get_couchdb(url_couchdb=url_couchdb)
    v = db.view(COUCHDB_VIEW, include_docs='true', key=collection_key) if collection_key else db.view(COUCH_VIEW, include_docs='true')
    doc_ids = []
    for r in v:
        dt_start = dt_end = datetime.datetime.now()
        report = harvest_image_for_doc(r.doc, db_couchdb=db)
        doc_ids.append(r.doc['_id'])
        dt_end = datetime.datetime.now()
        time.sleep((dt_end-dt_start).total_seconds()) #delay to not hammer
    return doc_ids

def main(collection_key=None, url_couchdb=COUCHDB_URL):
    print("COUCHDB: {}".format(url_couchdb))
    print(by_collection(collection_key, url_couchdb))

if __name__=='__main__':
    main(collection_key='uchida-yoshiko-papers')
    #main(collection_key='uchida-yoshiko-photograph-collection')
    #main(collection_key='1934-international-longshoremens-association-and-g')
