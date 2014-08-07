# harvest images for the given collection
# by this point the isShownBy value for the doc should be filled in. 
# this should point at the highest fidelity object file available 
# from the source.
# use brian's content md5s3stash to store the resulting image.

# should just be a call to md5s3stash
import datetime
import time
from md5s3stash import md5s3stash
import couchdb

BUCKET_BASE = 'ucldc'
SERVER_COUCHDB = 'https://54.84.142.143/couchdb'
DB_COUCHDB = 'ucldc'
COUCH_VIEW = 'all_provider_docs/by_provider_name'

def couchdb_pager(db, view_name='_all_docs',
                  startkey=None, startkey_docid=None,
                  endkey=None, endkey_docid=None, bulk=5000, **extra_options):
    # Request one extra row to resume the listing there later.
    options = {'limit': bulk + 1}
    print("EXTRA: {}".format(extra_options))
    if extra_options:
         options.update(extra_options)
    print("OPTS:{}".format(options))
    if startkey:
        options['startkey'] = startkey
        if startkey_docid:
            options['startkey_docid'] = startkey_docid
    if endkey:
        options['endkey'] = endkey
        if endkey_docid:
            options['endkey_docid'] = endkey_docid
    done = False
    while not done:
        view = db.view(view_name, **options)
        rows = []
        # If we got a short result (< limit + 1), we know we are done.
        if len(view) <= bulk:
            done = True
            rows = view.rows
        else:
            # Otherwise, continue at the new start position.
            rows = view.rows[:-1]
            last = view.rows[-1]
            options['startkey'] = last.key
            options['startkey_docid'] = last.id

        for row in rows:
            yield row


def get_isShownBy(doc):
    best_image = None
    x = 0
    thumb = doc['originalRecord'].get('thumbnail', None)
    if thumb:
        x = thumb['X']
        best_image = thumb
    ref_images = doc['originalRecord'].get('reference-image', [])
    if type(ref_images) == dict:
        ref_images = [ref_images]
    for obj in ref_images:
        if int(obj['X']) > x:
            x = int(obj['X'])
            best_image = obj
    if not best_image:
        raise KeyError('No image reference fields found')
    return best_image

#Need to make each download a separate job.
def main(collection_key=None, url_couchdb=SERVER_COUCHDB):
    '''If collection_key is none, trying to grab all of the images. (Not 
    recommended)
    '''
    s = couchdb.Server(url=url_couchdb)
    db = s[DB_COUCHDB]
    #v = db.view(COUCH_VIEW, include_docs='true', key=collection_key) if collection_key else db.view(COUCH_VIEW, include_docs='true')
    v = couchdb_pager(db, view_name=COUCH_VIEW, include_docs='true', key=collection_key) if collection_key else couchdb_pager(db, view_name=COUCH_VIEW, include_docs='true')
    for r in v:
        doc = r.doc
        msg = doc['_id']
        if 's3://' in doc.get('object', ''): #already downloaded
            msg = ' '.join((msg, 'already fetched image'))
            continue
        try:
            doc['isShownBy'] = doc.get('isShownBy', get_isShownBy(doc))
        except Exception, e:
            print("ERROR: Can't get isShownBy for {} : {}".format(doc['_id'], e))
            continue #next doc
        try:
            url_image = doc['isShownBy']['src']
            dt_start = dt_end = datetime.datetime.now()
            report = md5s3stash(url_image, bucket_base=BUCKET_BASE)
            dt_end = datetime.datetime.now()
            doc['object'] = report.s3_url
            db.save(doc)
            msg = ' '.join((msg, doc['object']))
        except KeyError, e:
            msg = ' '.join((msg, "ERROR: No isShownBy field"))
        except TypeError, e:
            msg = ' '.join((msg, "ERROR: {}".format(e)))
        except IOError, e:
            msg = ' '.join((msg, "ERROR: {}".format(e)))
        time.sleep((dt_end-dt_start).total_seconds())
        print msg

if __name__=='__main__':
    main()
    #main(collection_key='1934-international-longshoremens-association-and-g')
    #main(collection_key='uchida-yoshiko-photograph-collection')
