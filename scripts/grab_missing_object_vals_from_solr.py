import sys
py_version = sys.version_info
if py_version.major == 2 and py_version.minor == 7 and py_version.micro > 8:
        #disable ssl verification
        import ssl
        ssl._create_default_https_context = ssl._create_unverified_context

import os
from harvester.post_processing.run_transform_on_couchdb_docs import run_on_couchdb_by_collection
import solr

SOLR_URL = os.environ.get('URL_SOLR_API', None)
SOLR_API_KEY = os.environ.get('SOLR_API_KEY', None)

SOLR = solr.SearchHandler(
        solr.Solr(
            SOLR_URL,
            post_headers={
                'X-Authentication-Token': SOLR_API_KEY,
            },
        ),
        "/query"
)

def fill_object_values_from_solr(doc):
    '''If no object field, try to get from current solr
    '''
    if 'object' not in doc:
        query='harvest_id_s:"{}"'.format(doc['_id'])
        msg = "NO OBJECT FOR {}".format(doc['_id'])
        resp = SOLR(q=query,
            fields='harvest_id_s, reference_image_md5, id, collection_url, reference_image_dimensions',
                )
        results = resp.results
        if results:
            solr_doc =  results[0]
            if 'reference_image_md5' in solr_doc:
                doc['object'] = solr_doc['reference_image_md5']
                doc['object_dimensions'] = solr_doc['reference_image_dimensions'].split(':')
                print "OBJ DIM:{}".format(doc['object_dimensions'])
                print 'UPDATING OBJECT -- {}'.format(doc['_id'])
                return doc
        else:
            print 'NOT IN SOLR -- {}'.format(msg)
        return None

run_on_couchdb_by_collection(fill_object_values_from_solr,
        #collection_key="23066")
        collection_key="26094")

