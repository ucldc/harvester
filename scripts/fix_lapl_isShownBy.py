import sys
from harvester.post_processing.run_transform_on_couchdb_docs import run_on_couchdb_by_collection

def fix_lapl_isShownBy(doc):
    '''Some urls are broken but fixable.'''
    if 'sola1' in doc.get('isShownBy', ''):
        print 'Fixing {}'.format(doc['_id'])
        doc['isShownBy'] = doc['isShownBy'].replace('sola1', '00000')
        return doc
    else:
        return None

run_on_couchdb_by_collection(fix_lapl_isShownBy,
        collection_key="26094")
