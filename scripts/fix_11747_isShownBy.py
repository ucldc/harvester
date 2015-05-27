import sys
from harvester.post_processing.run_transform_on_couchdb_docs import run_on_couchdb_by_collection

def get_best_oac_image(doc):
    '''From the list of images, choose the largest one'''
    best_image = None
    if doc.has_key('originalRecord'): # guard weird input
        x = 0
        thumb = doc['originalRecord'].get('thumbnail', None)
        if thumb:
            if 'src' in thumb:
                x = thumb['X']
                best_image = thumb['src']
        ref_images = doc['originalRecord'].get('reference-image', [])
        if type(ref_images) == dict:
            ref_images = [ref_images]
        for obj in ref_images:
            if int(obj['X']) > x:
                x = int(obj['X'])
                best_image = obj['src']
        if best_image and not best_image.startswith('http'):
            best_image = '/'.join((URL_OAC_CONTENT_BASE, best_image))
    return best_image

url_content_base = 'http://content.cdlib.org/'
def fix_isShownBy_11747(doc):
    doc_ark = doc['isShownAt'].split('ark:')[1]
    doc_ark = 'ark:' + doc_ark
    doc['originalRecord']['thumbnail']['src'] = ''.join((url_content_base,
                                            doc_ark, '/thumbnail'))
    best_image = get_best_oac_image(doc)
    doc['isShownBy'] = best_image
    print "DOC: {} shownBy:{}".format(doc['_id'], doc['isShownBy'])
    return doc

run_on_couchdb_by_collection(fix_isShownBy_11747, collection_key="11747")
