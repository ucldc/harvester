'''This allows running a bit of code on couchdb docs.
code should take a json python object, modify it and hand back to the code
Not quite that slick yet, need way to pass in code or make this a decorator
'''
import importlib
from harvester.collection_registry_client import Collection
from harvester.couchdb_init import get_couchdb

COUCHDB_VIEW = 'all_provider_docs/by_provider_name'


def run_on_couchdb_by_collection(func, collection_key=None):
    '''If collection_key is none, trying to grab all of docs and modify
    func is a function that takes a couchdb doc in and returns it modified.
    (can take long time - not recommended)
    Function should return new document or None if no changes made
    '''
    _couchdb = get_couchdb()
    v = _couchdb.view(COUCHDB_VIEW, include_docs='true', key=collection_key) \
        if collection_key else _couchdb.view(COUCHDB_VIEW,
                                             include_docs='true')
    doc_ids = []
    n = 0
    for r in v:
        n += 1
        doc_new = func(r.doc)
        if doc_new and doc_new != doc:
            _couchdb.save(doc_new)
            doc_ids.append(r.doc['_id'])
        if n % 100 == 0:
            print '{} docs ran. Last doc:{}\n'.format(n, r.doc['_id'])
    return doc_ids

def run_on_couchdb_doc(docid, func):
    '''Run on a doc, by doc id'''
    _couchdb = get_couchdb()
    doc = _couchdb[docid]
    mod_name, func_name = func.rsplit('.', 1)
    fmod = importlib.import_module(mod_name)
    ffunc = getattr(fmod, func_name)
    doc_new = ffunc(doc)
    if doc_new and doc_new != doc:
        _couchdb.save(doc_new)
        return True
    return False


C_CACHE = {}
def update_collection_description(doc):
    cjson = doc['originalRecord']['collection'][0]
    # get collection description
    if 'description' not in cjson:
        if cjson['@id'] in C_CACHE:
            c = C_CACHE[cjson['@id']]
        else:
            c = Collection(url_api=cjson['@id'])
            C_CACHE[cjson['@id']] = c
        description = c['description'] if c['description'] else c['name']
        print('DOC: {} DESCRIP: {}'.format(
            doc['_id'], c['description'].encode('utf8')))
        doc['originalRecord']['collection'][0]['description'] = description
        doc['sourceResource']['collection'][0]['description'] = description
    return doc


def add_rights_and_type_to_collection(doc):
    cjson = doc['originalRecord']['collection'][0]
    # get collection description
    if cjson['@id'] in C_CACHE:
        c = C_CACHE[cjson['@id']]
    else:
        c = Collection(url_api=cjson['@id'])
        C_CACHE[cjson['@id']] = c
    doc['originalRecord']['collection'][0]['rights_status'] = c['rights_status']
    doc['originalRecord']['collection'][0]['rights_statement'] = c['rights_statement']
    doc['originalRecord']['collection'][0]['dcmi_type']=c['dcmi_type']
    if 'collection' in doc['sourceResource']:
        doc['sourceResource']['collection'][0]['rights_status'] = c['rights_status']
        doc['sourceResource']['collection'][0]['rights_statement'] = c['rights_statement']
        doc['sourceResource']['collection'][0]['dcmi_type'] = c['dcmi_type']
    else:
        doc['sourceResource']['collection'] = doc['originalRecord']['collection']
    return doc
