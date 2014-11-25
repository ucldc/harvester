import httplib
import json
from harvester.config import config

def _get_source(doc):
    '''Return the "source". For us use the registry collection url.
    if collection or collection field not found, report and reraise error.
    '''
    try:
        source = doc['originalRecord']['collection'][0]['@id']
        return source
    except KeyError, e:
        print("No originalRecord.collection for document ID: {}".format(
                                    doc['_id']))
        print("KEYS:{}".format(doc['originalRecord'].keys()))
        raise e

def _get_enriched_doc(doc, enrichment, port):
    '''Submit the document to the Akara enrichment endpoint'''
    source = _get_source(doc)
    conn = httplib.HTTPConnection("localhost", port)
    headers = {
            "Source": source,
            "Content-Type": "application/json",
            "Pipeline-item": enrichment,
            }
    conn.request("POST", "/enrich", json.dumps([doc['originalRecord']]), headers)
    resp = conn.getresponse()

    if not resp.status == 200:
        raise Exception("Error (status {}) for doc {}".format(
                                resp.status, doc['_id']))
    data = json.loads(resp.read())
    # there should only be one
    assert(len(data['enriched_records'].keys()) == 1)
    return data['enriched_records'][data['enriched_records'].keys()[0]]

def _update_doc(doc, newdoc):
    '''Update the original doc with new information from new doc, while
    not hammering any data in original doc.
    for now I'm modifying the input doc, but should probably return copy?
    '''
    doc['originalRecord'].update(newdoc['originalRecord'])
    del(newdoc['originalRecord'])
    doc['sourceResource'].update(newdoc['sourceResource'])
    del(newdoc['sourceResource'])
    doc.update(newdoc)
    return doc

def akara_enrich_doc(doc, enrichment, port=8889):
    '''Enrich a doc that already exists in couch
    gets passed document, enrichment string, akara port.
    '''
    newdoc = _get_enriched_doc(doc, enrichment, port)
    return _update_doc(doc, newdoc)
