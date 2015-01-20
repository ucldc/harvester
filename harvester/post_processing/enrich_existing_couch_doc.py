from __future__ import print_function
import sys
import httplib
import json
import argparse
import couchdb
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
            "Pipeline-item": enrichment.replace('\n',''),
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

# NOTE: the enrichment must include one that selects the _id from data
# data should already have the "collection" data in originalRecord
def akara_enrich_doc(doc, enrichment, port=8889):
    '''Enrich a doc that already exists in couch
    gets passed document, enrichment string, akara port.
    '''
    newdoc = _get_enriched_doc(doc, enrichment, port)
    return _update_doc(doc, newdoc)

def main(doc_id, enrichment, port=8889):
    '''Run akara_enrich_doc for one document and save result'''
    _config = config()
    url_couchdb = _config.DPLA.get("CouchDb", "URL")
    couchdb_name = _config.DPLA.get("CouchDb", "ItemDatabase")
    _couchdb = couchdb.Server(url=url_couchdb)[couchdb_name]
    indoc = _couchdb.get(doc_id)
    doc = akara_enrich_doc(indoc, enrichment, port)
    _couchdb[doc_id] = doc

if __name__=='__main__':
    parser = argparse.ArgumentParser(
            description='Run enrichments on couchdb document')
    parser.add_argument('doc_id', help='couchdb document _id')
    parser.add_argument('enrichment',
            help='Comma separated string of akara enrichments to run. \
                    Must include enrichment to select id.')
    args = parser.parse_args()
    main(args.doc_id, args.enrichment)
