from harvester.post_processing.run_transform_on_couchdb_docs import run_on_couchdb_by_collection

# pass in a Couchdb doc, get back one with de-duplicated sourceResource values
def dedupe_sourceresource(doc):
    ''' Look for duplicate values in the doc['sourceResource'] and 
    remove.
    Values must be *exactly* the same
    '''
    for key, value in doc['sourceResource'].items():
        if isinstance(value, list):
            # can't use set() because of dict values (non-hashable)
            new_list = []
            for item in value:
                if item not in new_list:
                    new_list.append(item)
            doc['sourceResource'][key] = new_list
    return doc

if __name__=='__main__':
    doc_ids = run_on_couchdb_by_collection(dedup_sourceresource)
    print "NUMBER OF DOCS DEDUPED:{}".format(len(doc_ids))
