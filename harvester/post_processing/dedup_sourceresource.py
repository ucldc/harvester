# pass in a Couchdb doc, get back one with de-duplicated sourceResource values


def dedup_sourceresource(doc):
    ''' Look for duplicate values in the doc['sourceResource'] and 
    remove.
    Values must be *exactly* the same
    '''
    for key, value in doc['sourceResource'].items():
        if not isinstance(value, basestring):
            new_list = []
            for item in value:
                if item not in new_list:
                    new_list.append(item)
            doc['sourceResource'][key] = new_list
    return doc
