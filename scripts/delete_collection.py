import sys
import argparse
import os
import couchdb
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter

COUCHDB_URL = os.environ.get('COUCHDB_URL', 'http://127.0.0.1:5984')
COUCHDB_DB = os.environ.get('COUCHDB_DB', 'ucldc')
COUCHDB_VIEW = 'all_provider_docs/by_provider_name'
username = os.environ.get('COUCHDB_USER', None)
password = os.environ.get('COUCHDB_PASSWORD', None)
url = COUCHDB_URL.split("//")
url_server = "{0}//{1}:{2}@{3}".format(url[0], username, password, url[1])

def confirm_deletion(cid):
    prompt = "Are you sure you want to delete all couchdb " + \
             "documents for %s? yes to confirm\n" % cid
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        else:
            return False

def delete_collection(cid):
    _couchdb = couchdb.Server(url_server)[COUCHDB_DB]
    rows = CouchDBCollectionFilter(collection_key=cid,
                                        couchdb_obj=_couchdb
                                        )
    deleted = []
    num_deleted = 0
    for row in rows:
        doc = _couchdb.get(row['id'])
        deleted.append(row['id'])
        _couchdb.delete(doc)
        num_deleted +=1
    return num_deleted, deleted 

if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description='Delete all documents in given collection')
    parser.add_argument('collection_id',
                        help='Registry id for the collection')
    args = parser.parse_args(sys.argv[1:])
    if confirm_deletion(args.collection_id):
        num, deleted_ids = delete_collection(args.collection_id)
        print "DELTED {} DOCS".format(num)
        print "DELETED IDS: {}".format(deleted_ids)
        print "DELTED {} DOCS".format(num)
    else:
        print "Exiting without deleting"
