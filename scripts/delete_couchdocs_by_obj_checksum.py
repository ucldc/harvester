#! /bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
from harvester.couchdb_init import get_couchdb
from harvester.couchdb_sync_db_by_collection import delete_id_list
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter

def confirm_deletion(count, objChecksum, cid):
    prompt = "\nDelete {0} documents with object checksum {1} from Collection {2}? yes to confirm\n".format(count, objChecksum, cid)
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        else:
            return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Delete all documents in given collection matching given object checksum. ' \
        'Use for metadata-only records that can only be identified by value in object field ' \
         'USAGE: delete_couchdocs_by_obj_checksum.py [collection id] [object value]')
    parser.add_argument('cid', help='Collection ID')
    parser.add_argument('objChecksum', help='CouchDB "object" value of documents to delete')
    args = parser.parse_args(sys.argv[1:])
    if not args.cid or not args.objChecksum:
        parser.print_help()
        sys.exit(27)

    ids = []
    _couchdb = get_couchdb()
    rows = CouchDBCollectionFilter(collection_key=args.cid, couchdb_obj=_couchdb)
    for row in rows:
        couchdoc = row.doc
        if 'object' in couchdoc and couchdoc['object'] == args.objChecksum:
            couchID = couchdoc['_id']
            ids.append(couchID)
    if not ids:
        print 'No docs found with object checksum matching {}'.format(args.objChecksum)
        sys.exit(27)

    if confirm_deletion(len(ids), args.objChecksum, args.cid):
        num_deleted, delete_ids = delete_id_list(ids, _couchdb=_couchdb)
        print 'Deleted {} documents'.format(num_deleted)
    else:
        print "Exiting without deleting"

# Copyright © 2016, Regents of the University of California
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# - Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# - Neither the name of the University of California nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
