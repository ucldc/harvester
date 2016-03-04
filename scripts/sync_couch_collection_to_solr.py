# -*- coding: utf-8 -*-
import datetime
import os
from solr import Solr, SolrException
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter
from harvester.couchdb_init import get_couchdb
from harvester.solr_updater import map_couch_to_solr_doc, push_doc_to_solr

# This works from inside an environment with default URLs for couch & solr 
URL_SOLR = os.environ.get('URL_SOLR', None)

def main(collection_key):
    v = CouchDBCollectionFilter(couchdb_obj=get_couchdb(),
                                collection_key=collection_key)
    solr_db = Solr(URL_SOLR)
    results = []
    for r in v:
        dt_start = dt_end = datetime.datetime.now()
        solr_doc = map_couch_to_solr_doc(r.doc)
        solr_doc = push_doc_to_solr(solr_doc, solr_db=solr_db)
        dt_end = datetime.datetime.now()
    solr_db.commit() #commit updates
    return results


if __name__=="__main__":
    import argparse
    import sys
    parser = argparse.ArgumentParser(description='Sync collection to production couchdb')
    parser.add_argument('collection_key', type=str,
            help='Numeric ID for collection')
    args = parser.parse_args(sys.argv[1:])
    print "DELETE COLLECTION TO CAPTURE ANY REMOVALS"
    print "hit any key to continue"
    raw_input()
    main(args.collection_key)

    # arg will be just id
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

