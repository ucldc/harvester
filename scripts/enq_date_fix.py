from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.post_processing.run_transform_on_couchdb_docs import run_on_couchdb_doc
from harvester.post_processing.fix_repeated_displayDate import fix_repeated_date
import harvester

import sys

fname = sys.argv[1]

cid_list = [ x.strip() for x in open(fname).readlines()]

for cid in cid_list:
  results = CouchDBJobEnqueue().queue_collection(
    cid,
    300,
    run_on_couchdb_doc,
    'harvester.post_processing.fix_repeated_displayDate.fix_repeated_date')
