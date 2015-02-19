import sys
from harvester.post_processing.run_transform_on_couchdb_docs import run_on_couchdb_by_collection

RIGHTS_STATEMENT_DEFAULT = 'Please contact the contributing institution for more information regarding the copyright status of this object'
RIGHTS_STATEMENT_DEFAULT = 'Please contact the contributing institution for the copyright status of this object'

#RIGHTS_STATEMENT_DEFAULT = 'Please contact the contributing institution for more information regarding the copyright status of this object. Its presence on this site does not necessarily mean it is free from copyright restrictions.'

def fix_rights_status(doc):
    if len(doc['sourceResource']['rights']) == 2:
        if doc['sourceResource']['rights'][0] == 'copyright unknown' and \
            doc['sourceResource']['rights'][1] == RIGHTS_STATEMENT_DEFAULT:
                print "FIXING FOR:{}".format(doc['_id'])
                doc['sourceResource']['rights'] = [doc['sourceResource']['rights'][1]]
    return doc

collection_ids = [
"11073",
"11075",
"11076",
"11084",
"11134",
"11167",
"11439",
"11575",
"11582",
"1161",
"11705",
]
###"10046",
###"10059",
###"10063",
###"10067",
###"10126",
###"10130",
###"1021",
###"10246",
###"10318",
###"10328",
###"10343",
###"10363",
###"10387",
###"10422",
###"10425",
###"10616",
###"10632",
###"10664",
###"10671",
###"10710",
###"10722",
###"10732",
###"108",
###"10885",
###"10935",
###"11",
###"11010",
###"11068",
###"11073",
###"11075",
###"11076",
###"11084",
###"11134",
###"11167",
###"11439",
###"11575",
###"11582",
###"1161",
###"11705",
###]

for c in collection_ids:
    print "RUN FOR {}".format(c)
    sys.stderr.flush()
    sys.stdout.flush()
    run_on_couchdb_by_collection(fix_rights_status,
        collection_key=c)
