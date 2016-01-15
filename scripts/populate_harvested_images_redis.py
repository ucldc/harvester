'''one time script to populate redis with harvested image object data'''
from harvester.config import config
from harvester.couchdb_init import get_couchdb
from harvester.couchdb_pager import couchdb_pager
from redis import Redis
import redis_collections

_config = config()

_redis = Redis(host=_config['redis_host'],
               port=_config['redis_port'],
               password=_config['redis_password'],
               socket_connect_timeout=_config['redis_connect_timeout'])

object_cache = redis_collections.Dict(key='ucldc:harvester:harvested-images',
                        redis=_redis)


_couchdb = get_couchdb(url=_config['couchdb_url'], dbname='ucldc')
v = couchdb_pager(_couchdb, include_docs='true')
for r in v:
    doc = r.doc
    if 'object' in doc:
        did = doc['_id']
        if 'object_dimensions' not in doc:
            print "NO DIMS for {} -- not caching".format(did)
        else:
            object_cache[did] = [doc['object'], doc['object_dimensions']]
            print "OBJECT CACHE : {} === {}".format(did, object_cache[did])
