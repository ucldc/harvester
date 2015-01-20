from harvester.config import parse_env

REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_CONNECT_TIMEOUT, ID_EC2_INGEST, ID_EC2_SOLR_BUILD = parse_env()
