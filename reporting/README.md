# harvester/reporting

in `report.ini`:
```ini
[calisphere]
solrUrl = .../solr/query
solrAuth = api_key

#for solr ingest indexes behind digest auth
[new-index]
solrUrl = https://harvest-prd.cdlib.org/solr/dc-collection/query
solrAuth = api_key
```
