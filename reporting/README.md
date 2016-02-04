# harvester/reporting

in `report.ini`:
```ini
[calisphere]
solrUrl = .../solr/query
solrAuth = X-Authentication-Token-header

#for solr ingest indexes behind digest auth
[new-index]
solrUrl = https://52.11.194.40/solr/dc-collection/query
digestUser = <your digest username or ask mark>
digestPswd = <your password>
```
