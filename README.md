UCLDC Harvester
===============

Harvester for the ucldc solr index. Pushes content into the raw solr index.

[![Build Status](https://travis-ci.org/ucldc/harvester.png?branch=master)](https://travis-ci.org/ucldc/harvester)

## To run the OPL image harvester script

### First, obtain a token from the preservica server.
* To do so, go here: https://us.preservica.com/Render/render/external?entity=TypeFile&entityRef=de9eacaf-b777-497c-8de9-21aab36d590a and login with the ops email & password.
* View the page source
* Find the value of the "token" parameter that has been put in the image link

### log on to the ingest-stg majorTom machine and run

```shell
python ~/code/havester/scripts/image_harvest_opl_preservica.py <collection id> <token value>
```
