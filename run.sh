#!/bin/bash

#RUN akara and worker code?
cd /code/dpla/ingestion

if [ ! -f /code/dpla/ingestion/akara.conf ]; then
    # setup akara conf
    function subst() { eval echo -E "$2"; }
    mapfile -c 1 -C subst < akara.ini.tmpl > akara.ini
    python setup.py install
    akara -f akara.conf setup
fi

if [ ! -f /code/harvester/rqw-settings.py ]; then
    # setup rqw-settings (need to pass in env vars)
    function subst() { eval echo -E "$2"; }
    mapfile -c 1 -C subst < /code/harvester/rqw-settings.py.tmpl > /code/harvester/rqw-settings.py
fi

#start -f for ignoring pid, -X for debug output to stdout, useful from docker logs
akara -f akara.conf start -f

cd /code/harvester
echo "REDIS_HOST=$REDIS_HOST"
echo "COUCHDB=$COUCHDB_URL"
rqworker --config rqw-settings --verbose
