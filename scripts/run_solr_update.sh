#!/bin/env bash

if [[ -n "$DEBUG" ]]; then 
  set -x
fi

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

. ~/.harvester-env

set +o nounset
. ~/workers_local/bin/activate
set -o nounset

dt=`date '+%Y%m%d_%H%M%S'`

python ~/code/harvester/harvester/solr_updater.py http://127.0.0.1:5984 ucldc \
  http://10.0.1.13:8080/solr &> /var/local/solr-indexes/log/solr-up-${dt}.out &
