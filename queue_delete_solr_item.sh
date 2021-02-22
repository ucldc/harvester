#!/usr/bin/env bash

# script to start from avram when collection selected for syncing collection
# to production couchdb
# queue_sync_couchdb.bash <url-to-collection-registry-api>

if [[ -n "$DEBUG" ]]; then 
  set -x
fi

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # http://stackoverflow.com/questions/59895
cd $DIR

. ~/.harvester-env
if [ -f ./bin/activate ]; then
set +u
. ./bin/activate
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
elif [ -f ~/workers_local/bin/activate ]; then
set +u
. ~/workers_local/bin/activate
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
else
    echo "NO ./bin/activate. You need to run \"virtualenv .\" . ./bin/activate"
    #exit 13;
fi
# 24hr timeout = 86400 secs
python scripts/queue_delete_solr_item.py ${@:1}
