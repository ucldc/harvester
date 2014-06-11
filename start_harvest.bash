#!/usr/bin/env bash

# script to start from avram when collection selected for harvesting
# start_harvest.bash <user-email> <url-to-collection-registry-api>

if [[ -n "$DEBUG" ]]; then 
  set -x
fi

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
##set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # http://stackoverflow.com/questions/59895
cd $DIR

. ./bin/activate
python queue_harvest.py "$1" "$2"
