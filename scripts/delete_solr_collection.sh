#run this from majorTom or a worker
if [[ -n "$DEBUG" ]]; then 
  set -x
fi

usage(){
    echo "Usage: delete_solr_collecdtion.sh <collection_id>"
    exit 1
}

if [ $# -ne 1 ];
  then
    usage
fi

c_url=https://registry.cdlib.org/api/v1/collection/${1}/
query="stream.body=<delete><query>collection_url:\"${c_url}\"</query></delete>"
url_get=${URL_SOLR}/update?${query}
echo $url_get

wget ${url_get}
