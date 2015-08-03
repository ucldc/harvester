import sys, os
import argparse
import solr
from harvester.collection_registry_client import Collection
import csv
import codecs
import cStringIO
import datetime
from harvester.couchdb_init import get_couchdb
from harvester.fetcher import OAC_XML_Fetcher

py_version = sys.version_info
if py_version.major == 2 and py_version.minor == 7 and py_version.micro > 8:
    #disable ssl verification
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

SOLR_URL = os.getenv('UCLDC_SOLR_URL',
        'https://ucldc-solr-stage.elasticbeanstalk.com/solr/')
COUCHDB_URL = os.getenv('COUCHDB_URL',
        'https://51.10.100.133/couchdb')

'https://ucldc-solr-stage.elasticbeanstalk.com/solr/query?q=*:*&rows=0&facet=true&facet.field=collection_url&facet.limit=1000'

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    Needed for unicode input, sure hope they fixed this in py 3
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def get_indexed_collection_list(SOLR):
    '''Use the facet query to extract the collection urls for 
    collections in index.
    Returns list of tuples (collection_url, #in coll)
    '''
    query_results = SOLR(q='*:*',
               rows=0,
               facet='true',
               facet_limit='-1',
               facet_field=['collection_url'],
        )
    collection_urls = query_results.facet_counts['facet_fields']['collection_url']
    return [(c_url, num) for c_url, num in collection_urls.items()]

def get_couch_count(cdb, cid):
    view_name='_design/all_provider_docs/_view/by_provider_name_count'
    results = cdb.view(view_name,
                    key=c.id)
    for row in results:
        return row.value

if __name__=="__main__":
    parser = argparse.ArgumentParser(
        description='Make csv report of indexed collections')
    parser.add_argument('auth_token', help='Authentication token')
    parser.add_argument('--solr_url', help='Solr index url')
    parser.add_argument('--couchdb_url', help='CouchDB url')
    args = parser.parse_args()
    solr_url = args.solr_url if args.solr_url else SOLR_URL
    print "SOLR_URL:{}".format(solr_url)
    SOLR = solr.SearchHandler(
                solr.Solr(
                    solr_url,
                    post_headers = {
                        'X-Authentication-Token': args.auth_token,
                        },
                ),
            "/query"
    )
    if args.couchdb_url:
        cdb = get_couchdb(url_couchdb=couchdb_url, dbname='ucldc')
    else:
        cdb = get_couchdb(dbname='ucldc')
    collections = get_indexed_collection_list(SOLR)
    date_to_minute = datetime.datetime.now().strftime('%Y%m%d-%H%M')
    fname = 'indexed_collections-{}.csv'.format(date_to_minute)
    with open(fname, 'wb') as csvfile:
        csvwriter = UnicodeWriter(csvfile)
        csvwriter.writerow(('Collection Name', 'Collection URL',
                'Number in index', 'Number in couchdb', 'Number in OAC',
                'Couch missing in solr', 'OAC missing in couch',
                'Repository Name', 'Repository URL',
                'Campus'))
        for c_url, num in collections:
            try:
                c = Collection(c_url)
            except ValueError, e:
                print "NO COLLECTION FOR :{}".format(c_url)
                continue
            couch_count = get_couch_count(cdb, c.id)
            solr_equal_couch = False
            if couch_count == num:
                solr_equal_couch = True
            oac_num = None
            couch_equal_oac = None
            if c.harvest_type == 'OAC':
                fetcher = OAC_XML_Fetcher(c.url_harvest, c.harvest_extra_data)
                oac_num = fetcher.totalDocs
                if couch_count == oac_num:
                    couch_equal_oac = True
                else:
                    couch_equal_oac = False
            csvwriter.writerow((c['name'], c_url, str(num), str(couch_count),
                str(oac_num), str(solr_equal_couch), str(couch_equal_oac),
                c.repository[0]['name'] if len(c.repository) else '',
                c.repository[0]['resource_uri']if len(c.repository) else '',
                c.campus[0]['name'] if len(c.campus) else ''))
    print 'Created {}'.format(fname)
