import sys, os
import argparse
import solr
from harvester.collection_registry_client import Collection
import csv
import codecs
import cStringIO
class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
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


py_version = sys.version_info
if py_version.major == 2 and py_version.minor == 7 and py_version.micro > 8:
    #disable ssl verification
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
   

SOLR_URL = os.getenv('UCLDC_SOLR_URL',
        'https://ucldc-solr-stage.elasticbeanstalk.com/solr/')

'https://ucldc-solr-stage.elasticbeanstalk.com/solr/query?q=*:*&rows=0&facet=true&facet.field=collection_url&facet.limit=1000'

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



if __name__=="__main__":
    parser = argparse.ArgumentParser(
        description='Make csv report of indexed collections')
    parser.add_argument('auth_token', help='Authentication token')
    parser.add_argument('--solr_url', help='Solr index url')
    args = parser.parse_args()
    solr_url = args.sorl_url if args.solr_url else SOLR_URL
    print "AUTH:{}".format(args.auth_token)
    SOLR = solr.SearchHandler(
                solr.Solr(
                    solr_url,
                    post_headers = {
                        'X-Authentication-Token': args.auth_token,
                        },
                ),
            "/query"
    )
    collections = get_indexed_collection_list(SOLR)
    with open('indexed_collections.csv', 'wb') as csvfile:
        csvwriter = UnicodeWriter(csvfile)
        csvwriter.writerow(('Collection Name', 'Collection URL',
                'Number in index', 'Repository Name', 'Repository URL',
                'Campus'))
        for c_url, num in collections:
            c = Collection(c_url)
            #print c.repository[0]
            csvwriter.writerow((c['name'], c_url, str(num),
                c.repository[0]['name'] if len(c.repository) else '',
                c.repository[0]['resource_uri']if len(c.repository) else '',
                c.campus[0]['name'] if len(c.campus) else ''))
