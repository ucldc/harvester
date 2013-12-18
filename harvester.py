import os
import sys
from sickle import Sickle
import solr

URL_SOLR = os.environ.get('URL_SOLR', 'http://107.21.228.130:8080/solr/dc-collection/')

class Harvester(object):
    '''Base class for harvest objects.'''
    def __init__(self, url_harvest, extra_data):
        self.url = url_harvest
        self.extra_data = extra_data

    def __iter__(self):
        return self

    def next(self):
        raise NotImplementedError


class OAIHarvester(Harvester):
    '''Harvester for oai'''
    def __init__(self, url_harvest, extra_data):
        super(OAIHarvester, self).__init__(url_harvest, extra_data)
        #TODO: check extra_data?
        self.oai_client = Sickle(url_harvest)
        self.records = self.oai_client.ListRecords(set=extra_data, metadataPrefix='oai_dc')

    def next(self):
        '''return a record iterator? then outside layer is a controller, same for all. Records are dicts that include:
        any metadata
        campus list
        repo list
        collection name
        '''
        sickle_rec = self.records.next()
        rec = sickle_rec.metadata
        return rec


class HarvestController(object):
    '''Controller for the harvesting. Selects correct harvester for the given 
    collection, then retrieves records for the given collection, massages them
    to match the solr schema and then sends to solr for updates.
    '''
    campus_valid = ['UCB', 'UCD', 'UCI', 'UCLA', 'UCM', 'UCSB', 'UCSC', 'UCSD', 'UCSF', 'CDL']
    harvest_types = { 'OAI': OAIHarvester,
        }
    dc_elements = ['title', 'creator', 'subject', 'description', 'publisher', 'contributor', 'date', 'type', 'format', 'identifier', 'source', 'language', 'relation', 'coverage', 'rights']

    def __init__(self, user_email, collection_name, campuses, repositories, harvest_type, url_harvest, extra_data):
        self.user_email = user_email
        self.collection_name = collection_name
        self.campuses = []
        for campus in campuses:
            if campus not in self.campus_valid:
                raise ValueError('Campus value '+campus+' in not one of '+str(self.campus_valid))
            self.campuses.append(campus)
        self.repositories = repositories
        self.harvester = self.harvest_types.get(harvest_type, None)(url_harvest, extra_data)
        self.solr = solr.Solr(URL_SOLR)

    def validate_input_dict(self, indata):
        '''Validate the data from the harvester. Currently only DC elements
        supported'''
        if not isinstance(indata, dict):
            raise TypeError("Input data must be a dictionary")
        for key, value in indata.items():
            if key not in self.dc_elements:
                raise ValueError('Input data must be in DC elements. Problem key is:' + unicode(key))

    def create_solr_id(self, identifier):
        '''Create an id that is good for solr. Take campus, repo and collection
        name to form prefix to individual item id. Ensures unique ids in solr,
        in case any local ids are identical.
        May do something smarter when known GUIDs (arks, doi, etc) are in use.
        Takes a list of possible identifiers and creates a string id.
        '''
        if not isinstance(identifier, list):
            raise TypeError('Identifier field should be a list')
        campusStr = '-'.join(self.campuses)
        repoStr = '-'.join(self.repositories)
        sID = '-'.join((campusStr, repoStr, self.collection_name, identifier[0]))
        return sID

    def create_solr_doc(self, indata):
        '''Create a document that is compatible with our solr index.
        Currently it is not auto updated, this code will need to be touched
        when solr schema changes
        '''
        self.validate_input_dict(indata)
        #dc.title required
        if 'title' not in indata:
            raise ValueError('Item must have a title')
        sDoc = indata
        sDoc['id'] = self.create_solr_id(sDoc['identifier'])
        sDoc['collection_name'] = self.collection_name
        sDoc['campus'] = self.campuses
        sDoc['repository'] = self.repositories
        return sDoc

    def harvest(self):
        '''Harvest the collection'''
        for rec in self.harvester:
            #validate record
            solrDoc = self.create_solr_doc(rec)
            self.solr.add(solrDoc, commit=True)

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, nargs='?', help='user email')
    parser.add_argument('collection_name', type=str, nargs='?',
            help='name of collection in registry')
    parser.add_argument('campuses', type=str, nargs='?',
            help='Comma delimited string of campuses')
    parser.add_argument('repositories', type=str, nargs='?',
            help='Comma delimited string of repositories')
    parser.add_argument('harvest_type', type=str, nargs='?', help='Type of harvest (Only OAI)')
    parser.add_argument('url_harvest', type=str, nargs='?', help='URL for harvest')
    parser.add_argument('extra_data', type=str, nargs='?', help='String of extra data required by type of harvest')
    args = parser.parse_args()
    campus_list = args.campuses.split(',')
    repository_list = args.repositories.split(':-:')
    harvester = HarvestController(args.user_email, args.collection_name, campus_list, repository_list, args.harvest_type, args.url_harvest, args.extra_data)
    harvester.harvest()
