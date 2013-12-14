import os
from sickle import Sickle

URL_SOLR = os.environ.get('URL_SOLR', 'http://107.21.228.130:8080/solr/dc-collection/')

class Harvester(object):
    '''Base class for harvest objects.'''
    campus_valid = ['UCB', 'UCD', 'UCI', 'UCLA', 'UCM', 'UCSB', 'UCSC', 'UCSD', 'UCSF', 'CDL']
    def __init__(self, user_email, collection_name, campuses, repositories, harvest_type, url_harvest, extra_data):
        self.user_email = user_email
        self.collection_name = collection_name
        self.campuses = []
        for campus in campuses:
            if campus not in self.campus_valid:
                raise ValueError('Campus value '+campus+' in not one of '+str(self.campus_valid))
            self.campuses.append(campus)
        self.repositories = repositories
        self.type = harvest_type
        self.url = url_harvest
        self.extra_data = extra_data

    def __iter__(self):
        return self

    def next(self):
        raise NotImplementedError

    def email_user(self, msg):
        '''Email the user who initiated the harvest'''
        pass


class OAIHarvester(Harvester):
    '''Harvester for oai'''
    def __init__(self, user_email, collection_name, campuses, repositories, url_harvest, extra_data):
        super(OAIHarvester, self).__init__(user_email, collection_name, campuses, repositories, 'OAI', url_harvest, extra_data)
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
        #add required metadata to oai record
        #campus, repo, collection name
        rec['collection_name'] = self.collection_name
        if 'publisher' not in rec:
            rec['publisher'] = []
        rec['publisher'].append(self.campuses)
        rec['publisher'].append(self.repositories)
        rec['campus'] = self.campuses
        rec['repository'] = self.repositories
        return rec


def get_harvester_for_collection_type(harvest_type):
    '''Get the correct harvester for a given type of harvest'''
    # the mapping needs to go somewhere
    harvest_types = { 'OAI': OAIHarvester,
            }
    return harvest_types.get(harvest_type, None)

def harvest_collection(user_email, collection_name, campuses, repositories, harvest_type, url_harvest, extra_data):
    '''Harvest a collection'''
    pass
