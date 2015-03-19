'''Objects to wrap the avram collection api'''
import os
from os.path import expanduser
import json
import ConfigParser
import requests


api_host = os.environ.get('REGISTRY_HOST', 'registry.cdlib.org')
api_path = '/api/v1/'
url_base = os.environ.get('URL_REGISTRY_API', ''.join(('https://', api_host)))


class ResourceIterator(object):
    '''An iterator over a registry api resource type'''
    def __init__(self, url_base, path, object_type):
        '''Assumes offset of 0 on first get'''
        self.url_base = url_base
        self.object_type = object_type
        self._get_next(path)
        self.total_returned = 0

    def __iter__(self):
        return self

    def _parse_result(self, json):
        '''parse the api json output'''
        self.total_count = json['meta']['total_count']
        self.limit = json['meta']['limit']
        self.offset = json['meta']['offset']
        self.page_end = self.offset + self.limit
        self.path_previous = json['meta']['previous']
        self.path_next = json['meta']['next']
        self.objects = json['objects']
        self.obj_list_index = -1  # this is current with set of objects

    def _get_next(self, url_next):
        '''get next result and parse'''
        self.url = self.url_base + url_next
        resp = requests.get(self.url).json()
        self._parse_result(resp)

    def next(self):
        '''Iterate over objects, get one at a time'''
        if self.total_returned > self.total_count:
            raise StopIteration
        self.obj_list_index += 1
        if self.obj_list_index + self.offset >= self.total_count:
            raise StopIteration
        if self.obj_list_index + self.offset >= self.page_end:
            if not self.path_next:
                raise StopIteration
            self._get_next(self.path_next)
            self.total_returned += 1
            self.obj_list_index += 1
        else:
            self.total_returned += 1
        # TODO: smarter conversion here
        return Collection(url_base=self.url_base, json_obj=self.objects[self.obj_list_index]) \
            if self.object_type == 'collection' \
            else self.objects[self.obj_list_index]


class Registry(object):
    '''A class to obtain tastypie api sets of objects from our registry.
    Objects can be a campus, respository or collection'''
    def __init__(self, url_base=url_base,
                 url_api=''.join((url_base, api_path))):
        self.url_base = url_base
        self.url_api = url_api
        resp = requests.get(url_api).json()
        self.endpoints = {}
        for obj_type, obj in resp.items():
            self.endpoints[obj_type] = obj['list_endpoint']

    def resource_iter(self, object_type, filter=None):
        '''Get an iterator for the resource at the given endpoint.
        '''
        if object_type not in self.endpoints:
            raise ValueError('Unknown type of resource {0}'.format(object_type))
        path = self.endpoints[object_type] if not filter \
            else self.endpoints[object_type] + '?' + filter
        return ResourceIterator(url_base, path, object_type)


class Collection(dict):
    '''A representation of the avram collection, as presented by the
    tastypie api
    '''
    def __init__(self, url_api=None, url_base=None, json_obj=None): 
        if url_base and json_obj:
            self.url = url_base + json_obj['resource_uri']
            self.update(json_obj)
            self.__dict__.update(json_obj)
        elif url_api:
            self.url = url_api
            api_json = requests.get(url_api).json()
            self.update(api_json)
            self.__dict__.update(api_json)
        else:
            raise Exception(
                'Must supply a url to collection api or json data and api base url')
        # use the django id for "provider", maybe url translated eventually
        self.id = self.provider = self['resource_uri'].strip('/').rsplit('/', 1)[1]
        self._auth = None

    def _build_contributor_list(self):
        '''Build the dpla style contributor list from the campus and
        repositories
        This will need review
        '''
        clist = []
        for campus in self.campus:
            campus_dict = dict(name=campus['slug'])
            campus_dict['@id'] = campus['resource_uri']
            clist.append(campus_dict)
        for repository in self.repository:
            repository_dict = dict(name=repository['slug'])
            repository_dict['@id'] = repository['resource_uri']
            clist.append(repository_dict)
        return clist

    @property
    def dpla_profile_obj(self):
        '''Return a json string appropriate for creating a dpla ingest profile.
        First create dictionary that is correct and then serialize'''
        if not self.enrichments_item:
            raise ValueError("NO ITEM ENRICHMENTS FOR COLLECTION, WILL FAIL!")
        profile = {}
        profile['name'] = self.provider
        profile['contributor'] = self._build_contributor_list()
        profile['enrichments_coll'] = ['/compare_with_schema', ]
        profile['thresholds'] = {
            "added": 100000,
            "changed": 100000,
            "deleted": 1000
        }
        profile['enrichments_item'] = [s.strip() for s in
                                       self.enrichments_item.split(',')]
        return profile

    @property
    def dpla_profile(self):
        return json.dumps(self.dpla_profile_obj)

    @property
    def auth(self):
        '''Return a username, password tuple suitable for authentication
        if the remote objects require auth for access.

        This is a bit of a hack, we know that the nuxeo style collections
        require Basic auth which is stored in our pynuxrc.

        If other types of collections require authentication to dowload
        objects, we'll have to come up with a clever way of storing that
        info in collection registry.
        '''
        if not self.harvest_type == 'NUX':
            return None
        if self._auth:
            return self._auth
        # return self.auth #ConfigParser doesn't like return trips
        # for now just grab auth directly from pynux, could also
        # have option for passing in.
        config = ConfigParser.SafeConfigParser()
        config.readfp(open(os.path.join(expanduser("~"), '.pynuxrc')))
        self._auth = (config.get('nuxeo_account', 'user'),
                      config.get('nuxeo_account', 'password'))
        return self._auth
