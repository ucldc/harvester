# -*- coding: utf-8 -*-
import requests
import json
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree as ET
from xmljson import badgerfish
from .fetcher import Fetcher

class PreservicaFetcher(Fetcher):
    '''Harvest from Preservica API
    https://demo.preservica.com/api/entity/documentation.html
    NOTE API calls may need updating in future:
    https://developers.preservica.com/blog/api-versioning-in-preservica-6-2
    '''

    def __init__(self, url_harvest, extra_data, **kwargs):
        super(PreservicaFetcher, self).__init__(url_harvest, extra_data)
        # parse extra data for username, password
        self.uname, self.pswd = extra_data.split(',')
        # build API Collection URL from url_harvest
        urlOne, urlTwo = url_harvest.split('SO_')
        collectionID = urlTwo.replace('/','')
        self.url_API = '/'.join(('https://us.preservica.com/api/entity/v6.0/structural-objects',
                            collectionID, 'children'))
        self.doc_total = 0
        self.doc_current = 0

    def _dochits_to_objset(self, docHits):
        objset = []
        #iterate through docHits
        for d in docHits:
            # need to descend two layers in API for object metadata
            url_object = d.text
            obj_resp = requests.get(url_object,
                                auth=HTTPBasicAuth(self.uname.strip(), self.pswd.strip()))
            objTree = ET.fromstring(obj_resp.content)
            for mdataRef in objTree.findall('{http://preservica.com/EntityAPI/v6.0}'
                                            'AdditionalInformation/{http://preservica.com/'
                                            'EntityAPI/v6.0}Metadata/{http://preservica.com/'
                                            'EntityAPI/v6.0}Fragment[@schema="http://www.open'
                                            'archives.org/OAI/2.0/oai_dc/"]'):
                url_mdata = mdataRef.text
            mdata_resp = requests.get(url_mdata,
                                auth=HTTPBasicAuth(self.uname.strip(), self.pswd.strip()))
            mdataTree = ET.fromstring(mdata_resp.content)
            object = [
                # strip namespace from JSON
                json.dumps(badgerfish.data(x)).replace('{http://purl.org/dc/elements/1.1/}','')
                for x in mdataTree.findall('{http://preservica.com/XIP/v6.0}MetadataContainer/'
                                           '{http://preservica.com/XIP/v6.0}Content/'
                                           '{http://www.openarchives.org/OAI/2.0/oai_dc/}dc')
            ]
            # need to inject Preservica ID into json for isShownAt/isShownBy
            preservica_id = url_object.split('information-objects/')[1]
            jRecord = json.loads(object[0])
            jRecord.update({"preservica_id": { "$": preservica_id } })
            object = [
                json.dumps(jRecord)
                ]
            objset.append(object)
        self.doc_current += len(docHits)
        if self.url_next:
            self.url_API = self.url_next
        return objset

    def next(self):
        resp = requests.get(self.url_API,
            auth=HTTPBasicAuth(self.uname.strip(), self.pswd.strip()))
        tree = ET.fromstring(resp.content)
        TotalResults = tree.find('{http://preservica.com/EntityAPI/v6.0}'
                                        'Paging/{http://preservica.com/'
                                        'EntityAPI/v6.0}TotalResults')
        self.doc_total = int(TotalResults.text)

        if self.doc_current >= self.doc_total:
            if self.doc_current != self.doc_total:
                raise ValueError(
                    "Number of documents fetched ({0}) doesn't match \
                    total reported by server ({1})".format(
                        self.doc_current, self.doc_total))
            else:
                raise StopIteration

        next_url = tree.find('{http://preservica.com/EntityAPI/v6.0}'
                                        'Paging/{http://preservica.com/'
                                        'EntityAPI/v6.0}Next')
        if next_url is not None:
            self.url_next = next_url.text
        else:
            self.url_next = None

        hits = tree.findall('{http://preservica.com/EntityAPI/v6.0}'
                                        'Children/{http://preservica.com/'
                                        'EntityAPI/v6.0}Child')
        return self._dochits_to_objset(hits)


# Copyright © 2016, Regents of the University of California
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# - Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# - Neither the name of the University of California nor the names of its
#   contributors may be used to endorse or promote products derived from this
#   software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
