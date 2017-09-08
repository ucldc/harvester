# -*- coding: utf-8 -*-
import requests
from requests.auth import HTTPBasicAuth
from xml.etree import ElementTree as ET
from xmljson import badgerfish
from .fetcher import Fetcher


class CMISAtomFeedFetcher(Fetcher):
    '''harvest a CMIS Atom Feed. Don't know how generic this is, just working
    with Oakland Public Library Preservica implementation.

    Right now this uses the "descendants" page for collections, this gets all
    the data for one collection from one http request then parses the resulting
    data. This might not work if we get collections much bigger than the
    current ones (~1000 objects max)
    '''

    def __init__(self, url_harvest, extra_data, **kwargs):
        '''Grab file and copy to local temp file'''
        super(CMISAtomFeedFetcher, self).__init__(url_harvest, extra_data)
        # parse extra data for username,password
        uname, pswd = extra_data.split(',')
        resp = requests.get(url_harvest,
                            auth=HTTPBasicAuth(uname.strip(), pswd.strip()))
        self.tree = ET.fromstring(resp.content)
        self.objects = [
            badgerfish.data(x)
            for x in self.tree.findall('./{http://www.w3.org/2005/Atom}'
                                       'entry/{http://docs.oasis-open.org/'
                                       'ns/cmis/restatom/200908/}children//'
                                       '{http://www.w3.org/2005/Atom}entry')
        ]
        self.objects_iter = iter(self.objects)

    def next(self):
        return self.objects_iter.next()


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
