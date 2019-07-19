# -*- coding: utf-8 -*-
import urllib
import json
from .fetcher import Fetcher


class IA_Fetcher(Fetcher):
    '''A fetcher for the Internet Archive.
    Put a dummy URL in harvesturl field such as 'https://example.edu'

    In the 'extra_data' field, put in the Lucene-formatted search parameters
    to return whatever body of records you want to constitute the Calisphere
    collection. For example:
        collection:environmentaldesignarchive AND subject:"edith heath"
    Will return all content with subject "edith heath" from the Environmental
    Design Archives collection. 'collection' is almost always a required field,
    as it should correspond to the institution/library we are harvesting from.
    All other metadata field searches are optional but available.

    More search query help and example queries here:
    https://archive.org/advancedsearch.php

    '''

    url_advsearch = 'https://archive.org/advancedsearch.php?' \
        'q={search_query}&rows=500&page={page_current}&output=json'

    def __init__(self, url_harvest, extra_data, **kwargs):
        self.url_base = url_harvest
        self.search_query = extra_data
        self.page_current = 1
        self.doc_current = 0

    def next(self):
        self.url_current = self.url_advsearch.format(
            page_current=self.page_current, search_query=self.search_query)
        search = urllib.urlopen(self.url_current).read()
        results = json.loads(search)
        self.doc_total = results["response"]["numFound"]

        if self.doc_current >= self.doc_total:
            if self.doc_current != self.doc_total:
                raise ValueError(
                    "Number of documents fetched ({0}) doesn't match \
                    total reported by server ({1})".format(
                        self.doc_current, self.doc_total))
            else:
                raise StopIteration

        docList = results['response']['docs']
        if len(docList) == 0:
            raise ValueError(
                "No results for URL ({0}) -- Check search syntax".format(
                    self.searchURL))
        else:
            objset = []
            for g in docList:
                objset.append(g)
        self.page_current += 1
        self.doc_current += len(objset)
        return objset


# Copyright © 2017, Regents of the University of California
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
