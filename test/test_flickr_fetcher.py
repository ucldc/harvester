# -*- coding: utf-8 -*-
from unittest import TestCase
import harvester.fetcher as fetcher
from test.utils import DIR_FIXTURES
from test.utils import LogOverrideMixin
import httpretty


class FlickrFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for the Flickr API.'''
    @httpretty.activate
    def testInit(self):
        '''Basic tdd start'''
        url = 'https://example.edu'
        user_id = 'testuser'
        page_size = 10
        url_first = fetcher.Flickr_Fetcher.url_get_photos_template.format(
            api_key='boguskey',
            user_id=user_id,
            per_page=page_size,
            page=1)
        httpretty.register_uri(
            httpretty.GET,
            url_first,
            body=open(DIR_FIXTURES+'/flickr-public_photos-1.xml').read())
        h = fetcher.Flickr_Fetcher(url, user_id, page_size=page_size)
        self.assertEqual(h.url_base, url)
        self.assertEqual(h.user_id, user_id)
        self.assertEqual(h.page_size, 10)
        self.assertEqual(h.page_current, 1)
        self.assertEqual(h.doc_current, 1)
        self.assertEqual(h.docs_fetched, 0)
        self.assertEqual(h.url_get_photos_template,
                         'https://api.flickr.com/services/rest/'
                         '?api_key={api_key}&user_id={user_id}&per_page'
                         '={per_page}&method='
                         'flickr.people.getPublicPhotos&page={page}')
        self.assertEqual(h.api_key, 'boguskey')
        self.assertEqual(h.url_current, url_first)
        self.assertEqual(h.docs_total, 30)
        self.assertEqual(h.url_get_photo_info_template,
                         'https://api.flickr.com/services/rest/'
                         '?api_key={api_key}&method='
                         'flickr.photos.getInfo&photo_id={photo_id}')
        h.doc_current = 30
        self.assertRaises(ValueError, h.next)
        h.docs_fetched = 30
        self.assertRaises(StopIteration, h.next)


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
