# -*- coding: utf-8 -*-
from __future__ import print_function
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
            api_key='boguskey', user_id=user_id, per_page=page_size, page=1)
        httpretty.register_uri(
            httpretty.GET,
            url_first,
            body=open(DIR_FIXTURES + '/flickr-public-photos-1.xml').read())
        h = fetcher.Flickr_Fetcher(url, user_id, page_size=page_size)
        self.assertEqual(h.url_base, url)
        self.assertEqual(h.user_id, user_id)
        self.assertEqual(h.page_size, 10)
        self.assertEqual(h.page_current, 1)
        self.assertEqual(h.doc_current, 0)
        self.assertEqual(h.docs_fetched, 0)
        self.assertEqual(h.url_get_photos_template,
                         'https://api.flickr.com/services/rest/'
                         '?api_key={api_key}&user_id={user_id}&per_page'
                         '={per_page}&method='
                         'flickr.people.getPublicPhotos&page={page}')
        self.assertEqual(h.api_key, 'boguskey')
        self.assertEqual(h.url_current, url_first)
        self.assertEqual(h.docs_total, 10)
        self.assertEqual(h.url_get_photo_info_template,
                         'https://api.flickr.com/services/rest/'
                         '?api_key={api_key}&method='
                         'flickr.photos.getInfo&photo_id={photo_id}')

    @httpretty.activate
    def test_fetching(self):
        url = 'https://example.edu'
        user_id = 'testuser'
        page_size = 10
        url_first = fetcher.Flickr_Fetcher.url_get_photos_template.format(
            api_key='boguskey', user_id=user_id, per_page=page_size, page=1)
        # Ugly but works
        httpretty.register_uri(
            httpretty.GET,
            url_first,
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-public-photos-1.xml')
                    .read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-public-photos-1.xml')
                    .read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-public-photos-2.xml')
                    .read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-public-photos-3.xml')
                    .read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-public-photos-4.xml')
                    .read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/flickr-photo-info-0.xml').read(
                    ),
                    status=200),
            ])
        h = fetcher.Flickr_Fetcher(url, user_id, page_size=page_size)
        h.doc_current = 10
        self.assertRaises(ValueError, h.next)
        h.docs_fetched = 10
        self.assertRaises(StopIteration, h.next)
        h = fetcher.Flickr_Fetcher(url, user_id, page_size=page_size)
        total = 0
        all_objs = []
        for objs in h:
            total += len(objs)
            all_objs.extend(objs)
        self.assertEqual(total, 10)
        self.assertEqual(len(all_objs), 10)
        photo_obj = all_objs[0]
        key_list_values = {
            'description': {
                'text':
                'PictionID:56100666 - Catalog:C87-047-040.tif - '
                'Title:Ryan Aeronautical Negative Collection Image - '
                'Filename:C87-047-040.tif - - Image from the Teledyne Ryan '
                'Archives, donated to SDASM in the 1990s. Many of these '
                'images are from Ryan\'s UAV program-----Please Tag these '
                'images so that the information can be permanently stored '
                'with the digital file.---Repository: <a href='
                '"http://www.sandiegoairandspace.org/library/stillimages.'
                'html" rel="nofollow">San Diego Air and Space Museum </a>'
            },
            'isfavorite': '0',
            'views': '499',
            'farm': '5',
            'people': {
                'haspeople': '0',
                'text': None
            },
            'visibility': {
                'text': None,
                'isfamily': '0',
                'isfriend': '0',
                'ispublic': '1'
            },
            'originalformat': 'jpg',
            'owner': {
                'text': None,
                'nsid': "49487266@N07",
                'username': "San Diego Air & Space Museum Archives",
                'realname': "SDASM Archives",
                'location': "",
                'iconserver': "4070",
                'iconfarm': "5",
                'path_alias': "sdasmarchives",
            },
            'rotation': '0',
            'id': '34394586825',
            'dates': {
                'text': None,
                'lastupdate': '1493683351',
                'posted': '1493683350',
                'taken': '2017-05-01 17:02:30',
                'takengranularity': '0',
                'takenunknown': '1',
            },
            'originalsecret': 'd46e9b19cc',
            'license': '7',
            'title': {
                'text': 'Ryan Aeronautical Image'
            },
            'media': 'photo',
            'notes': [{
                'x': '10',
                'authorname': 'Bees',
                'text': 'foo',
                'w': '50',
                'author': '12037949754@N01',
                'y': '10',
                'h': '50',
                'id': '313'
            }],
            'tags': [{
                'raw': 'woo yay',
                'text': 'wooyay',
                'id': '1234',
                'author': '12037949754@N01'
            }, {
                'raw': 'hoopla',
                'text': 'hoopla',
                'id': '1235',
                'author': '12037949754@N01'
            }],
            'publiceditability': {
                'text': None,
                'cancomment': '1',
                'canaddmeta': '1'
            },
            'comments': {
                'text': '0'
            },
            'server': '4169',
            'dateuploaded': '1493683350',
            'secret': '375e0b1706',
            'safety_level': '0',
            'urls': [{
                'text':
                'https://www.flickr.com/photos/sdasmarchives/34394586825/',
                'type': 'photopage'
            }],
            'usage': {
                'text': None,
                'canblog': '0',
                'candownload': '1',
                'canprint': '0',
                'canshare': '1'
            },
            'editability': {
                'text': None,
                'cancomment': '0',
                'canaddmeta': '0'
            },
        }
        self.assertEqual(len(photo_obj.keys()), len(key_list_values.keys()))
        for k, v in key_list_values.items():
            self.assertEqual(photo_obj[k], v)


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
