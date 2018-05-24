# -*- coding: utf-8 -*-
from __future__ import print_function
from unittest import TestCase
import harvester.fetcher as fetcher
from test.utils import DIR_FIXTURES
from test.utils import LogOverrideMixin
from mypretty import httpretty

# import httpretty


class YouTubeFetcherTestCase(LogOverrideMixin, TestCase):
    '''Test the fetcher for the youtube API.'''

    @httpretty.activate
    def testInit(self):
        '''Basic tdd start'''
        url = 'https://example.edu'
        playlist_id = 'testplaylist'
        page_size = 3
        url_first = fetcher.YouTube_Fetcher.url_playlistitems.format(
            api_key='boguskey',
            page_size=page_size,
            playlist_id=playlist_id,
            page_token='')
        httpretty.register_uri(
            httpretty.GET,
            url_first,
            body=open(DIR_FIXTURES + '/flickr-public-photos-1.xml').read())
        h = fetcher.YouTube_Fetcher(url, playlist_id, page_size=page_size)
        self.assertEqual(h.url_base, url)
        self.assertEqual(h.playlist_id, playlist_id)
        self.assertEqual(h.api_key, 'boguskey')
        self.assertEqual(h.page_size, page_size)
        self.assertEqual(h.playlistitems, {'nextPageToken': ''})
        self.assertEqual(
            h.url_playlistitems,
            'https://www.googleapis.com/youtube/v3/playlistItems'
            '?key={api_key}&maxResults={page_size}&part=contentDetails&'
            'playlistId={playlist_id}&pageToken={page_token}')
        self.assertEqual(
            h.url_video,
            'https://www.googleapis.com/youtube/v3/videos?'
            'key={api_key}&part=snippet&id={video_ids}'
            )

    @httpretty.activate
    def test_fetching(self):
        url = 'https://example.edu'
        playlist_id = 'testplaylist'
        page_size = 3
        url_first = fetcher.YouTube_Fetcher.url_playlistitems.format(
            api_key='boguskey',
            page_size=page_size,
            playlist_id=playlist_id,
            page_token='')
        url_vids = fetcher.YouTube_Fetcher.url_video
        # Ugly but works
        httpretty.register_uri(
            httpretty.GET,
            url_first,
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES +
                    '/youtube_playlist_with_next.json')
                    .read(),
                    status=200),
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/youtube_playlist_no_next.json')
                    .read(),
                    status=200),
            ])
        httpretty.register_uri(
            httpretty.GET,
            url_vids,
            body=open(DIR_FIXTURES + '/youtube_video.json').read(),
            status=200)
        h = fetcher.YouTube_Fetcher(url, playlist_id, page_size=page_size)
        vids = []
        for v in h:
            vids.extend(v)
        self.assertEqual(len(vids), 6)
        self.assertEqual(vids[0], {
            u'contentDetails': {
                u'definition': u'sd',
                u'projection': u'rectangular',
                u'caption': u'false',
                u'duration': u'PT19M35S',
                u'licensedContent': True,
                u'dimension': u'2d'
            },
            u'kind': u'youtube#video',
            u'etag':
            u'"m2yskBQFythfE4irbTIeOgYYfBU/-3AtVAYcRLEynWZprpf0OGaY8zo"',
            u'id': u'0Yx8zrbsUu8'
        })

    @httpretty.activate
    def test_single_fetching(self):
        url = 'http://single.edu'
        playlist_id = 'PLwtrWl_IBMJtjP5zMk6dVR-BRjzKqCPOM'
        url_vids = fetcher.YouTube_Fetcher.url_video
        httpretty.register_uri(
            httpretty.GET,
            url_vids,
            responses=[
                httpretty.Response(
                    body=open(DIR_FIXTURES + '/youtube_single_video.json').read(),
                    status=200)
            ])
        h = fetcher.YouTube_Fetcher(url, playlist_id)
        vids = []
        for v in h:
            vids.extend(v)
        self.assertEqual(len(vids), 1)
        self.assertEqual(vids[0], {
            u'contentDetails': {
                u'definition': u'sd',
                u'projection': u'rectangular',
                u'caption': u'false',
                u'duration': u'PT19M35S',
                u'licensedContent': True,
                u'dimension': u'2d'
            },
            u'kind': u'youtube#video',
            u'etag':
            u'"m2yskBQFythfE4irbTIeOgYYfBU/-3AtVAYcRLEynWZprpf0OGaY8zo"',
            u'id': u'0Yx8zrbsUu8'
        })


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
