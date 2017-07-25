# -*- coding: utf-8 -*-
import os
import urllib
import json
from .fetcher import Fetcher


class YouTube_Fetcher(Fetcher):
    '''A fetcher for the youtube API.
    Find the "upload" playlist id for the user.
    This will be the extra_data value in the registry.
    Put a dummy URL in harvest url
    '''

    url_playlistitems = 'https://www.googleapis.com/youtube/v3/playlistItems' \
        '?key={api_key}&maxResults={page_size}&part=contentDetails&' \
        'playlistId={playlist_id}&pageToken={page_token}'

    url_video = 'https://www.googleapis.com/youtube/v3/videos?' \
        'key={api_key}&part=snippet&id={video_ids}'

    def __init__(self, url_harvest, extra_data, page_size=50, **kwargs):
        self.url_base = url_harvest
        self.playlist_id = extra_data
        self.api_key = os.environ.get('YOUTUBE_API_KEY', 'boguskey')
        self.page_size = page_size
        self.playlistitems = {'nextPageToken': ''}

    def next(self):
        try:
            nextPageToken = self.playlistitems['nextPageToken']
        except KeyError:
            raise StopIteration
        self.playlistitems = json.loads(
            urllib.urlopen(
                self.url_playlistitems.format(
                    api_key=self.api_key,
                    page_size=self.page_size,
                    playlist_id=self.playlist_id,
                    page_token=nextPageToken)).read())
        video_ids = [
            i['contentDetails']['videoId'] for i in self.playlistitems['items']
        ]
        video_items = json.loads(
            urllib.urlopen(
                self.url_video.format(
                    api_key=self.api_key, video_ids=','.join(video_ids))).read(
                ))['items']
        return video_items


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
