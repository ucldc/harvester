# -*- coding: utf-8 -*-
import os
import urllib
import re
from xml.etree import ElementTree as ET
from .fetcher import Fetcher


class Flickr_Fetcher(Fetcher):
    '''A fetcher for the Flicr API.
    Currently, it takes a user id and grabs the flickr.people.getPublicPhotos
    to get the list of all photos.
    It then proceeds to use flickr.photos.getInfo to get metadata for the
    photos

    NOTE: This fetcher DOES NOT use the url_harvest. The extra_data should
    be the Flickr user id, such as 49487266@N07
    '''

    url_get_photos_template = 'https://api.flickr.com/services/rest/' \
        '?api_key={api_key}&user_id={user_id}&per_page={per_page}&method=' \
        'flickr.people.getPublicPhotos&page={page}'
    url_get_photo_info_template = 'https://api.flickr.com/services/rest/' \
        '?api_key={api_key}&method=flickr.photos.getInfo&photo_id={photo_id}'

    def __init__(self,
                 url_harvest,
                 extra_data,
                 page_size=500,
                 page_range=None,
                 **kwargs):
        self.url_base = url_harvest
        self.user_id = extra_data
        self.api_key = os.environ.get('FLICKR_API_KEY', 'boguskey')
        self.page_size = page_size
        self.page_current = 1
        self.doc_current = 0
        self.docs_fetched = 0
        xml = urllib.urlopen(self.url_current).read()
        total = re.search('total="(?P<total>\d+)"', xml)
        self.docs_total = int(total.group('total'))
        page_total = re.search('pages="(?P<page_total>\d+)"', xml)
        self.page_total = int(page_total.group('page_total'))
        if page_range:
            start, end = page_range.split(',')
            self.page_start = int(start)
            self.page_end = int(end)
            self.page_current = self.page_start
            if self.page_end >= self.page_total:
                self.page_end = self.page_total
                docs_last_page = self.docs_total - \
                    ((self.page_total - 1) * self.page_size)
                self.docs_total = (self.page_end - self.page_start) * \
                    self.page_size + docs_last_page
            else:
                self.docs_total = (self.page_end - self.page_start + 1) * \
                    self.page_size

    @property
    def url_current(self):
        return self.url_get_photos_template.format(
            api_key=self.api_key,
            user_id=self.user_id,
            per_page=self.page_size,
            page=self.page_current)

    def parse_tags_for_photo_info(self, info_tree):
        '''Parse the sub tags of a photo info objects and add to the
        photo dictionary.
        see: https://www.flickr.com/services/api/flickr.photos.getInfo.html
        for a description of the photo info xml
        '''
        tag_info = {}
        for t in info_tree:
            # need to handle tags, notes and urls as lists
            if t.tag in ('tags', 'notes', 'urls'):
                sub_list = []
                for subt in t.getchildren():
                    sub_obj = subt.attrib
                    sub_obj['text'] = subt.text
                    sub_list.append(sub_obj)
                tag_info[t.tag] = sub_list  # should i have empty lists?
            else:
                tobj = t.attrib
                tobj['text'] = t.text
                tag_info[t.tag] = tobj
        return tag_info

    def next(self):
        if self.doc_current >= self.docs_total:
            if self.docs_fetched != self.docs_total:
                raise ValueError(
                    "Number of documents fetched ({0}) doesn't match \
                    total reported by server ({1})"
                    .format(self.docs_fetched, self.docs_total))
            else:
                raise StopIteration
        if hasattr(self, 'page_end') and self.page_current > self.page_end:
            raise StopIteration
        # for the given page of public photos results,
        # for each <photo> tag, create an object with id, server & farm saved
        # then get the info for the photo and add to object
        # return the full list of objects to the harvest controller
        tree = ET.fromstring(urllib.urlopen(self.url_current).read())
        photo_list = tree.findall('.//photo')
        objset = []
        for photo in photo_list:
            photo_obj = photo.attrib
            url_photo_info = self.url_get_photo_info_template.format(
                api_key=self.api_key, photo_id=photo_obj['id'])
            ptree = ET.fromstring(urllib.urlopen(url_photo_info).read())
            photo_info = ptree.find('.//photo')
            photo_obj.update(photo_info.attrib)
            photo_obj.update(
                self.parse_tags_for_photo_info(photo_info.getchildren()))
            self.docs_fetched += 1
            objset.append(photo_obj)

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
