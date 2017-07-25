# -*- coding: utf-8 -*-
import urlparse
import json
import pynux.utils
import boto
from .fetcher import Fetcher
from deepharvest.deepharvest_nuxeo import DeepHarvestNuxeo

STRUCTMAP_S3_BUCKET = 'static.ucldc.cdlib.org/media_json'
NUXEO_MEDIUM_IMAGE_URL_FORMAT = "https://nuxeo.cdlib.org/Nuxeo/nxpicsfile/" \
    "default/{}/Medium:content/"
NUXEO_S3_THUMB_URL_FORMAT = "https://s3.amazonaws.com/" \
    "static.ucldc.cdlib.org/ucldc-nuxeo-thumb-media/{}"


class NuxeoFetcher(Fetcher):
    '''Harvest a Nuxeo FILE. Can be local or at a URL'''

    def __init__(self, url_harvest, extra_data, conf_pynux={}, **kwargs):
        '''
        uses pynux (https://github.com/ucldc/pynux) to grab objects from
        the Nuxeo API

        api url is set from url_harvest, overriding pynuxrc config and
        passed in conf.

        the pynux config file should have user & password
        and X-NXDocumemtProperties values filled in.
        '''
        super(NuxeoFetcher, self).__init__(url_harvest, extra_data, **kwargs)
        self._url = url_harvest
        self._path = extra_data
        self._nx = pynux.utils.Nuxeo(conf=conf_pynux)
        self._nx.conf['api'] = self._url
        self._structmap_bucket = STRUCTMAP_S3_BUCKET

        # get harvestable child objects
        conf_pynux['api'] = self._url
        self._dh = DeepHarvestNuxeo(self._path, '', conf_pynux=conf_pynux)

        self._children = iter(self._dh.fetch_objects())

    def _get_structmap_url(self, bucket, obj_key):
        '''Get structmap_url property for object'''
        structmap_url = "s3://{0}/{1}{2}".format(bucket, obj_key,
                                                 '-media.json')
        return structmap_url

    def _get_structmap_text(self, structmap_url):
        '''
           Get structmap_text for object. This is all the words from 'label'
           in the json.
           See https://github.com/ucldc/ucldc-docs/wiki/media.json
        '''
        structmap_text = ""

        bucketpath = self._structmap_bucket.strip("/")
        bucketbase = bucketpath.split("/")[0]
        parts = urlparse.urlsplit(structmap_url)

        # get contents of <nuxeo_id>-media.json file
        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucketbase)
        key = bucket.get_key(parts.path)
        if not key:  # media_json hasn't been harvested yet for this record
            self.logger.error('Media json at: {} missing.'.format(parts.path))
            return structmap_text
        mediajson = key.get_contents_as_string()
        mediajson_dict = json.loads(mediajson)

        # concatenate all of the words from 'label' in the json
        labels = []
        labels.append(mediajson_dict['label'])
        if 'structMap' in mediajson_dict:
            labels.extend([sm['label'] for sm in mediajson_dict['structMap']])
        structmap_text = ' '.join(labels)
        return structmap_text

    def _get_isShownBy(self, nuxeo_metadata):
        '''
            Get isShownBy value for object
            1) if object has image at parent level, use this
            2) if component(s) have image, use first one we can find
            3) if object has PDF or video at parent level,
                use image stashed on S3
            4) return None
        '''
        is_shown_by = None
        uid = nuxeo_metadata['uid']
        self.logger.info("About to get isShownBy for uid {}".format(uid))

        # 1) if object has image at parent level, use this
        if self._has_image(nuxeo_metadata):
            self.logger.info("Nuxeo doc with uid {} has an image at the "
                             "parent level".format(uid))
            is_shown_by = NUXEO_MEDIUM_IMAGE_URL_FORMAT.format(nuxeo_metadata[
                'uid'])
            self.logger.info("is_shown_by: {}".format(is_shown_by))
            return is_shown_by

        # 2) if component(s) have image, use first one we can find
        first_image_component_uid = self._get_first_image_component(
            nuxeo_metadata)
        self.logger.info("first_image_component_uid: {}".format(
            first_image_component_uid))
        if first_image_component_uid:
            self.logger.info("Nuxeo doc with uid {} has an image at the"
                             "component level".format(uid))
            is_shown_by = NUXEO_MEDIUM_IMAGE_URL_FORMAT.format(
                first_image_component_uid)
            self.logger.info("is_shown_by: {}".format(is_shown_by))
            return is_shown_by

        # 3) if object has PDF at parent level, use image stashed on S3
        if self._has_s3_thumbnail(nuxeo_metadata):
            self.logger.info("Nuxeo doc with uid {} has a thumbnail for"
                             "parent file (probably PDF) stashed on S3".format(
                                 uid))
            is_shown_by = NUXEO_S3_THUMB_URL_FORMAT.format(nuxeo_metadata[
                'uid'])
            self.logger.info("is_shown_by: {}".format(is_shown_by))
            return is_shown_by

        # 4) return None
        self.logger.info("Could not find any image for Nuxeo doc with uid "
                         "{}! Returning None".format(uid))
        return is_shown_by

    def _has_image(self, metadata):
        ''' based on json metadata, determine whether or not this Nuxeo doc
        has an image file associated
        '''

        if metadata['type'] != "SampleCustomPicture":
            return False

        properties = metadata['properties']
        file_content = properties.get('file:content')
        if file_content and 'data' in file_content:
            return True
        else:
            return False

    def _has_s3_thumbnail(self, metadata):
        ''' based on json metadata, determine whether or not this Nuxeo doc
        is PDF (or other non-image)
            that will have thumb image stashed on S3 for it '''
        if metadata['type'] not in ("CustomFile", "CustomVideo"):
            return False

        properties = metadata['properties']
        file_content = properties.get('file:content')
        if file_content and 'data' in file_content:
            return True
        else:
            return False

    def _get_first_image_component(self, parent_metadata):
        ''' get first image component we can find '''
        component_uid = None

        query = "SELECT * FROM Document WHERE ecm:parentId = '{}' AND " \
                "ecm:currentLifeCycleState != 'deleted' ORDER BY " \
                "ecm:pos".format(parent_metadata['uid'])
        for child in self._nx.nxql(query):
            child_metadata = self._nx.get_metadata(uid=child['uid'])
            if self._has_image(child_metadata):
                component_uid = child_metadata['uid']
                break

        return component_uid

    def next(self):
        '''Return Nuxeo record by record to the controller'''
        doc = self._children.next()
        self.metadata = self._nx.get_metadata(uid=doc['uid'])
        self.structmap_url = self._get_structmap_url(self._structmap_bucket,
                                                     doc['uid'])
        self.metadata['structmap_url'] = self.structmap_url
        self.metadata['structmap_text'] = self._get_structmap_text(
            self.structmap_url)
        self.metadata['isShownBy'] = self._get_isShownBy(self.metadata)

        return self.metadata


class UCLDCNuxeoFetcher(NuxeoFetcher):
    '''A nuxeo fetcher that verifies headers required for UCLDC metadata
    from the UCLDC Nuxeo instance.
    Essentially, this checks that the X-NXDocumentProperties is correct
    for the UCLDC
    '''

    def __init__(self, url_harvest, extra_data, conf_pynux={}, **kwargs):
        '''Check that required UCLDC properties in conf setting'''
        super(UCLDCNuxeoFetcher, self).__init__(url_harvest, extra_data,
                                                conf_pynux, **kwargs)
        assert ('dublincore' in self._nx.conf['X-NXDocumentProperties'])
        assert ('ucldc_schema' in self._nx.conf['X-NXDocumentProperties'])
        assert ('picture' in self._nx.conf['X-NXDocumentProperties'])


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
