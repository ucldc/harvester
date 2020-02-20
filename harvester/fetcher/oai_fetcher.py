# -*- coding: utf-8 -*-
import re
import tempfile
from urlparse import parse_qs
from .fetcher import Fetcher
from sickle import Sickle
from sickle.models import Record as SickleDCRecord
from pymarc import parse_xml_to_array
from lxml import etree


def etree_to_dict(t):
    d = {t.tag: map(etree_to_dict, t.iterchildren())}
    d.update(('@' + k, v) for k, v in t.attrib.iteritems())
    d['text'] = t.text
    return d

class SickleMARCRecord(SickleDCRecord):
    '''Extend the sickle Record to handle oai marc xml
    using pymarc's parse_xml_to_array function.

    parse_xml_to_array takes a file and returns an array
    of all records in the file, but in this case it's
    guaranteed to be just one record per file, because
    Sickle is handling iterating through the oai feed.

    SickleDCRecord definition:
    https://github.com/mloesch/sickle/blob/79d7c727af3a4437720116549d4c681e74799f7e/sickle/models.py#L120
    '''
    def __init__(self, record_element, strip_ns=True):
        super(SickleMARCRecord, self).__init__(
            record_element, strip_ns=strip_ns)
        if not self.deleted:
            marc_file = tempfile.TemporaryFile()
            metadata = self.xml.find(
                ".//" + self._oai_namespace + "metadata/")
            marc_file.write(
                etree.tostring(metadata, encoding='utf-8'))
            marc_file.seek(0)
            records = parse_xml_to_array(marc_file)
            self.metadata = records[0].as_json()

class SickleDIDLRecord(SickleDCRecord):
    '''Extend the Sickle Record to handle oai didl xml.
    Fills in data for the didl specific values

    After Record's __init__ runs, the self.metadata contains keys for the
    following DIDL data: DIDLInfo, Resource, Item, Component, Statement,
    Descriptor
    DIDLInfo contains created date for the data feed - drop
    Statement wraps the dc metadata

    Only the Resource & Component have unique data in them

    '''

    def __init__(self, record_element, strip_ns=True):
        super(SickleDIDLRecord, self).__init__(
            record_element, strip_ns=strip_ns)
        # need to grab the didl components here
        if not self.deleted:
            didl = self.xml.find('.//{urn:mpeg:mpeg21:2002:02-DIDL-NS}DIDL')
            didls = didl.findall('.//{urn:mpeg:mpeg21:2002:02-DIDL-NS}*')
            for element in didls:
                tag = re.sub(r'\{.*\}', '', element.tag)
                self.metadata[tag] = etree_to_dict(element)


class OAIFetcher(Fetcher):
    '''Fetcher for oai'''

    def __init__(self, url_harvest, extra_data, **kwargs):
        super(OAIFetcher, self).__init__(url_harvest, extra_data, **kwargs)
        # TODO: check extra_data?
        self.oai_client = Sickle(self.url)
        self._metadataPrefix = self.get_metadataPrefix(extra_data)
        # ensure not cached in module?
        self.oai_client.class_mapping['ListRecords'] = SickleDCRecord
        self.oai_client.class_mapping['GetRecord'] = SickleDCRecord
        if extra_data:  # extra data is set spec
            if 'set' in extra_data:
                params = parse_qs(extra_data)
                self._set = params['set'][0]
            else:
                self._set = extra_data
            # if metadataPrefix=didl, use didlRecord for parsing
            if self._metadataPrefix.lower() == 'didl':
                self.oai_client.class_mapping['ListRecords'] = SickleDIDLRecord
                self.oai_client.class_mapping['GetRecord'] = SickleDIDLRecord
            elif self._metadataPrefix.lower() == 'marcxml':
                self.oai_client.class_mapping['ListRecords'] = SickleMARCRecord
                self.oai_client.class_mapping['GetRecord'] = SickleMARCRecord
            self.records = self.oai_client.ListRecords(
                metadataPrefix=self._metadataPrefix,
                set=self._set,
                ignore_deleted=True)
        else:
            self.records = self.oai_client.ListRecords(
                metadataPrefix=self._metadataPrefix, ignore_deleted=True)

    def get_metadataPrefix(self, extra_data):
        '''Set the metadata format for the feed.
        If it is in extra_data, use that.
        Else, see if oai_qdc is supported, if so use that.
        Else, revert to oai_dc
        '''
        if extra_data:
            if 'metadataPrefix' in extra_data:
                params = parse_qs(extra_data)
                return params['metadataPrefix'][0]

        mdformats = [x for x in self.oai_client.ListMetadataFormats()]
        for f in mdformats:
            if f.metadataPrefix == 'oai_qdc':
                return 'oai_qdc'
        return 'oai_dc'

    def next(self):
        '''return a record iterator? then outside layer is a controller,
        same for all. Records are dicts that include:
        any metadata
        campus list
        repo list
        collection name
        '''
        while True:
            sickle_rec = self.records.next()
            if not sickle_rec.deleted:
                break  # good record to harvest, don't do deleted
                # update process looks for deletions
        rec = sickle_rec.metadata
        rec['datestamp'] = sickle_rec.header.datestamp
        rec['id'] = sickle_rec.header.identifier
        return rec


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
