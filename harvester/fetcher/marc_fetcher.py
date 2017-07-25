# -*- coding: utf-8 -*-
import urllib
import tempfile
from xml.etree import ElementTree as ET
from pymarc import MARCReader
import pymarc
from .fetcher import Fetcher


class MARCFetcher(Fetcher):
    '''Harvest a MARC FILE. Can be local or at a URL'''

    def __init__(self, url_harvest, extra_data, **kwargs):
        '''Grab file and copy to local temp file'''
        super(MARCFetcher, self).__init__(url_harvest, extra_data, **kwargs)
        self.url_marc_file = url_harvest
        self.marc_file = tempfile.TemporaryFile()
        self.marc_file.write(urllib.urlopen(self.url_marc_file).read())
        self.marc_file.seek(0)
        self.marc_reader = MARCReader(
            self.marc_file, to_unicode=True, utf8_handling='replace')

    def next(self):
        '''Return MARC record by record to the controller'''
        return self.marc_reader.next().as_dict()


class AlephMARCXMLFetcher(Fetcher):
    '''Harvest a MARC XML feed from Aleph. Currently used for the
    UCSB cylinders project'''

    def __init__(self, url_harvest, extra_data, page_size=500, **kwargs):
        '''Grab file and copy to local temp file'''
        super(AlephMARCXMLFetcher, self).__init__(url_harvest, extra_data,
                                                  **kwargs)
        self.ns = {'zs': "http://www.loc.gov/zing/srw/"}
        self.page_size = page_size
        self.url_base = url_harvest + '&maximumRecords=' + str(self.page_size)
        self.current_record = 1
        tree_current = self.get_current_xml_tree()
        self.num_records = self.get_total_records(tree_current)

    def get_url_current_chunk(self):
        '''Set the next URL to retrieve according to page size and current
        record'''
        return ''.join((self.url_base, '&startRecord=',
                        str(self.current_record)))

    def get_current_xml_tree(self):
        '''Return an ElementTree for the next xml_page'''
        url = self.get_url_current_chunk()
        return ET.fromstring(urllib.urlopen(url).read())

    def get_total_records(self, tree):
        '''Return the total number of records from the etree passed in'''
        return int(tree.find('.//zs:numberOfRecords', self.ns).text)

    def next(self):
        '''Return MARC records in sets to controller.
        Break when last record position == num_records
        '''
        if self.current_record >= self.num_records:
            raise StopIteration
        # get chunk from self.current_record to self.current_record + page_size
        tree = self.get_current_xml_tree()
        recs_xml = tree.findall('.//zs:record', self.ns)
        # advance current record to end of set
        self.current_record = int(recs_xml[-1].find('.//zs:recordPosition',
                                                    self.ns).text)
        self.current_record += 1
        # translate to pymarc records & return
        marc_xml_file = tempfile.TemporaryFile()
        marc_xml_file.write(ET.tostring(tree))
        marc_xml_file.seek(0)
        recs = [
            rec.as_dict() for rec in pymarc.parse_xml_to_array(marc_xml_file)
            if rec is not None
        ]
        return recs


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
