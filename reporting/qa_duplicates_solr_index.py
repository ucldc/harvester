# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import time
import xlsxwriter
from get_solr_json import get_solr_json, create_facet_dict

facet_query = { 'q': '*:*',
        'rows' : 0,
        'wt' : 'json',
        'facet': 'true',
        }

def get_facet_query(solr_url, field, **kwargs):
    '''Return a facet data dict to muck with based on "field")
    '''
    query = facet_query.copy()
    query.update({'facet.field':field})
    solr_json = get_solr_json(solr_url=solr_url, query=query, **kwargs)
    return create_facet_dict(solr_json, field)

def create_report_workbook(outdir): 
    today = datetime.date.today()
    fileout = os.path.join(outdir, '{}-{}.xlsx'.format(today, 
                                            'duplicates'))
    runtime = '{}'.format(time.ctime())

    # open the workbook
    workbook = xlsxwriter.Workbook(fileout)

    # formats
    header_format = workbook.add_format({'bold': True, })
    number_format = workbook.add_format()
    number_format.set_num_format('#,##0')
    return workbook, header_format, number_format

def main(solr_url='https://52.10.100.133/solr/dc-collection/query',
        outdir=None,
        api_key=None, digest_user=None, digest_pswd=None):
    field = 'reference_image_md5'
    #print "======FIELD:{} {} {}".format(field, digest_user, digest_pswd)
    #print "======FIELD:{} {} {}".format(field, api_key, solr_url)
    dup_md5 = get_facet_query(solr_url, field, api_key=api_key,
            digest_user=digest_user, digest_pswd=digest_pswd)
    #now for each md5, get the collection_url that it is in
    for md5, count in dup_md5.items():
        query = { 'q': md5,
            'rows' : 0,
            'wt' : 'json',
            'facet': 'true',
            'facet.field': 'collection_url'
            }
        collection_urls = create_facet_dict(get_solr_json(solr_url, 
                query=query, api_key=api_key,
                digest_user=digest_user, digest_pswd=digest_pswd),
                'collection_url')
        dup_md5[md5] = (count, collection_urls)
    workbook, header_format, number_format = create_report_workbook(outdir)
    page = workbook.add_worksheet(field)
    # headers
    page.write(0, 0, field, header_format)
    page.write(0, 1, 'Number Dups', header_format)
    page.write(0, 2, 'Collections', header_format)
    # width
    page.set_column(0, 0, 50, )
    page.set_column(1, 1, 10, )
    page.set_column(2, 10, 50, )
    row = 1
    for md5, data in dup_md5.items():
        page.write(row, 0, md5)
        page.write(row, 1, data[0])
        column = 2
        for c_url, num in data[1].items():
            coll_data = ' - '.join((c_url, str(num)))
            page.write(row, column, coll_data)
            column += 1
        row += 1


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--solr_url', type=str)
    parser.add_argument('--api_key', type=str)
    parser.add_argument('--digest_user', type=str)
    parser.add_argument('--digest_pswd', type=str)
    parser.add_argument('outdir', nargs=1, type=str)

    args = parser.parse_args()
   
    print "ARGS OUTDIR:{}".format(args.outdir)
    solr_url = args.solr_url if args.solr_url else 'https://52.10.100.133/solr/dc-collection/query'
    sys.exit(main(solr_url=solr_url, outdir=args.outdir[0],
        api_key=args.api_key,
        digest_user=args.digest_user, digest_pswd=args.digest_pswd))

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

