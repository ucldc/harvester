#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" calisphere new production index QA script """

import sys
import argparse
import re
from datetime import date
import itertools
import json
import csv
import xlsxwriter
import requests
from requests.auth import HTTPDigestAuth
from pprint import pprint as pp
import ConfigParser
import time
import datetime
import os

base_query = {
    'facet': 'true',
    'facet.field': [
        'collection_data',
    ],
    'facet.missing': 'on',
    'rows': 0,
    'facet.limit': -1, #give them all
}

def get_solr_json(solr_url, base_query, api_key=None, digest_user=None,
        digest_pswd=None):
    '''get the solr json response for the given URL.
    Use the requests library. The production API uses headers for auth,
    while the ingest environment uses http digest auth
    Returns an python object created from the json response.
    '''
    solr_auth = { 'X-Authentication-Token': api_key } if api_key else None
    digest_auth = HTTPDigestAuth(digest_user, digest_pswd) if digest_user else None
    return json.loads(requests.get(solr_url,
                                    headers=solr_auth,
                                    auth=digest_auth,
                                    params=base_query,
                                    verify=False).text)

def get_total_docs(json_results):
    '''Return total docs in the response'''
    return int(json_results.get('response').get('numFound'))

def create_facet_dict(json_results, facet_field):
    '''Create a dictionary consisting of keys = facet_field_values, values =
    facet_field coutns
    Takes a json result that must have the given facet field in it & the
    '''
    results = json_results.get('facet_counts').get('facet_fields').get(facet_field)
    #zip into array with elements of ('collection_data_value', count)
    facet_list = zip(*[iter(results)]*2)
    dmap = {}
    for val, count in facet_list:
        if count > 0: #reject ones that have 0 records?
            dmap[val] = count
    return dmap

def get_registry_collection_data():
    '''Return a dictionary of ready for publication collections doublelist data
    and a NOT ready for publication dict from the registry api
    '''

    url_base = 'https://registry.cdlib.org'
    url_base_api = url_base + '/api/v1/collection/'
    offset=0
    limit=100
    colls=[]
    while 1:
        url='{}?limit={}&offset={}'.format(url_base_api, limit, offset)
        objs = requests.get(url).json()['objects']
        if not len(objs):
            break
        colls.extend(objs)
        offset+=limit
    ready_for_pub = []
    not_ready_for_pub = []
    for c in colls:
        coll_doublelist = u'{}{}::{}'.format(url_base, c['resource_uri'], c['name'])
        c['doublelist']=coll_doublelist
        if c['ready_for_publication']:
            ready_for_pub.append(c)
        else:
            not_ready_for_pub.append(c)
    return ready_for_pub, not_ready_for_pub

def compare_datasets(prod_facet_dict, new_facet_dict):
    '''This does the heavy lifting.
    First, find what collections are in prod but not new & vice versa.
    Then for collections in both, compare the counts
    '''
    not_in_new = []
    not_in_prod = []
    count_equal = []
    new_less = []
    new_more = []
    prod_coll_set = set([ name for name, count in prod_facet_dict.items()])
    new_coll_set = set([ name for name, count in new_facet_dict.items()])
    pp('OLD SET LEN:{} NEW LEN:{}'.format(len(prod_coll_set),
                                        len(new_coll_set)))
    not_in_new_set = prod_coll_set.difference(new_coll_set)
    for coll in not_in_new_set:
        not_in_new.append((coll, prod_facet_dict[coll]))
    not_in_prod_set = new_coll_set.difference(prod_coll_set)
    for coll in not_in_prod_set:
        not_in_prod.append((coll, new_facet_dict[coll]))
    in_both = prod_coll_set.intersection(new_coll_set)
    for coll in in_both:
        count_prod = prod_facet_dict[coll]
        count_new = new_facet_dict[coll]
        if count_prod == count_new:
            count_equal.append((coll, count_prod, count_new))
        elif count_prod > count_new:
            new_less.append((coll, count_prod, count_new))
        else:
            new_more.append((coll, count_prod, count_new))

    return not_in_new, not_in_prod, count_equal, new_less, new_more 

def find_missing_ready_for_pub_collections(ready_for_pub, new_facet_dict):
    '''Return any collections that are marked "ready for publication" that
    aren't in new index
    Should be empty!
    '''
    missing = []
    for c in ready_for_pub:
        if c['doublelist'] not in new_facet_dict:
            missing.append(c)
    return missing


def find_not_ready_for_pub_collections(not_ready_for_pub, new_facet_dict):
    '''Find any collections marked NOT "ready for publication" that are in the 
    new index
    '''
    found = []
    for c in not_ready_for_pub:
        if c['doublelist'] in new_facet_dict:
            found.append(c)
    return found
                            
def create_totals_page(workbook, header_format, number_format,
        runtime, total_prod, total_new, type_ss_prod, type_ss_new):
    '''For the 2 pages reporting collections missing from an index,
    create a page
    '''
    page = workbook.add_worksheet('Index Totals')
    n_format = workbook.add_format()
    n_format.set_num_format(number_format.num_format)
    if total_prod > total_new:
        page.set_tab_color('red')
        n_format.set_bg_color('red')
    else:
        page.set_tab_color('green')
        n_format.set_bg_color('green')

    # headers
    page.write(0, 0, '', header_format)
    page.write(0, 1, 'Production', header_format)
    page.write(0, 2, 'New Index', header_format)
    page.write(0, 3, 'Difference', header_format)
    # width
    page.set_column(0, 0, 25, )
    page.set_column(1, 3, 10, )

    #write total docs
    page.write(1, 0, 'Total Docs', header_format)
    page.write(1, 1, total_prod, number_format)
    page.write(1, 2, total_new, number_format)
    page.write_formula(1, 3, '=C2-B2', n_format)

    row = 3
    for key, val in type_ss_prod.items():
        new_val = type_ss_new.get(key, 0)
        if key == None:
            key = 'None'
        page.write(row, 0, key, header_format)
        page.write(row, 1, val)
        page.write(row, 2, new_val)
        page.write_formula(row, 3, '=C{0}-B{1}'.format(row+1, row+1))
        row = row + 1
    page.write(row, 4, runtime)

def create_missing_collections_page(workbook, header_format, number_format,
        runtime, page_name, data, tab_color=None):
    '''For the 2 pages reporting collections missing from an index,
    create a page
    '''
    page = workbook.add_worksheet(page_name)
    if tab_color:
        page.set_tab_color(tab_color)
        n_format = workbook.add_format()
        n_format.set_bg_color(tab_color)
        n_format.set_num_format(number_format.num_format)
    else:
        n_format = number_format

    # headers
    page.write(0, 0, 'Collection URL', header_format)
    page.write(0, 1, 'Collection', header_format)
    page.write(0, 2, 'Count', header_format)
    # width
    page.set_column(0, 0, 40, )
    page.set_column(1, 1, 43, )
    page.set_column(2, 2, 10, )
    row = 1
    for item in data:
        c_url, c_name = item[0].split('::')
        page.write(row, 0, c_url)
        page.write(row, 1, c_name)
        page.write_number(row, 2, item[1], n_format)
        row = row + 1
    page.write_formula(row, 3, '=SUM(C2:C{})'.format(row))
    page.write(row, 4, runtime)

def create_counts_collections_page(workbook, header_format, number_format,
        runtime, page_name, data, tab_color=None):
    '''For the pages reporting collections with differing counts in the index,
    create a page
    '''
    page = workbook.add_worksheet(page_name)
    if tab_color:
        page.set_tab_color(tab_color)
        sum_format = workbook.add_format()
        sum_format.set_num_format(number_format.num_format)
        sum_format.set_bg_color(tab_color)
    else:
        sum_format = number_format
    # headers
    page.write(0, 0, 'Collection URL', header_format)
    page.write(0, 1, 'Collection', header_format)
    page.write(0, 2, 'Prod Count', header_format)
    page.write(0, 3, 'New Count', header_format)
    page.write(0, 4, 'Difference', header_format)
    # width
    page.set_column(0, 0, 40, )
    page.set_column(1, 1, 43, )
    page.set_column(2, 2, 10, )
    page.set_column(3, 3, 10, )
    page.set_column(4, 4, 10, )
    row = 1
    for item in data:
        c_url, c_name = item[0].split('::')
        page.write(row, 0, c_url)
        page.write(row, 1, c_name)
        page.write_number(row, 2, item[1], number_format)
        page.write_number(row, 3, item[2], number_format)
        page.write_formula(row, 4, '=C{}-D{}'.format(row+1, row+1), sum_format)
        row = row + 1
    page.write_formula(row, 5, '=SUM(E2:E{})'.format(row))
    page.write(row, 6, runtime)

def create_registry_publication_report(workbook, header_format, number_format,
        runtime, missing_ready_for_pub, not_ready_for_pub):
    '''Report any collections marked "ready for publication" that aren't in 
    new index & any the are NOT ready for publication that are in index
    '''
    pp("MISSING READY for PUB:{}".format(len(missing_ready_for_pub)))
    pp("FOUND NOT  READY:{}".format(len(not_ready_for_pub)))
    page = workbook.add_worksheet('Registry Publication State')
    page.set_tab_color('red')
    page.write(0, 0, 'Collection URL', header_format)
    page.write(0, 1, 'Collection', header_format)
    page.write(0, 2, 'Ready for Publication', header_format)
    page.write(0, 3, 'Index State', header_format)
    page.set_column(0, 0, 40, )
    page.set_column(1, 1, 43, )
    page.set_column(2, 2, 20, )
    page.set_column(3, 3, 10, )
    row = 1
    for c in missing_ready_for_pub:
        c_url, c_name = c['doublelist'].split('::')
        page.write(row, 0, c_url)
        page.write(row, 1, c_name)
        page.write(row, 2, c['ready_for_publication'])
        page.write(row, 3, 'MISSING')
        row +=1
    row += 3
    for c in not_ready_for_pub:
        c_url, c_name = c['doublelist'].split('::')
        page.write(row, 0, c_url)
        page.write(row, 1, c_name)
        page.write(row, 2, c['ready_for_publication'])
        page.write(row, 3, 'FOUND')
        row += 1
    row += 2
    page.write(row, 6, runtime)

def create_report_workbook(outdir, not_in_new, not_in_prod, count_equal,
                            new_less, new_more,
                            num_found_prod,
                            num_found_new,
                            type_ss_prod,
                            type_ss_new,
                            missing_ready_for_pub,
                            not_ready_for_pub):
    # now create a workbook, page one is In production but missing in new (BAD)
    # next is In new but not production (OK)
    # next is Equal Count (OK)
    # next is New Count less (BAD)
    # next is New Count more (OK)
    today = datetime.date.today()
    fileout = os.path.join(outdir, '{}-{}.xlsx'.format(today, 
                                            'production-to-new'))
    runtime = '{}'.format(time.ctime())

    # open the workbook
    workbook = xlsxwriter.Workbook(fileout)

    # formats
    header_format = workbook.add_format({'bold': True, })
    number_format = workbook.add_format()
    number_format.set_num_format('#,##0')

    #report totals
    create_totals_page(workbook, header_format, number_format,
        runtime, num_found_prod, num_found_new, type_ss_prod, type_ss_new)
    
    # set up a worksheet for each page
    # Collections not in the new index (BAD)
    create_missing_collections_page(workbook, header_format, number_format,
        runtime, 'Collections not in New Index', not_in_new, tab_color='red')

    # Collections with PRODUCTION COUNT GREATER (BAD!!!)
    create_counts_collections_page(workbook, header_format, number_format,
        runtime, 'PRODUCTION count GREATER', new_less, tab_color='red')

    # Collection not in current production (OK)
    create_missing_collections_page(workbook, header_format, number_format,
        runtime, 'Collections not in Production', not_in_prod,
        tab_color='yellow')

    # Collections with NEW COUNT GREATER (OK)
    create_counts_collections_page(workbook, header_format, number_format,
        runtime, 'New count greater', new_more,
        tab_color='yellow')

    # Collections with equal counts in both indexes (prod first)
    create_counts_collections_page(workbook, header_format, number_format,
        runtime, 'Index count EQUAL', count_equal,
        tab_color='green')

    if missing_ready_for_pub or not_ready_for_pub:
        create_registry_publication_report(workbook, header_format,
                number_format, runtime, missing_ready_for_pub,
                not_ready_for_pub)

    workbook.close()

def write_csv(outfile, data):
    '''Write a csv you can use to feed to other programs'''
    with open(outfile, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            c_url = row[0].split('::')[0]
            c_name = row[0].split('::')[1]
            split_row = [c_url, c_name]
            split_row.extend(row[1:])
            writer.writerow(split_row)

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('outdir', nargs=1,)

    if argv is None:
        argv = parser.parse_args()

    today = datetime.date.today()
    config = ConfigParser.SafeConfigParser()
    config.read('report.ini')

    #get totals for reporting on first page
    query_t = {
        'facet': 'true',
        'facet.field': [
            'type_ss',
            'facet_decade',
        ],
        'facet.missing': 'on',
        'rows': 0,
        'facet.limit': -1,
    }
    solr_url = config.get('calisphere', 'solrUrl')
    api_key = config.get('calisphere', 'solrAuth')
    production_totals = get_solr_json(solr_url, query_t, api_key=api_key)
    num_prod_docs = get_total_docs(production_totals)
    production_type_ss_dict = create_facet_dict(production_totals,
                                                'type_ss')
    solr_url = config.get('new-index', 'solrUrl')
    digest_user = config.get('new-index', 'digestUser')
    digest_pswd = config.get('new-index', 'digestPswd')
    new_totals = get_solr_json(solr_url, query_t, digest_user=digest_user,
            digest_pswd=digest_pswd)
    num_new_docs = get_total_docs(new_totals)
    new_type_ss_dict = create_facet_dict(new_totals,
                                        'type_ss')

    #get calisphere current index data
    solr_url = config.get('calisphere', 'solrUrl')
    api_key = config.get('calisphere', 'solrAuth')
    production_json = get_solr_json(solr_url, base_query, api_key=api_key)
    production_facet_dict = create_facet_dict(production_json,
                                                'collection_data')
    solr_url = config.get('new-index', 'solrUrl')
    digest_user = config.get('new-index', 'digestUser')
    digest_pswd = config.get('new-index', 'digestPswd')
    new_json = get_solr_json(solr_url, base_query, digest_user=digest_user,
            digest_pswd=digest_pswd)
    new_facet_dict = create_facet_dict(new_json,
                                        'collection_data')
    pp('OLD LEN:{} NEW LEN:{}'.format(len(production_facet_dict),
                                        len(new_facet_dict)))

    not_in_new, not_in_prod, count_equal, new_less, new_more = compare_datasets(production_facet_dict, new_facet_dict)
    ready_for_pub, not_ready_for_pub = get_registry_collection_data()
    pp("READY FOR PUB:{} NOT READY:{}".format(len(ready_for_pub),
        len(not_ready_for_pub)))
    missing_ready_for_pub = find_missing_ready_for_pub_collections(ready_for_pub, new_facet_dict)
    not_ready_for_pub = find_not_ready_for_pub_collections(not_ready_for_pub,
            new_facet_dict)

    pp('NOT IN NEW INDEX {}'.format(len(not_in_new)))
    pp('NOT IN PROD INDEX {}'.format(len(not_in_prod)))
    pp('COUNT EQUAL {}'.format(len(count_equal)))
    pp('NEW LESS {}'.format(len(new_less)))
    pp('NEW MORE {}'.format(len(new_more)))
    create_report_workbook(argv.outdir[0], not_in_new, not_in_prod, count_equal,
                            new_less, new_more,
                            num_found_prod=num_prod_docs,
                            num_found_new=num_new_docs,
                            type_ss_prod=production_type_ss_dict,
                            type_ss_new=new_type_ss_dict,
                            missing_ready_for_pub=missing_ready_for_pub,
                            not_ready_for_pub=not_ready_for_pub)
    write_csv(os.path.join(argv.outdir[0], '{}-collection_not_in_new.csv'.format(today)),
            not_in_new)
    write_csv(os.path.join(argv.outdir[0], '{}-missing_docs_in_new.csv'.format(today)),
            new_less)

if __name__ == "__main__":
    sys.exit(main())

"""
Copyright © 2016, Regents of the University of California
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from this
  software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""
