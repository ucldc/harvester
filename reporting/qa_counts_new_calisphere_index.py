#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" calisphere new production index QA script """

import os
import sys
import argparse
import re
from datetime import date
import itertools
import json
import csv
import xlsxwriter
import requests
from pprint import pprint as pp
import ConfigParser
import time
import datetime
from get_solr_json import get_solr_json, create_facet_dict

URL_CALISPHERE_BASE_COLLECTION = 'https://calisphere.org/collections/'

base_query = {
    'facet': 'true',
    'facet.field': [
        'collection_url',
    ],
    'facet.missing': 'on',
    'rows': 0,
    'facet.limit': -1, #give them all
}

def get_total_docs(json_results):
    '''Return total docs in the response'''
    return int(json_results.get('response').get('numFound'))

def get_registry_collection_data():
    '''Return a dictionary of ready for publication collections data
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
        for o in objs:
            o['url'] = u'{}{}'.format(url_base, o['resource_uri'])
        if not len(objs):
            break
        colls.extend(objs)
        offset+=limit
    ready_for_pub = [] 
    not_ready_for_pub = []
    all_collections_url_dict = {}
    for c in colls:
        all_collections_url_dict[c['url']] = c
        if c['ready_for_publication']:
            ready_for_pub.append(c)
        else:
            not_ready_for_pub.append(c)
    return all_collections_url_dict, ready_for_pub, not_ready_for_pub

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
        runtime, page_name, data, all_collections, tab_color=None):
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
    page.write(0, 4, 'Calisphere URL', header_format)
    page.write(0, 5, 'Institution', header_format)
    # width
    page.set_column(0, 0, 40, )
    page.set_column(1, 1, 43, )
    page.set_column(2, 2, 10, )
    page.set_column(4, 4, 40, )
    page.set_column(5, 5, 50, )
    row = 1
    for item in data:
        c_url = item[0]
        c_name = all_collections[c_url]['name']
        repo = all_collections[c_url]["repository"][0]["name"]
        campus = all_collections[c_url].get("campus")
        campus_name = None
        if campus:
            campus_name = campus[0]["name"]
        inst_name = '{}::{}'.format(campus_name, repo) if campus_name else repo
        c_id = c_url.rsplit('/', 2)[1]
        url_calisphere = URL_CALISPHERE_BASE_COLLECTION + c_id + '/'
        page.write(row, 0, c_url)
        page.write(row, 1, c_name)
        page.write_number(row, 2, item[1], n_format)
        page.write(row, 4, url_calisphere)
        page.write(row, 5, inst_name)
        row = row + 1
    page.write_formula(row, 3, '=SUM(C2:C{})'.format(row))
    page.write(row, 4, runtime)

def create_counts_collections_page(workbook, header_format, number_format,
        runtime, page_name, data, all_collections, tab_color=None):
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
        c_url = item[0]
        c_name = all_collections[c_url]['name']
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
        page.write(row, 0, c['url'])
        page.write(row, 1, c['name'])
        page.write(row, 2, c['ready_for_publication'])
        page.write(row, 3, 'MISSING')
        row +=1
    row += 3
    for c in not_ready_for_pub:
        page.write(row, 0, c['url'])
        page.write(row, 1, c['name'])
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
                            all_collections,
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
        runtime, 'Collections not in New Index', not_in_new, all_collections,
        tab_color='red')

    # Collections with PRODUCTION COUNT GREATER (BAD!!!)
    create_counts_collections_page(workbook, header_format, number_format,
        runtime, 'PRODUCTION count GREATER', new_less, all_collections,
        tab_color='red')

    # Collection not in current production (OK)
    create_missing_collections_page(workbook, header_format, number_format,
        runtime, 'Collections not in Production', not_in_prod, all_collections,
        tab_color='yellow')

    # Collections with NEW COUNT GREATER (OK)
    create_counts_collections_page(workbook, header_format, number_format,
        runtime, 'New count greater', new_more, all_collections,
        tab_color='yellow')

    # Collections with equal counts in both indexes (prod first)
    create_counts_collections_page(workbook, header_format, number_format,
        runtime, 'Index count EQUAL', count_equal, all_collections,
        tab_color='green')

    if missing_ready_for_pub or not_ready_for_pub:
        create_registry_publication_report(workbook, header_format,
                number_format, runtime, missing_ready_for_pub,
                not_ready_for_pub)

    return workbook

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
    solr_url_new = config.get('new-index', 'solrUrl')
    api_key_new = config.get('new-index', 'solrAuth')
    new_totals = get_solr_json(solr_url_new, query_t, api_key=api_key_new)
    num_new_docs = get_total_docs(new_totals)
    new_type_ss_dict = create_facet_dict(new_totals,
                                        'type_ss')

    #get calisphere current index data
    production_json = get_solr_json(solr_url, base_query, api_key=api_key)
    production_facet_dict = create_facet_dict(production_json,
                                                'collection_url')
    new_json = get_solr_json(solr_url_new, base_query, api_key=api_key_new)
    new_facet_dict = create_facet_dict(new_json,
                                        'collection_url')
    pp('OLD LEN:{} NEW LEN:{}'.format(len(production_facet_dict),
                                        len(new_facet_dict)))

    not_in_new, not_in_prod, count_equal, new_less, new_more = compare_datasets(production_facet_dict, new_facet_dict)
    all_collections, ready_for_pub, not_ready_for_pub = get_registry_collection_data()
    pp("READY FOR PUB:{} NOT READY:{}".format(len(ready_for_pub),
        len(not_ready_for_pub)))
    missing_ready_for_pub = [c for c in ready_for_pub if c['url'] not in new_facet_dict]
    not_ready_for_pub = [c for c in not_ready_for_pub if c['url'] in new_facet_dict]

    pp('NOT IN NEW INDEX {}'.format(len(not_in_new)))
    pp('NOT IN PROD INDEX {}'.format(len(not_in_prod)))
    pp('COUNT EQUAL {}'.format(len(count_equal)))
    pp('NEW LESS {}'.format(len(new_less)))
    pp('NEW MORE {}'.format(len(new_more)))
    workbook = create_report_workbook(argv.outdir[0], not_in_new, not_in_prod, count_equal,
                            new_less, new_more,
                            num_found_prod=num_prod_docs,
                            num_found_new=num_new_docs,
                            type_ss_prod=production_type_ss_dict,
                            type_ss_new=new_type_ss_dict,
                            all_collections=all_collections,
                            missing_ready_for_pub=missing_ready_for_pub,
                            not_ready_for_pub=not_ready_for_pub)

    #check the "coverage_ss" facet differences, need to be added 
    # to our coverage_lookup_table.csv if new values exist
    cov_query = {
        'facet': 'true',
        'facet.field': [
            'coverage_ss',
        ],
        'rows': 0,
        'facet.limit': -1, #give them all
        'facet.sort': 'count',
        'facet.mincount': 1,
    }
    production_json = get_solr_json(solr_url, cov_query, api_key=api_key)
    production_facet_dict = create_facet_dict(production_json, 'coverage_ss')
    new_json = get_solr_json(solr_url_new, cov_query, api_key=api_key_new)
    new_facet_dict = create_facet_dict(new_json, 'coverage_ss')
    not_in_new, not_in_prod, count_equal, new_less, new_more = compare_datasets(production_facet_dict, new_facet_dict)
    print("COVERAGE: NOT IN PROD: {}  NOT_IN_NEW: {}".format(not_in_prod,
        not_in_new))
    
    page = workbook.add_worksheet('New Coverage Values')
    header_format = workbook.add_format({'bold': True, })
    number_format = workbook.add_format()
    number_format.set_num_format('#,##0')
    if not_in_prod > 0:
        page.set_tab_color('red')
        number_format.set_bg_color('red')
    page.write(0, 0, 'New Coverage_ss Values', header_format)
    page.write(0, 1, 'Counts', header_format)
    # width
    page.set_column(0, 1, 25, )
    row = 2
    for value, count in not_in_prod:
        page.write(row, 0, value)
        page.write(row, 1, count, number_format)
        row = row + 1

    workbook.close()

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
