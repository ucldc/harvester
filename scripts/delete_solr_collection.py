#! /bin/env python
# -*- coding: utf-8 -*-
import os
import argparse
import requests
from harvester.solr_updater import delete_solr_collection

def confirm_deletion(cid):
    prompt = "Are you sure you want to delete all solr " + \
             "documents for %s? yes to confirm\n" % cid
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        else:
            return False


if __name__ == '__main__':
    URL_SOLR = os.environ['URL_SOLR']
    DATA_BRANCH = os.environ['DATA_BRANCH']
    parser = argparse.ArgumentParser(
        description='Delete all documents in given collection in solr '
        'for {0}'.format(DATA_BRANCH))
    parser.add_argument('collection_id', help='Registry id for the collection')
    parser.add_argument(
        '--yes',
        action='store_true',
        help="Don't prompt for deletion, just do it")
    args = parser.parse_args()
    if args.yes or confirm_deletion(args.collection_id):
        print 'DELETING COLLECTION {}'.format(args.collection_id)
        delete_solr_collection(URL_SOLR, args.collection_id)
    else:
        print "Exiting without deleting"

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
