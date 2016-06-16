# -*- coding: utf-8 -*-
import harvester.image_harvest
harvester.image_harvest.link_is_to_image = lambda x,y: True
from harvester.image_harvest import ImageHarvester
from harvester.image_harvest import COUCHDB_VIEW, BUCKET_BASES

class ImageHarvesterOPLPreservica(ImageHarvester):
    def __init__(self, cdb=None,
                 url_couchdb=None,
                 couchdb_name=None,
                 couch_view=COUCHDB_VIEW,
                 bucket_bases=BUCKET_BASES,
                 object_auth=None,
                 get_if_object=False,
                 url_cache=None,
                 hash_cache=None,
                 harvested_object_cache=None,
                 auth_token=None):
        super(ImageHarvesterOPLPreservica, self).__init__(cdb=cdb,
                 url_couchdb=url_couchdb,
                 couchdb_name=couchdb_name,
                 couch_view=couch_view,
                 bucket_bases=bucket_bases,
                 object_auth=object_auth,
                 get_if_object=get_if_object,
                 url_cache=url_cache,
                 hash_cache=hash_cache,
                 harvested_object_cache=harvested_object_cache)
        self.auth_token = auth_token

    def stash_image(self, doc):
        url_image_base = doc.get('isShownBy', None)
        if url_image_base:
            doc['isShownBy'] = url_image_base +'&token={}'.format(self.auth_token)
        else:
            return None
        report =  super(ImageHarvesterOPLPreservica, self).stash_image(doc)
        doc['isShownBy'] = url_image_base
        return report

def main(collection_key=None,
         url_couchdb=None,
         object_auth=None,
         get_if_object=False,
         auth_token=None):
    ImageHarvesterOPLPreservica(url_couchdb=url_couchdb,
                         object_auth=object_auth,
                         get_if_object=get_if_object,
                         auth_token=auth_token).by_collection(collection_key)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
            description='Run the image harvesting on a collection')
    parser.add_argument('collection_key', help='Registry collection id')
    parser.add_argument('auth_token', help='Authentication token from preservica')
    parser.add_argument('--object_auth', nargs='?',
            help='HTTP Auth needed to download images - username:password')
    parser.add_argument('--url_couchdb', nargs='?',
            help='Override url to couchdb')
    parser.add_argument('--get_if_object', action='store_true',
                        default=False,
            help='Should image harvester not get image if the object field exists for the doc (default: False, always get)')
    args = parser.parse_args()
    print(args)
    object_auth=None
    if args.object_auth:
        object_auth = (args.object_auth.split(':')[0],
                args.object_auth.split(':')[1])
    main(args.collection_key,
         object_auth=object_auth,
         url_couchdb=args.url_couchdb,
         get_if_object=args.get_if_object,
         auth_token=args.auth_token)

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

