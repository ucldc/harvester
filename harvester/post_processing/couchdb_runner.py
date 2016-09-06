import datetime
import time
from redis import Redis
from rq import Queue

from harvester.config import config
from harvester.couchdb_init import get_couchdb
from harvester.couchdb_pager import couchdb_pager

COUCHDB_VIEW = 'all_provider_docs/by_provider_name'


def get_collection_doc_ids(collection_id, url_couchdb_source=None):
    '''Use the by_provider_name view to get doc ids for a given collection
    '''
    _couchdb = get_couchdb(url=url_couchdb_source)
    v = CouchDBCollectionFilter(couchdb_obj=_couchdb,
                                collection_key=str(collection_id),
                                include_docs=False)
    doc_ids = []
    for r in v:
        doc_ids.append(r.id)
    return doc_ids


class CouchDBCollectionFilter(object):
    '''Class for selecting collections from the UCLDC couchdb data store.
    '''
    def __init__(self,
                 collection_key=None,
                 couchdb_obj=None,
                 url_couchdb=None,
                 couchdb_name=None,
                 couch_view=COUCHDB_VIEW,
                 include_docs=True
                 ):
        if not collection_key:
            collection_key = '{}'
        if couchdb_obj is None:
            if not url_couchdb or not couchdb_name:
                raise ValueError('Need url and name to couch database')
            self._couchdb = get_couchdb(url=url_couchdb, dbname=couchdb_name)
        else:
            self._couchdb = couchdb_obj
        self._view = couch_view
        self._view_iter = couchdb_pager(
                self._couchdb, self._view,
                key=collection_key,
                include_docs='true' if include_docs else 'false')

    def __iter__(self):
        return self._view_iter.__iter__()

    def next(self):
        return self._view_iter.next()


class CouchDBWorker(object):
    '''A class that can run functions on sets of couchdb documents
    maybe become some sort of decorator?
    Documents are mutable, so if the function mutates the document, it will
    be picked up here.
    ????Add the "save" keyword argument to save the document to db???
    functions should have call signature of (doc, *args, **kwargs)
    '''
    def __init__(self):
        self._couchdb = get_couchdb()

    def run_by_list_of_doc_ids(self, doc_ids, func, *args, **kwargs):
        '''For a list of ids, harvest images'''
        results = []
        for doc_id in doc_ids:
            doc = self._couchdb[doc_id]
            results.append((doc_id, func(doc, *args, **kwargs)))
        return results

    def run_by_collection(self, collection_key, func, *args, **kwargs):
        '''If collection_key is none, trying to grab all of the images. (Not
        recommended)
        '''
        v = CouchDBCollectionFilter(couchdb_obj=self._couchdb,
                                    collection_key=collection_key)
        results = []
        for r in v:
            dt_start = dt_end = datetime.datetime.now()
            result = func(r.doc, *args, **kwargs)
            results.append((r.doc['_id'], result))
            dt_end = datetime.datetime.now()
            time.sleep((dt_end-dt_start).total_seconds())
        return results


class CouchDBJobEnqueue(object):
    '''A class that will put a job on the RQ worker queue for each document
    selected. This should allow some parallelism.
    Functions passed to this enqueuing object should take a CouchDB doc id
    and should do whatever work & saving it needs to do on it.
    '''
    def __init__(self, rq_queue=None):
        self._config = config()
        self._couchdb = get_couchdb()
        self._redis = Redis(
                host=self._config['redis_host'],
                port=self._config['redis_port'],
                password=self._config['redis_password'],
                socket_connect_timeout=self._config['redis_connect_timeout'])
        self.rqname = self._config['rq_queue']
        if rq_queue:
            self.rqname = rq_queue
        if not self.rqname:
            raise ValueError(''.join(('Must set RQ_QUEUE env var',
                                      ' or pass in rq_queue to ',
                                      'CouchDBJobEnqueue')))
        self._rQ = Queue(self.rqname, connection=self._redis)

    def queue_list_of_ids(self, id_list, job_timeout, func,
                          *args, **kwargs):
        '''Enqueue jobs in the ingest infrastructure for a list of ids'''
        results = []
        for doc_id in id_list:
            this_args = [doc_id]
            if args:
                this_args.extend(args)
            this_args = tuple(this_args)
            print('Enqueing doc {} args: {} kwargs:{}'.format(doc_id,
                                                              this_args,
                                                              kwargs))
            result = self._rQ.enqueue_call(func=func,
                                           args=this_args,
                                           kwargs=kwargs,
                                           timeout=job_timeout)
            results.append(result)
        return results

    def queue_collection(self, collection_key, job_timeout, func,
                         *args, **kwargs):
        '''Queue a job in the RQ queue for each document in the collection.
        func is function to run and it must be accessible from the
        rq worker's virtualenv.
        func signature is func(doc_id, args, kwargs)
        Can't pass the document in because it all gets converted to a string
        and put into the RQ queue. Much easier to pass id and have worker deal
        with couchdb directly.
        '''
        v = CouchDBCollectionFilter(couchdb_obj=self._couchdb,
                                    collection_key=collection_key)
        results = []
        for r in v:
            doc = r.doc
            this_args = [doc['_id']]
            if args:
                this_args.extend(args)
            this_args = tuple(this_args)
            print('Enqueing doc {} args: {} kwargs:{}'.format(doc['_id'],
                                                              this_args,
                                                              kwargs))
            result = self._rQ.enqueue_call(func=func,
                                           args=this_args,
                                           kwargs=kwargs,
                                           timeout=job_timeout)
            results.append(result)
        if not results:
            print "NO RESULTS FOR COLLECTION: {}".format(collection_key)
        return results
