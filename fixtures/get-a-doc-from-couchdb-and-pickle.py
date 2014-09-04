# OUT: Python shell history and tab completion are enabled.
from couchdb import Server
s=Server('https://54.84.142.143/couchdb/')
db=s['ucldc']
resp=db.changes(since=288000)
results=resp['results']
doc=db.get(results[0]['id'])
dir(doc)
# OUT: ['__class__', '__cmp__', '__contains__', '__delattr__', '__delitem__', '__dict__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__getitem__', '__gt__', '__hash__', '__init__', '__iter__', '__le__', '__len__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__setitem__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', 'clear', 'copy', 'fromkeys', 'get', 'has_key', 'id', 'items', 'iteritems', 'iterkeys', 'itervalues', 'keys', 'pop', 'popitem', 'rev', 'setdefault', 'update', 'values', 'viewitems', 'viewkeys', 'viewvalues']
doc.keys()
# OUT: [u'_id', u'@id', u'_rev', u'ingestDate', u'ingestionSequence', u'isShownAt', u'sourceResource', u'@context', u'ingestType', u'dataProvider', u'originalRecord', u'id']
import pickle
pickled_doc=pickle.dumps(doc)
f=open('pickled_couchdb_doc','w')
pickle.dump(doc, f)
f.close()
