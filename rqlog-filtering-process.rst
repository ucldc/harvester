to filter various errors to get ids of collections that need changing.

Errors where the XTF query doesn't return anything:
===================================================


ack 'ValueError: http:'  -- divide by 3 to get number of these errors

ack -B13 'ValueError: http:' rqworker-*|ack 'api/v1'   -> list of lines with ids


Timeouts:
=========

ack -B1 'Job exceeded maximum timeout value' 

this can give list of "names" of collections that timeout




Validate v3 errors
==================
look in akara log



registry Connection Error:
==========================

The registry has been giving connection errors, they

ack "ConnectionError: HTTPSConnectionPool(host='registry.cdlib.org'"
