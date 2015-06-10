from unittest import TestCase
from mock import patch
from harvester.queue_harvest import main as queue_harvest_main
from harvester.queue_harvest import get_redis_connection, check_redis_queue
import sys
from test.utils import ConfigFileOverrideMixin

class QueueHarvestTestCase(TestCase):
    '''Test the queue harvester.
    For now will mock the RQ library.
    '''
    def testGetRedisConnection(self):
        r = get_redis_connection('127.0.0.1', '6379', 'PASS')
        self.assertEqual(str(type(r)), "<class 'redis.client.Redis'>")

    def testCheckRedisQ(self):
        with patch('redis.Redis.ping', return_value=False) as mock_redis:
            res = check_redis_queue('127.0.0.1', '6379', 'PASS')
            self.assertEqual(res, False)
        with patch('redis.Redis.ping', return_value=True) as mock_redis:
            res = check_redis_queue('127.0.0.1', '6379', 'PASS')
            self.assertEqual(res, True)

    def testMain(self):
        with self.assertRaises(Exception) as cm:
            with patch('harvester.queue_harvest.Redis') as mock_redis:
                mock_redis().ping.return_value = False
                queue_harvest_main('mark.redar@ucop.edu',
                    'https://registry.cdlib.org/api/v1/collection/178/; \
                            https://registry.cdlib.org/api/v1/collection/189',
                    redis_host='127.0.0.1',
                    redis_port='6379',
                    redis_pswd='X',
                    timeout=1,
                    poll_interval=1
                )
        self.assertIn('TIMEOUT (1s) WAITING FOR QUEUE.', cm.exception.message)
        with patch('harvester.queue_harvest.Redis', autospec=True) as mock_redis:
            mock_redis().ping.return_value = True
            results = queue_harvest_main('mark.redar@ucop.edu',
                'https://registry.cdlib.org/api/v1/collection/178/; \
                            https://registry.cdlib.org/api/v1/collection/189',
                redis_host='127.0.0.1',
                redis_port='6379',
                redis_pswd='X',
                rq_queue='normal-stage',
                timeout=1,
                poll_interval=1
                )
        mock_calls = [str(x) for x in mock_redis.mock_calls]
        self.assertEqual(len(mock_calls), 10)
        self.assertEqual(mock_redis.call_count, 3)
        #for i, call in enumerate(mock_calls):
        #    print str(i), call
        self.assertIn('call().ping()', mock_calls)
        self.assertEqual("call().sadd(u'rq:queues', u'rq:queue:normal-stage')", mock_calls[4])
        self.assertEqual("call().sadd(u'rq:queues', u'rq:queue:normal-stage')", mock_calls[7])
        self.assertIn("call().rpush(u'rq:queue:normal-stage", mock_calls[6])
        self.assertIn("call().rpush(u'rq:queue:normal-stage", mock_calls[9])
        self.assertIn("call().hmset('rq:job", mock_calls[5])
        self.assertIn(results[0].id, mock_calls[5])
        self.assertIn("call().hmset('rq:job", mock_calls[8])
        self.assertIn(results[1].id, mock_calls[8])
