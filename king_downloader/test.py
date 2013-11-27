# -*- coding: utf-8 -*-
import unittest
import time

__author__ = 'patrickz'

from celery_scraper2.core import *


class TestRequestItem(unittest.TestCase):
    def setUp(self):
        self.item1 = RequestItem(method='get',url='http://www.baidu.com/')
        self.item2 = RequestItem(method='get',url='http://www.baidu.com/',raw_info={'data1':111,'data2':222})

    def test_dumps(self):
        self.assertEqual(self.item1.dumps(), {'method':'get', 'url':'http://www.baidu.com/'})
        self.assertEqual(self.item2.dumps(), {'method':'get', 'url':'http://www.baidu.com/', 'raw_info':{'data1':111,'data2':222}})

    def test_to_msgpack(self):
        self.assertEqual(self.item1.to_msgpack(), msgpack.packb({'method':'get', 'url':'http://www.baidu.com/'}))
        self.assertEqual(self.item2.to_msgpack(), msgpack.packb({'method':'get', 'url':'http://www.baidu.com/', 'raw_info':{'data1':111,'data2':222}}))

class TestRedisRequestQueue(unittest.TestCase):
    def setUp(self):
        self.queue = RedisRequestQueue()
        self.queue.setup('localhost','test_q2')
        self.queue.push(RequestItem(method='get',url='http://www.baidu.com/'))
        self.queue.push(RequestItem(method='get',url='http://www.baidu.com/'))

    def test_pop(self):
        self.assertEqual(len(self.queue.pop()), 2)

    def tearDown(self):
        self.queue.clear()




class CustomBeforeProcessor(RequestProcessor):
    testdata = {}
    def process(self, *args, **kwargs):
        data = {'request': kwargs['request']}
        self.testdata["session_"+kwargs['request'].raw_info['index']] = data

class CustomAfterProcessor(RequestProcessor):
    testdata = {}
    def process(self, *args, **kwargs):
        data = {'request': kwargs['request'], 'result':kwargs['result']}
        self.testdata["session_"+kwargs['request'].raw_info['index']]=(data)

class TestBatchRequest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.queue = RedisRequestQueue()
        self.queue.setup('localhost','test_q')
        self.queue.push(RequestItem(method='get',url='http://www.baidu.com/', raw_info={'index':'1'}))
        self.queue.push(RequestItem(method='get',url='http://www.baidu.com/', raw_info={'index':'2'}))
        self.queue.push(RequestItem(method='get',url='http://www.baidu3333.com/', raw_info={'index':'3'}))
        self.engine = RequestEngine()
        self.before_processor = CustomBeforeProcessor()
        self.after_processor = CustomAfterProcessor()
        self.engine.register_processor(self.before_processor, 'before')
        self.engine.register_processor(self.after_processor, 'after')
        self.engine.setup_request_queue(self.queue)
        self.engine.request()
        while self.engine.active:
            time.sleep(2)
        self.queue.clear()

    def test_before_each(self):
        self.assertEqual(('session_1' in self.before_processor.testdata), True)
        self.assertEqual(('session_2' in self.before_processor.testdata), True)
        self.assertEqual(('session_3' in self.before_processor.testdata), True)

    def test_after_each(self):
        self.assertEqual(('session_1' in self.after_processor.testdata), True)
        self.assertEqual(('session_3' in self.after_processor.testdata), True)
        self.assertEqual(self.after_processor.testdata['session_1']['result'], True)
        self.assertEqual(self.after_processor.testdata['session_3']['result'], False)