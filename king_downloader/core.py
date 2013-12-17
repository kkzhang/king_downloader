# -*- coding: utf-8 -*-
import logging

from gevent.pool import Pool
from gevent.event import Event
import gevent
import msgpack
import requests
import signal
import traceback
import time
from king_downloader.utils import UserAgentProvider

__author__ = 'patrickz'

logger = logging.getLogger('king_downloader.core')
logger.propagate = True


class RequestItem:
    def __init__(self, method=None, url=None, load=None, processors=None, raw_info = None, **kwargs):
        if load is not None:
            self.kwargs = msgpack.unpackb(load)
            self.raw_info = self.kwargs.pop('raw_info', None)
            self.processors = self.kwargs.pop('processors', None)
        else:
            kwargs.update({
                'method' : method,
                'url' : url,
            })
            self.kwargs = kwargs
            self.processors= processors
            self.raw_info = raw_info

    def update(self,processors,raw_info, **kwargs):
        if processors: self.processors = processors
        if raw_info: self.raw_info = raw_info
        self.kwargs.update(kwargs)

    def dumps(self):
        data = self.kwargs.copy()
        if (self.raw_info is not None):
            data.update({
                'raw_info':self.raw_info})
        if (self.processors is not None):
            data.update({
                'processors': self.processors})
        return data

    def to_msgpack(self):
        data = self.dumps()
        return msgpack.packb(data)


class RequestQueue:
    _status = 1 # 1 for active, 0 for inactive: stop poping
    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    def pop(self, num = 10):
        raise NotImplementedError

    def push(self, request_item_object):
        raise NotImplementedError

    def quit(self):
        raise NotImplementedError


class RedisRequestQueue(RequestQueue):

    def setup(self, address, queue_name):
        import redis
        self.redis_address = address
        self.rd = redis.Redis(address)
        self.queue_name = queue_name

    def setup_by_redis_instance(self, redis_ins, queue_name):
        self.queue_name = queue_name
        self.rd = redis_ins


    def pop(self, num = 10):
        p = self.rd.pipeline()
        [p.rpop(self.queue_name) for i in xrange(num)]
        r = p.execute()
        _fr = filter(lambda x: x is not None, r)
        data =  map(lambda x: RequestItem(load=x), _fr)
        return data

    def push(self, *request_item_objects):
        p = self.rd.pipeline()
        [self.rd.lpush(self.queue_name, i.to_msgpack()) for i in request_item_objects]
        r = p.execute()
        return r

    def clear(self):
        self.rd.delete(self.queue_name)

class RequestProcessor(object):

    def process(self, *args, **kwargs):
        raise NotImplementedError

    def stop_request(self):
        return False

class RequestEngine:

    class ProcessorManager(object):
        def __init__(self):
            self._processor_map = {'default': None}
        def set(self, processor_name,  value):
            self._processor_map[processor_name] = value

        def route(self, processor_name, **kwargs):
            if processor_name is None:
                processor_name_indeed = 'default'
            else:
                processor_name_indeed = processor_name

            processor = self._processor_map[processor_name_indeed]
            if processor is None:
                pass
            elif hasattr(processor, '__call__'):
                return processor.__call__(**kwargs)


    def __init__(self,
                 pool_size = 20,
                 pop_interval = 1,
                 request_interval = 0,
                 max_empty_retry = 2,
                 request_timeout = 10,
                 each_size_from_queue = 10,
                 max_failure_allowed = -1):
        from gevent import monkey
        monkey.patch_all()
        self.pop_interval = pop_interval
        self.request_interval = request_interval
        self.pool = Pool(pool_size)
        self.quit_event = Event()
        self.max_empty_retry = max_empty_retry
        self.request_timeout = request_timeout
        self.each_size_from_queue = each_size_from_queue
        self.user_agent_provider = UserAgentProvider()
        self.max_failure_allowed = max_failure_allowed
        self._request_failure = 0
        self.proxy_provider = None
        self.processor_manager = RequestEngine.ProcessorManager()
        self.before_each = []
        self.after_each = []

        gevent.signal(signal.SIGINT, self.quit)
        gevent.signal(signal.SIGQUIT, self.quit)
        gevent.signal(signal.SIGTERM, self.quit)

    def setup_request_queue(self, request_queue_ins):
        self.request_queue = request_queue_ins

    @property
    def active(self):
        if not hasattr(self, '_active'):
            self._active = False
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    def before_each(self, *processors):
        self.before_each += processors

    def after_each(self, *processors):
        self.after_each += processors

    def worker_count(self):
        return self.pool.size - self.pool.free_count()

    def quit(self):
        self.quit_event.set()

    def request(self, override_req_args= {}):
        self.active = True
        empty_count = 0
        while True:
            if self.quit_event.is_set():

                logger.warning("Quiting Engine")
                if self.pool.size != self.pool.free_count():
                    time.sleep(1)
                    continue

                self.active = False
                logger.warning("Engine Gracefully Quit")
                break

            if (self.max_failure_allowed != -1 and self._request_failure >= self.max_failure_allowed):
                logger.warning( "Exceed Max Failures Count. Engine Stopping ..." )
                self.quit()
                continue

            reqs = self.request_queue.pop(self.each_size_from_queue)
            if (reqs is not None) and (len(reqs) > 0):

                for i in reqs:
                    self.pool.spawn(self._make_requests, request=i, override = override_req_args)
                    time.sleep(self.request_interval)
            else:
                empty_count +=1
                if (self.max_empty_retry != -1 and empty_count >= self.max_empty_retry):
                    logger.warning( "Exceed Max Empty. Engine Stopping ..." )
                    self.quit()
                    continue

            #while self.pool.free_count() == 0:
            time.sleep(self.pop_interval)

    def setup_user_agent_provider(self, provider):
        self.user_agent_provider = provider

    def setup_proxy_provider(self, provider):
        self.proxy_provider = provider

    def register_processor(self, processor, name='default'):
        self.processor_manager.set(name, processor)

    def _make_requests(self, request, override):
        empty_count = 0
        data= {} # Data flow

        is_failure_set = False
        request.kwargs.update(override)
        # Setting user agent
        if self.user_agent_provider:
            if 'headers' in request.kwargs:
                request.kwargs['headers'].update({'User-Agent': self.user_agent_provider.provide()})
            else:
                request.kwargs['headers'] = {'User-Agent': self.user_agent_provider.provide()}

        # Setting proxy provider
        if self.proxy_provider:
            proxy = self.proxy_provider.provide()
            if proxy is not None:
                # If Provider return None, not use proxy
                _proxy = {'http':proxy.proxy, 'https':proxy.proxy}
                if 'proxies' in request.kwargs:
                    request.kwargs['proxies'].update(_proxy)
                else:
                    request.kwargs['proxies'] = _proxy

                logger.warning("Using Proxy: %s" % str(_proxy))
            else:
                logger.warning("No Using Proxy")
        else:
            proxy = None


        ar = None
        result = False
        processors = {'before':None, 'after':None}
        if request.processors is not None:
            processors.update(request.processors)
        before_each_hook_result = None
        # Execute hook before every item
        try:
            logger.info("Executing before hook")
            before_each_hook_result = self.processor_manager.route(
                                                                   processor_name=processors['before'],
                                                                   request = request,
                                                                   extra = request.raw_info,
                                                                   data= data)

            for p in self.before_each:
                self.processor_manager.route(processor_name=p, request = request ,extra = request.raw_info, data= data)
        except:
            if not is_failure_set:
                self._request_failure += 1
                is_failure_set = True
            logger.error("Exception while before hook execution: "+ traceback.format_exc())
        # Execute request

        if before_each_hook_result != False:
            # Only if before hook return non-false
            try:
                logger.debug("Making request... (%s)" % str(request.kwargs))
                _timeout =  getattr(request.raw_info,'_timeout',self.request_timeout)
                logger.debug("Timeout setting: %s" % _timeout)
                with gevent.Timeout(_timeout, False) as timeout:
                    ar = requests.request(**request.kwargs)
                    ar.raw_info = request.raw_info
                    result = True
                if result is False:
                    raise Exception("Request timeout (%s)" % self.request_timeout)
            except:

                if not is_failure_set:
                    self._request_failure += 1
                    is_failure_set = True
                logger.error("Exception while requests execution: "+ traceback.format_exc())


            try:

                # Execute hook after every request
                logger.info("Executing after hook")
                self.processor_manager.route(
                                             processor_name=processors['after'],
                                             response = ar,
                                             request = request,
                                             extra = request.raw_info,
                                             result = result, data=data)

                for p in self.after_each:
                    self.processor_manager.route(processor_name=p,response = ar, request = request,extra = request.raw_info, result = result, data= data)

                # process proxy provider
                if proxy:
                    self.proxy_provider.callback(proxy, result=result, response = ar, request=request)
            except:
                if not is_failure_set:
                    self._request_failure += 1
                    is_failure_set = True
                logger.error("Exception while after hook execution", exc_info=True)