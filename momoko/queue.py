# -*- coding: utf-8 -*-
import tornado
from tornado.ioloop import PeriodicCallback
import time
import functools
from momoko.clients import AdispClient, AsyncClient
import uuid
try:
    from collections import OrderedDict
except ImportError:
    from utils import OrderedDict
import psycopg2
from psycopg2.extensions import adapt
from tornado import gen

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

class DbQueryQueue(object):

    def __init__(self, db, ioloop, poll_timeout=0.5, queue_length=5, noresult_poll_timeout=1, noresult_queue_length=5000):
        self.db = db

        if isinstance(self.db, AdispClient):
            self._execute = super(AdispClient, self.db).execute
            self._batch = super(AdispClient, self.db).batch
        else:
            self._execute = self.db.execute
            self._batch = self.db.batch

        self.ioloop = ioloop

        self.poll_timeout = poll_timeout
        self.queue = OrderedDict()
        self.queue_length = queue_length
        self.queue_poll_handler = None

        self.noresult_queue = OrderedDict()
        self.noresult_poll_timeout = noresult_poll_timeout
        self.noresult_queue_dumper = PeriodicCallback(self.noresult_timeout_check, self.noresult_poll_timeout * 1000, io_loop=self.ioloop)
        self.noresult_queue_length = noresult_queue_length
        self.noresult_last_time = None
        self.periodic_purge = PeriodicCallback(self.purge_expired, 30 * 1000, io_loop=self.ioloop)

    def start(self):
        '''
        Start queue polling
        '''
        self.noresult_last_time = time.time()
        self.noresult_queue_dumper.start()
        self.periodic_purge.start()
        self.queue_poll_handler = self.ioloop.add_timeout(time.time()+self.poll_timeout, self.poller)

    def stop(self):
        '''
        Stop queue polling
        '''
        self.noresult_queue_dumper.stop()
        self.periodic_purge.stop()
        if self.queue_poll_handler:
            self.ioloop.remove_timeout(self.queue_poll_handler)

    def format_sql(self, sql_tmpl, params=None):
        if params:
            if isinstance(params, dict):
                adapted = {}
                for k in params.keys():
                    if isinstance(params[k], unicode):
                        params[k] = params[k].encode('utf-8')
                    adapted[k] = adapt(params[k]).getquoted()
                sql = sql_tmpl % adapted
            elif isinstance(params, (list, tuple)):
                adapted = []
                for p in params:
                    if isinstance(p, unicode):
                        p = p.encode('utf-8')
                    adapted.append(adapt(p).getquoted())
                adapted = tuple(adapted)
            sql = sql_tmpl % adapted
        else:
            sql = sql_tmpl
        return sql

    def fetchone(self, sql_tmpl, params=None, callback=None, timeout=5*60):
        self.execute(sql_tmpl, params, command='fetchone', callback=callback, timeout=timeout)

    def fetchall(self, sql_tmpl, params=None, callback=None, timeout=5*60):
        self.execute(sql_tmpl, params, command='fetchall', callback=callback, timeout=timeout)

    def execute(self, sql_tmpl, params=None, command='', callback=None, timeout=5*60):
        assert command in ('fetchall', 'fetchone')
        sql = self.format_sql(sql_tmpl, params)
        key = str(uuid.uuid4())
        expires_at = time.time() + timeout
        self.queue[key] = (sql, callback, key, command, expires_at)

    @gen.engine
    def poller(self):
        i = 0
        self.temp_queries = OrderedDict()

        while self.queue and i < self.queue_length:
            i += 1
            uid, item = self.queue.popitem(last=False)  # FIFO
            self.temp_queries[uid] = item

        if not self.temp_queries:
            self.queue_poll_handler = self.ioloop.add_timeout(time.time()+self.poll_timeout, self.poller)
            return

        keys = []
        for sql, callback, uid, command, exp_at in self.temp_queries.values():
            keys.append(uid)
            self._execute(sql, callback=(yield gen.Callback(uid)))

        cursors = yield gen.WaitAll(keys)

        for uid, cursor in zip(keys, cursors):
            sql, callback, uid, command, exp_at = self.temp_queries.pop(uid)
            try:
                data = getattr(cursor, command)()
                callback(data)
            except Exception as e:
                print 'DBQUERY-QUEUE:ERROR:', e.message

        # if queue is not empty - call poller without timeout
        if self.queue:
            self.ioloop.add_callback(self.poller)
        else:
            self.queue_poll_handler = self.ioloop.add_timeout(time.time()+self.poll_timeout, self.poller)

    def purge_expired(self):
        cur_time = time.time()
        new_queue = OrderedDict()
        for k, v in self.queue.iteritems():
            callback = v[1]
            expires_at = v[4]
            if cur_time > expires_at:
                self.ioloop.add_callback(functools.partial(callback, None))
            else:
                new_queue[k] = v
        self.queue = new_queue

    def execute_noresult(self, sql_tmpl, params=None, callback=None):
        '''
        Execute query without result
        :param sql_tmpl: Template of SQL query of SQL query if params is None.
        :param params: Tuple, list or dict of params.
        :param callback: Callback to execute after query is complete.
        '''
        sql = self.format_sql(sql_tmpl, params)
        key = str(uuid.uuid4())
        self.noresult_queue[key] = (sql, callback)

        if len(self.noresult_queue) >= self.noresult_queue_length:
            self.ioloop.add_callback(self.execute_noresult_queue)

    def noresult_timeout_check(self):
        '''
        Check expiry of queue.
        '''
        if (time.time() - self.noresult_last_time) > self.noresult_poll_timeout:
            self.execute_noresult_queue()

    def execute_noresult_queue(self):
        '''
        Executes noresult queue in single query.
        After executing performs all callbacks.
        '''
        if not self.noresult_queue:
            return

        callbacks = []
        sql_defs = []
        while self.noresult_queue:
            uid, item = self.noresult_queue.popitem(last=False)
            sql, callback = item
            sql_defs.append(sql)
            if callback:
                callbacks.append(callback)
        sql = ';\n'.join(sql_defs)

        callback = functools.partial(self.perform_callbacks, callbacks=callbacks)
        self._execute(sql, callback=callback)

        self.noresult_last_time = time.time()

    def perform_callbacks(self, cursor, callbacks=None):
        for callback in callbacks:
            callback()
