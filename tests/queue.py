# -*- coding: utf-8 -*-
import unittest
import os, sys
import time

import momoko
from momoko.adisp import process, async
from tornado.testing import AsyncTestCase
from tornado import ioloop

from momoko.queue import DbQueryQueue

import settings


class DbQueueTestCase(AsyncTestCase):

    def setUp(self):
        super(DbQueueTestCase, self).setUp()

        self.db = momoko.AsyncClient({
            'host': settings.host,
            'port': settings.port,
            'database': settings.database,
            'user': settings.user,
            'password': settings.password,
            'min_conn': settings.min_conn,
            'max_conn': settings.max_conn,
            'cleanup_timeout': settings.cleanup_timeout,
            'ioloop': self.io_loop,
            'client_encoding': 'utf-8'
        })

        self.db_queue = DbQueryQueue(self.db, self.io_loop, poll_timeout=0.2, queue_length=2,
                                     noresult_queue_length=10000)
        self.db_queue.start()

    def tearDown(self):
        self.db_queue.stop()
        super(DbQueueTestCase, self).tearDown()

    def test_format_sql(self):
        sql = self.db_queue.format_sql('SELECT %s, %s', (1, 2))
        self.assertEqual(sql, 'SELECT 1, 2')

        sql = self.db_queue.format_sql('SELECT %s', (u'привет', ))
        self.assertEqual(sql, "SELECT 'привет'")

        sql = self.db_queue.format_sql('SELECT %(one)s, %(two)s', {'one': 1, 'two': 2})
        self.assertEqual(sql, 'SELECT 1, 2')

    def test_fetchall(self):
        expected = []

        def after_fetchall(data):
            self.assertEqual(data[0][0], expected.pop(0))
            if not expected:
                self.stop()

        for i in xrange(0, 100):
            expected.append(i)
            self.db_queue.fetchall('SELECT %s', (i,), callback=after_fetchall)

        self.wait()

    def test_query_without_result(self):
        sql = ''
        expected = []

        def after_execute():
            expected.pop(0)
            if not expected:
                self.stop()

        for i in xrange(0, 100):
            sql += 'SELECT %d;\n' % i
            expected.append(i)
            self.db_queue.execute_noresult(sql, callback=after_execute)

        self.wait(timeout=10)

if __name__ == '__main__':
    unittest.main()
