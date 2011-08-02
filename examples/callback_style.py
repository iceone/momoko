#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import momoko


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        if not hasattr(self.application, 'db'):
            self.application.db = momoko.Client({
                'host': 'localhost',
                'database': 'momoko',
                'user': 'frank',
                'password': '',
                'min_conn': 1,
                'max_conn': 20,
                'cleanup_timeout': 10
            })
        return self.application.db


class OverviewHandler(BaseHandler):
    @tornado.web.asynchronous
    def get(self):
        self.write('''
<ul>
    <li><a href="/query">A single query</a></li>
    <li><a href="/batch">A batch of queries</a></li>
    <li><a href="/chain">A chain of queries</a></li>
</ul>
        ''')
        self.finish()


class SingleQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    def get(self):
        self.db.execute('SELECT 42, 12, 40, 11;', callback=self._on_response)

    def _on_response(self, cursor):
        self.write('Query results: %s' % cursor.fetchall())
        self.finish()


class BatchQueryHandler(BaseHandler):
    @tornado.web.asynchronous
    def get(self):
        self.db.batch({
            'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
            'query2': 'SELECT 1, 2, 3, 4, 5;',
            'query3': 'SELECT 465767, 4567, 3454;'
        }, self._on_response)

    def _on_response(self, cursors):
        for key, cursor in cursors.items():
            self.write('Query results: %s = %s<br>' % (key, cursor.fetchall()))
        self.write('Done')
        self.finish()


class QueryChainHandler(BaseHandler):
    @tornado.web.asynchronous
    def get(self):
        self.db.chain([
            ['SELECT 42, 12, %s, 11;', (23,)],
            self._after_first_query,
            self._after_first_callable,
            'SELECT 1, 2, 3, 4, 5;',
            self._before_last_query,
            'SELECT %s, %s, %s, %s, %s;',
            self._on_response
        ])

    def _after_first_query(self, cursor):
        results = cursor.fetchall()
        return {
            'p1': results[0][0],
            'p2': results[0][1],
            'p3': results[0][2],
            'p4': results[0][3]
        }

    def _after_first_callable(self, p1, p2, p3, p4):
        self.write('Results of the first query in the chain: %s, %s, %s, %s<br>' % \
            (p1, p2, p3, p4))

    def _before_last_query(self, cursor):
        results = cursor.fetchall()
        return [i*16 for i in results[0]]

    def _on_response(self, cursor):
        self.write('Results of the last query in the chain: %s' % \
            cursor.fetchall())
        self.finish()


def main():
    try:
        tornado.options.parse_command_line()
        application = tornado.web.Application([
            (r'/', OverviewHandler),
            (r'/query', SingleQueryHandler),
            (r'/batch', BatchQueryHandler),
            (r'/chain', QueryChainHandler),
        ], debug=True)
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(8888)
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        print('Exit')


if __name__ == '__main__':
    main()
