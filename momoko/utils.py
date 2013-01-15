# -*- coding: utf-8 -*-
"""
    momoko.utils
    ~~~~~~~~~~~~

    Utilities for Momoko.

    :copyright: (c) 2011 by Frank Smit.
    :license: MIT, see LICENSE for more details.
"""


import functools

import psycopg2
import psycopg2.extensions
from tornado.ioloop import IOLoop
from UserDict import DictMixin


class QueryChain(object):
    """Run a chain of queries in the given order.

    A list/tuple with queries looks like this::

        (
            ['SELECT 42, 12, %s, 11;', (23,)],
            'SELECT 1, 2, 3, 4, 5;'
        )

    A query with paramaters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'paramaters here')]``. A query
    without paramaters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A tuple or with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A list with the resulting cursors is passed on to the callback.
    """
    def __init__(self, db, queries, callback):
        self._db = db
        self._cursors = []
        self._queries = list(queries)
        self._queries.reverse()
        self._callback = callback
        self._collect(None)

    def _collect(self, cursor):
        if cursor is not None:
            self._cursors.append(cursor)
        if not self._queries:
            if self._callback:
                self._callback(self._cursors)
            return
        query = self._queries.pop()
        if isinstance(query, str):
            query = [query]
        self._db.execute(*query, callback=self._collect)


class BatchQuery(object):
    """Run a batch of queries all at once.

    **Note:** Every query needs a free connection. So if three queries are
    are executed, three free connections are used.

    A dictionary with queries looks like this::

        {
            'query1': ['SELECT 42, 12, %s, %s;', (23, 56)],
            'query2': 'SELECT 1, 2, 3, 4, 5;',
            'query3': 'SELECT 465767, 4567, 3454;'
        }

    A query with paramaters is contained in a list: ``['some sql
    here %s, %s', ('and some', 'paramaters here')]``. A query
    without paramaters doesn't need to be in a list.

    :param db: A ``momoko.Client`` or ``momoko.AdispClient`` instance.
    :param queries: A dictionary with all the queries.
    :param callback: The function that needs to be executed once all the
                     queries are finished.
    :return: A dictionary with the same keys as the given queries with the
             resulting cursors as values is passed on to the callback.
    """
    def __init__(self, db, queries, callback):
        from clients import AdispClient
        self._db = db

        if isinstance(self._db, AdispClient):
            self.execute = super(AdispClient, self._db).execute
        else:
            self.execute = self._db.execute

        self._callback = callback
        self._queries = {}
        self._args = {}
        self._size = len(queries)

        for key, query in list(queries.items()):
            if isinstance(query, str):
                query = [query, ()]
            query.append(functools.partial(self._collect, key))
            self._queries[key] = query

        for query in list(self._queries.values()):
            self.execute(*query)

    def _collect(self, key, cursor):
        self._size = self._size - 1
        self._args[key] = cursor
        if not self._size and self._callback:
            self._callback(self._args)


class Poller(object):
    """A poller that polls the PostgreSQL connection and calls the callbacks
    when the connection state is ``POLL_OK``.

    :param connection: The connection that needs to be polled.
    :param callbacks: A tuple/list of callbacks.
    """
    # TODO: Accept new argument "is_connection"
    def __init__(self, connection, callbacks=(), ioloop=None):
        self._ioloop = ioloop or IOLoop.instance()
        self._connection = connection
        self._callbacks = callbacks

        self._update_handler()

    def _update_handler(self):
        try:
            state = self._connection.poll()
        except (psycopg2.Warning, psycopg2.Error) as error:
            # When a DatabaseError is raised it means that the connection has been
            # closed and polling it would raise an exception from then IOLoop.
            if not isinstance(error, psycopg2.DatabaseError):
                self._ioloop.update_handler(self._connection.fileno(), 0)

            if self._callbacks:
                for callback in self._callbacks:
                    try:
                        callback(error)
                    except Exception as e:
                        print 'DBQUERY-POLLER:CALLBACK_ERROR:', e.message
        else:
            if state == psycopg2.extensions.POLL_OK:
                for callback in self._callbacks:
                    callback()
            elif state == psycopg2.extensions.POLL_READ:
                self._ioloop.add_handler(self._connection.fileno(),
                    self._io_callback, IOLoop.READ)
            elif state == psycopg2.extensions.POLL_WRITE:
                self._ioloop.add_handler(self._connection.fileno(),
                    self._io_callback, IOLoop.WRITE)
            else:
                raise Exception('poll() returned {0}'.format(state))

    def _io_callback(self, *args):
        self._ioloop.remove_handler(self._connection.fileno())
        self._update_handler()


class OrderedDict(dict, DictMixin):

    def __init__(self, *args, **kwds):
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))
        try:
            self.__end
        except AttributeError:
            self.clear()
        self.update(*args, **kwds)

    def clear(self):
        self.__end = end = []
        end += [None, end, end]         # sentinel node for doubly linked list
        self.__map = {}                 # key --> [key, prev, next]
        dict.clear(self)

    def __setitem__(self, key, value):
        if key not in self:
            end = self.__end
            curr = end[1]
            curr[2] = end[1] = self.__map[key] = [key, curr, end]
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        key, prev, next = self.__map.pop(key)
        prev[2] = next
        next[1] = prev

    def __iter__(self):
        end = self.__end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.__end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def popitem(self, last=True):
        if not self:
            raise KeyError('dictionary is empty')
        if last:
            key = reversed(self).next()
        else:
            key = iter(self).next()
        value = self.pop(key)
        return key, value

    def __reduce__(self):
        items = [[k, self[k]] for k in self]
        tmp = self.__map, self.__end
        del self.__map, self.__end
        inst_dict = vars(self).copy()
        self.__map, self.__end = tmp
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def keys(self):
        return list(self)

    setdefault = DictMixin.setdefault
    update = DictMixin.update
    pop = DictMixin.pop
    values = DictMixin.values
    items = DictMixin.items
    iterkeys = DictMixin.iterkeys
    itervalues = DictMixin.itervalues
    iteritems = DictMixin.iteritems

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, self.items())

    def copy(self):
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        if isinstance(other, OrderedDict):
            if len(self) != len(other):
                return False
            for p, q in  zip(self.items(), other.items()):
                if p != q:
                    return False
            return True
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self == other
