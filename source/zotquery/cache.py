#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (c) 2015 deanishe@deanishe.net
#
# MIT Licence. See http://opensource.org/licenses/MIT
#
# Created on 2015-11-27
#

"""Simple cache using sqlite and JSON as a key-value store."""

from __future__ import print_function, absolute_import

from contextlib import contextmanager
import json
import logging
import os
import sqlite3
import sys
import time

from zotquery.config import log

SCHEMA = """
CREATE TABLE `dbinfo` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    `version` INTEGER NOT NULL,
    `last_updated` REAL DEFAULT 0.0
);

INSERT INTO `dbinfo` VALUES (1, 1, 0);

CREATE TABLE `data` (
    `key` TEXT PRIMARY KEY NOT NULL,
    `value` TEXT DEFAULT "{}"
)
"""

# log = logging.getLogger(__name__)


@contextmanager
def transaction(conn):
    """Context manager providing a DB cursor.

    Args:
        conn (sqlite3.connection): Database connection.

    Returns:
        sqlite3.cursor: Database cursor.

    """
    with conn:
        c = conn.cursor()
        yield c


class Cache(object):
    """Simple key-value store based on sqlite3 and JSON.

    Attributes:
        filepath (unicode): Path to the active cache database.

    Usage:
        >>> c = Cache('temp.sqlite3')
        >>> c.set('one', 1)
        True  # was inserted
        >>> c.set('two', 'II')
        True  # was inserted
        >>> c.set('three-thru-ten', [2, 3, 4, 5, 6, 7, 8, 9, 10])
        True  # was inserted
        >>> c.get('one')
        1
        >>> c.set('one', c.get('three-thru-ten'))
        True  # was updated
        >>> c.get('one')
        [2, 3, 4, 5, 6, 7, 8, 9, 10]

    """

    def __init__(self, filepath):
        """Create new Cache object.

        Args:
            filepath (unicode): Path to sqlite3 database.
        """
        self.filepath = filepath
        self._conn = None

    @property
    def conn(self):
        """Database connection.

        Initialises database if necessary.

        Returns:
            sqlite3.Connection: Connection to database.


        """
        if self._conn is None:
            conn = sqlite3.connect(self.filepath)
            conn.row_factory = sqlite3.Row
            try:
                c = conn.cursor()
                c.execute('SELECT * FROM `dbinfo`')
                log.debug('Opened cache %r', self.filepath)
            except sqlite3.OperationalError:
                log.debug('Initialising cache %r ...', self.filepath)
                c.executescript(SCHEMA)

            self._conn = conn

        return self._conn

    def get(self, key, default=None):
        """Retrieve cached value for `key`.

        Args:
            key (unicode): Cache key.
            default (object, optional): Returned if nothing is cached.

        Returns:
            object: Whatever you stored.

        """
        sql = 'SELECT `key`, `value` FROM `data` WHERE `key`=?'
        r = self.conn.execute(sql, (key,)).fetchone()
        # with transaction(self.conn) as c:
        #     c.execute(sql, (key,))
        #     r = c.fetchone()
        if r is None:
            return default

        return json.loads(r[b'value'])

    def set(self, key, data):
        """Set cache value for `key`.

        Args:
            key (unicode): Cache key.
            data (object): JSON-serialisable object.

        Returns:
            bool: `True` if cache is updated.
        """
        s = json.dumps(data)
        # Try to update dataset first
        sql = 'UPDATE `data` SET `value`=? WHERE `key`=?'
        with transaction(self.conn) as c:
            c.execute(sql, (s, key))

            if c.rowcount > 0:
                log.debug(u'Updated `%s`', key)
                self._set_updated()
                return True

        # Nothing was updated, so insert instead
        sql = 'INSERT INTO `data` (`key`, `value`) VALUES (?, ?)'
        with transaction(self.conn) as c:
            c.execute(sql, (key, s))

        if c.rowcount > 0:
                log.debug(u'Inserted `%s`', key)
                self._set_updated()
                return True

        return False

    def delete(self, key):
        """Remove cache entry for `key`.

        Args:
            key (unicode): Cache key.

        Returns:
            bool: `True` if cache was changed.
        """
        sql = 'DELETE FROM `data` WHERE `key`=?'
        with transaction(self.conn) as c:
            c.execute(sql, (key,))
        if c.rowcount > 0:
            log.debug(u'Deleted `%s`', key)
            self._set_updated()
            return True

        return False

    def keys(self):
        """Iterate over all cache keys.

        Yields:
            Unicode: Cache keys.
        """
        sql = 'SELECT `key` FROM `data`'
        for r in self.conn.execute(sql):
            yield r['key']

    def values(self):
        """Iterate over all cache values.

        Yields:
            object: Whatever you stored in there.
        """
        sql = 'SELECT `value` FROM `data`'
        for r in self.conn.execute(sql):
            yield json.loads(r['value'])

    def items(self):
        """Iterate over all (key, value) pairs in cache.

        Yields:
            tuple: `(key, value)` pairs.
        """
        sql = 'SELECT `key`, `value` FROM `data`'
        for r in self.conn.execute(sql):
            yield r['key'], json.loads(r['value'])

    @property
    def updated(self):
        """Time cache was last updated (edited)."""
        sql = 'SELECT `last_updated` FROM `dbinfo` WHERE `id` = 1'
        r = self.conn.execute(sql).fetchone()
        t = r[0]
        log.debug('Last updated %0.2fs ago', time.time() - t)
        return t

    def _set_updated(self, when=None):
        """Set `updated` to ``when`` or now."""
        t = when or time.time()
        sql = 'UPDATE `dbinfo` SET `last_updated` = ? WHERE `id` = 1'
        with transaction(self.conn) as c:
            c.execute(sql, (t,))

        if c.rowcount > 0:
                # log.debug(u'Cache updated at `%d`', t)
                return True

        return False



if __name__ == '__main__':
    from pprint import pprint
    logging.basicConfig(level=logging.DEBUG)
    filename = 'temp.sqlite3'
    c = Cache(filename)
    if len(sys.argv) == 2:  # dump/get
        if sys.argv[1] == 'dump':
            for t in c.items():
                pprint(t)
        else:
            pprint(c.get(sys.argv[1]))
    elif len(sys.argv) == 3:  # set
        pprint(c.set(sys.argv[1], sys.argv[2]))
    else:
        print("Usage: cache.py <key> [<value>]\n"
              "Get/set cache keys.")
        sys.exit(1)
