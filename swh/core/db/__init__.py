# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import datetime
import enum
import json
import os
import threading

from contextlib import contextmanager

import psycopg2
import psycopg2.extras


psycopg2.extras.register_uuid()


def escape(data):
    if data is None:
        return ''
    if isinstance(data, bytes):
        return '\\x%s' % binascii.hexlify(data).decode('ascii')
    elif isinstance(data, str):
        return '"%s"' % data.replace('"', '""')
    elif isinstance(data, datetime.datetime):
        # We escape twice to make sure the string generated by
        # isoformat gets escaped
        return escape(data.isoformat())
    elif isinstance(data, dict):
        return escape(json.dumps(data))
    elif isinstance(data, list):
        return escape("{%s}" % ','.join(escape(d) for d in data))
    elif isinstance(data, psycopg2.extras.Range):
        # We escape twice here too, so that we make sure
        # everything gets passed to copy properly
        return escape(
            '%s%s,%s%s' % (
                '[' if data.lower_inc else '(',
                '-infinity' if data.lower_inf else escape(data.lower),
                'infinity' if data.upper_inf else escape(data.upper),
                ']' if data.upper_inc else ')',
            )
        )
    elif isinstance(data, enum.IntEnum):
        return escape(int(data))
    else:
        # We don't escape here to make sure we pass literals properly
        return str(data)


class BaseDb:
    """Base class for swh.*.*Db.

    cf. swh.storage.db.Db, swh.archiver.db.ArchiverDb

    """

    @classmethod
    def connect(cls, *args, **kwargs):
        """factory method to create a DB proxy

        Accepts all arguments of psycopg2.connect; only some specific
        possibilities are reported below.

        Args:
            connstring: libpq2 connection string

        """
        conn = psycopg2.connect(*args, **kwargs)
        return cls(conn)

    @classmethod
    def from_pool(cls, pool):
        return cls(pool.getconn(), pool=pool)

    def __init__(self, conn, pool=None):
        """create a DB proxy

        Args:
            conn: psycopg2 connection to the SWH DB
            pool: psycopg2 pool of connections

        """
        self.conn = conn
        self.pool = pool

    def __del__(self):
        if self.pool:
            self.pool.putconn(self.conn)

    def cursor(self, cur_arg=None):
        """get a cursor: from cur_arg if given, or a fresh one otherwise

        meant to avoid boilerplate if/then/else in methods that proxy stored
        procedures

        """
        if cur_arg is not None:
            return cur_arg
        else:
            return self.conn.cursor()
    _cursor = cursor  # for bw compat

    @contextmanager
    def transaction(self):
        """context manager to execute within a DB transaction

        Yields:
            a psycopg2 cursor

        """
        with self.conn.cursor() as cur:
            try:
                yield cur
                self.conn.commit()
            except Exception:
                if not self.conn.closed:
                    self.conn.rollback()
                raise

    def copy_to(self, items, tblname, columns, default_values={},
                cur=None, item_cb=None):
        """Copy items' entries to table tblname with columns information.

        Args:
            items (dict): dictionary of data to copy over tblname.
            tblname (str): destination table's name.
            columns ([str]): keys to access data in items and also the
              column names in the destination table.
            default_values (dict): dictionnary of default values to use when
              inserting entried int the tblname table.
            cur: a db cursor; if not given, a new cursor will be created.
            item_cb (fn): optional function to apply to items's entry.
        """

        read_file, write_file = os.pipe()

        def writer():
            cursor = self.cursor(cur)
            with open(read_file, 'r') as f:
                cursor.copy_expert('COPY %s (%s) FROM STDIN CSV' % (
                    tblname, ', '.join(columns)), f)

        write_thread = threading.Thread(target=writer)
        write_thread.start()

        try:
            with open(write_file, 'w') as f:
                for d in items:
                    if item_cb is not None:
                        item_cb(d)
                    line = [escape(d.get(k, default_values.get(k)))
                            for k in columns]
                    f.write(','.join(line))
                    f.write('\n')
        finally:
            # No problem bubbling up exceptions, but we still need to make sure
            # we finish copying, even though we're probably going to cancel the
            # transaction.
            write_thread.join()

    def mktemp(self, tblname, cur=None):
        self.cursor(cur).execute('SELECT swh_mktemp(%s)', (tblname,))
