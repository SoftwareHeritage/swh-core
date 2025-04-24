# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import datetime
import enum
import json
import logging
import os
import sys
import threading
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Type, TypeVar

import psycopg
from psycopg.types.range import Range
import psycopg_pool

logger = logging.getLogger(__name__)


def render_array(data) -> str:
    """Render the data as a postgresql array"""
    # From https://www.postgresql.org/docs/11/arrays.html#ARRAYS-IO
    # "The external text representation of an array value consists of items that are
    # interpreted according to the I/O conversion rules for the array's element type,
    # plus decoration that indicates the array structure. The decoration consists of
    # curly braces ({ and }) around the array value plus delimiter characters between
    # adjacent items. The delimiter character is usually a comma (,)"
    return "{%s}" % ",".join(render_array_element(e) for e in data)


def render_array_element(element) -> str:
    """Render an element from an array."""
    if element is None:
        # From https://www.postgresql.org/docs/11/arrays.html#ARRAYS-IO
        # "If the value written for an element is NULL (in any case variant), the
        # element is taken to be NULL."
        return "NULL"
    elif isinstance(element, (list, tuple)):
        # From https://www.postgresql.org/docs/11/arrays.html#ARRAYS-INPUT
        # "Each val is either a constant of the array element type, or a subarray."
        return render_array(element)
    else:
        # From https://www.postgresql.org/docs/11/arrays.html#ARRAYS-IO
        # "When writing an array value you can use double quotes around any individual
        # array element. [...] Empty strings and strings matching the word NULL must be
        # quoted, too. To put a double quote or backslash in a quoted array element
        # value, precede it with a backslash."
        ret = value_as_pg_text(element)
        return '"%s"' % ret.replace("\\", "\\\\").replace('"', '\\"')


def value_as_pg_text(data: Any) -> str:
    """Render the given data in the postgresql text format.

    NULL values are handled **outside** of this function (either by
    :func:`render_array_element`, or by :meth:`BaseDb.copy_to`.)
    """

    if data is None:
        raise ValueError("value_as_pg_text doesn't handle NULLs")

    if isinstance(data, bytes):
        return "\\x%s" % data.hex()
    elif isinstance(data, datetime.datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return json.dumps(data)
    elif isinstance(data, (list, tuple)):
        return render_array(data)
    elif isinstance(data, Range):
        return "%s%s,%s%s" % (
            "[" if data.lower_inc else "(",
            "-infinity" if data.lower_inf else value_as_pg_text(data.lower),
            "infinity" if data.upper_inf else value_as_pg_text(data.upper),
            "]" if data.upper_inc else ")",
        )
    elif isinstance(data, enum.IntEnum):
        return str(int(data))
    else:
        return str(data)


def escape_copy_column(column: str) -> str:
    """Escape the text representation of a column for use by COPY."""
    # From https://www.postgresql.org/docs/11/sql-copy.html
    # File Formats > Text Format
    # "Backslash characters (\) can be used in the COPY data to quote data characters
    # that might otherwise be taken as row or column delimiters. In particular, the
    # following characters must be preceded by a backslash if they appear as part of a
    # column value: backslash itself, newline, carriage return, and the current
    # delimiter character."
    ret = (
        column.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )

    return ret


BaseDbType = TypeVar("BaseDbType", bound="BaseDb")


def _force_utc_conn(conn: psycopg.Connection[Any]) -> None:
    """Set the connection to the UTC timezone instead of the local one

    This is done to avoid psycopg returning timestamps in the local timezone
    instead of the UTC one.

    This is apparently the only way to do this, there are no arguments we could
    pass to connect nor attributes we could set on the connection.
    """
    with conn.transaction():
        conn.execute("SET TIME ZONE 'UTC'")


class BaseDb:
    """Base class for swh.*.*Db.

    cf. swh.storage.db.Db, swh.archiver.db.ArchiverDb

    """

    @classmethod
    def connect(cls: Type[BaseDbType], *args, **kwargs) -> BaseDbType:
        """factory method to create a DB proxy

        Accepts all arguments of psycopg.connect; only some specific
        possibilities are reported below.

        Args:
            connstring: libpq2 connection string

        """
        conn = psycopg.connect(*args, **kwargs)
        return cls(conn)

    @classmethod
    def from_pool(
        cls: Type[BaseDbType], pool: psycopg_pool.ConnectionPool
    ) -> BaseDbType:
        conn = pool.getconn()
        return cls(conn, pool=pool)

    def __init__(
        self,
        conn: psycopg.Connection[Any],
        pool: Optional[psycopg_pool.ConnectionPool] = None,
    ):
        """create a DB proxy

        Args:
            conn: psycopg connection to the SWH DB
            pool: psycopg pool of connections

        """
        _force_utc_conn(conn)
        self.conn = conn
        self.pool = pool

    def close(self):
        return self.conn.close()

    def put_conn(self) -> None:
        if self.pool:
            self.pool.putconn(self.conn)

    def cursor(self, cur_arg: Optional[psycopg.Cursor] = None) -> psycopg.Cursor:
        """get a cursor: from cur_arg if given, or a fresh one otherwise

        meant to avoid boilerplate if/then/else in methods that proxy stored
        procedures

        """
        if cur_arg is not None:
            return cur_arg
        else:
            return self.conn.cursor()

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        return self.conn.__exit__(*args, **kwargs)

    _cursor = cursor  # for bw compat

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Cursor]:
        """context manager to execute within a DB transaction

        Yields:
            a psycopg cursor

        """
        with self.conn.cursor() as cur:
            try:
                yield cur
                self.conn.commit()
            except Exception:
                if not self.conn.closed:
                    self.conn.rollback()
                raise

    def copy_to(
        self,
        items: Iterable[Mapping[str, Any]],
        tblname: str,
        columns: Iterable[str],
        cur: Optional[psycopg.Cursor] = None,
        item_cb: Optional[Callable[[Any], Any]] = None,
        default_values: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Run the COPY command to insert the `columns` of each element of `items` into
        `tblname`.

        Args:
            items: dictionaries of data to copy into `tblname`.
            tblname: name of the destination table.
            columns: columns of the destination table. Elements of `items` must have
              these set as keys.
            default_values: dictionary of default values to use when inserting entries
              in `tblname`.
            cur: a db cursor; if not given, a new cursor will be created.
            item_cb: optional callback, run on each element of `items`, when it is
              copied.

        """
        if default_values is None:
            default_values = {}

        read_file, write_file = os.pipe()
        exc_info = None

        def writer():
            nonlocal exc_info
            cursor = self.cursor(cur)
            with open(read_file, "r") as f:
                try:
                    with cursor.copy(
                        "COPY %s (%s) FROM STDIN" % (tblname, ", ".join(columns))
                    ) as c:
                        while data := f.read(4096):
                            c.write(data)

                except Exception:
                    # Tell the main thread about the exception
                    exc_info = sys.exc_info()

        write_thread = threading.Thread(target=writer)
        write_thread.start()

        try:
            with open(write_file, "w") as f:
                # From https://www.postgresql.org/docs/11/sql-copy.html
                # File Formats > Text Format
                # "When the text format is used, the data read or written is a text file
                # with one line per table row. Columns in a row are separated by the
                # delimiter character."
                # NULL
                # "The default is \N (backslash-N) in text format."
                # DELIMITER
                # "The default is a tab character in text format."
                for d in items:
                    if item_cb is not None:
                        item_cb(d)
                    line = []
                    for k in columns:
                        value = d.get(k, default_values.get(k))
                        try:
                            if value is None:
                                line.append("\\N")
                            else:
                                line.append(escape_copy_column(value_as_pg_text(value)))
                        except Exception as e:
                            logger.error(
                                "Could not escape value `%r` for column `%s`:"
                                "Received exception: `%s`",
                                value,
                                k,
                                e,
                            )
                            raise e from None
                    f.write("\t".join(line))
                    f.write("\n")

        finally:
            # No problem bubbling up exceptions, but we still need to make sure
            # we finish copying, even though we're probably going to cancel the
            # transaction.
            write_thread.join()
            if exc_info:
                # postgresql returned an error, let's raise it.
                raise exc_info[1].with_traceback(exc_info[2])

    def mktemp(self, tblname: str, cur: Optional[psycopg.Cursor] = None):
        self.cursor(cur).execute("SELECT swh_mktemp(%s)", (tblname,))
