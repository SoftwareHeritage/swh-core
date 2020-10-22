# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
import re
from typing import Optional, Union

import psycopg2
import psycopg2.extensions

logger = logging.getLogger(__name__)


def stored_procedure(stored_proc):
    """decorator to execute remote stored procedure, specified as argument

    Generally, the body of the decorated function should be empty. If it is
    not, the stored procedure will be executed first; the function body then.

    """

    def wrap(meth):
        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            cur = kwargs.get("cur", None)
            self._cursor(cur).execute("SELECT %s()" % stored_proc)
            meth(self, *args, **kwargs)

        return _meth

    return wrap


def jsonize(value):
    """Convert a value to a psycopg2 JSON object if necessary"""
    if isinstance(value, dict):
        return psycopg2.extras.Json(value)

    return value


def connect_to_conninfo(
    db_or_conninfo: Union[str, psycopg2.extensions.connection]
) -> psycopg2.extensions.connection:
    """Connect to the database passed in argument

    Args:
        db_or_conninfo: A database connection, or a database connection info string

    Returns:
        a connected database handle

    Raises:
        psycopg2.Error if the database doesn't exist
    """
    if isinstance(db_or_conninfo, psycopg2.extensions.connection):
        return db_or_conninfo

    if "=" not in db_or_conninfo and "//" not in db_or_conninfo:
        # Database name
        db_or_conninfo = f"dbname={db_or_conninfo}"

    db = psycopg2.connect(db_or_conninfo)

    return db


def swh_db_version(
    db_or_conninfo: Union[str, psycopg2.extensions.connection]
) -> Optional[int]:
    """Retrieve the swh version of the database.

    If the database is not initialized, this logs a warning and returns None.

    Args:
      db_or_conninfo: A database connection, or a database connection info string

    Returns:
        Either the version of the database, or None if it couldn't be detected
    """
    try:
        db = connect_to_conninfo(db_or_conninfo)
    except psycopg2.Error:
        logger.exception("Failed to connect to `%s`", db_or_conninfo)
        # Database not initialized
        return None

    try:
        with db.cursor() as c:
            query = "select version from dbversion order by dbversion desc limit 1"
            try:
                c.execute(query)
                return c.fetchone()[0]
            except psycopg2.errors.UndefinedTable:
                return None
    except Exception:
        logger.exception("Could not get version from `%s`", db_or_conninfo)
        return None


def swh_db_flavor(
    db_or_conninfo: Union[str, psycopg2.extensions.connection]
) -> Optional[str]:
    """Retrieve the swh flavor of the database.

    If the database is not initialized, or the database doesn't support
    flavors, this returns None.

    Args:
      db_or_conninfo: A database connection, or a database connection info string

    Returns:
        The flavor of the database, or None if it could not be detected.
    """
    try:
        db = connect_to_conninfo(db_or_conninfo)
    except psycopg2.Error:
        logger.exception("Failed to connect to `%s`", db_or_conninfo)
        # Database not initialized
        return None

    try:
        with db.cursor() as c:
            query = "select swh_get_dbflavor()"
            try:
                c.execute(query)
                return c.fetchone()[0]
            except psycopg2.errors.UndefinedFunction:
                # function not found: no flavor
                return None
    except Exception:
        logger.exception("Could not get flavor from `%s`", db_or_conninfo)
        return None


# The following code has been imported from psycopg2, version 2.7.4,
# https://github.com/psycopg/psycopg2/tree/5afb2ce803debea9533e293eef73c92ffce95bcd
# and modified by Software Heritage.
#
# Original file: lib/extras.py
#
# psycopg2 is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.


def _paginate(seq, page_size):
    """Consume an iterable and return it in chunks.
    Every chunk is at most `page_size`. Never return an empty chunk.
    """
    page = []
    it = iter(seq)
    while 1:
        try:
            for i in range(page_size):
                page.append(next(it))
            yield page
            page = []
        except StopIteration:
            if page:
                yield page
            return


def _split_sql(sql):
    """Split *sql* on a single ``%s`` placeholder.
    Split on the %s, perform %% replacement and return pre, post lists of
    snippets.
    """
    curr = pre = []
    post = []
    tokens = re.split(br"(%.)", sql)
    for token in tokens:
        if len(token) != 2 or token[:1] != b"%":
            curr.append(token)
            continue

        if token[1:] == b"s":
            if curr is pre:
                curr = post
            else:
                raise ValueError("the query contains more than one '%s' placeholder")
        elif token[1:] == b"%":
            curr.append(b"%")
        else:
            raise ValueError(
                "unsupported format character: '%s'"
                % token[1:].decode("ascii", "replace")
            )

    if curr is pre:
        raise ValueError("the query doesn't contain any '%s' placeholder")

    return pre, post


def execute_values_generator(cur, sql, argslist, template=None, page_size=100):
    """Execute a statement using SQL ``VALUES`` with a sequence of parameters.
    Rows returned by the query are returned through a generator.
    You need to consume the generator for the queries to be executed!

    :param cur: the cursor to use to execute the query.
    :param sql: the query to execute. It must contain a single ``%s``
        placeholder, which will be replaced by a `VALUES list`__.
        Example: ``"INSERT INTO mytable (id, f1, f2) VALUES %s"``.
    :param argslist: sequence of sequences or dictionaries with the arguments
        to send to the query. The type and content must be consistent with
        *template*.
    :param template: the snippet to merge to every item in *argslist* to
        compose the query.

        - If the *argslist* items are sequences it should contain positional
          placeholders (e.g. ``"(%s, %s, %s)"``, or ``"(%s, %s, 42)``" if there
          are constants value...).
        - If the *argslist* items are mappings it should contain named
          placeholders (e.g. ``"(%(id)s, %(f1)s, 42)"``).

        If not specified, assume the arguments are sequence and use a simple
        positional template (i.e.  ``(%s, %s, ...)``), with the number of
        placeholders sniffed by the first element in *argslist*.
    :param page_size: maximum number of *argslist* items to include in every
        statement. If there are more items the function will execute more than
        one statement.
    :param yield_from_cur: Whether to yield results from the cursor in this
        function directly.

    .. __: https://www.postgresql.org/docs/current/static/queries-values.html

    After the execution of the function the `cursor.rowcount` property will
    **not** contain a total result.
    """
    # we can't just use sql % vals because vals is bytes: if sql is bytes
    # there will be some decoding error because of stupid codec used, and Py3
    # doesn't implement % on bytes.
    if not isinstance(sql, bytes):
        sql = sql.encode(psycopg2.extensions.encodings[cur.connection.encoding])
    pre, post = _split_sql(sql)

    for page in _paginate(argslist, page_size=page_size):
        if template is None:
            template = b"(" + b",".join([b"%s"] * len(page[0])) + b")"
        parts = pre[:]
        for args in page:
            parts.append(cur.mogrify(template, args))
            parts.append(b",")
        parts[-1:] = post
        cur.execute(b"".join(parts))
        yield from cur
