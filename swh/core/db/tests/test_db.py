# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect
import os.path
import tempfile
import unittest
from unittest.mock import Mock, MagicMock

from hypothesis import strategies, given
import psycopg2
import pytest

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction, db_transaction_generator
from .db_testing import (
    SingleDbTestFixture,
    db_create,
    db_destroy,
    db_close,
)


INIT_SQL = """
create table test_table
(
    i       int,
    txt     text,
    bytes   bytea
);
"""

db_rows = strategies.lists(
    strategies.tuples(
        strategies.integers(-2147483648, +2147483647),
        strategies.text(
            alphabet=strategies.characters(
                blacklist_categories=["Cs"],  # surrogates
                blacklist_characters=[
                    "\x00",  # pgsql does not support the null codepoint
                    "\r",  # pgsql normalizes those
                ],
            ),
        ),
        strategies.binary(),
    )
)


@pytest.mark.db
def test_connect():
    db_name = db_create("test-db2", dumps=[])
    try:
        db = BaseDb.connect("dbname=%s" % db_name)
        with db.cursor() as cur:
            cur.execute(INIT_SQL)
            cur.execute("insert into test_table values (1, %s, %s);", ("foo", b"bar"))
            cur.execute("select * from test_table;")
            assert list(cur) == [(1, "foo", b"bar")]
    finally:
        db_close(db.conn)
        db_destroy(db_name)


@pytest.mark.db
class TestDb(SingleDbTestFixture, unittest.TestCase):
    TEST_DB_NAME = "test-db"

    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, "init.sql"), "a") as fd:
                fd.write(INIT_SQL)

            cls.TEST_DB_DUMP = os.path.join(td, "*.sql")

            super().setUpClass()

    def setUp(self):
        super().setUp()
        self.db = BaseDb(self.conn)

    def test_initialized(self):
        cur = self.db.cursor()
        cur.execute("insert into test_table values (1, %s, %s);", ("foo", b"bar"))
        cur.execute("select * from test_table;")
        self.assertEqual(list(cur), [(1, "foo", b"bar")])

    def test_reset_tables(self):
        cur = self.db.cursor()
        cur.execute("insert into test_table values (1, %s, %s);", ("foo", b"bar"))
        self.reset_db_tables("test-db")
        cur.execute("select * from test_table;")
        self.assertEqual(list(cur), [])

    @given(db_rows)
    def test_copy_to(self, data):
        # the table is not reset between runs by hypothesis
        self.reset_db_tables("test-db")

        items = [dict(zip(["i", "txt", "bytes"], item)) for item in data]
        self.db.copy_to(items, "test_table", ["i", "txt", "bytes"])

        cur = self.db.cursor()
        cur.execute("select * from test_table;")
        self.assertCountEqual(list(cur), data)

    def test_copy_to_thread_exception(self):
        data = [(2 ** 65, "foo", b"bar")]

        items = [dict(zip(["i", "txt", "bytes"], item)) for item in data]
        with self.assertRaises(psycopg2.errors.NumericValueOutOfRange):
            self.db.copy_to(items, "test_table", ["i", "txt", "bytes"])


def test_db_transaction(mocker):
    expected_cur = object()

    called = False

    class Storage:
        @db_transaction()
        def endpoint(self, cur=None, db=None):
            nonlocal called
            called = True
            assert cur is expected_cur

    storage = Storage()

    # 'with storage.get_db().transaction() as cur:' should cause
    # 'cur' to be 'expected_cur'
    db_mock = Mock()
    db_mock.transaction.return_value = MagicMock()
    db_mock.transaction.return_value.__enter__.return_value = expected_cur
    mocker.patch.object(storage, "get_db", return_value=db_mock, create=True)

    put_db_mock = mocker.patch.object(storage, "put_db", create=True)

    storage.endpoint()

    assert called
    put_db_mock.assert_called_once_with(db_mock)


def test_db_transaction__with_generator():
    with pytest.raises(ValueError, match="generator"):

        class Storage:
            @db_transaction()
            def endpoint(self, cur=None, db=None):
                yield None


def test_db_transaction_signature():
    """Checks db_transaction removes the 'cur' and 'db' arguments."""

    def f(self, foo, *, bar=None):
        pass

    expected_sig = inspect.signature(f)

    @db_transaction()
    def g(self, foo, *, bar=None, db=None, cur=None):
        pass

    actual_sig = inspect.signature(g)

    assert actual_sig == expected_sig


def test_db_transaction_generator(mocker):
    expected_cur = object()

    called = False

    class Storage:
        @db_transaction_generator()
        def endpoint(self, cur=None, db=None):
            nonlocal called
            called = True
            assert cur is expected_cur
            yield None

    storage = Storage()

    # 'with storage.get_db().transaction() as cur:' should cause
    # 'cur' to be 'expected_cur'
    db_mock = Mock()
    db_mock.transaction.return_value = MagicMock()
    db_mock.transaction.return_value.__enter__.return_value = expected_cur
    mocker.patch.object(storage, "get_db", return_value=db_mock, create=True)

    put_db_mock = mocker.patch.object(storage, "put_db", create=True)

    list(storage.endpoint())

    assert called
    put_db_mock.assert_called_once_with(db_mock)


def test_db_transaction_generator__with_nongenerator():
    with pytest.raises(ValueError, match="generator"):

        class Storage:
            @db_transaction_generator()
            def endpoint(self, cur=None, db=None):
                pass


def test_db_transaction_generator_signature():
    """Checks db_transaction removes the 'cur' and 'db' arguments."""

    def f(self, foo, *, bar=None):
        pass

    expected_sig = inspect.signature(f)

    @db_transaction_generator()
    def g(self, foo, *, bar=None, db=None, cur=None):
        yield None

    actual_sig = inspect.signature(g)

    assert actual_sig == expected_sig
