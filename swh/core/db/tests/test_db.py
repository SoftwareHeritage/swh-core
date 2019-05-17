# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
import tempfile
import unittest

from hypothesis import strategies, given
import pytest

from swh.core.db import BaseDb
from .db_testing import (
    SingleDbTestFixture, db_create, db_destroy, db_close,
)


INIT_SQL = '''
create table test_table
(
    i       int,
    txt     text,
    bytes   bytea
);
'''

db_rows = strategies.lists(strategies.tuples(
    strategies.integers(-2147483648, +2147483647),
    strategies.text(
        alphabet=strategies.characters(
            blacklist_categories=['Cs'],  # surrogates
            blacklist_characters=[
                '\x00',  # pgsql does not support the null codepoint
                '\r',  # pgsql normalizes those
            ]
        ),
    ),
    strategies.binary(),
))


@pytest.mark.db
def test_connect():
    db_name = db_create('test-db2', dumps=[])
    try:
        db = BaseDb.connect('dbname=%s' % db_name)
        with db.cursor() as cur:
            cur.execute(INIT_SQL)
            cur.execute("insert into test_table values (1, %s, %s);",
                        ('foo', b'bar'))
            cur.execute("select * from test_table;")
            assert list(cur) == [(1, 'foo', b'bar')]
    finally:
        db_close(db.conn)
        db_destroy(db_name)


@pytest.mark.db
class TestDb(SingleDbTestFixture, unittest.TestCase):
    TEST_DB_NAME = 'test-db'

    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as td:
            with open(os.path.join(td, 'init.sql'), 'a') as fd:
                fd.write(INIT_SQL)

            cls.TEST_DB_DUMP = os.path.join(td, '*.sql')

            super().setUpClass()

    def setUp(self):
        super().setUp()
        self.db = BaseDb(self.conn)

    def test_initialized(self):
        cur = self.db.cursor()
        cur.execute("insert into test_table values (1, %s, %s);",
                    ('foo', b'bar'))
        cur.execute("select * from test_table;")
        self.assertEqual(list(cur), [(1, 'foo', b'bar')])

    def test_reset_tables(self):
        cur = self.db.cursor()
        cur.execute("insert into test_table values (1, %s, %s);",
                    ('foo', b'bar'))
        self.reset_db_tables('test-db')
        cur.execute("select * from test_table;")
        self.assertEqual(list(cur), [])

    @given(db_rows)
    def test_copy_to(self, data):
        # the table is not reset between runs by hypothesis
        self.reset_db_tables('test-db')

        items = [dict(zip(['i', 'txt', 'bytes'], item)) for item in data]
        self.db.copy_to(items, 'test_table', ['i', 'txt', 'bytes'])

        cur = self.db.cursor()
        cur.execute('select * from test_table;')
        self.assertCountEqual(list(cur), data)
