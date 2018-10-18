# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import unittest

import pytest

from swh.core.logger import PostgresHandler
from swh.core.tests.db_testing import SingleDbTestFixture

from swh.core.tests import SQL_DIR


@pytest.mark.db
class PgLogHandler(SingleDbTestFixture, unittest.TestCase):

    TEST_DB_DUMP = os.path.join(SQL_DIR, 'log-schema.sql')

    def setUp(self):
        super().setUp()
        self.modname = 'swh.core.tests.test_logger'
        self.logger = logging.Logger(self.modname, logging.DEBUG)
        self.logger.addHandler(PostgresHandler('dbname=' + self.TEST_DB_NAME))

    def tearDown(self):
        logging.shutdown()
        super().tearDown()

    def test_log(self):
        self.logger.info('notice',
                         extra={'swh_type': 'test entry', 'swh_data': 42})
        self.logger.warn('warning')

        with self.conn.cursor() as cur:
            cur.execute('SELECT level, message, data, src_module FROM log')
            db_log_entries = cur.fetchall()

        self.assertIn(('info', 'notice', {'type': 'test entry', 'data': 42},
                       self.modname),
                      db_log_entries)
        self.assertIn(('warning', 'warning', {}, self.modname), db_log_entries)
