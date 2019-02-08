# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

import pytest

from swh.core.logger import PostgresHandler

from swh.core.tests import SQL_DIR

DUMP_FILE = os.path.join(SQL_DIR, 'log-schema.sql')


@pytest.fixture
def swh_db_logger(postgresql_proc, postgresql):

    cursor = postgresql.cursor()
    with open(DUMP_FILE) as fobj:
        cursor.execute(fobj.read())
    postgresql.commit()
    modname = 'swh.core.tests.test_logger'
    logger = logging.Logger(modname, logging.DEBUG)
    dsn = 'postgresql://{user}@{host}:{port}/{dbname}'.format(
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        user='postgres',
        dbname='tests')
    logger.addHandler(PostgresHandler(dsn))
    return logger


@pytest.mark.db
def test_log(swh_db_logger, postgresql):
    logger = swh_db_logger
    modname = logger.name

    logger.info('notice',
                extra={'swh_type': 'test entry', 'swh_data': 42})
    logger.warning('warning')

    with postgresql.cursor() as cur:
        cur.execute('SELECT level, message, data, src_module FROM log')
        db_log_entries = cur.fetchall()

    assert ('info', 'notice', {'type': 'test entry', 'data': 42},
            modname) in db_log_entries
    assert ('warning', 'warning', {}, modname) in db_log_entries
