# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import psycopg2
import socket

from psycopg2.extras import Json


EXTRA_LOGDATA_PREFIX = 'swh_'


def db_level_of_py_level(lvl):
    """convert a log level of the logging module to a log level suitable for the
    logging Postgres DB

    """
    return logging.getLevelName(lvl).lower()


class PostgresHandler(logging.Handler):
    """log handler that store messages in a Postgres DB

    See swh-core/sql/log-schema.sql for the DB schema.

    All logging methods can be used as usual. Additionally, arbitrary metadata
    can be passed to logging methods, requesting that they will be stored in
    the DB as a single JSONB value. To do so, pass a dictionary to the 'extra'
    kwarg of any logging method; all keys in that dictionary that start with
    EXTRA_LOGDATA_PREFIX (currently: 'swh_') will be extracted to form the
    JSONB dictionary. The prefix will be stripped and not included in the DB.

    Note: the logger name will be used to fill the 'module' DB column.

    Sample usage:

        logging.basicConfig(level=logging.INFO)
        h = PostgresHandler('dbname=softwareheritage-log')
        logging.getLogger().addHandler(h)

        logger.info('not so important notice',
                    extra={'swh_type': 'swh_logging_test',
                           'swh_meditation': 'guru'})
        logger.warn('something weird just happened, did you see that?')

    """

    def __init__(self, connstring):
        """
        Create a Postgres log handler.

        Args:
            config: configuration dictionary, with a key "log_db" containing a
               libpq connection string to the log DB
        """
        super().__init__()

        self.connstring = connstring

        # Attributes for pid-safe psycopg2 connection handling
        self.__conn = None
        self.__conn_pid = None

        self.fqdn = socket.getfqdn()  # cache FQDN value

    def _connect(self):
        return psycopg2.connect(self.connstring)

    @property
    def conn(self):
        mypid = os.getpid()
        # Reconnect if we changed pid or the connection is broken
        if not self.__conn or self.__conn_pid != mypid or self.__conn.closed:
            self.__conn = self._connect()
            self.__conn_pid = mypid

        return self.__conn

    def close(self):
        # Only close the connection if we created it
        if self.__conn and self.__conn_pid == os.getpid():
            self.__conn.close()
        super().close()

    def emit(self, record):
        log_data = record.__dict__

        msg = self.format(record)

        extra_data = {k[len(EXTRA_LOGDATA_PREFIX):]: v
                      for k, v in log_data.items()
                      if k.startswith(EXTRA_LOGDATA_PREFIX)}
        log_entry = (db_level_of_py_level(log_data['levelno']), msg,
                     Json(extra_data), log_data['name'], self.fqdn,
                     os.getpid())

        with self.conn.cursor() as cur:
            cur.execute('INSERT INTO log '
                        '(level, message, data, src_module, src_host, src_pid)'
                        'VALUES (%s, %s, %s, %s, %s, %s)',
                        log_entry)
            self.conn.commit()
