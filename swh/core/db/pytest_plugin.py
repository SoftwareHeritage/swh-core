# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
from importlib import import_module
import logging
import subprocess
from typing import List, Optional, Set, Union

from _pytest.fixtures import FixtureRequest
import psycopg2
import pytest
from pytest_postgresql.janitor import DatabaseJanitor

from swh.core.utils import numfile_sortkey as sortkey

# to keep mypy happy regardless pytest-postgresql version
try:
    _pytest_pgsql_get_config_module = import_module("pytest_postgresql.config")
except ImportError:
    # pytest_postgresql < 3.0.0
    _pytest_pgsql_get_config_module = import_module("pytest_postgresql.factories")

_pytest_postgresql_get_config = getattr(_pytest_pgsql_get_config_module, "get_config")


logger = logging.getLogger(__name__)


class SWHDatabaseJanitor(DatabaseJanitor):
    """SWH database janitor implementation with a a different setup/teardown policy than
    than the stock one. Instead of dropping, creating and initializing the database for
    each test, it creates and initializes the db once, then truncates the tables (and
    sequences) in between tests.

    This is needed to have acceptable test performances.

    """

    def __init__(
        self,
        user: str,
        host: str,
        port: str,
        dbname: str,
        version: Union[str, float],
        dump_files: Union[None, str, List[str]] = None,
        no_truncate_tables: Set[str] = set(),
    ) -> None:
        super().__init__(user, host, port, dbname, version)
        if not hasattr(self, "dbname") and hasattr(self, "db_name"):
            # pytest_postgresql < 3.0.0
            self.dbname = getattr(self, "db_name")
        if dump_files is None:
            self.dump_files = []
        elif isinstance(dump_files, str):
            self.dump_files = sorted(glob.glob(dump_files), key=sortkey)
        else:
            self.dump_files = dump_files
        # do no truncate the following tables
        self.no_truncate_tables = set(no_truncate_tables)

    def db_setup(self):
        conninfo = (
            f"host={self.host} user={self.user} port={self.port} dbname={self.dbname}"
        )

        for fname in self.dump_files:
            subprocess.check_call(
                [
                    "psql",
                    "--quiet",
                    "--no-psqlrc",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-d",
                    conninfo,
                    "-f",
                    fname,
                ]
            )

    def db_reset(self):
        """Truncate tables (all but self.no_truncate_tables set) and sequences

        """
        with psycopg2.connect(
            dbname=self.dbname, user=self.user, host=self.host, port=self.port,
        ) as cnx:
            with cnx.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s",
                    ("public",),
                )
                all_tables = set(table for (table,) in cur.fetchall())
                tables_to_truncate = all_tables - self.no_truncate_tables

                for table in tables_to_truncate:
                    cur.execute("TRUNCATE TABLE %s CASCADE" % table)

                cur.execute(
                    "SELECT sequence_name FROM information_schema.sequences "
                    "WHERE sequence_schema = %s",
                    ("public",),
                )
                seqs = set(seq for (seq,) in cur.fetchall())
                for seq in seqs:
                    cur.execute("ALTER SEQUENCE %s RESTART;" % seq)
            cnx.commit()

    def init(self):
        """Initialize db. Create the db if it does not exist. Reset it if it exists."""
        with self.cursor() as cur:
            cur.execute(
                "SELECT COUNT(1) FROM pg_database WHERE datname=%s;", (self.dbname,)
            )
            db_exists = cur.fetchone()[0] == 1
            if db_exists:
                cur.execute(
                    "UPDATE pg_database SET datallowconn=true WHERE datname = %s;",
                    (self.dbname,),
                )
                self.db_reset()
                return

        # initialize the inexistent db
        with self.cursor() as cur:
            cur.execute('CREATE DATABASE "{}";'.format(self.dbname))
        self.db_setup()

    def drop(self):
        """The original DatabaseJanitor implementation prevents new connections from happening,
           destroys current opened connections and finally drops the database.

           We actually do not want to drop the db so we instead do nothing and resets
           (truncate most tables and sequences) the db instead, in order to have some
           acceptable performance.

        """
        pass


# the postgres_fact factory fixture below is mostly a copy of the code
# from pytest-postgresql. We need a custom version here to be able to
# specify our version of the DBJanitor we use.
def postgresql_fact(
    process_fixture_name: str,
    dbname: Optional[str] = None,
    dump_files: Union[str, List[str]] = "",
    no_truncate_tables: Set[str] = {"dbversion"},
):
    @pytest.fixture
    def postgresql_factory(request: FixtureRequest):
        """Fixture factory for PostgreSQL.

        :param FixtureRequest request: fixture request object
        :rtype: psycopg2.connection
        :returns: postgresql client
        """
        config = _pytest_postgresql_get_config(request)
        proc_fixture = request.getfixturevalue(process_fixture_name)

        pg_host = proc_fixture.host
        pg_port = proc_fixture.port
        pg_user = proc_fixture.user
        pg_options = proc_fixture.options
        pg_db = dbname or config["dbname"]
        with SWHDatabaseJanitor(
            pg_user,
            pg_host,
            pg_port,
            pg_db,
            proc_fixture.version,
            dump_files=dump_files,
            no_truncate_tables=no_truncate_tables,
        ):
            connection = psycopg2.connect(
                dbname=pg_db,
                user=pg_user,
                host=pg_host,
                port=pg_port,
                options=pg_options,
            )
            yield connection
            connection.close()

    return postgresql_factory
