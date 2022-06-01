# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
from importlib import import_module
import logging
import subprocess
from typing import Callable, Iterable, Iterator, List, Optional, Sequence, Set, Union
import warnings

from _pytest.fixtures import FixtureRequest
from deprecated import deprecated
import psycopg2
import pytest
from pytest_postgresql.compat import check_for_psycopg2, connection
from pytest_postgresql.executor import PostgreSQLExecutor
from pytest_postgresql.executor_noop import NoopExecutor
from pytest_postgresql.janitor import DatabaseJanitor

from swh.core.db.db_utils import initialize_database_for_module
from swh.core.utils import basename_sortkey

# to keep mypy happy regardless pytest-postgresql version
try:
    _pytest_pgsql_get_config_module = import_module("pytest_postgresql.config")
except ImportError:
    # pytest_postgresql < 3.0.0
    _pytest_pgsql_get_config_module = import_module("pytest_postgresql.factories")

_pytest_postgresql_get_config = getattr(_pytest_pgsql_get_config_module, "get_config")


logger = logging.getLogger(__name__)

initialize_database_for_module = deprecated(
    version="2.10",
    reason="Use swh.core.db.db_utils.initialize_database_for_module instead.",
)(initialize_database_for_module)

warnings.warn(
    "This pytest plugin is deprecated, it should not be used any more.",
    category=DeprecationWarning,
)


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
        port: int,
        dbname: str,
        version: Union[str, float],
        password: Optional[str] = None,
        isolation_level: Optional[int] = None,
        connection_timeout: int = 60,
        dump_files: Optional[Union[str, Sequence[str]]] = None,
        no_truncate_tables: Set[str] = set(),
        no_db_drop: bool = False,
    ) -> None:
        super().__init__(user, host, port, dbname, version)
        # do no truncate the following tables
        self.no_truncate_tables = set(no_truncate_tables)
        self.no_db_drop = no_db_drop
        self.dump_files = dump_files

    def psql_exec(self, fname: str) -> None:
        conninfo = (
            f"host={self.host} user={self.user} port={self.port} dbname={self.dbname}"
        )

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

    def db_reset(self) -> None:
        """Truncate tables (all but self.no_truncate_tables set) and sequences"""
        with psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            host=self.host,
            port=self.port,
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

    def _db_exists(self, cur, dbname):
        cur.execute(
            "SELECT EXISTS "
            "(SELECT datname FROM pg_catalog.pg_database WHERE datname= %s);",
            (dbname,),
        )
        row = cur.fetchone()
        return (row is not None) and row[0]

    def init(self) -> None:
        """Create database in postgresql out of a template it if it exists, bare
        creation otherwise."""
        template_name = f"{self.dbname}_tmpl"
        logger.debug("Initialize DB %s", self.dbname)
        with self.cursor() as cur:
            tmpl_exists = self._db_exists(cur, template_name)
            db_exists = self._db_exists(cur, self.dbname)
            if not db_exists:
                if tmpl_exists:
                    logger.debug(
                        "Create %s from template %s", self.dbname, template_name
                    )
                    cur.execute(
                        f'CREATE DATABASE "{self.dbname}" TEMPLATE "{template_name}";'
                    )
                else:
                    logger.debug("Create %s from scratch", self.dbname)
                    cur.execute(f'CREATE DATABASE "{self.dbname}";')
                if self.dump_files:
                    logger.warning(
                        "Using dump_files on the postgresql_fact fixture "
                        "is deprecated. See swh.core documentation for more "
                        "details."
                    )
                    for dump_file in gen_dump_files(self.dump_files):
                        logger.info(f"Loading {dump_file}")
                        self.psql_exec(dump_file)
            else:
                logger.debug("Reset %s", self.dbname)
                self.db_reset()

    def drop(self) -> None:
        """Drop database in postgresql."""
        if self.no_db_drop:
            with self.cursor() as cur:
                self._terminate_connection(cur, self.dbname)
        else:
            super().drop()


# the postgres_fact factory fixture below is mostly a copy of the code
# from pytest-postgresql. We need a custom version here to be able to
# specify our version of the DBJanitor we use.
@deprecated(version="2.10", reason="Use stock pytest_postgresql factory instead")
def postgresql_fact(
    process_fixture_name: str,
    dbname: Optional[str] = None,
    load: Optional[Sequence[Union[Callable, str]]] = None,
    isolation_level: Optional[int] = None,
    modname: Optional[str] = None,
    dump_files: Optional[Union[str, List[str]]] = None,
    no_truncate_tables: Set[str] = {"dbversion"},
    no_db_drop: bool = False,
) -> Callable[[FixtureRequest], Iterator[connection]]:
    """
    Return connection fixture factory for PostgreSQL.

    :param process_fixture_name: name of the process fixture
    :param dbname: database name
    :param load: SQL, function or function import paths to automatically load
                 into our test database
    :param isolation_level: optional postgresql isolation level
                            defaults to server's default
    :param modname: (swh) module name for which the database is created
    :dump_files: (deprecated, use load instead) list of sql script files to
                 execute after the database has been created
    :no_truncate_tables: list of table not to truncate between tests (only used
                         when no_db_drop is True)
    :no_db_drop: if True, keep the database between tests; in which case, the
                 database is reset (see SWHDatabaseJanitor.db_reset()) by truncating
                 most of the tables. Note that this makes de facto tests (potentially)
                 interdependent, use with extra caution.
    :returns: function which makes a connection to postgresql
    """

    @pytest.fixture
    def postgresql_factory(request: FixtureRequest) -> Iterator[connection]:
        """
        Fixture factory for PostgreSQL.

        :param request: fixture request object
        :returns: postgresql client
        """
        check_for_psycopg2()
        proc_fixture: Union[PostgreSQLExecutor, NoopExecutor] = request.getfixturevalue(
            process_fixture_name
        )

        pg_host = proc_fixture.host
        pg_port = proc_fixture.port
        pg_user = proc_fixture.user
        pg_password = proc_fixture.password
        pg_options = proc_fixture.options
        pg_db = dbname or proc_fixture.dbname
        pg_load = load or []
        assert pg_db is not None

        with SWHDatabaseJanitor(
            pg_user,
            pg_host,
            pg_port,
            pg_db,
            proc_fixture.version,
            pg_password,
            isolation_level=isolation_level,
            dump_files=dump_files,
            no_truncate_tables=no_truncate_tables,
            no_db_drop=no_db_drop,
        ) as janitor:
            db_connection: connection = psycopg2.connect(
                dbname=pg_db,
                user=pg_user,
                password=pg_password,
                host=pg_host,
                port=pg_port,
                options=pg_options,
            )
            for load_element in pg_load:
                janitor.load(load_element)
            try:
                yield db_connection
            finally:
                db_connection.close()

    return postgresql_factory


def gen_dump_files(dump_files: Union[str, Iterable[str]]) -> Iterator[str]:
    """Generate files potentially resolving glob patterns if any"""
    if isinstance(dump_files, str):
        dump_files = [dump_files]
    for dump_file in dump_files:
        if glob.has_magic(dump_file):
            # if the dump_file is a glob pattern one, resolve it
            yield from (
                fname for fname in sorted(glob.glob(dump_file), key=basename_sortkey)
            )
        else:
            # otherwise, just return the filename
            yield dump_file
