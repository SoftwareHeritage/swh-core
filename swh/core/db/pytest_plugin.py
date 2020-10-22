# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from typing import Optional, Set

import psycopg2
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import SWHDatabaseJanitor

logger = logging.getLogger(__name__)


# the postgres_fact factory fixture below is mostly a copy of the code
# from pytest-postgresql. We need a custom version here to be able to
# specify our version of the DBJanitor we use.
def postgresql_fact(
    process_fixture_name: str,
    db_name: Optional[str] = None,
    dump_files: str = "",
    no_truncate_tables: Set[str] = {"dbversion"},
):
    @pytest.fixture
    def postgresql_factory(request):
        """Fixture factory for PostgreSQL.

        :param FixtureRequest request: fixture request object
        :rtype: psycopg2.connection
        :returns: postgresql client
        """
        config = factories.get_config(request)
        proc_fixture = request.getfixturevalue(process_fixture_name)

        pg_host = proc_fixture.host
        pg_port = proc_fixture.port
        pg_user = proc_fixture.user
        pg_options = proc_fixture.options
        pg_db = db_name or config["dbname"]
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
