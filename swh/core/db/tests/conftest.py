# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from click.testing import CliRunner
from hypothesis import HealthCheck
import psycopg2
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import import_swhmodule

os.environ["LC_ALL"] = "C.UTF-8"

# we use getattr here to keep mypy happy regardless hypothesis version
function_scoped_fixture_check = (
    [getattr(HealthCheck, "function_scoped_fixture")]
    if hasattr(HealthCheck, "function_scoped_fixture")
    else []
)


def create_role_guest(**kwargs):
    with psycopg2.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            cur.execute("CREATE ROLE guest NOINHERIT LOGIN PASSWORD 'guest'")


postgresql_proc = factories.postgresql_proc(
    load=[create_role_guest],
)


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture()
def mock_import_swhmodule(request, mocker, datadir):
    """This bypasses the module manipulation to make import_swhmodule return a mock
    object suitable for data test files listing via get_sql_for_package.

    For a given backend `test:<cls>`, return a MagicMock object with a __name__
    set to `test` and __file__ pointing to `data/<cls>/__init__.py`.

    The Mock object also defines a `get_datastore()` attribute on which the
    `current_version` attribute is set to 3.

    """
    mock = mocker.MagicMock

    def import_swhmodule_mock(modname):
        if modname.startswith("test"):
            # this insanity really should be cleaned up...
            if ":" in modname:
                modname, cls = modname.split(":", 1)
            else:
                cls = "postgresql"
            if "." in modname:
                dirname = modname.split(".", 1)[1]
            else:
                dirname = cls

            m = request.node.get_closest_marker("init_version")
            if m:
                version = m.kwargs.get("version", 1)
            else:
                version = 3

            def get_datastore(*args, **kw):
                return mock(current_version=version)

            return mock(
                __name__=modname,
                __file__=os.path.join(datadir, dirname, "__init__.py"),
                get_datastore=get_datastore,
            )
        else:
            return import_swhmodule(modname)

    return mocker.patch("swh.core.db.db_utils.import_swhmodule", import_swhmodule_mock)


@pytest.fixture()
def mock_get_swh_backend_module(request, mocker, datadir, mock_import_swhmodule):
    """This bypasses the swh.core backend loading mechanism

    It mock both the entry_point based module loading tool
    (get_swh_backend_module) and the "normal" module loader (import_swhmodule).

    For a given backend `test:<cls>`, return a MagicMock object with a __name__
    set to `test` and __file__ pointing to `data/<cls>/__init__.py`.

    The Mock object also defines a `get_datastore()` attribute on which the
    `current_version` attribute is set to 3.

    Typical usage::

      def test_xxx(cli_runner, mock_import_swhmodule):
        conninfo = craft_conninfo(test_db, "new-db")
        module_name = "test"
        # the command below will use sql scripts from
        #     swh/core/db/tests/data/postgresal/sql/*.sql
        # 'postgresql' being the default backend cls.
        cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])

    """
    mock = mocker.MagicMock

    def get_swh_backend_module_mock(swh_package, cls):

        assert swh_package == "test"

        m = request.node.get_closest_marker("init_version")
        if m:
            version = m.kwargs.get("version", 1)
        else:
            version = 3

        def get_datastore(*args, **kw):
            return mock(current_version=version)

        return f"{swh_package}.{cls}", mock(
            __name__=swh_package,
            __file__=os.path.join(datadir, cls, "__init__.py"),
            get_datastore=get_datastore,
        )

    return mocker.patch(
        "swh.core.config.get_swh_backend_module", get_swh_backend_module_mock
    )
