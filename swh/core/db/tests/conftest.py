# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from click.testing import CliRunner
from hypothesis import HealthCheck
import pytest

from swh.core.db.db_utils import import_swhmodule

os.environ["LC_ALL"] = "C.UTF-8"

# we use getattr here to keep mypy happy regardless hypothesis version
function_scoped_fixture_check = (
    [getattr(HealthCheck, "function_scoped_fixture")]
    if hasattr(HealthCheck, "function_scoped_fixture")
    else []
)


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture()
def mock_import_swhmodule(mocker, datadir):
    """This bypasses the module manipulation to make import_swhmodule return a mock
    object suitable for data test files listing via get_sql_for_package.

    For a given module `test.<mod>`, return a MagicMock object with a __name__
    set to `<mod>` and __file__ pointing to `data/<mod>/__init__.py`.

    The Mock object also defines a `get_datastore()` attribute on which the
    `get_current_version()` exists and will return 42.

    Typical usage::

      def test_xxx(cli_runner, mock_import_swhmodule):
        conninfo = craft_conninfo(test_db, "new-db")
        module_name = "test.cli"
        # the command below will use sql scripts from
        #     swh/core/db/tests/data/cli/sql/*.sql
        cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])

    """
    mock = mocker.MagicMock

    def import_swhmodule_mock(modname):
        if modname.startswith("test."):
            dirname = modname.split(".", 1)[1]

            def get_datastore(*args, **kw):
                return mock(get_current_version=lambda: 42)

            return mock(
                __name__=modname,
                __file__=os.path.join(datadir, dirname, "__init__.py"),
                get_datastore=get_datastore,
            )
        else:
            return import_swhmodule(modname)

    return mocker.patch("swh.core.db.db_utils.import_swhmodule", import_swhmodule_mock)
