# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pathlib

from click.testing import CliRunner
from hypothesis import HealthCheck
import pytest

from swh.core.db.db_utils import get_sql_for_package
from swh.core.utils import numfile_sortkey as sortkey

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
def mock_package_sql(mocker, datadir):
    """This bypasses the module manipulation to only returns the data test files.

    For a given module `test.mod`, look for sql files in the directory `data/mod/*.sql`.

    Typical usage::

      def test_xxx(cli_runner, mock_package_sql):
        conninfo = craft_conninfo(test_db, "new-db")
        module_name = "test.cli"
        # the command below will use sql scripts from swh/core/db/tests/data/cli/*.sql
        cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    """

    def get_sql_for_package_mock(modname, upgrade=False):
        if modname.startswith("test."):
            sqldir = pathlib.Path(datadir) / modname.split(".", 1)[1]
            if upgrade:
                sqldir /= "upgrades"
            return sorted(sqldir.glob("*.sql"), key=lambda x: sortkey(x.name))
        return get_sql_for_package(modname)

    mock_sql_files = mocker.patch(
        "swh.core.db.db_utils.get_sql_for_package", get_sql_for_package_mock
    )
    return mock_sql_files
