# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import glob
from os import path

from click.testing import CliRunner
import pytest

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.pytest_plugin import postgresql_fact
from swh.core.tests.test_cli import assert_section_contains


@pytest.fixture
def cli_runner():
    return CliRunner()


def test_cli_swh_help(swhmain, cli_runner):
    swhmain.add_command(swhdb)
    result = cli_runner.invoke(swhmain, ["-h"])
    assert result.exit_code == 0
    assert_section_contains(
        result.output, "Commands", "db  Software Heritage database generic tools."
    )


help_db_snippets = (
    (
        "Usage",
        (
            "Usage: swh db [OPTIONS] COMMAND [ARGS]...",
            "Software Heritage database generic tools.",
        ),
    ),
    (
        "Commands",
        (
            "create      Create a database for the Software Heritage <module>.",
            "init        Initialize a database for the Software Heritage <module>.",
            "init-admin  Execute superuser-level initialization steps",
        ),
    ),
)


def test_cli_swh_db_help(swhmain, cli_runner):
    swhmain.add_command(swhdb)
    result = cli_runner.invoke(swhmain, ["db", "-h"])
    assert result.exit_code == 0
    for section, snippets in help_db_snippets:
        for snippet in snippets:
            assert_section_contains(result.output, section, snippet)


@pytest.fixture()
def mock_package_sql(mocker, datadir):
    """This bypasses the module manipulation to only returns the data test files.

    """
    from swh.core.utils import numfile_sortkey as sortkey

    mock_sql_files = mocker.patch("swh.core.cli.db.get_sql_for_package")
    sql_files = sorted(glob.glob(path.join(datadir, "cli", "*.sql")), key=sortkey)
    mock_sql_files.return_value = sql_files
    return mock_sql_files


# We do not want the truncate behavior for those tests
test_db = postgresql_fact(
    "postgresql_proc", db_name="clidb", no_truncate_tables={"dbversion", "origin"}
)


@pytest.fixture
def swh_db_cli(cli_runner, monkeypatch, test_db):
    """This initializes a cli_runner and sets the correct environment variable expected by
       the cli to run appropriately (when not specifying the --db-name flag)

    """
    db_params = test_db.get_dsn_parameters()
    monkeypatch.setenv("PGHOST", db_params["host"])
    monkeypatch.setenv("PGUSER", db_params["user"])
    monkeypatch.setenv("PGPORT", db_params["port"])

    return cli_runner, db_params


def craft_conninfo(test_db, dbname=None) -> str:
    """Craft conninfo string out of the test_db object. This also allows to override the
    dbname."""
    db_params = test_db.get_dsn_parameters()
    if dbname:
        params = copy.deepcopy(db_params)
        params["dbname"] = dbname
    else:
        params = db_params
    return "postgresql://{user}@{host}:{port}/{dbname}".format(**params)


def test_cli_swh_db_create_and_init_db(cli_runner, test_db, mock_package_sql):
    """Create a db then initializing it should be ok

    """
    module_name = "something"

    conninfo = craft_conninfo(test_db, "new-db")
    # This creates the db and installs the necessary admin extensions
    result = cli_runner.invoke(swhdb, ["create", module_name, "--db-name", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # This initializes the schema and data
    result = cli_runner.invoke(swhdb, ["init", module_name, "--db-name", conninfo])

    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # the origin value in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, installed during db creation step)
    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_fail_without_creation_first(
    cli_runner, test_db, mock_package_sql
):
    """Init command on an inexisting db cannot work

    """
    module_name = "anything"  # it's mocked here
    conninfo = craft_conninfo(test_db, "inexisting-db")

    result = cli_runner.invoke(swhdb, ["init", module_name, "--db-name", conninfo])
    # Fails because we cannot connect to an inexisting db
    assert result.exit_code == 1, f"Unexpected output: {result.output}"


def test_cli_swh_db_initialization_fail_without_extension(
    cli_runner, test_db, mock_package_sql
):
    """Init command cannot work without privileged extension.

       In this test, the schema needs privileged extension to work.

    """
    module_name = "anything"  # it's mocked here
    conninfo = craft_conninfo(test_db)

    result = cli_runner.invoke(swhdb, ["init", module_name, "--db-name", conninfo])
    # Fails as the function `public.digest` is not installed, init-admin calls is needed
    # first (the next tests show such behavior)
    assert result.exit_code == 1, f"Unexpected output: {result.output}"


def test_cli_swh_db_initialization_works_with_flags(
    cli_runner, test_db, mock_package_sql
):
    """Init commands with carefully crafted libpq conninfo works

    """
    module_name = "anything"  # it's mocked here
    conninfo = craft_conninfo(test_db)

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--db-name", conninfo]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(swhdb, ["init", module_name, "--db-name", conninfo])

    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(test_db.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_with_env(swh_db_cli, mock_package_sql, test_db):
    """Init commands with standard environment variables works

    """
    module_name = "anything"  # it's mocked here
    cli_runner, db_params = swh_db_cli
    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--db-name", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--db-name", db_params["dbname"]]
    )

    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(test_db.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_idempotent(swh_db_cli, mock_package_sql, test_db):
    """Multiple runs of the init commands are idempotent

    """
    module_name = "anything"  # mocked
    cli_runner, db_params = swh_db_cli

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--db-name", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--db-name", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--db-name", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--db-name", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(test_db.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1
