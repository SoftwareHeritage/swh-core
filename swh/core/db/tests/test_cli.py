# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import os
import traceback

import pytest
import yaml

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import import_swhmodule, swh_db_module, swh_db_version
from swh.core.tests.test_cli import assert_section_contains


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


@pytest.fixture
def swh_db_cli(cli_runner, monkeypatch, postgresql):
    """This initializes a cli_runner and sets the correct environment variable expected by
       the cli to run appropriately (when not specifying the --dbname flag)

    """
    db_params = postgresql.get_dsn_parameters()
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


def test_cli_swh_db_create_and_init_db(cli_runner, postgresql, mock_import_swhmodule):
    """Create a db then initializing it should be ok

    """
    module_name = "test.cli"

    conninfo = craft_conninfo(postgresql, "new-db")
    # This creates the db and installs the necessary admin extensions
    result = cli_runner.invoke(swhdb, ["create", module_name, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # This initializes the schema and data
    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])

    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # the origin value in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, installed during db creation step)
    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_fail_without_creation_first(
    cli_runner, postgresql, mock_import_swhmodule
):
    """Init command on an inexisting db cannot work

    """
    module_name = "test.cli"  # it's mocked here
    conninfo = craft_conninfo(postgresql, "inexisting-db")

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    # Fails because we cannot connect to an inexisting db
    assert result.exit_code == 1, f"Unexpected output: {result.output}"


def test_cli_swh_db_initialization_fail_without_extension(
    cli_runner, postgresql, mock_import_swhmodule
):
    """Init command cannot work without privileged extension.

       In this test, the schema needs privileged extension to work.

    """
    module_name = "test.cli"  # it's mocked here
    conninfo = craft_conninfo(postgresql)

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    # Fails as the function `public.digest` is not installed, init-admin calls is needed
    # first (the next tests show such behavior)
    assert result.exit_code == 1, f"Unexpected output: {result.output}"


def test_cli_swh_db_initialization_works_with_flags(
    cli_runner, postgresql, mock_import_swhmodule
):
    """Init commands with carefully crafted libpq conninfo works

    """
    module_name = "test.cli"  # it's mocked here
    conninfo = craft_conninfo(postgresql)

    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])

    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_with_env(
    swh_db_cli, mock_import_swhmodule, postgresql
):
    """Init commands with standard environment variables works

    """
    module_name = "test.cli"  # it's mocked here
    cli_runner, db_params = swh_db_cli
    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--dbname", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--dbname", db_params["dbname"]]
    )

    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_idempotent(
    swh_db_cli, mock_import_swhmodule, postgresql
):
    """Multiple runs of the init commands are idempotent

    """
    module_name = "test.cli"  # mocked
    cli_runner, db_params = swh_db_cli

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--dbname", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--dbname", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--dbname", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--dbname", db_params["dbname"]]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_create_and_init_db_new_api(
    cli_runner, postgresql, mock_import_swhmodule, mocker, tmp_path
):
    """Create a db then initializing it should be ok for a "new style" datastore

    """
    module_name = "test.cli_new"

    conninfo = craft_conninfo(postgresql)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(yaml.dump({module_name: {"cls": "postgresql", "db": conninfo}}))
    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", module_name])

    assert (
        result.exit_code == 0
    ), f"Unexpected output: {traceback.print_tb(result.exc_info[2])}"

    # the origin value in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, installed during db creation step)
    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_upgrade_new_api(cli_runner, postgresql, datadir, mocker, tmp_path):
    """Upgrade scenario for a "new style" datastore

    """
    module_name = "test.cli_new"

    # the `current_version` variable is the version that will be returned by
    # any call to `get_current_version()` in this test session, thanks to the
    # local mocked version of import_swhmodule() below.
    current_version = 1

    # custom version of the mockup to make it easy to change the
    # current_version returned by get_current_version()
    # TODO: find a better solution for this...
    def import_swhmodule_mock(modname):
        if modname.startswith("test."):
            dirname = modname.split(".", 1)[1]

            def get_datastore(cls, **kw):
                return mocker.MagicMock(get_current_version=lambda: current_version)

            return mocker.MagicMock(
                __name__=modname,
                __file__=os.path.join(datadir, dirname, "__init__.py"),
                name=modname,
                get_datastore=get_datastore,
            )

        return import_swhmodule(modname)

    mocker.patch("swh.core.db.db_utils.import_swhmodule", import_swhmodule_mock)
    conninfo = craft_conninfo(postgresql)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(yaml.dump({module_name: {"cls": "postgresql", "db": conninfo}}))
    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", module_name])

    assert (
        result.exit_code == 0
    ), f"Unexpected output: {traceback.print_tb(result.exc_info[2])}"

    assert swh_db_version(conninfo) == 1

    # the upgrade should not do anything because the datastore does advertise
    # version 1
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", module_name])
    assert swh_db_version(conninfo) == 1

    # advertise current version as 3, a simple upgrade should get us there, but
    # no further
    current_version = 3
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", module_name])
    assert swh_db_version(conninfo) == 3

    # an attempt to go further should not do anything
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "upgrade", module_name, "--to-version", 5]
    )
    assert swh_db_version(conninfo) == 3
    # an attempt to go lower should not do anything
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "upgrade", module_name, "--to-version", 2]
    )
    assert swh_db_version(conninfo) == 3

    # advertise current version as 6, an upgrade with --to-version 4 should
    # stick to the given version 4 and no further
    current_version = 6
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "upgrade", module_name, "--to-version", 4]
    )
    assert swh_db_version(conninfo) == 4
    assert "migration was not complete" in result.output

    # attempt to upgrade to a newer version than current code version fails
    result = cli_runner.invoke(
        swhdb,
        ["-C", cfgfile, "upgrade", module_name, "--to-version", current_version + 1],
    )
    assert result.exit_code != 0
    assert swh_db_version(conninfo) == 4

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.execute("drop table dbmodule")
    assert swh_db_module(conninfo) is None

    # db migration should recreate the missing dbmodule table
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", module_name])
    assert result.exit_code == 0
    assert "Warning: the database does not have a dbmodule table." in result.output
    assert (
        "Write the module information (test.cli_new) in the database? [Y/n]"
        in result.output
    )
    assert swh_db_module(conninfo) == module_name
