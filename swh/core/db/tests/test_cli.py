# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
from pytest_postgresql import factories
import yaml

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import swh_db_module, swh_db_version
from swh.core.tests.test_cli import assert_result, assert_section_contains

postgresql2 = factories.postgresql("postgresql_proc", dbname="tests2")


def test_cli_swh_help(swhmain, cli_runner):
    swhmain.add_command(swhdb)
    result = cli_runner.invoke(swhmain, ["-h"])
    assert_result(result)
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
    assert_result(result)
    for section, snippets in help_db_snippets:
        for snippet in snippets:
            assert_section_contains(result.output, section, snippet)


@pytest.fixture
def swh_db_cli(cli_runner, monkeypatch, postgresql):
    """This initializes a cli_runner and sets the correct environment variable expected by
    the cli to run appropriately (when not specifying the --dbname flag)

    """
    monkeypatch.setenv("PGHOST", postgresql.info.host)
    monkeypatch.setenv("PGUSER", postgresql.info.user)
    monkeypatch.setenv("PGPORT", str(postgresql.info.port))

    return cli_runner, postgresql.info


def craft_conninfo(test_db, dbname=None) -> str:
    """Craft conninfo string out of the test_db object. This also allows to override the
    dbname."""
    db_params = test_db.info
    dbname = dbname if dbname else db_params.dbname
    return f"postgresql://{db_params.user}@{db_params.host}:{db_params.port}/{dbname}"


@pytest.mark.parametrize(
    "module_table",
    [
        ("test", "origin"),
        ("test.postgresql", "origin"),
        ("test:postgresql", "origin"),
        ("test:cli2", "origin2"),
        ("test.cli2", "origin2"),
    ],
)
def test_cli_swh_db_create_and_init_db(
    cli_runner, postgresql, mock_get_entry_points, module_table
):
    """Create a db then initializing it should be ok"""
    module_name, table = module_table
    conninfo = craft_conninfo(postgresql, f"db-{module_name}")
    # This creates the db and installs the necessary admin extensions
    result = cli_runner.invoke(swhdb, ["create", module_name, "--dbname", conninfo])
    assert_result(result)

    # This initializes the schema and data
    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    assert_result(result)

    # the origin value in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, installed during db creation step)
    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute(f"select * from {table}")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_fail_without_creation_first(
    cli_runner, postgresql, mock_import_module
):
    """Init command on an inexisting db cannot work"""
    module_name = "test"  # it's mocked here
    conninfo = craft_conninfo(postgresql, "inexisting-db")

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    # Fails because we cannot connect to an inexisting db
    assert result.exit_code == 1, f"Unexpected output: {result.output}"
    assert 'FATAL:  database "inexisting-db" does not exist' in result.output


def test_cli_swh_db_initialization_fail_without_extension(
    cli_runner, postgresql, mock_import_module
):
    """Init command cannot work without privileged extension.

    In this test, the schema needs privileged extension to work.

    """
    module_name = "test.postgresql"  # it's mocked here
    conninfo = craft_conninfo(postgresql)

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    # Fails as the function `public.digest` is not installed, init-admin calls is needed
    # first (the next tests show such behavior)
    assert result.exit_code == 1, f"Unexpected output: {result.output}"
    assert (
        "ERROR:  function public.digest(text, unknown) does not exist" in result.output
    )


def test_cli_swh_db_initialization_works_with_flags(
    cli_runner,
    postgresql,
    mock_get_entry_points,
):
    """Init commands with carefully crafted libpq conninfo works"""
    module_name = "test"  # it's mocked here
    conninfo = craft_conninfo(postgresql)

    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert_result(result)

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    assert_result(result)
    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.info.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_with_env(
    swh_db_cli, mock_get_entry_points, postgresql
):
    """Init commands with standard environment variables works"""
    module_name = "test"  # it's mocked here
    cli_runner, db_params = swh_db_cli
    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--dbname", db_params.dbname]
    )
    assert_result(result)
    result = cli_runner.invoke(
        swhdb,
        [
            "init",
            module_name,
            "--dbname",
            db_params.dbname,
        ],
    )
    assert_result(result)

    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.info.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_initialization_idempotent(
    swh_db_cli, mock_get_entry_points, postgresql
):
    """Multiple runs of the init commands are idempotent"""
    module_name = "test"  # mocked
    cli_runner, db_params = swh_db_cli

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--dbname", db_params.dbname]
    )
    assert_result(result)

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--dbname", db_params.dbname]
    )
    assert_result(result)

    result = cli_runner.invoke(
        swhdb, ["init-admin", module_name, "--dbname", db_params.dbname]
    )
    assert_result(result)

    result = cli_runner.invoke(
        swhdb, ["init", module_name, "--dbname", db_params.dbname]
    )
    assert_result(result)

    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.info.dsn).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


@pytest.mark.parametrize("with_module_config_key", [True, False])
def test_cli_swh_db_create_and_init_db_new_api(
    cli_runner,
    postgresql,
    mock_get_entry_points,
    mocker,
    tmp_path,
    with_module_config_key,
):
    """Create a db then initializing it should be ok for a "new style" datastore"""
    module_name = "test"

    conninfo = craft_conninfo(postgresql)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(yaml.dump({module_name: {"cls": "postgresql", "db": conninfo}}))
    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert_result(result)

    cli_cmd = ["-C", cfgfile, "init", module_name]
    if with_module_config_key:
        cli_cmd.extend(["--module-config-key", module_name])
    result = cli_runner.invoke(swhdb, cli_cmd)
    assert_result(result)

    # the origin value in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, installed during db creation step)
    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_init_report_sqlsh_error(
    cli_runner,
    postgresql,
    mock_get_entry_points,
    mocker,
    tmp_path,
):
    """Create a db then initializing it should be ok for a "new style" datastore"""
    module_name = "test:fail"

    conninfo = craft_conninfo(postgresql)

    # This initializes the schema and data
    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert_result(result)

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    assert result.exit_code == 1
    assert (
        "test/fail/sql/40-funcs.sql:6: "
        "ERROR:  function public.digest(text, unknown) does not exist"
    ) in result.output


@pytest.mark.init_version(version=2)
def test_cli_swh_db_upgrade_new_api(
    request,
    cli_runner,
    mock_get_entry_points,
    postgresql,
    datadir,
    mocker,
    tmp_path,
):
    """Upgrade scenario for a "new style" datastore"""
    module_name = "test"

    current_version = request.node.get_closest_marker("init_version").kwargs["version"]

    conninfo = craft_conninfo(postgresql)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(yaml.dump({module_name: {"cls": "postgresql", "db": conninfo}}))
    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == 2

    # the upgrade should not do anything because the datastore does advertise
    # version 1
    current_version = 1
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 2

    # advertise current version as 3, a simple upgrade should get us there, but
    # no further
    current_version = 3
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 3

    # an attempt to go further should generate an error
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "upgrade", module_name, "--to-version", 5]
    )
    assert result.exit_code != 0
    assert swh_db_version(conninfo) == 3
    # an attempt to go lower should not do anything
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "upgrade", module_name, "--to-version", 2]
    )
    assert_result(result)
    assert swh_db_version(conninfo) == 3

    # advertise current version as 6, an upgrade with --to-version 4 should
    # stick to the given version 4 and no further
    current_version = 6
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "upgrade", module_name, "--to-version", 4]
    )
    assert_result(result)
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
    assert_result(result)
    assert "Warning: the database does not have a dbmodule table." in result.output
    assert (
        "Write the module information (test:postgresql) in the database? [Y/n]"
        in result.output
    )
    assert swh_db_module(conninfo) == "test:postgresql"


@pytest.mark.init_version(version=5)
def test_cli_swh_db_init_version_ok(
    request,
    cli_runner,
    mock_get_entry_points,
    postgresql,
    datadir,
    mocker,
    tmp_path,
):
    """Upgrade scenario for a "new style" datastore"""
    module_name = "test"

    # the `current_version` variable is the version that will be returned by
    # any call to `get_current_version()` in this test session, thanks to the
    # local mocked version of import_swhmodule() below.
    current_version = request.node.get_closest_marker("init_version").kwargs["version"]
    conninfo = craft_conninfo(postgresql)

    # call the db init stuff WITHOUT a config file
    result = cli_runner.invoke(swhdb, ["init-admin", module_name, "--dbname", conninfo])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    assert_result(result)

    assert swh_db_version(conninfo) == current_version


def test_cli_swh_db_version(swh_db_cli, mock_get_entry_points, postgresql):
    module_name = "test"
    cli_runner, db_params = swh_db_cli

    conninfo = craft_conninfo(postgresql, "test-db-version")
    # This creates the db and installs the necessary admin extensions
    result = cli_runner.invoke(swhdb, ["create", module_name, "--dbname", conninfo])
    assert_result(result)
    # This initializes the schema and data
    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    assert_result(result)

    actual_db_version = swh_db_version(conninfo)

    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute("select version from dbversion order by version desc limit 1")
        expected_version = cur.fetchone()[0]
        assert actual_db_version == expected_version

    assert_result(result)
    assert (
        f"initialized (flavor default) at version {expected_version}" in result.output
    )


@pytest.mark.parametrize("initialize_all", [True, False])
def test_cli_swh_db_initadmin_and_init_db_from_config_path(
    cli_runner,
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
    initialize_all,
):
    """Test init-admin and init commands with db cnx string coming from the config file

    It will test both the case where db cnx location in the config file are
    given and the automated mode (aka with --initialize-all).

    """
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(
        f"""
test:
  cls: something
  backend:
    cls: pipeline
    steps:
      - cls: postgresql
        db: {conninfo}
      - cls: stuff
        backend:
          cls: cli2
          cli_db: {conninfo2}
    """
    )
    if initialize_all:
        result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init-admin", "-a", "test"])
        assert_result(result)
        result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", "-a", "test"])
        assert_result(result)
    else:
        for config_path in (
            "test.backend.steps.0",
            "test.backend.steps.1.backend",
        ):
            result = cli_runner.invoke(
                swhdb, ["-C", cfgfile, "init-admin", "-p", config_path]
            )
            assert_result(result)
            result = cli_runner.invoke(
                swhdb, ["-C", cfgfile, "init", "-p", config_path]
            )
            assert_result(result)

    # the origin value in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, installed during db creation step)
    with BaseDb.connect(conninfo).cursor() as cur:
        cur.execute("select * from origin")
        origins = cur.fetchall()
        assert len(origins) == 1

    # same with the origin2 table
    with BaseDb.connect(conninfo2).cursor() as cur:
        cur.execute("select * from origin2")
        origins = cur.fetchall()
        assert len(origins) == 1


def test_cli_swh_db_list_config_path(
    cli_runner,
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
):
    """Test the swh db list command"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(
        f"""
test:
  cls: something
  backend:
    cls: pipeline
    steps:
      - cls: postgresql
        db: {conninfo}
      - cls: cli
        backend:
          cls: cli2
          cli_db: {conninfo2}
    """
    )
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "list"])
    assert_result(result)
    assert (
        result.output
        == f"""\
test.backend.steps.0 postgresql {conninfo}
test.backend.steps.1.backend cli2 {conninfo2}
"""
    )


def test_cli_swh_db_version_from_config(
    cli_runner,
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
):
    """Test the swh db list command"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(
        f"""
test:
  cls: something
  backend:
    cls: pipeline
    steps:
      - cls: postgresql
        db: {conninfo}
      - cls: cli
        backend:
          cls: cli2
          db: {conninfo2}
    """
    )
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init-admin", "-a", "test"])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", "-a", "test"])
    assert_result(result)

    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "version", "test"])
    # this one should fail, there is no "natural" config entry in this config
    # file for the 'test' module
    assert result.exit_code != 0

    # but we can ask for each entry
    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "version", "-p", "test.backend.steps.0"]
    )
    assert_result(result)
    assert (
        result.output
        == """
module: test:postgresql
flavor: default
current code version: 3
version: 3
"""
    )

    result = cli_runner.invoke(
        swhdb, ["-C", cfgfile, "version", "-p", "test.backend.steps.1.backend"]
    )
    assert_result(result)
    assert (
        result.output
        == """
module: test:cli2
current code version: 3
version: 3
"""
    )

    # or all at once
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "version", "--all", "test"])
    assert_result(result)
    assert (
        result.output
        == """
module: test:postgresql
flavor: default
current code version: 3
version: 3

module: test:cli2
current code version: 3
version: 3
"""
    )


@pytest.mark.init_version(version=1)
def test_cli_swh_db_upgrade_from_config(
    request,
    cli_runner,
    mock_get_entry_points,
    postgresql,
    postgresql2,
    datadir,
    mocker,
    tmp_path,
):
    """Test the upgrade cli tool reading db cnx from a nested config file"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(
        f"""
test:
  cls: something
  backend:
    cls: pipeline
    steps:
      - cls: postgresql
        db: {conninfo}
      - cls: cli
        backend:
          cls: cli2
          cli_db: {conninfo2}
    """
    )

    module_name = "test"

    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init-admin", "-a", module_name])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", "-a", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == 1
    assert swh_db_version(conninfo2) == 1

    for module_name, config_path, cnxstr in (
        ("test:postgresql", "test.backend.steps.0", conninfo),
        ("test:cli2", "test.backend.steps.1.backend", conninfo2),
    ):
        current_version = 1
        # XXX hack hack hack: change the current test (pytest.)marker's
        # init_version arg, this one is used in mock_import_swhmodule...
        request.node.get_closest_marker("init_version").kwargs[
            "version"
        ] = current_version
        # the upgrade should not do anything because the datastore does advertise
        # version 1
        result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", "-p", config_path])
        assert_result(result)
        assert swh_db_version(cnxstr) == 1

        # advertise current version as 3, a simple upgrade should get us there, but
        # no further
        current_version = 2
        request.node.get_closest_marker("init_version").kwargs[
            "version"
        ] = current_version
        result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", "-p", config_path])
        assert_result(result)
        assert swh_db_version(cnxstr) == 2

        # an attempt to go further should not do anything
        result = cli_runner.invoke(
            swhdb, ["-C", cfgfile, "upgrade", "-p", config_path, "--to-version", 5]
        )
        assert result.exit_code != 0
        assert swh_db_version(cnxstr) == 2
        # an attempt to go lower should not do anything
        result = cli_runner.invoke(
            swhdb, ["-C", cfgfile, "upgrade", "-p", config_path, "--to-version", 1]
        )
        assert_result(result)
        assert swh_db_version(cnxstr) == 2

        # advertise current version as 6, an upgrade with --to-version 4 should
        # stick to the given version 4 and no further
        current_version = 6
        request.node.get_closest_marker("init_version").kwargs[
            "version"
        ] = current_version
        result = cli_runner.invoke(
            swhdb, ["-C", cfgfile, "upgrade", "-p", config_path, "--to-version", 4]
        )
        assert_result(result)
        assert swh_db_version(cnxstr) == 4
        assert "migration was not complete" in result.output

        # attempt to upgrade to a newer version than current code version fails
        result = cli_runner.invoke(
            swhdb,
            [
                "-C",
                cfgfile,
                "upgrade",
                config_path,
                "--to-version",
                current_version + 1,
            ],
        )
        assert result.exit_code != 0
        assert swh_db_version(cnxstr) == 4

        cnx = BaseDb.connect(cnxstr)
        with cnx.transaction() as cur:
            cur.execute("drop table dbmodule")
        assert swh_db_module(cnxstr) is None

        # db migration should recreate the missing dbmodule table
        result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", "-p", config_path])
        assert_result(result)
        assert "Warning: the database does not have a dbmodule table." in result.output
        assert (
            f"Write the module information ({module_name}) in the database? [Y/n]"
            in result.output
        )
        assert swh_db_module(cnxstr) == module_name


@pytest.mark.init_version(version=1)
def test_cli_swh_db_upgrade_all(
    request,
    cli_runner,
    mock_get_entry_points,
    postgresql,
    postgresql2,
    datadir,
    mocker,
    tmp_path,
):
    """Test the upgrade cli tool reading db cnx from a nested config file"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(
        f"""
test:
  cls: something
  backend:
    cls: pipeline
    steps:
      - cls: postgresql
        db: {conninfo}
      - cls: cli
        backend:
          cls: cli2
          db: {conninfo2}
    """
    )

    module_name = "test"

    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init-admin", "-a", module_name])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "init", "-a", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == 1
    assert swh_db_version(conninfo2) == 1

    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", "-a", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 1
    assert swh_db_version(conninfo2) == 1

    current_version = 6
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version

    result = cli_runner.invoke(swhdb, ["-C", cfgfile, "upgrade", "-a", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 6
    assert swh_db_version(conninfo2) == 6
