# Copyright (C) 2019-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from click.testing import CliRunner
import pytest
from pytest_postgresql import factories
import yaml

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import swh_db_module, swh_db_version
from swh.core.tests.test_cli import assert_result, assert_section_contains

postgresql2 = factories.postgresql("postgresql_proc", dbname="tests2")


def assert_no_pending_transaction(cursor):
    sql = """SELECT * FROM pg_stat_activity WHERE state = 'idle in transaction'"""
    idle = cursor.execute(sql).fetchall()
    assert idle == []


# tests --help


def test_cli_swh_help(swhmain, cli_runner):
    swhmain.add_command(swhdb)
    result = cli_runner.invoke(swhmain, ["-h"])
    assert_result(result)
    assert_section_contains(
        result.output, "Commands", "Software Heritage database generic tools."
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


def craft_conninfo(test_db, dbname=None) -> str:
    """Craft conninfo string out of the test_db object. This also allows to override the
    dbname."""
    db_params = test_db.info
    dbname = dbname if dbname else db_params.dbname
    return f"postgresql://{db_params.user}@{db_params.host}:{db_params.port}/{dbname}"


@pytest.fixture
def cli_db_runner(postgresql, tmp_path):
    """This initializes a cli_runner and sets the SWH_CONFIG_FILENAME environment variable"""
    conninfo = craft_conninfo(postgresql)
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(yaml.dump({"test": {"cls": "postgresql", "db": conninfo}}))
    return CliRunner(env={"SWH_CONFIG_FILENAME": str(cfgfile)})


# tests for create, init and init-admin commands


@pytest.mark.parametrize(
    "module_table",
    [
        ("test", "origin"),
        ("test:postgresql", "origin"),
        ("test:cli2", "origin2"),
    ],
)
def test_cli_swh_db_create_and_init_db_using_dbname_option(
    cli_runner, postgresql, mock_get_entry_points, module_table
):
    """Create and initializing a db without config file using --dbname option

    Note: the sql setup scripts are found in the tests/data/<pkg>/<cls>
    directory, so test:cli2 will look in ./data/test/cli2/sql for example.
    """
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
    with BaseDb.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select * from {table}")
            origins = cur.fetchall()
            assert len(origins) == 1


def test_cli_swh_db_initialization_fail_without_creation_first(
    cli_runner, postgresql, mock_import_module
):
    """Init command on an missing db cannot work"""
    module_name = "test"  # it's mocked here
    conninfo = craft_conninfo(postgresql, "inexisting-db")

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    # Fails because we cannot connect to an inexisting db
    assert result.exit_code == 1, f"Unexpected output: {result.output}"
    assert 'FATAL:  database "inexisting-db" does not exist' in result.output


def test_cli_swh_db_initialization_fail_without_extension(
    cli_runner, postgresql, mock_get_entry_points
):
    """Init command cannot work without privileged extension.

    In this test, the schema needs privileged extension to work.

    """
    module_name = "test:postgresql"  # it's mocked here
    conninfo = craft_conninfo(postgresql)

    result = cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])
    # Fails as the function `public.digest` is not installed, init-admin calls is needed
    # first (the next tests show such behavior)
    assert result.exit_code == 1, f"Unexpected output: {result.output}"
    assert (
        "ERROR:  function public.digest(text, unknown) does not exist" in result.output
    )


def test_cli_swh_db_initialization_from_config(
    cli_db_runner, mock_get_entry_points, postgresql
):
    """Init commands using cnx params from a simple config file"""
    module_name = "test"  # it's mocked here
    result = cli_db_runner.invoke(swhdb, ["init-admin", module_name])
    assert_result(result)
    result = cli_db_runner.invoke(
        swhdb,
        [
            "init",
            module_name,
        ],
    )
    assert_result(result)

    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.info.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select * from origin")
            origins = cur.fetchall()
            assert len(origins) == 1


def test_cli_swh_db_initialization_idempotent(
    cli_db_runner, mock_get_entry_points, postgresql
):
    """Multiple runs of the init commands are idempotent"""
    module_name = "test"  # mocked

    result = cli_db_runner.invoke(swhdb, ["init-admin", module_name])
    assert_result(result)

    result = cli_db_runner.invoke(swhdb, ["init", module_name])
    assert_result(result)

    result = cli_db_runner.invoke(swhdb, ["init-admin", module_name])
    assert_result(result)

    result = cli_db_runner.invoke(swhdb, ["init", module_name])
    assert_result(result)

    # the origin values in the scripts uses a hash function (which implementation wise
    # uses a function from the pgcrypt extension, init-admin calls installs it)
    with BaseDb.connect(postgresql.info.dsn) as conn:
        with conn.cursor() as cur:
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


@pytest.mark.parametrize("argtype", ["all", "config_path", "pkg:cls"])
def test_cli_swh_db_initialization_from_config_multiple(
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
    argtype,
):
    """Test init-admin and init commands from a complex config file

    It will test both the case where db cnx location in the config file are
    given -- either by config path or via the pkg:cls syntax -- and the
    automated mode (aka with --all).

    """
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(f"""
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
          db: {conninfo2}
    """)
    cli_runner = CliRunner(env={"SWH_CONFIG_FILENAME": str(cfgfile)})

    if argtype == "all":
        # the 'init(-admin) --all test' scenario
        args = [
            ["--all", "test"],
        ]
    elif argtype == "config_path":
        # the 'init(-admin) -p pkg.path.to' scenarios
        args = [
            ["-p", cpath]
            for cpath in (
                "test.backend.steps.0",
                "test.backend.steps.1.backend",
            )
        ]
    else:
        # the 'init(-admin) pkg:cls' scenarios
        args = [["test:postgresql"], ["test:cli2"]]

    for arg in args:
        result = cli_runner.invoke(swhdb, ["init-admin"] + arg)
        assert_result(result)
        result = cli_runner.invoke(swhdb, ["init"] + arg)
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


def test_cli_swh_db_initialization_from_config_aliases(
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
):
    """Test init-admin and init commands from a complex config file

    It will test both the case where db cnx location in the config file are
    given -- either by config path or via the pkg:cls syntax -- and the
    automated mode (aka with --all).

    """
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    # This initializes the schema and data
    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(f"""
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
          db: {conninfo2}

test_admin:
    pkg: test
    cls: postgresql
    db: {conninfo}

test_cli2_admin:
    pkg: test
    cls: cli2
    db: {conninfo2}
    """)
    cli_runner = CliRunner(env={"SWH_CONFIG_FILENAME": str(cfgfile)})

    # the 'init(-admin) pkg:cls' scenarios
    args = [["test_admin"], ["test_cli2_admin"]]

    for arg in args:
        result = cli_runner.invoke(swhdb, ["init-admin"] + arg)
        assert_result(result)
        result = cli_runner.invoke(swhdb, ["init"] + arg)
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


# tests for version management


@pytest.mark.init_version(version=5)
def test_cli_swh_db_init_version_ok(
    request,
    cli_db_runner,
    mock_get_entry_points,
    postgresql,
    datadir,
    mocker,
    tmp_path,
):
    module_name = "test"

    # the `current_version` variable is the version that will be returned by
    # any call to `get_current_version()` in this test session, thanks to the
    # local mocked version of import_swhmodule() below.
    current_version = 5
    conninfo = craft_conninfo(postgresql)

    # call the db init stuff WITHOUT a config file
    result = cli_db_runner.invoke(swhdb, ["init-admin", module_name])
    assert_result(result)
    result = cli_db_runner.invoke(swhdb, ["init", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == current_version


def test_cli_swh_db_version(cli_db_runner, mock_get_entry_points, postgresql):
    module_name = "test"

    conninfo = craft_conninfo(postgresql)
    # This creates the db and installs the necessary admin extensions
    result = cli_db_runner.invoke(swhdb, ["create", module_name])
    assert_result(result)
    # This initializes the schema and data
    result = cli_db_runner.invoke(swhdb, ["init-admin", module_name])
    assert_result(result)
    result = cli_db_runner.invoke(swhdb, ["init", module_name])
    assert_result(result)

    actual_db_version = swh_db_version(conninfo)

    with BaseDb.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute("select version from dbversion order by version desc limit 1")
            expected_version = cur.fetchone()[0]
            assert actual_db_version == expected_version

    assert_result(result)
    assert (
        f"initialized (flavor default) at version {expected_version}" in result.output
    )


def test_cli_swh_db_list_config_path(
    cli_runner,
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
):
    """Test the 'swh db list' command"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(f"""
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
    """)
    result = cli_runner.invoke(
        swhdb, ["list"], env={"SWH_CONFIG_FILENAME": str(cfgfile)}
    )
    assert_result(result)
    assert result.output == f"""\
test.backend.steps.0 test:postgresql {conninfo}
test.backend.steps.1.backend test:cli2 {conninfo2}
"""


def test_cli_swh_db_version_from_config(
    cli_runner,
    postgresql,
    postgresql2,
    mock_get_entry_points,
    mocker,
    tmp_path,
):
    """Test the 'swh db version' command"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(f"""
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
    """)
    env = {"SWH_CONFIG_FILENAME": str(cfgfile)}
    result = cli_runner.invoke(swhdb, ["init-admin", "-a", "test"], env=env)
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", "-a", "test"], env=env)
    assert_result(result)

    # we can ask version for each entry
    result = cli_runner.invoke(
        swhdb, ["version", "-p", "test.backend.steps.0"], env=env
    )
    assert_result(result)
    assert result.output == """
module: test:postgresql
flavor: default
current code version: 3
version: 3
"""

    result = cli_runner.invoke(
        swhdb,
        ["version", "-p", "test.backend.steps.1.backend"],
        env=env,
    )
    assert_result(result)
    assert result.output == """
module: test:cli2
current code version: 3
version: 3
"""

    # or all at once
    result = cli_runner.invoke(swhdb, ["version", "--all", "test"], env=env)
    assert_result(result)
    assert result.output == """
module: test:postgresql
flavor: default
current code version: 3
version: 3

module: test:cli2
current code version: 3
version: 3
"""


# tests for upgrade management


@pytest.mark.init_version(version=2)
def test_cli_swh_db_upgrade(
    request,
    cli_db_runner,
    mock_get_entry_points,
    postgresql,
    datadir,
    tmp_path,
):
    """Simple upgrade scenario

    Only one backend entry. Upgrade in several steps (aka test the
    '--to-version' option).

    """
    module_name = "test"

    current_version = request.node.get_closest_marker("init_version").kwargs["version"]
    assert current_version == 2

    conninfo = craft_conninfo(postgresql)

    result = cli_db_runner.invoke(swhdb, ["init-admin", module_name])
    assert_result(result)
    result = cli_db_runner.invoke(swhdb, ["init", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == 2

    # the upgrade should not do anything because the datastore does advertise
    # version 1
    current_version = 1
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version
    result = cli_db_runner.invoke(swhdb, ["upgrade", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 2

    # advertise current version as 3, a simple upgrade should get us there, but
    # no further
    current_version = 3
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version
    result = cli_db_runner.invoke(swhdb, ["upgrade", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 3

    # an attempt to go further should generate an error
    result = cli_db_runner.invoke(swhdb, ["upgrade", module_name, "--to-version", 5])
    assert result.exit_code != 0
    assert swh_db_version(conninfo) == 3
    # an attempt to go lower should not do anything
    result = cli_db_runner.invoke(swhdb, ["upgrade", module_name, "--to-version", 2])
    assert_result(result)
    assert swh_db_version(conninfo) == 3

    # advertise current version as 6, an upgrade with --to-version 4 should
    # stick to the given version 4 and no further
    current_version = 6
    request.node.get_closest_marker("init_version").kwargs["version"] = current_version
    result = cli_db_runner.invoke(swhdb, ["upgrade", module_name, "--to-version", 4])
    assert_result(result)
    assert swh_db_version(conninfo) == 4
    assert "migration was not complete" in result.output

    # attempt to upgrade to a newer version than current code version fails
    result = cli_db_runner.invoke(
        swhdb,
        ["upgrade", module_name, "--to-version", current_version + 1],
    )
    assert result.exit_code != 0
    assert swh_db_version(conninfo) == 4

    cnx = BaseDb.connect(conninfo)
    with cnx:
        with cnx.transaction() as cur:
            assert_no_pending_transaction(cur)
            cur.execute("drop table dbmodule")
        assert swh_db_module(conninfo) is None

    # db migration should recreate the missing dbmodule table
    result = cli_db_runner.invoke(swhdb, ["upgrade", module_name], input="Y")
    assert_result(result)
    assert "Warning: the database does not have a dbmodule table." in result.output
    assert (
        "Write the module information (test:postgresql) in the database? [Y/n]"
        in result.output
    )
    assert swh_db_module(conninfo) == "test:postgresql"


@pytest.mark.init_version(version=1)
@pytest.mark.parametrize(
    "use_config_path",
    [True, False],
    ids=["use config path", "use pkg:cls"],
)
def test_cli_swh_db_upgrade_from_config_path(
    request,
    mock_get_entry_points,
    postgresql,
    postgresql2,
    datadir,
    mocker,
    tmp_path,
    use_config_path,
):
    """Test the upgrade cli tool -- one at a time -- from a nested config file

    Test both the 'upgrade -p cfg.path.to.entry' form and the 'upgrade pkg:cls'
    one.
    """
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(f"""
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
    """)

    cli_runner = CliRunner(env={"SWH_CONFIG_FILENAME": str(cfgfile)})
    module_name = "test"

    # needed because the of the parametrization of the test...
    # first call will let the marker set to 6
    request.node.get_closest_marker("init_version").kwargs["version"] = 1

    result = cli_runner.invoke(swhdb, ["init-admin", "--all", module_name])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", "--all", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == 1
    assert swh_db_version(conninfo2) == 1

    for module_name, config_path, cnxstr in (
        ("test:postgresql", "test.backend.steps.0", conninfo),
        ("test:cli2", "test.backend.steps.1.backend", conninfo2),
    ):
        if use_config_path:
            args = ["upgrade", "-p", config_path]
        else:
            args = ["upgrade", module_name]
        current_version = 1
        # XXX hack hack hack: change the current test (pytest.)marker's
        # init_version arg, this one is used in mock_import_swhmodule...
        request.node.get_closest_marker("init_version").kwargs[
            "version"
        ] = current_version
        # the upgrade should not do anything because the datastore does advertise
        # version 1
        result = cli_runner.invoke(swhdb, args)
        assert_result(result)
        assert swh_db_version(cnxstr) == 1

        # advertise current version as 3, a simple upgrade should get us there, but
        # no further
        current_version = 2
        request.node.get_closest_marker("init_version").kwargs[
            "version"
        ] = current_version
        result = cli_runner.invoke(swhdb, args)
        assert_result(result)
        assert swh_db_version(cnxstr) == 2

        # an attempt to go further should not do anything
        result = cli_runner.invoke(swhdb, args + ["--to-version", 5])
        assert result.exit_code != 0
        assert swh_db_version(cnxstr) == 2
        # an attempt to go lower should not do anything
        result = cli_runner.invoke(swhdb, args + ["--to-version", 1])
        assert_result(result)
        assert swh_db_version(cnxstr) == 2

        # advertise current version as 6, an upgrade with --to-version 4 should
        # stick to the given version 4 and no further
        current_version = 6
        request.node.get_closest_marker("init_version").kwargs[
            "version"
        ] = current_version
        result = cli_runner.invoke(swhdb, args + ["--to-version", 4])
        assert_result(result)
        assert swh_db_version(cnxstr) == 4
        assert "migration was not complete" in result.output

        # attempt to upgrade to a newer version than current code version fails
        result = cli_runner.invoke(
            swhdb,
            args
            + [
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
        result = cli_runner.invoke(swhdb, args, input="Y")
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
    mock_get_entry_points,
    postgresql,
    postgresql2,
    datadir,
    mocker,
    tmp_path,
):
    """Test the 'upgrade --all' cli tool from a nested config file"""
    conninfo = craft_conninfo(postgresql)
    conninfo2 = craft_conninfo(postgresql2)

    cfgfile = tmp_path / "config.yml"
    cfgfile.write_text(f"""
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
    """)

    module_name = "test"

    cli_runner = CliRunner(env={"SWH_CONFIG_FILENAME": str(cfgfile)})

    result = cli_runner.invoke(swhdb, ["init-admin", "--all", module_name])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", "--all", module_name])
    assert_result(result)

    assert swh_db_version(conninfo) == 1
    assert swh_db_version(conninfo2) == 1

    result = cli_runner.invoke(swhdb, ["upgrade", "--all", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 1
    assert swh_db_version(conninfo2) == 1

    request.node.get_closest_marker("init_version").kwargs["version"] = 6

    result = cli_runner.invoke(swhdb, ["upgrade", "--all", module_name])
    assert_result(result)
    assert swh_db_version(conninfo) == 6
    assert swh_db_version(conninfo2) == 6
