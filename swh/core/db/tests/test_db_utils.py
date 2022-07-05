# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta
from os import path

import pytest

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import (
    get_database_info,
    get_sql_for_package,
    now,
    swh_db_module,
    swh_db_upgrade,
    swh_db_version,
    swh_db_versions,
    swh_set_db_module,
)

from .test_cli import craft_conninfo


@pytest.mark.parametrize("module", ["test.cli", "test.cli_new"])
def test_get_sql_for_package(mock_import_swhmodule, module):
    files = get_sql_for_package(module)
    assert files
    assert [f.name for f in files] == [
        "0-superuser-init.sql",
        "15-flavor.sql",
        "30-schema.sql",
        "40-funcs.sql",
        "50-data.sql",
    ]


@pytest.mark.parametrize("module", ["test.cli", "test.cli_new"])
def test_db_utils_versions(cli_runner, postgresql, mock_import_swhmodule, module):
    """Check get_database_info, swh_db_versions and swh_db_module work ok

    This test checks db versions for both a db with "new style" set of sql init
    scripts (i.e. the dbversion table is not created in these scripts, but by
    the populate_database_for_package() function directly, via the 'swh db
    init' command) and an "old style" set (dbversion created in the scripts)S.

    """
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(
        swhdb, ["init", module, "--dbname", conninfo, "--initial-version", 10]
    )
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # check the swh_db_module() function
    assert swh_db_module(conninfo) == module

    # the dbversion and dbmodule tables exists and are populated
    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    # check also the swh_db_versions() function
    versions = swh_db_versions(conninfo)

    assert dbmodule == module
    assert dbversion == 10
    assert dbflavor == "default"
    # check also the swh_db_versions() function
    versions = swh_db_versions(conninfo)
    assert len(versions) == 1
    assert versions[0][0] == 10
    if module == "test.cli":
        assert versions[0][1] == datetime.fromisoformat(
            "2016-02-22T15:56:28.358587+00:00"
        )
        assert versions[0][2] == "Work In Progress"
    else:
        # new scheme but with no datastore (so no version support from there)
        assert versions[0][2] == "DB initialization"

    # add a few versions in dbversion
    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.executemany(
            "insert into dbversion(version, release, description) values (%s, %s, %s)",
            [(i, now(), f"Upgrade to version {i}") for i in range(11, 15)],
        )

    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == module
    assert dbversion == 14
    assert dbflavor == "default"

    versions = swh_db_versions(conninfo)
    assert len(versions) == 5
    for i, (version, ts, desc) in enumerate(versions):
        assert version == (14 - i)  # these are in reverse order
        if version > 10:
            assert desc == f"Upgrade to version {version}"
            assert (now() - ts) < timedelta(seconds=1)


@pytest.mark.parametrize("module", ["test.cli_new"])
def test_db_utils_upgrade(
    cli_runner, postgresql, mock_import_swhmodule, module, datadir
):
    """Check swh_db_upgrade"""
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    assert swh_db_version(conninfo) == 1
    new_version = swh_db_upgrade(conninfo, module)
    assert new_version == 6
    assert swh_db_version(conninfo) == 6

    versions = swh_db_versions(conninfo)
    # get rid of dates to ease checking
    versions = [(v[0], v[2]) for v in versions]
    assert versions[-1] == (1, "DB initialization")
    sqlbasedir = path.join(datadir, module.split(".", 1)[1], "sql", "upgrades")

    assert versions[1:-1] == [
        (i, f"Upgraded to version {i} using {sqlbasedir}/{i:03d}.sql")
        for i in range(5, 1, -1)
    ]
    assert versions[0] == (6, "Updated version from upgrade script")

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.execute("select url from origin where url like 'version%'")
        result = cur.fetchall()
        assert result == [("version%03d" % i,) for i in range(2, 7)]
        cur.execute(
            "select url from origin where url = 'this should never be executed'"
        )
        result = cur.fetchall()
        assert not result


@pytest.mark.parametrize("module", ["test.cli_new"])
def test_db_utils_swh_db_upgrade_sanity_checks(
    cli_runner, postgresql, mock_import_swhmodule, module, datadir
):
    """Check swh_db_upgrade"""
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.execute("drop table dbmodule")

    # try to upgrade with a unset module
    with pytest.raises(ValueError):
        swh_db_upgrade(conninfo, module)

    # check the dbmodule is unset
    assert swh_db_module(conninfo) is None

    # set the stored module to something else
    swh_set_db_module(conninfo, f"{module}2")
    assert swh_db_module(conninfo) == f"{module}2"

    # try to upgrade with a different module
    with pytest.raises(ValueError):
        swh_db_upgrade(conninfo, module)

    # revert to the proper module in the db
    swh_set_db_module(conninfo, module, force=True)
    assert swh_db_module(conninfo) == module
    # trying again is a noop
    swh_set_db_module(conninfo, module)
    assert swh_db_module(conninfo) == module

    # drop the dbversion table
    with cnx.transaction() as cur:
        cur.execute("drop table dbversion")
    # an upgrade should fail due to missing stored version
    with pytest.raises(ValueError):
        swh_db_upgrade(conninfo, module)


@pytest.mark.parametrize("module", ["test.cli", "test.cli_new"])
@pytest.mark.parametrize("flavor", [None, "default", "flavorA", "flavorB"])
def test_db_utils_flavor(cli_runner, postgresql, mock_import_swhmodule, module, flavor):
    """Check populate_database_for_package handle db flavor properly"""
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    cmd = ["init", module, "--dbname", conninfo]
    if flavor:
        cmd.extend(["--flavor", flavor])
    result = cli_runner.invoke(swhdb, cmd)
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # check the swh_db_module() function
    assert swh_db_module(conninfo) == module

    # the dbversion and dbmodule tables exists and are populated
    dbmodule, _dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == module
    assert dbflavor == (flavor or "default")
