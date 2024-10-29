# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
from os import path

from psycopg2.errors import InsufficientPrivilege
import pytest

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import (
    swh_db_module,
    swh_db_upgrade,
    swh_db_version,
    swh_db_versions,
    swh_set_db_module,
)
from swh.core.db.db_utils import get_database_info, get_sql_for_package, now
from swh.core.db.db_utils import parse_dsn_or_dbname as parse_dsn
from swh.core.tests.test_cli import assert_result

from .test_cli import craft_conninfo


def test_get_sql_for_package(mock_import_module):
    module = "test.postgresql"

    files = get_sql_for_package(module)
    assert files
    assert [f.name for f in files] == [
        "0-superuser-init.sql",
        "15-flavor.sql",
        "30-schema.sql",
        "40-funcs.sql",
        "50-data.sql",
    ]


def test_db_utils_versions(cli_runner, postgresql, mock_get_entry_points):
    """Check get_database_info, swh_db_versions and swh_db_module work ok

    This test checks db versions is properly initialized by the cli db init
    script.

    mock_import_swhmodule should set the initial version to 3.
    """
    module = "test"
    db_module = "test:postgresql"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert_result(result)

    # check the swh_db_module() function
    assert swh_db_module(conninfo) == db_module

    # the dbversion and dbmodule tables exists and are populated
    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    # check also the swh_db_versions() function
    versions = swh_db_versions(conninfo)

    assert dbmodule == db_module
    assert dbversion == 3
    assert dbflavor == "default"
    # check also the swh_db_versions() function
    versions = swh_db_versions(conninfo)
    assert len(versions) == 1
    assert versions[0][0] == 3
    assert versions[0][2] == "DB initialization"

    # add a few versions in dbversion
    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.executemany(
            "insert into dbversion(version, release, description) values (%s, %s, %s)",
            [(i, now(), f"Upgrade to version {i}") for i in range(4, 8)],
        )

    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == db_module
    assert dbversion == 7
    assert dbflavor == "default"

    versions = swh_db_versions(conninfo)
    assert len(versions) == 5

    for i, (version, ts, desc) in enumerate(versions):
        assert version == (7 - i)  # these are in reverse order
        if version > 3:
            assert desc == f"Upgrade to version {version}"
            assert (now() - ts) < timedelta(seconds=1)


def test_db_utils_upgrade(cli_runner, postgresql, mock_get_entry_points, datadir):
    """Check swh_db_upgrade"""
    module = "test"
    db_module = "test:postgresql"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert_result(result)

    assert swh_db_version(conninfo) == 3
    new_version = swh_db_upgrade(conninfo, db_module)
    assert new_version == 6
    assert swh_db_version(conninfo) == 6

    versions = swh_db_versions(conninfo)
    # get rid of dates to ease checking
    versions = [(v[0], v[2]) for v in versions]
    assert versions[-1] == (3, "DB initialization")
    sqlbasedir = path.join(datadir, "test", "postgresql", "sql", "upgrades")

    assert versions[1:-1] == [
        (i, f"Upgraded to version {i} using {sqlbasedir}/{i:03d}.sql")
        for i in range(5, 3, -1)
    ]
    assert versions[0] == (6, "Updated version from upgrade script")

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.execute("select url from origin where url like 'version%'")
        result = cur.fetchall()

        assert result == [("version%03d" % i,) for i in range(4, 7)]
        cur.execute(
            "select url from origin where url = 'this should never be executed'"
        )
        result = cur.fetchall()
        assert not result


def test_db_utils_swh_db_upgrade_sanity_checks(
    cli_runner, postgresql, mock_get_entry_points, datadir
):
    """Check swh_db_upgrade"""
    module = "test"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert_result(result)
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert_result(result)

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


@pytest.mark.parametrize("flavor", [None, "default", "flavorA", "flavorB"])
def test_db_utils_flavor(cli_runner, postgresql, mock_get_entry_points, flavor):
    """Check populate_database_for_package handle db flavor properly"""
    module = "test"
    db_module = "test:postgresql"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert_result(result)
    cmd = ["init", module, "--dbname", conninfo]
    if flavor:
        cmd.extend(["--flavor", flavor])
    result = cli_runner.invoke(swhdb, cmd)
    assert_result(result)

    # check the swh_db_module() function
    assert swh_db_module(conninfo) == db_module

    # the dbversion and dbmodule tables exists and are populated
    dbmodule, _dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == db_module
    assert dbflavor == (flavor or "default")


def test_db_utils_guest_permissions(cli_runner, postgresql, mock_get_entry_points):
    """Check populate_database_for_package handle db flavor properly"""
    module = "test"
    conninfo = craft_conninfo(postgresql)
    # breakpoint()
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert_result(result)
    cmd = ["init", module, "--dbname", conninfo]
    result = cli_runner.invoke(swhdb, cmd)
    assert_result(result)

    # check select permissions have been granted tguest
    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        cur.execute("select * from pg_roles where rolname='guest'")
        assert cur.rowcount == 1
        query = """
SELECT table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'guest'
"""
        cur.execute(query)
        assert cur.rowcount == 4
        assert set(cur.fetchall()) == {
            ("dbflavor", "SELECT"),
            ("origin", "SELECT"),
            ("dbversion", "SELECT"),
            ("dbmodule", "SELECT"),
        }

    guest_dsn = {**parse_dsn(conninfo), **{"user": "guest", "password": "guest"}}
    gcnx = BaseDb.connect(**guest_dsn)
    # check guest user can actually query a table
    with gcnx.transaction() as gcur:
        gcur.execute("select * from dbversion limit 1")
        assert gcur.rowcount == 1

    with gcnx.transaction() as gcur:
        # check guest user CANNOT create a new table
        with pytest.raises(InsufficientPrivilege):
            gcur.execute("create table toto(id int)")
    with gcnx.transaction() as gcur:
        # check guest user CANNOT drop a new table
        with pytest.raises(InsufficientPrivilege):
            gcur.execute("drop table origin")
    with gcnx.transaction() as gcur:
        # check guest user CANNOT insert data in a table
        with pytest.raises(InsufficientPrivilege):
            gcur.execute(
                """
INSERT INTO origin(url, hash)
VALUES ('https://nowhere.com', hash_sha1('https://nowhere.com'))
            """
            )
