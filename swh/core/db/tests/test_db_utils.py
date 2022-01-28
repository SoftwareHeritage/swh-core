# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta

import pytest

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import get_database_info, swh_db_module, swh_db_versions

from .test_cli import craft_conninfo, now


@pytest.mark.parametrize("module", ["test.cli", "test.cli_new"])
def test_db_utils_versions(cli_runner, postgresql, mock_package_sql, module):
    """Check get_database_info, swh_db_versions and swh_db_module work ok

    This test checks db versions for both a db with "new style" set of sql init
    scripts (i.e. the dbversion table is not created in these scripts, but by
    the populate_database_for_package() function directly, via the 'swh db
    init' command) and an "old style" set (dbversion created in the scripts)S.

    """
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    # the dbversion and dbmodule tables exists and are populated
    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == module
    if module == "test.cli":
        # old style: backend init script set the db version
        assert dbversion == 1
    else:
        # new style: they do not (but we do not have support for this in swh.core yet)
        assert dbversion is None
    assert dbflavor is None

    # check also the swh_db_module() function
    assert swh_db_module(conninfo) == module

    # check also the swh_db_versions() function
    versions = swh_db_versions(conninfo)
    if module == "test.cli":
        assert len(versions) == 1
        assert versions[0][0] == 1
        assert versions[0][1] == datetime.fromisoformat(
            "2016-02-22T15:56:28.358587+00:00"
        )
        assert versions[0][2] == "Work In Progress"
    else:
        assert not versions
    # add a few versions in dbversion
    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        if module == "test.cli_new":
            # add version 1 to make it simpler for checks below
            cur.execute(
                "insert into dbversion(version, release, description) "
                "values(1, NOW(), 'Wotk in progress')"
            )
        cur.executemany(
            "insert into dbversion(version, release, description) values (%s, %s, %s)",
            [(i, now(), f"Upgrade to version {i}") for i in range(2, 6)],
        )

    dbmodule, dbversion, dbflavor = get_database_info(conninfo)
    assert dbmodule == module
    assert dbversion == 5
    assert dbflavor is None

    versions = swh_db_versions(conninfo)
    assert len(versions) == 5
    for i, (version, ts, desc) in enumerate(versions):
        assert version == (5 - i)  # these are in reverse order
        if version > 1:
            assert desc == f"Upgrade to version {version}"
            assert (now() - ts) < timedelta(seconds=1)
