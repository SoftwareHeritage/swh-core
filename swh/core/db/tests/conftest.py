# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib import import_module
import os
from pathlib import Path

from click.testing import CliRunner
from hypothesis import HealthCheck
import psycopg2
import pytest
from pytest_postgresql import factories

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
def mock_import_module(request, mocker, datadir):
    mock = mocker.MagicMock

    def import_module_mocker(name, package=None):
        if not name.startswith("swh.test"):
            return import_module(name, package)

        m = request.node.get_closest_marker("init_version")
        if m:
            version = m.kwargs.get("version", 1)
        else:
            version = 3
        if name.startswith("swh."):
            name = name[4:]
        modpath = name.split(".")

        def get_datastore(*args, **kw):
            return mock(current_version=version)

        return mock(
            __name__=name.split(".")[-1],
            __file__=os.path.join(datadir, *modpath, "__init__.py"),
            get_datastore=get_datastore,
        )

    return mocker.patch("swh.core.db.db_utils.import_module", import_module_mocker)


@pytest.fixture()
def mock_get_entry_points(request, mocker, datadir, mock_import_module):
    mock = mocker.MagicMock

    def get_entry_points_mocker(group):
        m = request.node.get_closest_marker("init_version")
        if m:
            version = m.kwargs.get("version", 1)
        else:
            version = 3

        class EntryPoints(dict):
            def __iter__(self):
                return iter(self.values())

        entrypoints = EntryPoints()
        for entry in (Path(datadir) / "test").iterdir():
            if entry.is_dir():
                ep = mock(
                    module=f"swh.test.{entry.name}",
                    load=lambda: mock(current_version=version),
                )
                # needed to overwrite the Mock's name argument, see
                # https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
                ep.name = entry.name
                entrypoints[entry.name] = ep
        return entrypoints

    return mocker.patch("swh.core.config.get_entry_points", get_entry_points_mocker)


# for bw compat
mock_get_swh_backend_module = mock_get_entry_points
