# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

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
