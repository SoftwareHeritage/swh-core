# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import os

from pytest_postgresql import factories

from swh.core.db import BaseDb
from swh.core.db.pytest_plugin import gen_dump_files, postgresql_fact

SQL_DIR = os.path.join(os.path.dirname(__file__), "data")

test_postgresql_proc = factories.postgresql_proc(
    dbname="fun",
    load=sorted(glob.glob(f"{SQL_DIR}/*.sql")),  # type: ignore[arg-type]
    # type ignored because load is typed as Optional[List[...]] instead of an
    # Optional[Sequence[...]] in pytest_postgresql<4
)

# db with special policy for tables dbversion and people
postgres_fun = postgresql_fact(
    "test_postgresql_proc", no_db_drop=True, no_truncate_tables={"dbversion", "people"},
)

postgres_fun2 = postgresql_fact(
    "test_postgresql_proc",
    dbname="fun2",
    load=sorted(glob.glob(f"{SQL_DIR}/*.sql")),
    no_truncate_tables={"dbversion", "people"},
    no_db_drop=True,
)


def test_smoke_test_fun_db_is_up(postgres_fun):
    """This ensures the db is created and configured according to its dumps files.

    """
    with BaseDb.connect(postgres_fun.dsn).cursor() as cur:
        cur.execute("select count(*) from dbversion")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 5

        cur.execute("select count(*) from fun")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 3

        cur.execute("select count(*) from people")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 2

        # in data, we requested a value already so it starts at 2
        cur.execute("select nextval('serial')")
        val = cur.fetchone()[0]
        assert val == 2


def test_smoke_test_fun2_db_is_up(postgres_fun2):
    """This ensures the db is created and configured according to its dumps files.

    """
    with BaseDb.connect(postgres_fun2.dsn).cursor() as cur:
        cur.execute("select count(*) from dbversion")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 5

        cur.execute("select count(*) from fun")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 3

        cur.execute("select count(*) from people")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 2

        # in data, we requested a value already so it starts at 2
        cur.execute("select nextval('serial')")
        val = cur.fetchone()[0]
        assert val == 2


def test_smoke_test_fun_db_is_still_up_and_got_reset(postgres_fun):
    """This ensures that within another tests, the 'fun' db is still up, created (and not
    configured again). This time, most of the data has been reset:
    - except for tables 'dbversion' and 'people' which were left as is
    - the other tables from the schema (here only "fun") got truncated
    - the sequences got truncated as well

    """
    with BaseDb.connect(postgres_fun.dsn).cursor() as cur:
        # db version is excluded from the truncate
        cur.execute("select count(*) from dbversion")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 5

        # people is also allowed not to be truncated
        cur.execute("select count(*) from people")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 2

        # table and sequence are reset
        cur.execute("select count(*) from fun")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 0

        cur.execute("select nextval('serial')")
        val = cur.fetchone()[0]
        assert val == 1


# db with no special policy for tables truncation, all tables are reset
postgres_people = postgresql_fact(
    "postgresql_proc",
    dbname="people",
    dump_files=f"{SQL_DIR}/*.sql",
    no_truncate_tables=set(),
    no_db_drop=True,
)


def test_gen_dump_files():
    files = [os.path.basename(fn) for fn in gen_dump_files(f"{SQL_DIR}/*.sql")]
    assert files == ["0-schema.sql", "1-data.sql"]


def test_smoke_test_people_db_up(postgres_people):
    """'people' db is up and configured

    """
    with BaseDb.connect(postgres_people.dsn).cursor() as cur:
        cur.execute("select count(*) from dbversion")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 5

        cur.execute("select count(*) from people")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 2

        cur.execute("select count(*) from fun")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 3

        cur.execute("select nextval('serial')")
        val = cur.fetchone()[0]
        assert val == 2


def test_smoke_test_people_db_up_and_reset(postgres_people):
    """'people' db is up and got reset on every tables and sequences

    """
    with BaseDb.connect(postgres_people.dsn).cursor() as cur:
        # tables are truncated after the first round
        cur.execute("select count(*) from dbversion")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 0

        # tables are truncated after the first round
        cur.execute("select count(*) from people")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 0

        # table and sequence are reset
        cur.execute("select count(*) from fun")
        nb_rows = cur.fetchone()[0]
        assert nb_rows == 0

        cur.execute("select nextval('serial')")
        val = cur.fetchone()[0]
        assert val == 1


# db with no initialization step, an empty db
postgres_no_init = postgresql_fact("postgresql_proc", dbname="something")


def test_smoke_test_db_no_init(postgres_no_init):
    """We can connect to the db nonetheless

    """
    with BaseDb.connect(postgres_no_init.dsn).cursor() as cur:
        cur.execute("select now()")
        data = cur.fetchone()[0]
        assert data is not None
