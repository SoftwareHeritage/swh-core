# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
from datetime import datetime, timezone
import functools
from importlib import import_module
import logging
from os import path
import pathlib
import re
import subprocess
from types import ModuleType
from typing import Collection, Dict, Iterator, List, Optional, Tuple, Union, cast

import psycopg2
import psycopg2.errors
import psycopg2.extensions
from psycopg2.extensions import connection as pgconnection
from psycopg2.extensions import encodings as pgencodings
from psycopg2.extensions import make_dsn
from psycopg2.extensions import parse_dsn as _parse_dsn

from swh.core.config import get_swh_backend_module
from swh.core.utils import numfile_sortkey as sortkey

logger = logging.getLogger(__name__)


def now():
    return datetime.now(tz=timezone.utc)


def stored_procedure(stored_proc):
    """decorator to execute remote stored procedure, specified as argument

    Generally, the body of the decorated function should be empty. If it is
    not, the stored procedure will be executed first; the function body then.

    """

    def wrap(meth):
        @functools.wraps(meth)
        def _meth(self, *args, **kwargs):
            cur = kwargs.get("cur", None)
            self._cursor(cur).execute("SELECT %s()" % stored_proc)
            meth(self, *args, **kwargs)

        return _meth

    return wrap


def jsonize(value):
    """Convert a value to a psycopg2 JSON object if necessary"""
    if isinstance(value, dict):
        return psycopg2.extras.Json(value)

    return value


@contextmanager
def connect_to_conninfo(
    db_or_conninfo: Union[str, pgconnection]
) -> Iterator[pgconnection]:
    """Connect to the database passed as argument.

    Args:
        db_or_conninfo: A database connection, or a database connection info string

    Returns:
        a connected database handle or None if the database is not initialized

    """
    if isinstance(db_or_conninfo, pgconnection):
        yield db_or_conninfo
    else:
        if "=" not in db_or_conninfo and "//" not in db_or_conninfo:
            # Database name
            db_or_conninfo = f"dbname={db_or_conninfo}"

        try:
            db = psycopg2.connect(db_or_conninfo)
        except psycopg2.Error:
            logger.exception("Failed to connect to `%s`", db_or_conninfo)
        else:
            yield db


def swh_db_version(db_or_conninfo: Union[str, pgconnection]) -> Optional[int]:
    """Retrieve the swh version of the database.

    If the database is not initialized, this logs a warning and returns None.

    Args:
      db_or_conninfo: A database connection, or a database connection info string

    Returns:
        Either the version of the database, or None if it couldn't be detected
    """
    try:
        with connect_to_conninfo(db_or_conninfo) as db:
            if not db:
                return None
            with db.cursor() as c:
                query = "select version from dbversion order by dbversion desc limit 1"
                try:
                    c.execute(query)
                    result = c.fetchone()
                    if result:
                        return result[0]
                except psycopg2.errors.UndefinedTable:
                    return None
    except Exception:
        logger.exception("Could not get version from `%s`", db_or_conninfo)
    return None


def swh_db_versions(
    db_or_conninfo: Union[str, pgconnection]
) -> Optional[List[Tuple[int, datetime, str]]]:
    """Retrieve the swh version history of the database.

    If the database is not initialized, this logs a warning and returns None.

    Args:
      db_or_conninfo: A database connection, or a database connection info string

    Returns:
        Either the version of the database, or None if it couldn't be detected
    """
    try:
        with connect_to_conninfo(db_or_conninfo) as db:
            if not db:
                return None
            with db.cursor() as c:
                query = (
                    "select version, release, description "
                    "from dbversion order by dbversion desc"
                )
                try:
                    c.execute(query)
                    return cast(List[Tuple[int, datetime, str]], c.fetchall())
                except psycopg2.errors.UndefinedTable:
                    return None
    except Exception:
        logger.exception("Could not get versions from `%s`", db_or_conninfo)
        return None


def swh_db_upgrade(
    conninfo: str, modname: str, to_version: Optional[int] = None
) -> int:
    """Upgrade the database at `conninfo` for module `modname`

    This will run migration scripts found in the `sql/upgrades` subdirectory of
    the module `modname`. By default, this will upgrade to the latest declared version.

    Args:
      conninfo: A database connection, or a database connection info string
      modname: datastore module the database stores content for
      to_version: if given, update the database to this version rather than the latest

    """

    if to_version is None:
        to_version = 99999999

    db_module, db_version, db_flavor = get_database_info(conninfo)
    if db_version is None:
        raise ValueError("Unable to retrieve the current version of the database")
    if db_module is None:
        raise ValueError("Unable to retrieve the module of the database")
    if ":" in db_module and ":" not in modname:
        logger.warn(
            f"modname {modname} should have been given as a backend reference "
            "(<package>:<cls>); this can happen for swh package not yet updated "
            "to swh.core>=3.6; using 'postgresql' as cls."
        )
        modname = f"{modname}:postgresql"
    if db_module != modname:
        raise ValueError(
            f"The stored module of the database {db_module} "
            f"is different than the given one {modname}"
        )

    sqlfiles = [
        fname
        for fname in get_sql_for_package(modname, upgrade=True)
        if "-" not in fname.stem and db_version < int(fname.stem) <= to_version
    ]
    if not sqlfiles:
        return db_version

    for sqlfile in sqlfiles:
        new_version = int(path.splitext(path.basename(sqlfile))[0])
        logger.info("Executing migration script '%s'", sqlfile)
        if db_version is not None and (new_version - db_version) > 1:
            logger.error(
                f"There are missing migration steps between {db_version} and "
                f"{new_version}. It might be expected but it most unlikely is not. "
                "Will stop here."
            )
            return db_version

        execute_sqlfiles([sqlfile], conninfo, db_flavor)

        # check if the db version has been updated by the upgrade script
        db_version = swh_db_version(conninfo)
        assert db_version is not None
        if db_version == new_version:
            # nothing to do, upgrade script did the job
            pass
        elif db_version == new_version - 1:
            # it has not (new style), so do it
            swh_set_db_version(
                conninfo,
                new_version,
                desc=f"Upgraded to version {new_version} using {sqlfile}",
            )
            db_version = swh_db_version(conninfo)
        else:
            # upgrade script did it wrong
            logger.error(
                f"The upgrade script {sqlfile} did not update the dbversion table "
                f"consistently ({db_version} vs. expected {new_version}). "
                "Will stop migration here. Please check your migration scripts."
            )
            return db_version
    return new_version


def swh_db_module(db_or_conninfo: Union[str, pgconnection]) -> Optional[str]:
    """Retrieve the swh module used to create the database.

    If the database is not initialized, this logs a warning and returns None.

    Args:
      db_or_conninfo: A database connection, or a database connection info string

    Returns:
        Either the module of the database, or None if it couldn't be detected
    """
    try:
        with connect_to_conninfo(db_or_conninfo) as db:
            if not db:
                return None
            with db.cursor() as c:
                query = "select dbmodule from dbmodule limit 1"
                try:
                    c.execute(query)
                    resp = c.fetchone()
                    if resp:
                        return resp[0]
                except psycopg2.errors.UndefinedTable:
                    return None
    except Exception:
        logger.exception("Could not get module from `%s`", db_or_conninfo)
    return None


def swh_set_db_module(
    db_or_conninfo: Union[str, pgconnection], module: str, force=False
) -> None:
    """Set the swh module used to create the database.

    Fails if the dbmodule is already set or the table does not exist.

    Args:
      db_or_conninfo: A database connection, or a database connection info string
      module: the swh module to register
    """
    update = False

    current_module = swh_db_module(db_or_conninfo)
    if current_module is not None:
        if current_module == module:
            logger.info("The database module is already set to %s", module)
            return

        if ":" not in current_module and ":" in module:
            logger.warning(
                "The database module needs to be updated (from '%s' to '%s')",
                current_module,
                module,
            )
        elif not force:
            raise ValueError(
                "The database module is already set to a value %s "
                "different than given %s",
                current_module,
                module,
            )
        # force is True
        update = True

    with connect_to_conninfo(db_or_conninfo) as db:
        if not db:
            return None

        # recreate the dbmodule table if need be
        # XXX is this still relevant somehow?
        sqlfiles = [
            fname
            for fname in get_sql_for_package("swh.core.db")
            if "dbmodule" in fname.stem
        ]
        execute_sqlfiles(sqlfiles, db_or_conninfo)

        with db.cursor() as c:
            if update:
                query = "update dbmodule set dbmodule = %s"
            else:
                query = "insert into dbmodule(dbmodule) values (%s)"
            c.execute(query, (module,))
        db.commit()


def swh_set_db_version(
    db_or_conninfo: Union[str, pgconnection],
    version: int,
    ts: Optional[datetime] = None,
    desc: str = "Work in progress",
) -> None:
    """Set the version of the database.

    Fails if the dbversion table does not exists.

    Args:
      db_or_conninfo: A database connection, or a database connection info string
      version: the version to add
    """
    if ts is None:
        ts = now()

    with connect_to_conninfo(db_or_conninfo) as db:
        if not db:
            return None
        with db.cursor() as c:
            query = (
                "insert into dbversion(version, release, description) "
                "values (%s, %s, %s)"
            )
            c.execute(query, (version, ts, desc))
            db.commit()


def swh_db_flavor(db_or_conninfo: Union[str, pgconnection]) -> Optional[str]:
    """Retrieve the swh flavor of the database.

    If the database is not initialized, or the database doesn't support
    flavors, this returns None.

    Args:
      db_or_conninfo: A database connection, or a database connection info string

    Returns:
        The flavor of the database, or None if it could not be detected.
    """
    try:
        with connect_to_conninfo(db_or_conninfo) as db:
            if not db:
                return None
            with db.cursor() as c:
                query = "select swh_get_dbflavor()"
                try:
                    c.execute(query)
                    result = c.fetchone()
                    assert result is not None  # to keep mypy happy
                    return result[0]
                except psycopg2.errors.UndefinedFunction:
                    # function not found: no flavor
                    return None
    except Exception:
        logger.exception("Could not get flavor from `%s`", db_or_conninfo)
        return None


# The following code has been imported from psycopg2, version 2.7.4,
# https://github.com/psycopg/psycopg2/tree/5afb2ce803debea9533e293eef73c92ffce95bcd
# and modified by Software Heritage.
#
# Original file: lib/extras.py
#
# psycopg2 is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.


def _paginate(seq, page_size):
    """Consume an iterable and return it in chunks.
    Every chunk is at most `page_size`. Never return an empty chunk.
    """
    page = []
    it = iter(seq)
    while 1:
        try:
            for i in range(page_size):
                page.append(next(it))
            yield page
            page = []
        except StopIteration:
            if page:
                yield page
            return


def _split_sql(sql):
    """Split *sql* on a single ``%s`` placeholder.
    Split on the %s, perform %% replacement and return pre, post lists of
    snippets.
    """
    curr = pre = []
    post = []
    tokens = re.split(rb"(%.)", sql)
    for token in tokens:
        if len(token) != 2 or token[:1] != b"%":
            curr.append(token)
            continue

        if token[1:] == b"s":
            if curr is pre:
                curr = post
            else:
                raise ValueError("the query contains more than one '%s' placeholder")
        elif token[1:] == b"%":
            curr.append(b"%")
        else:
            raise ValueError(
                "unsupported format character: '%s'"
                % token[1:].decode("ascii", "replace")
            )

    if curr is pre:
        raise ValueError("the query doesn't contain any '%s' placeholder")

    return pre, post


def execute_values_generator(cur, sql, argslist, template=None, page_size=100):
    """Execute a statement using SQL ``VALUES`` with a sequence of parameters.
    Rows returned by the query are returned through a generator.
    You need to consume the generator for the queries to be executed!

    :param cur: the cursor to use to execute the query.
    :param sql: the query to execute. It must contain a single ``%s``
        placeholder, which will be replaced by a `VALUES list`__.
        Example: ``"INSERT INTO mytable (id, f1, f2) VALUES %s"``.
    :param argslist: sequence of sequences or dictionaries with the arguments
        to send to the query. The type and content must be consistent with
        *template*.
    :param template: the snippet to merge to every item in *argslist* to
        compose the query.

        - If the *argslist* items are sequences it should contain positional
          placeholders (e.g. ``"(%s, %s, %s)"``, or ``"(%s, %s, 42)``" if there
          are constants value...).
        - If the *argslist* items are mappings it should contain named
          placeholders (e.g. ``"(%(id)s, %(f1)s, 42)"``).

        If not specified, assume the arguments are sequence and use a simple
        positional template (i.e.  ``(%s, %s, ...)``), with the number of
        placeholders sniffed by the first element in *argslist*.
    :param page_size: maximum number of *argslist* items to include in every
        statement. If there are more items the function will execute more than
        one statement.
    :param yield_from_cur: Whether to yield results from the cursor in this
        function directly.

    .. __: https://www.postgresql.org/docs/current/static/queries-values.html

    After the execution of the function the `cursor.rowcount` property will
    **not** contain a total result.
    """
    # we can't just use sql % vals because vals is bytes: if sql is bytes
    # there will be some decoding error because of stupid codec used, and Py3
    # doesn't implement % on bytes.
    if not isinstance(sql, bytes):
        sql = sql.encode(pgencodings[cur.connection.encoding])
    pre, post = _split_sql(sql)

    for page in _paginate(argslist, page_size=page_size):
        if template is None:
            template = b"(" + b",".join([b"%s"] * len(page[0])) + b")"
        parts = pre[:]
        for args in page:
            parts.append(cur.mogrify(template, args))
            parts.append(b",")
        parts[-1:] = post
        cur.execute(b"".join(parts))
        yield from cur


def import_swhmodule(modname: str) -> Optional[ModuleType]:
    # TODO: move import_swhmodule in swh.core.config, but swh-scrubber needs to
    # be aware of that befaore it can happen...
    if ":" in modname:
        # new style: look for the actual module in the 'swh.<package>.classes'
        # entrypoint
        package, cls = modname.split(":", 1)
        modname, _ = get_swh_backend_module(swh_package=package, cls=cls)

    if not modname.startswith("swh."):
        modname = f"swh.{modname}"
    try:
        m = import_module(modname)
    except ImportError as exc:
        logger.error(f"Could not load the {modname} module: {exc}")
        return None
    return m


def get_sql_for_package(modname: str, upgrade: bool = False) -> List[pathlib.Path]:
    """Return the (sorted) list of sql script files for the given swh module

    If upgrade is True, return the list of available migration scripts,
    otherwise, return the list of initialization scripts.
    """
    m = import_swhmodule(modname)
    if m is None:
        raise ValueError(f"Module {modname} cannot be loaded")
    if m.__file__ is None:
        raise ValueError(f"Module {modname} is not valid (no __file__)")
    moddir = pathlib.Path(m.__file__).parent

    while not (moddir / "sql").is_dir():
        # for bw compat: look for the sql/ dir in the parent directory
        moddir = moddir.parent
        if moddir.name == "swh":
            raise ValueError(
                "Module {} does not provide a db schema (no sql/ dir)".format(modname)
            )
    sqldir = moddir / "sql"
    if upgrade:
        sqldir /= "upgrades"
    return sorted(sqldir.glob("*.sql"), key=lambda x: sortkey(x.name))


def populate_database_for_package(
    modname: str, conninfo: str, flavor: Optional[str] = None
) -> Tuple[bool, Optional[int], Optional[str]]:
    """Populate the database, pointed at with ``conninfo``,
    using the SQL files found in the package ``modname``.
    Also fill the 'dbmodule' table with the given ``modname``.

    Args:
      modname: Name of the module of which we're loading the files
      conninfo: connection info string for the SQL database
      flavor: the module-specific flavor which we want to initialize the database under

    Returns:
      Tuple with three elements: whether the database has been initialized; the current
      version of the database; if it exists, the flavor of the database.
    """

    current_version = swh_db_version(conninfo)
    if current_version is not None:
        dbflavor = swh_db_flavor(conninfo)
        # check/update the dbmodule table
        swh_set_db_module(conninfo, modname)
        return False, current_version, dbflavor

    def globalsortkey(key):
        "like sortkey but only on basenames"
        return sortkey(path.basename(key))

    sqlfiles = get_sql_for_package(modname) + get_sql_for_package("swh.core.db")
    sqlfiles = sorted(sqlfiles, key=lambda x: sortkey(x.stem))
    sqlfiles = [fpath for fpath in sqlfiles if "-superuser-" not in fpath.stem]
    execute_sqlfiles(sqlfiles, conninfo, flavor)

    # populate the dbmodule table
    swh_set_db_module(conninfo, modname)

    current_db_version = swh_db_version(conninfo)
    dbflavor = swh_db_flavor(conninfo)
    return True, current_db_version, dbflavor


def initialize_database_for_module(
    modname: str, version: int, flavor: Optional[str] = None, **kwargs
):
    """Helper function to initialize and populate a database for the given module

    This aims at helping the usage of pytest_postgresql for swh.core.db based datastores.
    Typical usage will be (here for swh.storage)::

      from pytest_postgresql import factories

      storage_postgresql_proc = factories.postgresql_proc(
        load=[partial(initialize_database_for_module, modname="storage", version=42)]
      )
      storage_postgresql = factories.postgresql("storage_postgresql_proc")

    """
    conninfo = psycopg2.connect(**kwargs).dsn
    init_admin_extensions(modname, conninfo)
    populate_database_for_package(modname, conninfo, flavor)
    try:
        swh_set_db_version(conninfo, version)
    except psycopg2.errors.UniqueViolation:
        logger.warn(
            "Version already set by db init scripts. "
            f"This generally means the swh.{modname} package needs to be "
            "updated for swh.core>=1.2"
        )


def get_database_info(
    conninfo: str,
) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """Get version, flavor and module of the db"""
    dbmodule = swh_db_module(conninfo)
    dbversion = swh_db_version(conninfo)
    dbflavor = None
    if dbversion is not None:
        dbflavor = swh_db_flavor(conninfo)
    return (dbmodule, dbversion, dbflavor)


def parse_dsn_or_dbname(dsn_or_dbname: str) -> Dict[str, str]:
    """Parse a psycopg2 dsn, falling back to supporting plain database names as well"""
    try:
        return _parse_dsn(dsn_or_dbname)
    except psycopg2.ProgrammingError:
        # psycopg2 failed to parse the DSN; it's probably a database name,
        # handle it as such
        return _parse_dsn(f"dbname={dsn_or_dbname}")


def init_admin_extensions(modname: str, conninfo: str) -> None:
    """The remaining initialization process -- running -superuser- SQL files -- is done
    using the given conninfo, thus connecting to the newly created database

    """
    sqlfiles = get_sql_for_package(modname)
    sqlfiles = [fname for fname in sqlfiles if "-superuser-" in fname.stem]
    execute_sqlfiles(sqlfiles, conninfo)


def create_database_for_package(
    modname: str, conninfo: str, template: str = "template1"
):
    """Create the database pointed at with ``conninfo``, and initialize it using
    -superuser- SQL files found in the package ``modname``.

    Args:
      modname: Name of the module of which we're loading the files
      conninfo: connection info string or plain database name for the SQL database
      template: the name of the database to connect to and use as template to create
        the new database

    """
    # Use the given conninfo string, but with dbname replaced by the template dbname
    # for the database creation step
    creation_dsn = parse_dsn_or_dbname(conninfo)
    dbname = creation_dsn["dbname"]
    creation_dsn["dbname"] = template
    logger.debug("db_create dbname=%s (from %s)", dbname, template)
    subprocess.check_call(
        [
            "psql",
            "--quiet",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            make_dsn(**creation_dsn),
            "-c",
            f'CREATE DATABASE "{dbname}"',
        ]
    )
    init_admin_extensions(modname, conninfo)


def execute_sqlfiles(
    sqlfiles: Collection[pathlib.Path],
    db_or_conninfo: Union[str, pgconnection],
    flavor: Optional[str] = None,
):
    """Execute a list of SQL files on the database pointed at with ``db_or_conninfo``.

    Args:
      sqlfiles: List of SQL files to execute
      db_or_conninfo: A database connection, or a database connection info string
      flavor: the database flavor to initialize
    """
    if isinstance(db_or_conninfo, str):
        conninfo = db_or_conninfo
    else:
        conninfo = db_or_conninfo.dsn

    psql_command = [
        "psql",
        "--quiet",
        "--no-psqlrc",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        conninfo,
    ]

    flavor_set = False
    for sqlfile in sqlfiles:
        logger.debug(f"execute SQL file {sqlfile} dbname={conninfo}")
        subprocess.run(
            psql_command + ["-f", str(sqlfile)], check=True, capture_output=True
        )

        if (
            flavor is not None
            and not flavor_set
            and sqlfile.name.endswith("-flavor.sql")
        ):
            logger.debug("Setting database flavor %s", flavor)
            query = f"insert into dbflavor (flavor) values ('{flavor}')"
            subprocess.check_call(psql_command + ["-c", query])
            flavor_set = True

    if flavor is not None and not flavor_set:
        logger.warn(
            "Asked for flavor %s, but module does not support database flavors",
            flavor,
        )

    # Grant read-access to guest user on all tables of the schema (if possible)
    with connect_to_conninfo(db_or_conninfo) as db:
        try:
            with db.cursor() as c:
                query = "grant select on all tables in schema public to guest"
                c.execute(query)
            db.commit()
        except Exception:
            logger.warning("Grant read-only access to guest user failed. Skipping.")
