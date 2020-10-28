#!/usr/bin/env python3
# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from os import environ, path
from typing import Collection, Dict, Optional, Tuple
import warnings

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group

warnings.filterwarnings("ignore")  # noqa prevent psycopg from telling us sh*t


logger = logging.getLogger(__name__)


@swh_cli_group.group(name="db", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Configuration file.",
)
@click.pass_context
def db(ctx, config_file):
    """Software Heritage database generic tools."""
    from swh.core.config import read as config_read

    ctx.ensure_object(dict)
    if config_file is None:
        config_file = environ.get("SWH_CONFIG_FILENAME")
    cfg = config_read(config_file)
    ctx.obj["config"] = cfg


@db.command(name="create", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--db-name",
    "-d",
    help="Database name.",
    default="softwareheritage-dev",
    show_default=True,
)
@click.option(
    "--template",
    "-T",
    help="Template database from which to build this database.",
    default="template1",
    show_default=True,
)
def db_create(module, db_name, template):
    """Create a database for the Software Heritage <module>.

    and potentially execute superuser-level initialization steps.

    Example:

      swh db create -d swh-test storage

    If you want to specify non-default postgresql connection parameters, please
    provide them using standard environment variables or by the mean of a
    properly crafted libpq connection URI. See psql(1) man page (section
    ENVIRONMENTS) for details.

    Note: this command requires a postgresql connection with superuser permissions.

    Example:

      PGPORT=5434 swh db create indexer
      swh db create -d postgresql://superuser:passwd@pghost:5433/swh-storage storage

    """

    logger.debug("db_create %s dn_name=%s", module, db_name)
    create_database_for_package(module, db_name, template)


@db.command(name="init-admin", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--db-name",
    "-d",
    help="Database name.",
    default="softwareheritage-dev",
    show_default=True,
)
def db_init_admin(module: str, db_name: str) -> None:
    """Execute superuser-level initialization steps (e.g pg extensions, admin functions,
    ...)

    Example:

      PGPASSWORD=... swh db init-admin -d swh-test scheduler

    If you want to specify non-default postgresql connection parameters, please
    provide them using standard environment variables or by the mean of a
    properly crafted libpq connection URI. See psql(1) man page (section
    ENVIRONMENTS) for details.

    Note: this command requires a postgresql connection with superuser permissions (e.g
    postgres, swh-admin, ...)

    Example:

      PGPORT=5434 swh db init-admin scheduler
      swh db init-admin -d postgresql://superuser:passwd@pghost:5433/swh-scheduler \
        scheduler

    """
    logger.debug("db_init_admin %s db_name=%s", module, db_name)
    init_admin_extensions(module, db_name)


@db.command(name="init", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--db-name",
    "-d",
    help="Database name.",
    default="softwareheritage-dev",
    show_default=True,
)
@click.option(
    "--flavor", help="Database flavor.", default=None,
)
def db_init(module, db_name, flavor):
    """Initialize a database for the Software Heritage <module>.

    Example:

      swh db init -d swh-test storage

    If you want to specify non-default postgresql connection parameters,
    please provide them using standard environment variables.
    See psql(1) man page (section ENVIRONMENTS) for details.

    Examples:

      PGPORT=5434 swh db init indexer
      swh db init -d postgresql://user:passwd@pghost:5433/swh-storage storage
      swh db init --flavor read_replica -d swh-storage storage

    """

    logger.debug("db_init %s flavor=%s dn_name=%s", module, flavor, db_name)

    initialized, dbversion, dbflavor = populate_database_for_package(
        module, db_name, flavor
    )

    # TODO: Ideally migrate the version from db_version to the latest
    # db version

    click.secho(
        "DONE database for {} {}{} at version {}".format(
            module,
            "initialized" if initialized else "exists",
            f" (flavor {dbflavor})" if dbflavor is not None else "",
            dbversion,
        ),
        fg="green",
        bold=True,
    )

    if flavor is not None and dbflavor != flavor:
        click.secho(
            f"WARNING requested flavor '{flavor}' != recorded flavor '{dbflavor}'",
            fg="red",
            bold=True,
        )


def get_sql_for_package(modname):
    import glob
    from importlib import import_module

    from swh.core.utils import numfile_sortkey as sortkey

    if not modname.startswith("swh."):
        modname = "swh.{}".format(modname)
    try:
        m = import_module(modname)
    except ImportError:
        raise click.BadParameter("Unable to load module {}".format(modname))

    sqldir = path.join(path.dirname(m.__file__), "sql")
    if not path.isdir(sqldir):
        raise click.BadParameter(
            "Module {} does not provide a db schema " "(no sql/ dir)".format(modname)
        )
    return sorted(glob.glob(path.join(sqldir, "*.sql")), key=sortkey)


def populate_database_for_package(
    modname: str, conninfo: str, flavor: Optional[str] = None
) -> Tuple[bool, int, Optional[str]]:
    """Populate the database, pointed at with `conninfo`, using the SQL files found in
    the package `modname`.

    Args:
      modname: Name of the module of which we're loading the files
      conninfo: connection info string for the SQL database
      flavor: the module-specific flavor which we want to initialize the database under
    Returns:
      Tuple with three elements: whether the database has been initialized; the current
      version of the database; if it exists, the flavor of the database.
    """
    from swh.core.db.db_utils import swh_db_flavor, swh_db_version

    current_version = swh_db_version(conninfo)
    if current_version is not None:
        dbflavor = swh_db_flavor(conninfo)
        return False, current_version, dbflavor

    sqlfiles = get_sql_for_package(modname)
    sqlfiles = [fname for fname in sqlfiles if "-superuser-" not in fname]
    execute_sqlfiles(sqlfiles, conninfo, flavor)

    current_version = swh_db_version(conninfo)
    assert current_version is not None
    dbflavor = swh_db_flavor(conninfo)
    return True, current_version, dbflavor


def parse_dsn_or_dbname(dsn_or_dbname: str) -> Dict[str, str]:
    """Parse a psycopg2 dsn, falling back to supporting plain database names as well"""
    import psycopg2
    from psycopg2.extensions import parse_dsn as _parse_dsn

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
    sqlfiles = [fname for fname in sqlfiles if "-superuser-" in fname]
    execute_sqlfiles(sqlfiles, conninfo)


def create_database_for_package(
    modname: str, conninfo: str, template: str = "template1"
):
    """Create the database pointed at with `conninfo`, and initialize it using
    -superuser- SQL files found in the package `modname`.

    Args:
      modname: Name of the module of which we're loading the files
      conninfo: connection info string or plain database name for the SQL database
      template: the name of the database to connect to and use as template to create
                the new database

    """
    import subprocess

    from psycopg2.extensions import make_dsn

    # Use the given conninfo string, but with dbname replaced by the template dbname
    # for the database creation step
    creation_dsn = parse_dsn_or_dbname(conninfo)
    db_name = creation_dsn["dbname"]
    creation_dsn["dbname"] = template
    logger.debug("db_create db_name=%s (from %s)", db_name, template)
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
            f'CREATE DATABASE "{db_name}"',
        ]
    )
    init_admin_extensions(modname, conninfo)


def execute_sqlfiles(
    sqlfiles: Collection[str], conninfo: str, flavor: Optional[str] = None
):
    """Execute a list of SQL files on the database pointed at with `conninfo`.

    Args:
      sqlfiles: List of SQL files to execute
      conninfo: connection info string for the SQL database
      flavor: the database flavor to initialize
    """
    import subprocess

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
        logger.debug(f"execute SQL file {sqlfile} db_name={conninfo}")
        subprocess.check_call(psql_command + ["-f", sqlfile])

        if flavor is not None and not flavor_set and sqlfile.endswith("-flavor.sql"):
            logger.debug("Setting database flavor %s", flavor)
            query = f"insert into dbflavor (flavor) values ('{flavor}')"
            subprocess.check_call(psql_command + ["-c", query])
            flavor_set = True

    if flavor is not None and not flavor_set:
        logger.warn(
            "Asked for flavor %s, but module does not support database flavors", flavor,
        )
