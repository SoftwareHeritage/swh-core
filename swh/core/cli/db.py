#!/usr/bin/env python3
# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from os import environ, path
from typing import Tuple
import warnings

import click

from swh.core.cli import CONTEXT_SETTINGS

warnings.filterwarnings("ignore")  # noqa prevent psycopg from telling us sh*t


logger = logging.getLogger(__name__)


@click.group(name="db", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Configuration file.",
)
@click.pass_context
def db(ctx, config_file):
    """Software Heritage database generic tools.
    """
    from swh.core.config import read as config_read

    ctx.ensure_object(dict)
    if config_file is None:
        config_file = environ.get("SWH_CONFIG_FILENAME")
    cfg = config_read(config_file)
    ctx.obj["config"] = cfg


@db.command(name="init", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def init(ctx):
    """Initialize the database for every Software Heritage module found in the
    configuration file. For every configuration section in the config file
    that:

    1. has the name of an existing swh package,
    2. has credentials for a local db access,

    it will run the initialization scripts from the swh package against the
    given database.

    Example for the config file::

    \b
      storage:
        cls: local
        args:
          db: postgresql:///?service=swh-storage
      objstorage:
        cls: remote
        args:
          url: http://swh-objstorage:5003/

    the command:

      swh db -C /path/to/config.yml init

    will initialize the database for the `storage` section using initialization
    scripts from the `swh.storage` package.
    """
    for modname, cfg in ctx.obj["config"].items():
        if cfg.get("cls") == "local" and cfg.get("args", {}).get("db"):
            try:
                initialized, dbversion = populate_database_for_package(
                    modname, cfg["args"]["db"]
                )
            except click.BadParameter:
                logger.info(
                    "Failed to load/find sql initialization files for %s", modname
                )

        click.secho(
            "DONE database for {} {} at version {}".format(
                modname, "initialized" if initialized else "exists", dbversion
            ),
            fg="green",
            bold=True,
        )


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--db-name",
    "-d",
    help="Database name.",
    default="softwareheritage-dev",
    show_default=True,
)
@click.option(
    "--create-db/--no-create-db",
    "-C",
    help="Attempt to create the database.",
    default=False,
)
def db_init(module, db_name, create_db):
    """Initialize a database for the Software Heritage <module>. By
    default, does not attempt to create the database.

    Example:

      swh db-init -d swh-test storage

    If you want to specify non-default postgresql connection parameters,
    please provide them using standard environment variables.
    See psql(1) man page (section ENVIRONMENTS) for details.

    Example:

      PGPORT=5434 swh db-init indexer

    """

    logger.debug("db_init %s dn_name=%s", module, db_name)

    if create_db:
        from swh.core.db.tests.db_testing import pg_createdb

        # Create the db (or fail silently if already existing)
        pg_createdb(db_name, check=False)

    initialized, dbversion = populate_database_for_package(module, db_name)

    # TODO: Ideally migrate the version from db_version to the latest
    # db version

    click.secho(
        "DONE database for {} {} at version {}".format(
            module, "initialized" if initialized else "exists", dbversion
        ),
        fg="green",
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
    return list(sorted(glob.glob(path.join(sqldir, "*.sql")), key=sortkey))


def populate_database_for_package(modname: str, conninfo: str) -> Tuple[bool, int]:
    """Populate the database, pointed at with `conninfo`, using the SQL files found in
    the package `modname`.

    Args:
      modname: Name of the module of which we're loading the files
      conninfo: connection info string for the SQL database
    Returns:
      Tuple with two elements: whether the database has been initialized; the current
      version of the database.
    """
    import subprocess

    from swh.core.db.tests.db_testing import swh_db_version

    current_version = swh_db_version(conninfo)
    if current_version is not None:
        return False, current_version

    sqlfiles = get_sql_for_package(modname)

    for sqlfile in sqlfiles:
        subprocess.check_call(
            [
                "psql",
                "--quiet",
                "--no-psqlrc",
                "-v",
                "ON_ERROR_STOP=1",
                "-d",
                conninfo,
                "-f",
                sqlfile,
            ]
        )

    current_version = swh_db_version(conninfo)
    return True, current_version
