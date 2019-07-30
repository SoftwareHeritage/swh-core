#!/usr/bin/env python3
# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import glob
import logging
from os import path, environ
import subprocess
import warnings

warnings.filterwarnings("ignore")  # noqa prevent psycopg from telling us sh*t

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.config import read as config_read

logger = logging.getLogger(__name__)


@click.group(name="db", context_settings=CONTEXT_SETTINGS)
@click.option("--config-file", "-C", default=None,
              type=click.Path(exists=True, dir_okay=False),
              help="Configuration file.")
@click.pass_context
def db(ctx, config_file):
    """Software Heritage database generic tools.
    """
    ctx.ensure_object(dict)
    if config_file is None:
        config_file = environ.get('SWH_CONFIG_FILENAME')
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
        if cfg.get("cls") == "local" and cfg.get("args"):
            try:
                sqlfiles = get_sql_for_package(modname)
            except click.BadParameter:
                logger.info(
                    "Failed to load/find sql initialization files for %s",
                    modname)

            if sqlfiles:
                conninfo = cfg["args"]["db"]
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


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('module', nargs=-1, required=True)
@click.option('--db-name', '-d', help='Database name.',
              default='softwareheritage-dev', show_default=True)
@click.option('--create-db/--no-create-db', '-C',
              help='Attempt to create the database.',
              default=False)
def db_init(module, db_name, create_db):
    """Initialise a database for the Software Heritage <module>.  By
    default, does not attempt to create the database.

    Example:

      swh db-init -d swh-test storage

    If you want to specify non-default postgresql connection parameters,
    please provide them using standard environment variables.
    See psql(1) man page (section ENVIRONMENTS) for details.

    Example:

      PGPORT=5434 swh db-init indexer

    """
    # put import statements here so we can keep startup time of the main swh
    # command as short as possible
    from swh.core.db.tests.db_testing import (
        pg_createdb, pg_restore, DB_DUMP_TYPES,
        swh_db_version
    )

    logger.debug('db_init %s dn_name=%s', module, db_name)
    dump_files = []

    for modname in module:
        dump_files.extend(get_sql_for_package(modname))

    if create_db:
        # Create the db (or fail silently if already existing)
        pg_createdb(db_name, check=False)
    # Try to retrieve the db version if any
    db_version = swh_db_version(db_name)
    if not db_version:  # Initialize the db
        dump_files = [(x, DB_DUMP_TYPES[path.splitext(x)[1]])
                      for x in dump_files]
        for dump, dtype in dump_files:
            click.secho('Loading {}'.format(dump), fg='yellow')
            pg_restore(db_name, dump, dtype)

        db_version = swh_db_version(db_name)

    # TODO: Ideally migrate the version from db_version to the latest
    # db version

    click.secho('DONE database is {} version {}'.format(db_name, db_version),
                fg='green', bold=True)


def get_sql_for_package(modname):
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
            "Module {} does not provide a db schema "
            "(no sql/ dir)".format(modname))
    return list(sorted(glob.glob(path.join(sqldir, "*.sql")), key=sortkey))
