#!/usr/bin/env python3
# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import warnings
warnings.filterwarnings("ignore")  # noqa prevent psycopg from telling us sh*t

import click

from swh.core.cli import CONTEXT_SETTINGS

logger = logging.getLogger(__name__)


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
    from os import path
    import glob
    from importlib import import_module
    from swh.core.utils import numfile_sortkey as sortkey
    from swh.core.db.tests.db_testing import (
        pg_createdb, pg_restore, DB_DUMP_TYPES,
        swh_db_version
    )

    logger.debug('db_init %s dn_name=%s', module, db_name)
    dump_files = []

    for modname in module:
        if not modname.startswith('swh.'):
            modname = 'swh.{}'.format(modname)
        try:
            m = import_module(modname)
        except ImportError:
            raise click.BadParameter(
                'Unable to load module {}'.format(modname))

        sqldir = path.join(path.dirname(m.__file__), 'sql')
        if not path.isdir(sqldir):
            raise click.BadParameter(
                'Module {} does not provide a db schema '
                '(no sql/ dir)'.format(modname))
        dump_files.extend(sorted(glob.glob(path.join(sqldir, '*.sql')),
                                 key=sortkey))

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
