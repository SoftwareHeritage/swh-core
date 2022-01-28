#!/usr/bin/env python3
# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from os import environ
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
    "--dbname",
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
def db_create(module, dbname, template):
    """Create a database for the Software Heritage <module>.

    and potentially execute superuser-level initialization steps.

    Example::

        swh db create -d swh-test storage

    If you want to specify non-default postgresql connection parameters, please
    provide them using standard environment variables or by the mean of a
    properly crafted libpq connection URI. See psql(1) man page (section
    ENVIRONMENTS) for details.

    Note: this command requires a postgresql connection with superuser permissions.

    Example::

        PGPORT=5434 swh db create indexer
        swh db create -d postgresql://superuser:passwd@pghost:5433/swh-storage storage

    """
    from swh.core.db.db_utils import create_database_for_package

    logger.debug("db_create %s dn_name=%s", module, dbname)
    create_database_for_package(module, dbname, template)


@db.command(name="init-admin", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--dbname",
    "--db-name",
    "-d",
    help="Database name.",
    default="softwareheritage-dev",
    show_default=True,
)
def db_init_admin(module: str, dbname: str) -> None:
    """Execute superuser-level initialization steps (e.g pg extensions, admin functions,
    ...)

    Example::

        PGPASSWORD=... swh db init-admin -d swh-test scheduler

    If you want to specify non-default postgresql connection parameters, please
    provide them using standard environment variables or by the mean of a
    properly crafted libpq connection URI. See psql(1) man page (section
    ENVIRONMENTS) for details.

    Note: this command requires a postgresql connection with superuser permissions (e.g
    postgres, swh-admin, ...)

    Example::

        PGPORT=5434 swh db init-admin scheduler
        swh db init-admin -d postgresql://superuser:passwd@pghost:5433/swh-scheduler \
          scheduler

    """
    from swh.core.db.db_utils import init_admin_extensions

    logger.debug("db_init_admin %s dbname=%s", module, dbname)
    init_admin_extensions(module, dbname)


@db.command(name="init", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--dbname",
    "--db-name",
    "-d",
    help="Database name or connection URI.",
    default=None,
    show_default=False,
)
@click.option(
    "--flavor", help="Database flavor.", default=None,
)
@click.pass_context
def db_init(ctx, module, dbname, flavor):
    """Initialize a database for the Software Heritage <module>.

    The database connection string comes from the configuration file (see
    option ``--config-file`` in ``swh db --help``) in the section named after
    the MODULE argument.

    Example::

        $ cat conf.yml
        storage:
          cls: postgresql
          db: postgresql://user:passwd@pghost:5433/swh-storage
          objstorage:
            cls: memory

        $ swh db -C conf.yml init storage  # or
        $ SWH_CONFIG_FILENAME=conf.yml swh db init storage

    Note that the connection string can also be passed directly using the
    '--db-name' option, but this usage is about to be deprecated.

    """
    from swh.core.db.db_utils import populate_database_for_package

    if dbname is None:
        # use the db cnx from the config file; the expected config entry is the
        # given module name
        cfg = ctx.obj["config"].get(module, {})
        dbname = get_dburl_from_config(cfg)

    if not dbname:
        raise click.BadParameter(
            "Missing the postgresql connection configuration. Either fix your "
            "configuration file or use the --dbname option."
        )

    logger.debug("db_init %s flavor=%s dbname=%s", module, flavor, dbname)

    initialized, dbversion, dbflavor = populate_database_for_package(
        module, dbname, flavor
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


def get_dburl_from_config(cfg):
    if cfg.get("cls") != "postgresql":
        raise click.BadParameter(
            "Configuration cls must be set to 'postgresql' for this command."
        )
    if "args" in cfg:
        # for bw compat
        cfg = cfg["args"]
    return cfg.get("db")
