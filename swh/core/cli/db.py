#!/usr/bin/env python3
# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from os import environ
import warnings

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group

warnings.filterwarnings("ignore")  # noqa prevent psycopg from side-tracking us


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
    "--flavor",
    help="Database flavor.",
    default=None,
)
@click.option(
    "--initial-version", help="Database initial version.", default=1, show_default=True
)
@click.pass_context
def db_init(ctx, module, dbname, flavor, initial_version):
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
    from swh.core.db.db_utils import (
        get_database_info,
        import_swhmodule,
        populate_database_for_package,
        swh_set_db_version,
    )

    cfg = None
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
    if dbversion is None:
        if cfg is not None:
            # db version has not been populated by sql init scripts (new style),
            # let's do it; instantiate the data source to retrieve the current
            # (expected) db version
            datastore_factory = getattr(import_swhmodule(module), "get_datastore", None)
            if datastore_factory:
                datastore = datastore_factory(**cfg)
                if not hasattr(datastore, "current_version"):
                    logger.warning(
                        "Datastore %s does not declare the "
                        "'current_version' attribute",
                        datastore,
                    )
                else:
                    code_version = datastore.current_version
                    logger.info(
                        "Initializing database version to %s from the %s datastore",
                        code_version,
                        module,
                    )
                    swh_set_db_version(dbname, code_version, desc="DB initialization")

    dbversion = get_database_info(dbname)[1]
    if dbversion is None:
        logger.info(
            "Initializing database version to %s "
            "from the command line option --initial-version",
            initial_version,
        )
        swh_set_db_version(dbname, initial_version, desc="DB initialization")

    dbversion = get_database_info(dbname)[1]
    assert dbversion is not None

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


@db.command(name="version", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--all/--no-all",
    "show_all",
    help="Show version history.",
    default=False,
    show_default=True,
)
@click.option("--module-config-key", help="Module config key to lookup.", default=None)
@click.pass_context
def db_version(ctx, module, show_all, module_config_key=None):
    """Print the database version for the Software Heritage.

    Example::

        swh db version -d swh-test
        swh db version scheduler
        swh db version scrubber --module-config-key=scrubber_db

    """
    from swh.core.db.db_utils import get_database_info, import_swhmodule

    # use the db cnx from the config file; the expected config entry is either the given
    # module_config_key or defaulting to the module name (if module_config_key is not
    # provided)
    cfg = ctx.obj["config"].get(module_config_key or module, {})
    dbname = get_dburl_from_config(cfg)

    if not dbname:
        raise click.BadParameter(
            "Missing the postgresql connection configuration. Either fix your "
            "configuration file or use the --dbname option."
        )

    logger.debug("db_version dbname=%s", dbname)

    db_module, db_version, db_flavor = get_database_info(dbname)
    if db_module is None:
        click.secho(
            "WARNING the database does not have a dbmodule table.", fg="red", bold=True
        )
        db_module = module
    assert db_module == module, f"{db_module} (in the db) != {module} (given)"

    click.secho(f"module: {db_module}", fg="green", bold=True)

    if db_flavor is not None:
        click.secho(f"flavor: {db_flavor}", fg="green", bold=True)

    # instantiate the data source to retrieve the current (expected) db version
    datastore_factory = getattr(import_swhmodule(db_module), "get_datastore", None)
    if datastore_factory:
        datastore = datastore_factory(**cfg)
        code_version = datastore.current_version
        click.secho(
            f"current code version: {code_version}",
            fg="green" if code_version == db_version else "red",
            bold=True,
        )

    if not show_all:
        click.secho(f"version: {db_version}", fg="green", bold=True)
    else:
        from swh.core.db.db_utils import swh_db_versions

        versions = swh_db_versions(dbname)
        for version, tstamp, desc in versions:
            click.echo(f"{version} [{tstamp}] {desc}")


@db.command(name="upgrade", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--to-version",
    type=int,
    help="Upgrade up to version VERSION",
    metavar="VERSION",
    default=None,
)
@click.option(
    "--interactive/--non-interactive",
    help="Do not ask questions (use default answer to all questions)",
    default=True,
)
@click.option(
    "--module-config-key", help="Module configuration key to lookup.", default=None
)
@click.pass_context
def db_upgrade(ctx, module, to_version, interactive, module_config_key):
    """Upgrade the database for given module (to a given version if specified).

    Examples::

        swh db upgrade storage
        swh db upgrade scheduler --to-version=10
        swh db upgrade scrubber --to-version=10 --module-config-key=scrubber_db

    """
    from swh.core.db.db_utils import (
        get_database_info,
        import_swhmodule,
        swh_db_upgrade,
        swh_set_db_module,
    )

    # use the db cnx from the config file; the expected config entry is either the given
    # module_config_key or defaulting to the module name (if module_config_key is not
    # provided)
    cfg = ctx.obj["config"].get(module_config_key or module, {})
    dbname = get_dburl_from_config(cfg)

    if not dbname:
        raise click.BadParameter(
            "Missing the postgresql connection configuration. Either fix your "
            "configuration file or use the --dbname option."
        )

    logger.debug("db_version dbname=%s", dbname)

    db_module, db_version, db_flavor = get_database_info(dbname)
    if db_module is None:
        click.secho(
            "Warning: the database does not have a dbmodule table.",
            fg="yellow",
            bold=True,
        )
        if interactive and not click.confirm(
            f"Write the module information ({module}) in the database?", default=True
        ):
            raise click.BadParameter("Migration aborted.")
        swh_set_db_module(dbname, module)
        db_module = module

    if db_module != module:
        raise click.BadParameter(
            f"Error: the given module ({module}) does not match the value "
            f"stored in the database ({db_module})."
        )

    # instantiate the data source to retrieve the current (expected) db version
    datastore_factory = getattr(import_swhmodule(db_module), "get_datastore", None)
    if not datastore_factory:
        raise click.UsageError(
            "You cannot use this command on old-style datastore backend {db_module}"
        )
    datastore = datastore_factory(**cfg)
    ds_version = datastore.current_version
    if to_version is None:
        to_version = ds_version
    if to_version > ds_version:
        raise click.UsageError(
            f"The target version {to_version} is larger than the current version "
            f"{ds_version} of the datastore backend {db_module}"
        )

    if to_version == db_version:
        click.secho(
            f"No migration needed: the current version is {db_version}",
            fg="yellow",
        )
    else:
        new_db_version = swh_db_upgrade(dbname, module, to_version)
        click.secho(f"Migration to version {new_db_version} done", fg="green")
        if new_db_version < ds_version:
            click.secho(
                "Warning: migration was not complete: "
                f"the current version is {ds_version}",
                fg="yellow",
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
