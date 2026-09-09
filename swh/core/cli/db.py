#!/usr/bin/env python3
# Copyright (C) 2018-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from subprocess import CalledProcessError
from typing import Any, Dict, List, Optional, Tuple
import warnings

import click

from swh.core.cli import CONTEXT_SETTINGS, setup_config
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
    deprecated=True,
)
@click.pass_context
def db(ctx, config_file):
    """Software Heritage database generic tools."""
    setup_config(ctx, config_file)


@db.command(name="create", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.option(
    "--dbname",
    "--db-name",
    "-d",
    help="Database name.",
    default=None,
)
@click.option(
    "-a",
    "--all",
    "create_all",
    help="create all db found in the config file for the swh 'module'",
    default=False,
    is_flag=True,
)
@click.option(
    "--template",
    "-T",
    help="Template database from which to build this database.",
    default="template1",
    show_default=True,
)
@click.pass_context
def db_create(ctx, module, dbname, create_all, template):
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

        \b
        PGPORT=5434 swh db create indexer
        swh db create -d postgresql://superuser:passwd@pghost:5433/swh-storage storage

    """
    from subprocess import SubprocessError

    from swh.core.db.db_utils import create_database_for_package

    args = handle_cmd_args(
        cfg=ctx.obj["config"],
        module=module,
        do_all=create_all,
        dbname=dbname,
        is_path=False,
    )

    for cfgname, package, cls, fullmodule, backend_class, dbname, cfg in args:
        try:
            create_database_for_package(fullmodule, dbname, template)
            click.echo(f"{package}:{cls} Created database {dbname}")
        except SubprocessError:
            click.echo(f"{package}:{cls} Database creation {dbname} failed", err=True)


@db.command(name="init-admin", context_settings=CONTEXT_SETTINGS)
@click.argument("module", metavar="MODULE-OR-CONFIG-PATH", required=True)
@click.option(
    "--dbname",
    "--db-name",
    "-d",
    help="Database name.",
    default=None,
)
@click.option(
    "-a",
    "--all",
    "initialize_all",
    help="superuser initialize all db found in the config file for the swh 'module'",
    default=False,
    is_flag=True,
)
@click.option(
    "-p",
    "--module-is-path",
    "module_is_path",
    help="If set, inpterpret the given 'module' as a config 'json path' within the config file",
    default=False,
    is_flag=True,
)
@click.pass_context
def db_init_admin(
    ctx, module: str, dbname: Optional[str], initialize_all: bool, module_is_path: bool
) -> None:
    """Execute superuser-level initialization steps (e.g pg extensions, admin functions,
    ...)

    Note: this command requires a postgresql connection with superuser permissions (e.g
    postgres, swh-admin, ...)

    If given, a db connection string will be used to connect to the database
    and execute the initialization steps for the given module.

    Example::

        PGPASSWORD=... swh db init-admin -d swh-test scheduler

    If you want to specify non-default postgresql connection parameters, you can
    provide them using standard environment variables or by the mean of a
    properly crafted libpq connection URI. See psql(1) man page (section
    ENVIRONMENTS) for details.

    Examples::

        \b
        PGPORT=5434 swh db init-admin scheduler
        swh db init-admin -d postgresql://superuser:passwd@pghost:5433/swh-scheduler \
          scheduler

    If the db connection string is not given, it will be looked for in the
    configuration file. Note that this step needs admin right on the database,
    so this usage is not meant for production environment (but rather test
    environments.)

    Example::

        \b
        $ cat conf.yml
        storage:
          cls: postgresql
          db: postgresql://user:passwd@pghost:5433/swh-storage
          objstorage:
            cls: memory

        \b
        $ swh db -C conf.yml init-admin storage

    With '-p', the module is interpreted as a 'path' in the configuration file
    where the configuration entry for the targeted database connection string
    can be found. For example::

        \b
        $ cat conf.yml
        storage:
          cls: pipeline
          steps:
            - cls: masking
              db: postgresql:///?service=swh-masking-proxy
            - cls: buffer
            - cls: postgresql
              db: postgresql://user:passwd@pghost:5433/swh-storage
              objstorage:
                cls: memory

        \b
        $ swh db -C conf.yml init-admin -p storage.steps.2

    The --all option allows to execute superuser-level
    initialization steps for all the datasabases found in the config file for
    the <module>. For example::

        \b
        $ cat conf.yml
        storage:
          cls: pipeline
          steps:
            - cls: masking
              db: postgresql:///?service=swh-masking-proxy
            - cls: buffer
            - cls: postgresql
              db: postgresql://user:passwd@pghost:5433/swh-storage
              objstorage:
                cls: memory

        \b
        $ swh db -C conf.yml init-admin -a storage

    will run the superuser-level init for both the masking and main storage
    declared in the 'storage' section of the config file.

    """
    from swh.core.db.db_utils import init_admin_extensions

    args = handle_cmd_args(
        cfg=ctx.obj["config"],
        module=module,
        do_all=initialize_all,
        dbname=dbname,
        is_path=module_is_path,
    )
    for cfgname, package, cls, fullmodule, _, dbname, cfg in args:
        dbmodule = f"{package}:{cls}"
        logger.debug(
            "db_init_admin %s:%s (actually %s) dbname=%s",
            package,
            cls,
            dbmodule,
            dbname,
        )
        try:
            init_admin_extensions(dbmodule, dbname)
            click.echo(f"{package}:{cls} Database {dbname} initialized (admin)")
        except CalledProcessError as exc:
            click.secho("Error during database setup", fg="red", bold=True)
            click.echo(str(exc))
            click.echo("Process output:")
            click.echo(exc.stderr)
            raise click.Abort()


@db.command(name="list", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=False)
@click.pass_context
def db_list(ctx, module):
    """List found DB configs under the <module> in the config file"""
    from swh.core.config import list_db_config_entries

    cfg = ctx.obj["config"]
    for cfgname, swhmod, cls, path, dbcfg, db in list_db_config_entries(cfg):
        if module and module != swhmod:
            continue
        print(f"{path} {swhmod}:{cls} {db}")


@db.command(name="init", context_settings=CONTEXT_SETTINGS)
@click.argument("module", metavar="MODULE-OR-CONFIG-PATH", required=True)
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
    "-a",
    "--all",
    "initialize_all",
    help="initialize all db found in the config file for the swh 'module'",
    default=False,
    is_flag=True,
)
@click.option(
    "-p",
    "--module-is-path",
    "module_is_path",
    help="If set, inpterpret the given 'module' as a config 'json path' within the config file",
    default=False,
    is_flag=True,
)
@click.pass_context
def db_init(ctx, module, dbname, flavor, initialize_all, module_is_path):
    """Initialize a database for the Software Heritage <module>.

    As for the 'init-admin' command, the database connection string can come
    either from the --dbname option or the configuration file (see option
    ``--config-file`` in ``swh db --help``) in the section named after the
    MODULE argument in most cases.

    When retrieved from within the configuration, the db connection string can
    be looked after from any location in the configuration using the
    <module-or-config-path> option. For example::

        \b
        $ cat conf.yml
        storage:
          cls: pipeline
          steps:
            - cls: masking
              db: postgresql:///?service=swh-masking-proxy
            - cls: buffer
            - cls: postgresql
              db: postgresql://user:passwd@pghost:5433/swh-storage
              objstorage:
                cls: memory

        \b
        $ swh db -C conf.yml init storage  # or
        $ SWH_CONFIG_FILENAME=conf.yml swh db init storage # or
        $ swh db init --dbname postgresql://user:passwd@pghost:5433/swh-storage storage

        $ # to initialize the "main" storage db (expected to be the last element
        $ # of a pipeline config),
        $ # or to initialize the masking_db:
        $ swh db -C conf.yml init -p storage.steps.0

    Usage of --module-config-key is now deprecated in favor of "full-path"
    module/config entry.

    """
    import gc

    # TODO: sanity check all the incompatible options...
    # XXX it probably does not make much sense to have a non-None flavor when
    # initializing several db at once... this case should raise an error

    args = handle_cmd_args(
        cfg=ctx.obj["config"],
        module=module,
        do_all=initialize_all,
        dbname=dbname,
        is_path=module_is_path,
    )

    for cfgname, package, cls, fullmodule, backend_class, dbname, cfg in args:
        initialize_one(
            cfgname, package, cls, fullmodule, backend_class, flavor, dbname, cfg
        )

    # It seems to be required in some cases to prevent psycopg from complaining
    # that it cannot terminate pool worker in a timely manner...
    gc.collect()


def initialize_one(cfgname, package, cls, module, backend_class, flavor, dbname, cfg):
    from swh.core.config import import_swhmodule
    from swh.core.db.db_utils import (
        get_database_info,
        populate_database_for_package,
        swh_set_db_version,
    )

    logger.debug("Initializing database %s:%s (%s)", package, cls, cfgname)

    # identify the datastore version first, otherwise if we start populating
    # the database but the backend cannot be initialized to retrieve the
    # current version, then we are in a broken state where the database is
    # mostly set up but the version; which is then used to decide whether the
    # db is initialized or not for any subsequent 'swh db' command...
    datastore_factory = getattr(import_swhmodule(module), "get_datastore", None)
    if datastore_factory is None and backend_class is not None:

        def datastore_factory(cls, **cfg):
            return backend_class(**cfg)

    code_version = None
    if datastore_factory:
        # TODO: try not to instantiate the backend class; most of the time, the
        #       current_version will be a class attribute...
        datastore = datastore_factory(**cfg)
        if hasattr(datastore, "current_version"):
            code_version = datastore.current_version
            logger.debug(
                "Initializing database version %s from the %s datastore",
                code_version,
                module,
            )
    if code_version is None:
        click.secho(
            "Error: cannot identify the backend datastore version", fg="red", bold=True
        )
        raise click.Abort()

    logger.debug("db_init %s flavor=%s dbname=%s", module, flavor, dbname)
    dbmodule = f"{package}:{cls}"
    try:
        initialized, dbversion, dbflavor = populate_database_for_package(
            dbmodule, dbname, flavor
        )
    except CalledProcessError as exc:
        click.secho("Error during database setup", fg="red", bold=True)
        click.echo(str(exc))
        click.echo("Process output:")
        click.echo(exc.stderr)
        raise click.Abort()

    if dbversion is None:
        # db version needs to be populated; instantiate the data source to
        # retrieve the current (expected) db version
        swh_set_db_version(dbname, code_version, desc="DB initialization")

    dbversion = get_database_info(dbname)[1]
    if dbversion is None:
        click.secho(
            "ERROR: database for {} {}{} BUT db version could not be set".format(
                module,
                "initialized" if initialized else "exists",
                f" (flavor {dbflavor})" if dbflavor is not None else "",
            ),
            fg="red",
            bold=True,
        )
    else:
        fl = f"(flavor {dbflavor}) " if dbflavor is not None else ""
        if dbversion < code_version:
            fg = "yellow"
        else:
            fg = "green"
        click.secho(
            f"{package}:{cls} Database {'initialized' if initialized else 'exists'} "
            f"{fl}"
            f"at version {dbversion}",
            fg=fg,
            bold=True,
        )
        if dbversion < code_version:
            click.secho(
                f"{package}:{cls} Current version is {code_version}; migration required",
                fg="red",
            )

    if flavor is not None and dbflavor != flavor:
        click.secho(
            f"WARNING requested flavor '{flavor}' != recorded flavor '{dbflavor}'",
            fg="red",
            bold=True,
        )


@db.command(name="shell", context_settings=CONTEXT_SETTINGS)
@click.argument("module", required=True)
@click.argument("psqlargs", nargs=-1)
@click.option(
    "--dbname",
    "--db-name",
    "-d",
    help="Database name or connection URI.",
    default=None,
    show_default=False,
)
@click.pass_context
def db_shell(ctx, module, psqlargs, dbname):
    """A subcommand to ease starting a psql shell using swh module configuration file.
    This may be useful for extra troubleshooting session when the other 'swh db' clis
    are not enough.

    Any addition argument (PSQLARGS) will be passed to the psql command. To
    pass options to psql, put them after a --, e.g.

      swh db shell vulns -- -c 'select * from dbversion'

    """

    from os import execlp

    if dbname is None:
        # use the db cnx from the config file; the expected config entry is either the given
        # module name
        cfg = ctx.obj["config"].get(module, {})
        dbname, cfg = get_dburl_from_config(module, cfg)

    if not dbname:
        raise click.BadParameter(
            "Missing the postgresql connection configuration. Either fix your "
            "configuration file or use the [-d|--dbname|--db-name] option."
        )

    dbname_censored = " ".join(
        elem for elem in dbname.split() if not elem.startswith("password=")
    )
    logger.info("Opening database shell for %r", dbname_censored)

    execlp("psql", dbname, *psqlargs)


@db.command(name="version", context_settings=CONTEXT_SETTINGS)
@click.argument("module", metavar="MODULE-OR-CONFIG-PATH", required=False)
@click.option(
    "--history",
    "show_history",
    help="Show version history.",
    default=False,
    is_flag=True,
)
@click.option(
    "-a",
    "--all",
    "all_backends",
    help="show version for all db found in the config file for the swh 'module'",
    default=False,
    is_flag=True,
)
@click.option(
    "-p",
    "--module-is-path",
    "module_is_path",
    help="If set, inpterpret the given 'module' as a config 'json path' within the config file",
    default=False,
    is_flag=True,
)
@click.pass_context
def db_version(ctx, module, show_history, all_backends, module_is_path):
    """Print the database version for the Software Heritage.

    Example::

        \b
        swh db version -d swh-test
        swh db version scheduler
        swh db version scrubber:scrubber_db
        swh db version --all scrubber

    """
    from swh.core.config import (
        get_swh_backend_module,
        import_swhmodule,
        list_db_config_entries,
    )
    from swh.core.db.db_utils import get_database_info, swh_db_versions

    if not module and all_backends:
        backends = (
            (cfgname, pkg, cls, path, get_swh_backend_module(pkg, cls)[1], cnxstr, cfg)
            for (cfgname, pkg, cls, path, cfg, cnxstr) in list_db_config_entries(
                ctx.obj["config"]
            )
        )
    else:
        backends = handle_cmd_args(
            cfg=ctx.obj["config"],
            module=module,
            do_all=all_backends,
            is_path=module_is_path,
        )

    for cfgname, package, cls, _, backend_class, cnxstr, cfg in backends:
        click.echo("")
        db_module, db_version, db_flavor = get_database_info(cnxstr)
        if db_module is None:
            click.secho(
                f"WARNING the database for {package}:{cls} does not have a dbmodule table.",
                fg="red",
                bold=True,
            )
            db_module = f"{package}:{cls}"

        click.secho(f"module: {db_module}", fg="green", bold=True)
        if ":" not in db_module:
            click.secho(
                f"The module registered in the database ({db_module}) needs to be updated;"
                "\nYou should run 'swh db upgrade'",
                fg="yellow",
                bold=True,
            )
        if db_flavor is not None:
            click.secho(f"flavor: {db_flavor}", fg="green", bold=True)

        code_version: int | None = None
        if hasattr(backend_class, "current_version"):
            code_version = backend_class.current_version
        else:
            # instantiate the data source to retrieve the current (expected) db version
            datastore_factory = getattr(
                import_swhmodule(db_module), "get_datastore", None
            )
            if not datastore_factory and backend_class is not None:

                def datastore_factory(cls, **cfg):
                    return backend_class(**cfg)

            if datastore_factory:
                datastore = datastore_factory(**cfg)
                code_version = datastore.current_version

        if code_version is not None:
            click.secho(
                f"current code version: {code_version}",
                fg="green" if code_version == db_version else "red",
                bold=True,
            )

        if not show_history:
            click.secho(f"version: {db_version}", fg="green", bold=True)
        else:
            versions = swh_db_versions(cnxstr)
            for version, tstamp, desc in versions:
                click.echo(f"{version} [{tstamp}] {desc}")


@db.command(name="upgrade", context_settings=CONTEXT_SETTINGS)
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
    "-a",
    "--all",
    "upgrade_all",
    help="upgrade all db found in the config file for the swh 'module'",
    default=False,
    is_flag=True,
)
@click.option(
    "-p",
    "--module-is-path",
    "module_is_path",
    help="If set, inpterpret the given 'module' as a config 'json path' within the config file",
    default=False,
    is_flag=True,
)
@click.pass_context
def db_upgrade(
    ctx,
    module,
    dbname,
    to_version,
    interactive,
    upgrade_all,
    module_is_path,
):
    """Upgrade the database for given module (to a given version if specified).

    Examples::

        \b
        swh db upgrade storage
        swh db upgrade scheduler --to-version=10
        swh db upgrade scrubber:scrubber_db --to-version=10

    """
    from swh.core.config import import_swhmodule
    from swh.core.db.db_utils import (
        get_database_info,
        swh_db_upgrade,
        swh_set_db_module,
    )

    # TODO: check options consistency

    backends = handle_cmd_args(
        cfg=ctx.obj["config"],
        module=module,
        do_all=upgrade_all,
        dbname=dbname,
        is_path=module_is_path,
    )
    for cfgname, package, cls, fullmodule, backend_class, dbname, cfg in backends:
        logger.debug("db_version dbname=%s", dbname)
        go_to_version = to_version
        db_module, db_version, db_flavor = get_database_info(dbname)
        backend = f"{package}:{cls}"
        if db_module is None:
            click.secho(
                "Warning: the database does not have a dbmodule table.",
                fg="yellow",
                bold=True,
            )
            if interactive and not click.confirm(
                f"Write the module information ({backend}) in the database?",
                default=True,
            ):
                raise click.BadParameter("Migration aborted.")
        if db_module is None or (db_module != backend and ":" not in db_module):
            # module stored in the db needs updating
            swh_set_db_module(dbname, backend)
            click.secho(
                "The module registered in the database has been updated "
                f"from '{db_module}' to '{backend}'",
                fg="red",
                bold=True,
            )
            db_module, db_version, db_flavor = get_database_info(dbname)

        if db_module != backend:
            raise click.BadParameter(
                f"Error: the given module ({module}) does not match the value "
                f"stored in the database ({db_module})."
            )

        ds_version: int | None = None
        if hasattr(backend_class, "current_version"):
            ds_version = backend_class.current_version
        else:
            # instantiate the data source to retrieve the current (expected) db version
            datastore_factory = getattr(
                import_swhmodule(fullmodule), "get_datastore", None
            )

            if datastore_factory is None and backend_class is not None:

                def datastore_factory(cls, **cfg):
                    return backend_class(**cfg)

            if not datastore_factory:
                raise click.UsageError(
                    "You cannot use this command on old-style datastore backend {db_module}"
                )
            datastore = datastore_factory(**cfg)
            ds_version = datastore.current_version

        assert ds_version is not None

        if go_to_version is None:
            go_to_version = ds_version
        if go_to_version > ds_version:
            raise click.UsageError(
                f"The target version {go_to_version} is larger than the current version "
                f"{ds_version} of the datastore backend {db_module}"
            )

        if go_to_version == db_version:
            click.secho(
                f"No migration needed for '{backend}': the current version is {db_version}",
                fg="yellow",
            )
        else:
            new_db_version = swh_db_upgrade(dbname, backend, go_to_version)
            click.secho(f"Migration to version {new_db_version} done", fg="green")
            if new_db_version < ds_version:
                click.secho(
                    "Warning: migration was not complete: "
                    f"the current version is {ds_version}",
                    fg="yellow",
                )


def get_dburl_from_config(module, cfg):
    if cfg.get("cls") == "pipeline":
        # We know the database itself will always
        # come last in a pipeline configuration.
        # TODO: get rid of this implicit behavior
        cfg = cfg["steps"][-1]
    if cfg.get("cls") != "postgresql":
        raise click.BadParameter(
            "Configuration cls must be set to 'postgresql' for this command."
        )
    if "args" in cfg:
        # for bw compat
        cfg = cfg["args"]
    return cfg.get("db"), cfg


def get_dburl_from_config_key(cfg, path):
    # the first section level in the config may contain dotted keys, so deal
    # with it

    swhmod = None
    for key in cfg:
        if path.startswith(f"{key}."):
            swhmod = key
            cfgpath = path[len(key) + 1 :].split(".")
            break
        if key == path:
            swhmod = key
            cfgpath = []
            break

    if not swhmod:
        raise ValueError(f"Module configuration key for <{path}> was not found!")

    cfg = cfg[swhmod]

    for key_e in cfgpath:
        if isinstance(cfg, list):
            cfg = cfg[int(key_e)]
        else:
            cfg = cfg[key_e]

    assert isinstance(cfg, dict)
    if cnxstr := cfg.get("db"):
        return swhmod, cfg, cnxstr
    else:
        raise ValueError(
            f"no database connection string found in the configuration at {path}"
        )


def handle_cmd_args(
    cfg: Dict[str, Any],
    module: str,
    is_path: bool = False,
    do_all: bool = False,
    dbname: Optional[str] = None,
) -> List[Tuple[str, str, str, str, Optional[type], str, Dict[str, Any]]]:
    """Helper function to build the list of backends to handle in a cli command

    For each identified backend, returns a tuple:
      (package, module, backend_class, cnxstr, cfg)
    where:
      - `package`: the (swh) package this backend is implemented in (e.g.
        'storage', 'scheduler' etc.)
      - `module`: the (full path) module in which the backend class can be found
        (e.g. 'swh.storage.postgresql.storage')
      - `backend_class`: the class implementing the backend,
      - `cnxstr`: the (libpq) connection string to the database,
      - `cfg`: the config structure in which this backend is configured; if the
        backend is defined in a nested configuration, this is only the most
        specific config structure for this backend.

    For each backend, the code implementing the backend class needs to be
    registered in the `swh.<package>.classes` entry point under the
    `cfg["cls"]` name. However, there is bw compatibility for swh packages not
    yet updated to register these backends in the entry points.

    If `dbname` is given, return only one element in the list, with a config made
    of {'cls': 'postgresql', 'db': dbname}.

    If `do_all` is True, look for every backend in the configuration (cfg) under
    the section `module`.

    If `module` is a simple word ('storage', 'scheduler', etc.), look for the
    last db backend found in the config file under the `module` section.

    If `is_path` is True, interpret the 'module' as the path to the config entry
    to use in the config file.

    If `module` is an actual module path (e.g. 'storage.proxies.masking'), then
    `dbname` must be given and the configuration is not looked for in the
    config file.

    Note: this rather complex logic will be simplified when all the swh
    packages are updated and do not need bw compat handling code any more.
    """
    from swh.core.config import get_swh_backend_module, list_db_config_entries

    if is_path and do_all:
        raise ValueError("Cannot use both 'all' and a specific config target")
    if is_path and ":" in module:
        raise ValueError(
            "Cannot use both 'all' with a qualified package name "
            f"<module>:<cls> (here: {module})"
        )
    cls: str | None = None
    if dbname:
        if ":" in module:
            module, cls = module.split(":", 1)
        else:
            cls = "postgresql"
        # config file is not used at all here
        dbcfg = {"cls": cls, "db": dbname}
        fullmodule, backend_class = get_swh_backend_module(swh_package=module, cls=cls)
        return [("", module, cls, fullmodule, backend_class, dbname, dbcfg)]

    if is_path:
        config_entries = [
            entry for entry in list_db_config_entries(cfg) if entry[3] == module
        ]
    else:
        if ":" in module:
            module, cls = module.split(":", 1)
        else:
            cls = None
        config_entries = [
            entry
            for entry in list_db_config_entries(cfg)
            if entry[0] == module and (cls is None or entry[2] == cls)
        ]

    if not do_all:
        config_entries = config_entries[-1:]

    backends = []
    for cfgmod, pkg, cls, path, dbcfg, cnxstr in config_entries:
        pkg = dbcfg.get("pkg", cfgmod)
        fullmodule, backend_class = get_swh_backend_module(swh_package=pkg, cls=cls)
        backends.append((cfgmod, pkg, cls, fullmodule, backend_class, cnxstr, dbcfg))
    return backends
