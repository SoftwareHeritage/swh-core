# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import warnings

from backports.entry_points_selectable import entry_points
import click

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

logger = logging.getLogger(__name__)


class SWHHelpFormatter(click.HelpFormatter):
    """A subclass of click's HelpFormatter which converts some :ref: to links
    to the SWH documentation
    """

    _template = (
        "{key} key: https://docs.softwareheritage.org/devel/configuration.html#{ref}"
    )

    def write_text(self, text):
        import re

        text = re.sub(
            ":ref:`(?P<ref>cli-config-(?P<key>.*?))`",
            lambda m: self._template.format(**m.groupdict()),
            text,
        )
        text = re.sub(
            ":mod:`(.*?)`",
            r"https://docs.softwareheritage.org/devel/apidoc/\1.html",
            text,
        )
        return super().write_text(text)


class AliasedGroup(click.Group):
    """A simple Group that supports:

    * command aliases
    * notes related to options

    """

    def __init__(self, name=None, commands=None, **attrs):
        self.option_notes = attrs.pop("option_notes", None)
        self.aliases = {}
        super().__init__(name, commands, **attrs)

    def get_command(self, ctx, cmd_name):
        return super().get_command(ctx, self.aliases.get(cmd_name, cmd_name))

    def add_alias(self, name, alias):
        if not isinstance(name, str):
            name = name.name
        self.aliases[alias] = name

    def format_options(self, ctx, formatter):
        click.Command.format_options(self, ctx, formatter)
        if self.option_notes:
            with formatter.section("Notes"):
                formatter.write_text(self.option_notes)
        self.format_commands(ctx, formatter)


def clean_exit_on_signal(signal, frame):
    """Raise a SystemExit exception to let command-line clients wind themselves
    down on exit"""
    raise SystemExit(0)


def validate_loglevel_params(ctx, param, values):
    """Validate the --log-level parameters, with multiple values."""
    if values is None:
        return None

    from swh.core.logging import BadLogLevel, validate_loglevel

    try:
        module_log_levels = [validate_loglevel(value) for value in values]
    except BadLogLevel as e:
        raise click.BadParameter(e)
    return module_log_levels


@click.group(
    context_settings=CONTEXT_SETTINGS,
    cls=AliasedGroup,
    option_notes="""\
If both options are present, --log-level values will override the configuration
in --log-config.

The --log-config YAML must conform to the logging.config.dictConfig schema
documented at https://docs.python.org/3/library/logging.config.html.
""",
)
@click.option(
    "--log-level",
    "-l",
    "log_levels",
    envvar="SWH_LOG_LEVEL",
    default=None,
    callback=validate_loglevel_params,
    multiple=True,
    help=(
        "Log level (defaults to INFO). "
        "Can override the log level for a specific module, by using the "
        "``specific.module:LOGLEVEL`` syntax (e.g. ``--log-level swh.core:DEBUG`` "
        "will enable DEBUG logging for swh.core)."
    ),
)
@click.option(
    "--log-config",
    default=None,
    type=click.Path(exists=True, readable=True),
    help="Python yaml logging configuration file.",
)
@click.option(
    "--sentry-dsn", default=None, help="DSN of the Sentry instance to report to"
)
@click.option(
    "--sentry-debug/--no-sentry-debug",
    default=False,
    hidden=True,
    help="Enable debugging of sentry",
)
@click.pass_context
def swh(ctx, log_levels, log_config, sentry_dsn, sentry_debug):
    """Command line interface for Software Heritage."""
    import signal

    from swh.core.logging import logging_configure

    from ..sentry import init_sentry

    signal.signal(signal.SIGTERM, clean_exit_on_signal)
    signal.signal(signal.SIGINT, clean_exit_on_signal)

    init_sentry(sentry_dsn=sentry_dsn, debug=sentry_debug)

    set_default_loglevel = logging_configure(log_levels, log_config)

    ctx.ensure_object(dict)
    ctx.obj["log_level"] = set_default_loglevel
    ctx.__class__.formatter_class = SWHHelpFormatter


def main():
    # Even though swh() sets up logging, we need an earlier basic logging setup
    # for the next few logging statements
    logging.basicConfig()
    # load plugins that define cli sub commands
    for entry_point in entry_points(group="swh.cli.subcommands"):
        try:
            cmd = entry_point.load()
            if isinstance(cmd, click.BaseCommand):
                # for BW compat, auto add click commands
                warnings.warn(
                    f"{entry_point.name}: automagic addition of click commands "
                    f"to the main swh group is deprecated",
                    DeprecationWarning,
                )
                swh.add_command(cmd, name=entry_point.name)
            # otherwise it's expected to be a module which has been loaded
            # it's the responsibility of the click commands/groups in this
            # module to transitively have the main swh group as parent.
        except Exception as e:
            logger.warning("Could not load subcommand %s: %r", entry_point.name, e)

    return swh(auto_envvar_prefix="SWH")


if __name__ == "__main__":
    main()
