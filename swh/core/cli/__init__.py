# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import logging.config
import signal

import click
import pkg_resources
import yaml

from ..sentry import init_sentry

LOG_LEVEL_NAMES = ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

logger = logging.getLogger(__name__)


class AliasedGroup(click.Group):
    '''A simple Group that supports command aliases, as well as notes related to
    options'''

    def __init__(self, name=None, commands=None, **attrs):
        self.option_notes = attrs.pop('option_notes', None)
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
            with formatter.section('Notes'):
                formatter.write_text(self.option_notes)
        self.format_commands(ctx, formatter)


def clean_exit_on_signal(signal, frame):
    """Raise a SystemExit exception to let command-line clients wind themselves
    down on exit"""
    raise SystemExit(0)


@click.group(
    context_settings=CONTEXT_SETTINGS, cls=AliasedGroup,
    option_notes='''\
If both options are present, --log-level will override the root logger
configuration set in --log-config.

The --log-config YAML must conform to the logging.config.dictConfig schema
documented at https://docs.python.org/3/library/logging.config.html.
'''
)
@click.option('--log-level', '-l', default=None,
              type=click.Choice(LOG_LEVEL_NAMES),
              help="Log level (defaults to INFO).")
@click.option('--log-config', default=None,
              type=click.File('r'),
              help="Python yaml logging configuration file.")
@click.option('--sentry-dsn', default=None,
              help="DSN of the Sentry instance to report to")
@click.option('--sentry-debug/--no-sentry-debug',
              default=False, hidden=True,
              help="Enable debugging of sentry")
@click.pass_context
def swh(ctx, log_level, log_config, sentry_dsn, sentry_debug):
    """Command line interface for Software Heritage.
    """
    signal.signal(signal.SIGTERM, clean_exit_on_signal)
    signal.signal(signal.SIGINT, clean_exit_on_signal)

    init_sentry(sentry_dsn, debug=sentry_debug)

    if log_level is None and log_config is None:
        log_level = 'INFO'

    if log_config:
        logging.config.dictConfig(yaml.safe_load(log_config.read()))

    if log_level:
        log_level = logging.getLevelName(log_level)
        logging.root.setLevel(log_level)

    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level


def main():
    # Even though swh() sets up logging, we need an earlier basic logging setup
    # for the next few logging statements
    logging.basicConfig()
    # load plugins that define cli sub commands
    for entry_point in pkg_resources.iter_entry_points('swh.cli.subcommands'):
        try:
            cmd = entry_point.load()
            swh.add_command(cmd, name=entry_point.name)
        except Exception as e:
            logger.warning('Could not load subcommand %s: %s',
                           entry_point.name, str(e))

    return swh(auto_envvar_prefix='SWH')


if __name__ == '__main__':
    main()
