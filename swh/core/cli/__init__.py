# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import pkg_resources

LOG_LEVEL_NAMES = ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

logger = logging.getLogger(__name__)


class AliasedGroup(click.Group):
    'A simple Group that supports command aliases'

    @property
    def aliases(self):
        if not hasattr(self, '_aliases'):
            self._aliases = {}
        return self._aliases

    def get_command(self, ctx, cmd_name):
        return super().get_command(ctx, self.aliases.get(cmd_name, cmd_name))

    def add_alias(self, name, alias):
        if not isinstance(name, str):
            name = name.name
        self.aliases[alias] = name


@click.group(context_settings=CONTEXT_SETTINGS, cls=AliasedGroup)
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(LOG_LEVEL_NAMES),
              help="Log level (default to INFO)")
@click.pass_context
def swh(ctx, log_level):
    """Command line interface for Software Heritage.
    """
    log_level = logging.getLevelName(log_level)
    logging.root.setLevel(log_level)
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level


def main():
    logging.basicConfig()
    # load plugins that define cli sub commands
    for entry_point in pkg_resources.iter_entry_points('swh.cli.subcommands'):
        cmd = entry_point.load()
        swh.add_command(cmd, name=entry_point.name)

    return swh(auto_envvar_prefix='SWH')


if __name__ == '__main__':
    main()
