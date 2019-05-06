# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import logging
import pkg_resources

logger = logging.getLogger(__name__)


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(logging._nameToLevel.keys()),
              help="Log level (default to INFO)")
@click.pass_context
def swh(ctx, log_level):
    """Software Heritage Tool
    """
    logger.setLevel(log_level)
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = logging._nameToLevel[log_level]


def main():
    logging.basicConfig()
    # load plugins that define cli sub commands
    for entry_point in pkg_resources.iter_entry_points('swh.cli.subcommands'):
        cmd = entry_point.load()
        swh.add_command(cmd, name=entry_point.name)

    return swh(auto_envvar_prefix='SWH')


if __name__ == '__main__':
    main()
