#

import logging

import click
from click.testing import CliRunner

from swh.core.cli import swh as swhmain


help_msg = '''Usage: swh [OPTIONS] COMMAND [ARGS]...

  Command line interface for Software Heritage.

Options:
  -l, --log-level [NOTSET|DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Log level (default to INFO)
  -h, --help                      Show this message and exit.
'''


def test_swh_help():
    runner = CliRunner()
    result = runner.invoke(swhmain, ['-h'])
    assert result.exit_code == 0
    assert result.output == help_msg

    result = runner.invoke(swhmain, ['--help'])
    assert result.exit_code == 0
    assert result.output == help_msg


def test_command():
    @swhmain.command(name='test')
    @click.pass_context
    def swhtest(ctx):
        click.echo('Hello SWH!')

    runner = CliRunner()
    result = runner.invoke(swhmain, ['test'])
    assert result.exit_code == 0
    assert result.output.strip() == 'Hello SWH!'


def test_loglevel_default(caplog):
    @swhmain.command(name='test')
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 20
        click.echo('Hello SWH!')

    runner = CliRunner()
    result = runner.invoke(swhmain, ['test'])
    assert result.exit_code == 0
    print(result.output)
    assert result.output.strip() == '''Hello SWH!'''


def test_loglevel_error(caplog):
    @swhmain.command(name='test')
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 40
        click.echo('Hello SWH!')

    runner = CliRunner()
    result = runner.invoke(swhmain, ['-l', 'ERROR', 'test'])
    assert result.exit_code == 0
    assert result.output.strip() == '''Hello SWH!'''


def test_loglevel_debug(caplog):
    @swhmain.command(name='test')
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 10
        click.echo('Hello SWH!')

    runner = CliRunner()
    result = runner.invoke(swhmain, ['-l', 'DEBUG', 'test'])
    assert result.exit_code == 0
    assert result.output.strip() == '''Hello SWH!'''


def test_aliased_command():
    @swhmain.command(name='canonical-test')
    @click.pass_context
    def swhtest(ctx):
        'A test command.'
        click.echo('Hello SWH!')
    swhmain.add_alias(swhtest, 'othername')

    runner = CliRunner()

    # check we have only 'canonical-test' listed in the usage help msg
    result = runner.invoke(swhmain, ['-h'])
    assert result.exit_code == 0
    assert 'canonical-test  A test command.' in result.output
    assert 'othername' not in result.output

    # check we can execute the cmd with 'canonical-test'
    result = runner.invoke(swhmain, ['canonical-test'])
    assert result.exit_code == 0
    assert result.output.strip() == '''Hello SWH!'''

    # check we can also execute the cmd with the alias 'othername'
    result = runner.invoke(swhmain, ['othername'])
    assert result.exit_code == 0
    assert result.output.strip() == '''Hello SWH!'''
