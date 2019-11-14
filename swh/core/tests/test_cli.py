#

import logging
import textwrap

import click
from click.testing import CliRunner
import pytest

from swh.core.cli import swh as swhmain


help_msg = '''Usage: swh [OPTIONS] COMMAND [ARGS]...

  Command line interface for Software Heritage.

Options:
  -l, --log-level [NOTSET|DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Log level (defaults to INFO).
  --log-config FILENAME           Python yaml logging configuration file.
  -h, --help                      Show this message and exit.

Notes:
  If both options are present, --log-level will override the root logger
  configuration set in --log-config.

  The --log-config YAML must conform to the logging.config.dictConfig schema
  documented at https://docs.python.org/3/library/logging.config.html.
'''


def test_swh_help():
    runner = CliRunner()
    result = runner.invoke(swhmain, ['-h'])
    assert result.exit_code == 0
    assert result.output.startswith(help_msg)

    result = runner.invoke(swhmain, ['--help'])
    assert result.exit_code == 0
    assert result.output.startswith(help_msg)


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


@pytest.fixture
def log_config_path(tmp_path):
    log_config = textwrap.dedent('''\
    ---
    version: 1
    formatters:
      formatter:
        format: 'custom format:%(name)s:%(levelname)s:%(message)s'
    handlers:
      console:
        class: logging.StreamHandler
        stream: ext://sys.stdout
        formatter: formatter
        level: DEBUG
    root:
      level: DEBUG
      handlers:
        - console
    loggers:
      dontshowdebug:
        level: INFO
    ''')

    (tmp_path / 'log_config.yml').write_text(log_config)

    yield str(tmp_path / 'log_config.yml')


def test_log_config(caplog, log_config_path):
    @swhmain.command(name='test')
    @click.pass_context
    def swhtest(ctx):
        logging.debug('Root log debug')
        logging.info('Root log info')
        logging.getLogger('dontshowdebug').debug('Not shown')
        logging.getLogger('dontshowdebug').info('Shown')

    runner = CliRunner()
    result = runner.invoke(
        swhmain, [
            '--log-config', log_config_path,
            'test',
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == '\n'.join([
        'custom format:root:DEBUG:Root log debug',
        'custom format:root:INFO:Root log info',
        'custom format:dontshowdebug:INFO:Shown',
    ])


def test_log_config_log_level_interaction(caplog, log_config_path):
    @swhmain.command(name='test')
    @click.pass_context
    def swhtest(ctx):
        logging.debug('Root log debug')
        logging.info('Root log info')
        logging.getLogger('dontshowdebug').debug('Not shown')
        logging.getLogger('dontshowdebug').info('Shown')

    runner = CliRunner()
    result = runner.invoke(
        swhmain, [
            '--log-config', log_config_path,
            '--log-level', 'INFO',
            'test',
        ],
    )

    assert result.exit_code == 0
    assert result.output.strip() == '\n'.join([
        'custom format:root:INFO:Root log info',
        'custom format:dontshowdebug:INFO:Shown',
    ])


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
