#

from click.testing import CliRunner

from swh.core.cli.db import db as swhdb


help_msg = '''Usage: swh [OPTIONS] COMMAND [ARGS]...

  Command line interface for Software Heritage.

Options:
  -l, --log-level [NOTSET|DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Log level (defaults to INFO).
  --log-config FILENAME           Python yaml logging configuration file.
  --sentry-dsn TEXT               DSN of the Sentry instance to report to
  -h, --help                      Show this message and exit.

Notes:
  If both options are present, --log-level will override the root logger
  configuration set in --log-config.

  The --log-config YAML must conform to the logging.config.dictConfig schema
  documented at https://docs.python.org/3/library/logging.config.html.

Commands:
  db  Software Heritage database generic tools.
'''


def test_swh_help(swhmain):
    swhmain.add_command(swhdb)
    runner = CliRunner()
    result = runner.invoke(swhmain, ['-h'])
    assert result.exit_code == 0
    assert result.output == help_msg


help_db_msg = '''Usage: swh db [OPTIONS] COMMAND [ARGS]...

  Software Heritage database generic tools.

Options:
  -C, --config-file FILE  Configuration file.
  -h, --help              Show this message and exit.

Commands:
  init  Initialize the database for every Software Heritage module found in...
'''


def test_swh_db_help(swhmain):
    swhmain.add_command(swhdb)
    runner = CliRunner()
    result = runner.invoke(swhmain, ['db', '-h'])
    assert result.exit_code == 0
    assert result.output == help_db_msg
