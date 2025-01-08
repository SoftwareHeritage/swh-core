# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from importlib.metadata import distribution
import logging
import textwrap
import traceback
from typing import List

import click
from click.testing import CliRunner
import pytest
import yaml

help_msg_snippets = (
    (
        "Usage",
        (
            "swh [OPTIONS] COMMAND [ARGS]...",
            "Command line interface for Software Heritage.",
        ),
    ),
    (
        "Options",
        (
            "-l, --log-level",
            "--log-config",
            "--sentry-dsn",
            "-h, --help",
        ),
    ),
)


def assert_result(result):
    if result.exception:
        assert result.exit_code == 0, (
            "Unexpected exception: "
            f"{''.join(traceback.format_tb(result.exc_info[2]))}"
            f"\noutput: {result.output}"
        )
    else:
        assert result.exit_code == 0, f"Unexpected output: {result.output}"


def get_section(cli_output: str, section: str) -> List[str]:
    """Get the given `section` of the `cli_output`"""
    result = []
    in_section = False
    for line in cli_output.splitlines():
        if not line:
            continue

        if in_section:
            if not line.startswith(" "):
                break
        else:
            if line.startswith(section):
                in_section = True

        if in_section:
            result.append(line)

    return result


def assert_section_contains(cli_output: str, section: str, snippet: str) -> bool:
    """Check that a given `section` of the `cli_output` contains the given `snippet`"""
    section_lines = get_section(cli_output, section)
    assert section_lines, "Section %s not found in output %r" % (section, cli_output)

    for line in section_lines:
        if snippet in line:
            return True
    else:
        assert False, "%r not found in section %r of output %r" % (
            snippet,
            section,
            cli_output,
        )


@pytest.fixture(autouse=True)
def reset_root_loglevel():
    root_level = logging.root.level
    yield
    logging.root.setLevel(root_level)


@pytest.fixture
def patched_dictconfig(mocker):
    yield mocker.patch("swh.core.logging.dictConfig", autospec=True)


def test_swh_help(swhmain):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["-h"])
    assert_result(result)
    for section, snippets in help_msg_snippets:
        for snippet in snippets:
            assert_section_contains(result.output, section, snippet)

    result = runner.invoke(swhmain, ["--help"])
    assert_result(result)
    for section, snippets in help_msg_snippets:
        for snippet in snippets:
            assert_section_contains(result.output, section, snippet)


def test_command(swhmain, mocker):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    result = runner.invoke(swhmain, ["test"])
    sentry_sdk_init.assert_called_once_with(
        dsn=None,
        debug=False,
        integrations=[],
        release="0.0.0",
        environment=None,
        traces_sample_rate=None,
    )
    assert_result(result)
    assert result.output.strip() == "Hello SWH!"


def test_loglevel_default(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 20
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["test"])
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""


def test_loglevel_error(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 40
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["-l", "ERROR", "test"])
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""


def test_loglevel_debug(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 10
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["-l", "DEBUG", "test"])
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""


def test_sentry(swhmain, mocker):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    result = runner.invoke(swhmain, ["--sentry-dsn", "test_dsn", "test"])
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        debug=False,
        integrations=[],
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_sentry_debug(swhmain, mocker):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    result = runner.invoke(
        swhmain, ["--sentry-dsn", "test_dsn", "--sentry-debug", "test"]
    )
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        debug=True,
        integrations=[],
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_sentry_env(swhmain, mocker):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    env = {
        "SWH_SENTRY_DSN": "test_dsn",
        "SWH_SENTRY_DEBUG": "1",
    }
    result = runner.invoke(swhmain, ["test"], env=env, auto_envvar_prefix="SWH")
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        debug=True,
        integrations=[],
        release=None,
        environment=None,
        traces_sample_rate=None,
    )


def test_sentry_env_main_package(swhmain, mocker):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    sentry_sdk_init = mocker.patch("sentry_sdk.init")
    env = {
        "SWH_SENTRY_DSN": "test_dsn",
        "SWH_MAIN_PACKAGE": "swh.core",
        "SWH_SENTRY_ENVIRONMENT": "tests",
    }
    result = runner.invoke(swhmain, ["test"], env=env, auto_envvar_prefix="SWH")
    assert_result(result)

    version = distribution("swh.core").version

    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        debug=False,
        integrations=[],
        release="swh.core@" + version,
        environment="tests",
        traces_sample_rate=None,
    )


@pytest.fixture
def log_config_path(tmp_path):
    log_config = textwrap.dedent(
        """\
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
    """
    )

    (tmp_path / "log_config.yml").write_text(log_config)

    yield str(tmp_path / "log_config.yml")


def test_log_config(log_config_path, swhmain, patched_dictconfig):
    """Check that --log-config properly calls :func:`logging.config.dictConfig`."""

    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        patched_dictconfig.assert_called_once_with(
            yaml.safe_load(open(log_config_path, "r"))
        )

        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(
        swhmain,
        [
            "--log-config",
            log_config_path,
            "test",
        ],
    )

    assert_result(result)
    assert result.output.strip() == "Hello SWH!"


def test_log_config_log_level_interaction(log_config_path, swhmain, patched_dictconfig):
    """Check that --log-config and --log-level work properly together, by calling
    :func:`logging.config.dictConfig` then setting the loglevel of the root logger."""

    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == logging.DEBUG
        patched_dictconfig.assert_called_once_with(
            yaml.safe_load(open(log_config_path, "r"))
        )

        click.echo("Hello SWH!")

    assert logging.root.level != logging.DEBUG

    runner = CliRunner()
    result = runner.invoke(
        swhmain,
        [
            "--log-config",
            log_config_path,
            "--log-level",
            "DEBUG",
            "test",
        ],
    )

    assert_result(result)
    assert result.output.strip() == "Hello SWH!"


def test_multiple_log_level_behavior(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.getLevelName(logging.root.level) == "DEBUG"
        assert logging.getLevelName(logging.getLogger("dontshowdebug").level) == "INFO"
        return 0

    runner = CliRunner()
    result = runner.invoke(
        swhmain,
        [
            "--log-level",
            "DEBUG",
            "--log-level",
            "dontshowdebug:INFO",
            "test",
        ],
    )

    assert_result(result)


def test_invalid_log_level(swhmain):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["--log-level", "broken:broken:DEBUG"])

    assert result.exit_code != 0
    assert "Invalid log level specification" in result.output

    runner = CliRunner()
    result = runner.invoke(swhmain, ["--log-level", "UNKNOWN"])

    assert result.exit_code != 0
    assert "Log level UNKNOWN unknown" in result.output


def test_aliased_command(swhmain):
    @swhmain.command(name="canonical-test")
    @click.pass_context
    def swhtest(ctx):
        "A test command."
        click.echo("Hello SWH!")

    swhmain.add_alias(swhtest, "othername")

    runner = CliRunner()

    # check we have only 'canonical-test' listed in the usage help msg
    result = runner.invoke(swhmain, ["-h"])
    assert_result(result)
    assert "canonical-test  A test command." in result.output
    assert "othername" not in result.output

    # check we can execute the cmd with 'canonical-test'
    result = runner.invoke(swhmain, ["canonical-test"])
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""

    # check we can also execute the cmd with the alias 'othername'
    result = runner.invoke(swhmain, ["othername"])
    assert_result(result)
    assert result.output.strip() == """Hello SWH!"""


def test_documentation(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        """Does a thing

        This needs the following config:

        \b
        * :ref:`cli-config-storage`
        * :ref:`cli-config-scheduler`

        and calls :mod:`swh.core.test.test_cli`
        """
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["test", "--help"])
    assert_result(result)
    assert result.output == textwrap.dedent(
        """\
        Usage: swh test [OPTIONS]

          Does a thing

          This needs the following config:

          * storage key: https://docs.softwareheritage.org/devel/configuration.html#cli-config-storage
          * scheduler key: https://docs.softwareheritage.org/devel/configuration.html#cli-config-scheduler

          and calls
          https://docs.softwareheritage.org/devel/apidoc/swh.core.test.test_cli.html

        Options:
          -h, --help  Show this message and exit.
        """  # noqa
    )
