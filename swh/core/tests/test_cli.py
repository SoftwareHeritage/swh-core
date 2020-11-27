# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import textwrap
from typing import List
from unittest.mock import patch

import click
from click.testing import CliRunner
import pkg_resources
import pytest

help_msg_snippets = (
    (
        "Usage",
        (
            "swh [OPTIONS] COMMAND [ARGS]...",
            "Command line interface for Software Heritage.",
        ),
    ),
    ("Options", ("-l, --log-level", "--log-config", "--sentry-dsn", "-h, --help",)),
)


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


def test_swh_help(swhmain):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["-h"])
    assert result.exit_code == 0
    for section, snippets in help_msg_snippets:
        for snippet in snippets:
            assert_section_contains(result.output, section, snippet)

    result = runner.invoke(swhmain, ["--help"])
    assert result.exit_code == 0
    for section, snippets in help_msg_snippets:
        for snippet in snippets:
            assert_section_contains(result.output, section, snippet)


def test_command(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    with patch("sentry_sdk.init") as sentry_sdk_init:
        result = runner.invoke(swhmain, ["test"])
    sentry_sdk_init.assert_not_called()
    assert result.exit_code == 0
    assert result.output.strip() == "Hello SWH!"


def test_loglevel_default(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 20
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["test"])
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""


def test_loglevel_error(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 40
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["-l", "ERROR", "test"])
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""


def test_loglevel_debug(caplog, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.root.level == 10
        click.echo("Hello SWH!")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["-l", "DEBUG", "test"])
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""


def test_sentry(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    with patch("sentry_sdk.init") as sentry_sdk_init:
        result = runner.invoke(swhmain, ["--sentry-dsn", "test_dsn", "test"])
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn", debug=False, integrations=[], release=None, environment=None,
    )


def test_sentry_debug(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    with patch("sentry_sdk.init") as sentry_sdk_init:
        result = runner.invoke(
            swhmain, ["--sentry-dsn", "test_dsn", "--sentry-debug", "test"]
        )
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn", debug=True, integrations=[], release=None, environment=None,
    )


def test_sentry_env(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    with patch("sentry_sdk.init") as sentry_sdk_init:
        env = {
            "SWH_SENTRY_DSN": "test_dsn",
            "SWH_SENTRY_DEBUG": "1",
        }
        result = runner.invoke(swhmain, ["test"], env=env, auto_envvar_prefix="SWH")
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn", debug=True, integrations=[], release=None, environment=None,
    )


def test_sentry_env_main_package(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        click.echo("Hello SWH!")

    runner = CliRunner()
    with patch("sentry_sdk.init") as sentry_sdk_init:
        env = {
            "SWH_SENTRY_DSN": "test_dsn",
            "SWH_MAIN_PACKAGE": "swh.core",
            "SWH_SENTRY_ENVIRONMENT": "tests",
        }
        result = runner.invoke(swhmain, ["test"], env=env, auto_envvar_prefix="SWH")
    assert result.exit_code == 0

    version = pkg_resources.get_distribution("swh.core").version

    assert result.output.strip() == """Hello SWH!"""
    sentry_sdk_init.assert_called_once_with(
        dsn="test_dsn",
        debug=False,
        integrations=[],
        release="swh.core@" + version,
        environment="tests",
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


def test_log_config(log_config_path, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        logging.debug("Root log debug")
        logging.info("Root log info")
        logging.getLogger("dontshowdebug").debug("Not shown")
        logging.getLogger("dontshowdebug").info("Shown")

    runner = CliRunner()
    result = runner.invoke(swhmain, ["--log-config", log_config_path, "test",],)

    assert result.exit_code == 0
    assert result.output.strip() == "\n".join(
        [
            "custom format:root:DEBUG:Root log debug",
            "custom format:root:INFO:Root log info",
            "custom format:dontshowdebug:INFO:Shown",
        ]
    )


def test_log_config_log_level_interaction(log_config_path, swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        logging.debug("Root log debug")
        logging.info("Root log info")
        logging.getLogger("dontshowdebug").debug("Not shown")
        logging.getLogger("dontshowdebug").info("Shown")

    runner = CliRunner()
    result = runner.invoke(
        swhmain, ["--log-config", log_config_path, "--log-level", "INFO", "test",],
    )

    assert result.exit_code == 0
    assert result.output.strip() == "\n".join(
        [
            "custom format:root:INFO:Root log info",
            "custom format:dontshowdebug:INFO:Shown",
        ]
    )


def test_multiple_log_level_behavior(swhmain):
    @swhmain.command(name="test")
    @click.pass_context
    def swhtest(ctx):
        assert logging.getLevelName(logging.root.level) == "DEBUG"
        assert logging.getLevelName(logging.getLogger("dontshowdebug").level) == "INFO"
        return 0

    runner = CliRunner()
    result = runner.invoke(
        swhmain, ["--log-level", "DEBUG", "--log-level", "dontshowdebug:INFO", "test",]
    )

    assert result.exit_code == 0, result.output


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
    assert result.exit_code == 0
    assert "canonical-test  A test command." in result.output
    assert "othername" not in result.output

    # check we can execute the cmd with 'canonical-test'
    result = runner.invoke(swhmain, ["canonical-test"])
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""

    # check we can also execute the cmd with the alias 'othername'
    result = runner.invoke(swhmain, ["othername"])
    assert result.exit_code == 0
    assert result.output.strip() == """Hello SWH!"""
