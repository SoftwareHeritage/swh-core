# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from click.testing import CliRunner
import pytest

from swh.core.tests.test_cli import assert_result


@pytest.fixture
def swhmain(swhmain):
    from swh.core.cli.backend import backend as swhbackend

    swhmain.add_command(swhbackend)
    return swhmain


def test_backend_list_ok(swhmain, mock_get_entry_points):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["backend", "list", "test"])
    assert_result(result)
    assert result.output.strip() == "backend1 A mockup backend for tests"


def test_backend_list_empty(swhmain, mock_get_entry_points):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["backend", "list", "wrong_package"])
    assert result.exit_code == 1
    assert "No backend found for package 'wrong_package'" in result.output.strip()


def test_backend_list_cls_ok(swhmain, mock_get_entry_points):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["backend", "list", "test", "backend1"])
    assert_result(result)
    assert result.output.strip() == "test:backend1\n\nA mockup backend for tests"


def test_backend_list_cls_no_package(swhmain, mock_get_entry_points):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["backend", "list", "wrong_package", "backend1"])
    assert result.exit_code == 1
    assert (
        "No backend 'backend1' found for package 'wrong_package'"
        in result.output.strip()
    )


def test_backend_list_cls_no_backend(swhmain, mock_get_entry_points):
    runner = CliRunner()
    result = runner.invoke(swhmain, ["backend", "list", "test", "backend2"])
    assert result.exit_code == 1
    assert "No backend 'backend2' found for package 'test'" in result.output.strip()
