# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from click.testing import CliRunner
import pytest

from swh.core.pytest_plugin import requests_mock_datadir_factory
from swh.core.tests.test_cli import assert_result


def response_context_callback(request, context):
    """Add link headers to mocked Sentry REST API responses"""
    base_url = f"{request.scheme}://{request.netloc}{request.path}"
    if not request.query:
        context.headers["Link"] = f'<{base_url}?cursor=0:100:0>; rel="next"'
    else:
        context.headers["Link"] = f'<{base_url}?cursor=0:200:0>; rel="next"'


requests_mock_sentry = requests_mock_datadir_factory(
    response_context_callback=response_context_callback
)


@pytest.fixture
def swhmain(swhmain):
    from swh.core.cli.sentry import sentry as swhsentry

    swhmain.add_command(swhsentry)
    return swhmain


def test_sentry_extract_origin_urls(swhmain, requests_mock_sentry):
    runner = CliRunner()
    result = runner.invoke(
        swhmain, ["sentry", "extract-origin-urls", "-t", "sentry-token", "-i", "112726"]
    )
    assert_result(result)
    expected_output = """
opam+https://opam.ocaml.org/packages/bdd/
opam+https://opam.ocaml.org/packages/bitv/
opam+https://opam.ocaml.org/packages/cgi/
opam+https://opam.ocaml.org/packages/combine/
"""
    assert result.output.strip() == expected_output.strip("\n")
