# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import os
from pathlib import Path

from click.testing import CliRunner
import pytest

from swh.core.cli.nar import nar_hash_cli, nar_serialize_cli
from swh.core.tarball import uncompress


@pytest.fixture
def cli_runner():
    return CliRunner()


def assert_output_contains(cli_output: str, snippet: str) -> bool:
    for line in cli_output.splitlines():
        if not line:
            continue

        if snippet in line:
            return True
    else:
        assert False, "%r not found in output %r" % (
            snippet,
            cli_output,
        )


def test_nar_cli_help(cli_runner):
    result = cli_runner.invoke(nar_hash_cli, ["--help"])

    assert result.exit_code == 0
    assert_output_contains(result.output, "Compute NAR hash of a given path.")


def test_nar_cli_tarball(cli_runner, tmpdir, tarball_with_nar_hashes):
    tarball_path, nar_hashes = tarball_with_nar_hashes

    directory_path = Path(tmpdir)
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    assert list(nar_hashes.keys()) == ["sha256"]

    result = cli_runner.invoke(
        nar_hash_cli, ["--hash-algo", "sha256", str(directory_path)]
    )

    assert result.exit_code == 0
    assert_output_contains(result.output, nar_hashes["sha256"])


def test_nar_cli_content(cli_runner, content_with_nar_hashes):
    content_path, nar_hashes = content_with_nar_hashes

    result = cli_runner.invoke(
        nar_hash_cli, ["-H", "sha256", "-f", "hex", content_path]
    )

    assert result.exit_code == 0

    assert_output_contains(result.output, nar_hashes["sha256"])


def test_nar_serialize_directory(cli_runner, tmpdir, tarball_with_nar_hashes):
    tarball_path, nar_hashes = tarball_with_nar_hashes

    directory_path = Path(tmpdir / "tarball")
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    assert list(nar_hashes.keys()) == ["sha256"]

    output_path = os.path.join(tmpdir, "output.nar")
    result = cli_runner.invoke(
        nar_serialize_cli,
        ["-o", output_path, str(directory_path)],
    )

    assert result.exit_code == 0

    with open(output_path, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == nar_hashes["sha256"]


def test_nar_serialize_content(cli_runner, tmpdir, content_with_nar_hashes):
    content_path, nar_hashes = content_with_nar_hashes

    assert list(nar_hashes.keys()) == ["sha256"]

    output_path = os.path.join(tmpdir, "output.nar")
    result = cli_runner.invoke(
        nar_serialize_cli,
        ["-o", output_path, str(content_path)],
    )

    assert result.exit_code == 0

    with open(output_path, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == nar_hashes["sha256"]
