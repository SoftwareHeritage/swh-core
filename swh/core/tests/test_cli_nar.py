# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from bz2 import compress as bz2_compress
import hashlib
from lzma import compress as xz_compress
import os
from pathlib import Path

from click.testing import CliRunner
import pytest

from swh.core.cli.nar import nar_hash_cli, nar_serialize_cli, nar_unpack_cli
from swh.core.nar import compute_nar_hashes, nar_serialize
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


compression_func = {
    "none": lambda data: data,
    "bz2": lambda data: bz2_compress(data),
    "xz": lambda data: xz_compress(data),
}


@pytest.mark.parametrize(
    "compression",
    ["none", "bz2", "xz"],
    ids=["no compression", "bz2 compression", "xz compression"],
)
def test_nar_unpack_directory(cli_runner, tmpdir, tarball_with_nar_hashes, compression):
    tarball_path, nar_hashes = tarball_with_nar_hashes

    directory_path = Path(tmpdir / "tarball")
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    nar_path = os.path.join(tmpdir, "archive.nar")
    if compression != "none":
        nar_path += f".{compression}"

    with open(nar_path, "wb") as nar:
        nar.write(compression_func[compression](nar_serialize(directory_path)))

    dest_path = os.path.join(tmpdir, "nar_unpacked")

    result = cli_runner.invoke(
        nar_unpack_cli,
        [nar_path, dest_path],
    )

    assert result.exit_code == 0

    assert compute_nar_hashes(dest_path, is_tarball=False) == nar_hashes


@pytest.mark.parametrize(
    "compression",
    ["none", "bz2", "xz"],
    ids=["no compression", "bz2 compression", "xz compression"],
)
def test_nar_unpack_content(cli_runner, tmpdir, content_with_nar_hashes, compression):
    content_path, nar_hashes = content_with_nar_hashes

    nar_path = os.path.join(tmpdir, "archive.nar")
    if compression != "none":
        nar_path += f".{compression}"

    with open(nar_path, "wb") as nar:
        nar.write(compression_func[compression](nar_serialize(content_path)))

    dest_path = os.path.join(tmpdir, "nar_unpacked")

    result = cli_runner.invoke(
        nar_unpack_cli,
        [nar_path, dest_path],
    )

    assert result.exit_code == 0

    assert compute_nar_hashes(dest_path, is_tarball=False) == nar_hashes
