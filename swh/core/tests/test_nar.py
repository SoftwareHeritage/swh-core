# Copyright (C) 2023-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import os
from pathlib import Path

from swh.core.nar import Nar, compute_nar_hashes, nar_serialize, nar_unpack
from swh.core.tarball import uncompress


def test_nar_tarball(tmpdir, tarball_with_nar_hashes):
    tarball_path, nar_hashes = tarball_with_nar_hashes

    directory_path = Path(tmpdir)
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    nar = Nar(hash_names=list(nar_hashes.keys()))
    nar.serialize(directory_path)
    assert nar.hexdigest() == nar_hashes


def test_nar_tarball_hash_formats(tmpdir, tarball_path):
    directory_path = Path(tmpdir)
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    nar = Nar(hash_names=["sha256"])
    nar.serialize(directory_path)

    assert nar.hexdigest() == {
        "sha256": "45db8a27ccfae60b5233003c54c2d6b5ed6f0a1299dd9bbebc8f06cf649bc9c0"
    }
    assert nar.b32digest() == {
        "sha256": "1h69kdjcy1lgpjz9ppcr2856zvdmsv158g006d90prpsrhkqmns5"
    }
    assert nar.b64digest() == {"sha256": "RduKJ8z65gtSMwA8VMLWte1vChKZ3Zu+vI8Gz2SbycA="}


def test_nar_tarball_with_executable(tmpdir, tarball_with_executable_with_nar_hashes):
    """Compute nar on tarball with executable files inside should not mismatch"""
    tarball_path, nar_hashes = tarball_with_executable_with_nar_hashes

    directory_path = Path(tmpdir)
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    nar = Nar(hash_names=list(nar_hashes.keys()))
    nar.serialize(directory_path)
    assert nar.hexdigest() == nar_hashes


def test_nar_content(content_with_nar_hashes):
    content_path, nar_hashes = content_with_nar_hashes

    nar = Nar(hash_names=list(nar_hashes.keys()))
    nar.serialize(content_path)
    assert nar.hexdigest() == nar_hashes


def test_nar_executable(executable_with_nar_hashes):
    """Compute nar on file with executable bit set should not mismatch"""
    content_path, nar_hashes = executable_with_nar_hashes

    nar = Nar(hash_names=list(nar_hashes.keys()))
    nar.serialize(content_path)
    assert nar.hexdigest() == nar_hashes


def test_nar_exclude_vcs(tmpdir, mocker):
    directory_path = Path(tmpdir)

    file_path = directory_path / "file"
    file_path.write_text("file")

    git_path = directory_path / ".git"
    git_path.mkdir()

    git_file_path = git_path / "foo"
    git_file_path.write_text("foo")

    subdir_path = directory_path / "bar"
    subdir_path.mkdir()

    git_subdir_path = subdir_path / ".git"
    git_subdir_path.mkdir()

    svn_subdir_path = subdir_path / ".svn"
    svn_subdir_path.mkdir()

    git_subdir_file_path = git_subdir_path / "baz"
    git_subdir_file_path.write_text("baz")

    nar = Nar(hash_names=["sha1"], exclude_vcs=True, vcs_type="git")

    serializeEntry = mocker.spy(nar, "_serializeEntry")

    nar.serialize(directory_path)

    # check .git subdirs were not taken into account for nar hash computation
    assert mocker.call(Path(git_path)) not in serializeEntry.mock_calls
    assert mocker.call(Path(git_subdir_path)) not in serializeEntry.mock_calls

    # check .svn subdir was taken into account for nar hash computation
    serializeEntry.assert_any_call(Path(svn_subdir_path))

    assert nar.hexdigest() == {"sha1": "f1b641c46888a1002e340c9425ef8ec890605858"}


def test_nar_serialize_directory(tmpdir, tarball_with_nar_hashes):
    tarball_path, _ = tarball_with_nar_hashes

    directory_path = Path(tmpdir)
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    nar = Nar(hash_names=["sha256"])
    assert {
        "sha256": hashlib.sha256(nar.serialize(directory_path)).hexdigest()
    } == nar.hexdigest()


def test_nar_serialize_content(content_with_nar_hashes):
    content_path, _ = content_with_nar_hashes

    nar = Nar(hash_names=["sha256"])
    assert {
        "sha256": hashlib.sha256(nar.serialize(content_path)).hexdigest()
    } == nar.hexdigest()


def test_nar_unpack_directory(tmpdir, tarball_with_nar_hashes):
    tarball_path, nar_hashes = tarball_with_nar_hashes

    directory_path = Path(tmpdir / "tarball")
    directory_path.mkdir(parents=True, exist_ok=True)
    uncompress(str(tarball_path), dest=str(directory_path))

    nar_archive_path = os.path.join(tmpdir, "archive.nar")

    with open(nar_archive_path, "wb") as f:
        f.write(nar_serialize(directory_path))

    nar_unpacked_path = os.path.join(tmpdir, "unpacked_nar_archive")

    nar_unpack(nar_archive_path, nar_unpacked_path)

    nar_unpack_hashes = compute_nar_hashes(nar_unpacked_path, is_tarball=False)

    assert nar_unpack_hashes == nar_hashes


def test_nar_unpack_content(tmpdir, content_with_nar_hashes):
    content_path, nar_hashes = content_with_nar_hashes

    nar_archive_path = os.path.join(tmpdir, "archive.nar")

    with open(nar_archive_path, "wb") as f:
        f.write(nar_serialize(content_path))

    nar_unpacked_path = os.path.join(tmpdir, "unpacked_content")

    nar_unpack(nar_archive_path, nar_unpacked_path)

    nar_unpack_hashes = compute_nar_hashes(nar_unpacked_path, is_tarball=False)

    assert nar_unpack_hashes == nar_hashes
