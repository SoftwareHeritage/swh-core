# Copyright (C) 2018-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path

import pytest


@pytest.fixture
def tarball_path(datadir):
    """Return tarball filepath fetched by TarballDirectoryLoader test runs."""
    return path.join(datadir, "https_example.org", "archives_dummy-hello.tar.gz")


@pytest.fixture
def tarball_with_executable_path(datadir):
    """Return tarball filepath (which contains executable) fetched by
    TarballDirectoryLoader test runs."""
    return path.join(
        datadir, "https_example.org", "archives_dummy-hello-with-executable.tar.gz"
    )


@pytest.fixture
def content_path(datadir):
    """Return filepath fetched by ContentLoader test runs."""
    return path.join(
        datadir, "https_common-lisp.net", "project_asdf_archives_asdf-3.3.5.lisp"
    )


@pytest.fixture
def executable_path(datadir):
    """Return executable filepath fetched by ContentLoader test runs."""
    return path.join(datadir, "https_example.org", "test-executable.sh")


@pytest.fixture
def tarball_with_nar_hashes(tarball_path):
    return (
        tarball_path,
        {"sha256": "45db8a27ccfae60b5233003c54c2d6b5ed6f0a1299dd9bbebc8f06cf649bc9c0"},
    )


@pytest.fixture
def tarball_with_executable_with_nar_hashes(tarball_with_executable_path):
    return (
        tarball_with_executable_path,
        {"sha256": "2c2b619d2dc235bff286762550c7f86eb34c9f88ec83a8ae426d75604d3a815b"},
    )


@pytest.fixture
def content_with_nar_hashes(content_path):
    return (
        content_path,
        {"sha256": "0b555a4d13e530460425d1dc20332294f151067fb64a7e49c7de501f05b0a41a"},
    )


@pytest.fixture
def executable_with_nar_hashes(executable_path):
    return (
        executable_path,
        {"sha256": "d29c24cee7dfc0f015b022e9af1c913f165edfaf918fde966d82e2006013a8ce"},
    )
