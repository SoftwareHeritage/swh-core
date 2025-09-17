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
        {
            "md5": "cc0af4d7817887d93ff05c5c024d5b6f",
            "sha1": "475dd3c13f0bc021ff7d9b19c4bdc0bf996dac54",
            "sha256": "45db8a27ccfae60b5233003c54c2d6b5ed6f0a1299dd9bbebc8f06cf649bc9c0",
            "sha512": (
                "51712dc9505b1f859fe82d33eee11772744fa843c212816a144a96edaba8b62"
                "68e1ffec751a26de1cc033979211f2abdb43763f107f775b3322f8ff5b92a64c4"
            ),
        },
    )


@pytest.fixture
def tarball_with_executable_with_nar_hashes(tarball_with_executable_path):
    return (
        tarball_with_executable_path,
        {
            "md5": "5ec4289356b33be12279f94845bc8936",
            "sha1": "242b45b43d6b994ff31725752787dc9f886cbd38",
            "sha256": "2c2b619d2dc235bff286762550c7f86eb34c9f88ec83a8ae426d75604d3a815b",
            "sha512": (
                "ed86ff626a19187436646c135c4592e58f0a14d885f2841733f7030e6b75947"
                "e35ec9233089ebb63dc8ba02fe12faaee902a0b8e34814da7c06a4567a936d92e"
            ),
        },
    )


@pytest.fixture
def content_with_nar_hashes(content_path):
    return (
        content_path,
        {
            "md5": "24e4f07ccf36365e9d5366eab0e7f1a7",
            "sha1": "f6e6f876535907eb1f1f9c8ff8df4ab24381cc96",
            "sha256": "0b555a4d13e530460425d1dc20332294f151067fb64a7e49c7de501f05b0a41a",
            "sha512": (
                "ba822a15df2d1a726150abbfb8201531778405dd07be76d122092d52d3da09a"
                "5cee05bdc4a9a387cd01352fd19209367f7d8eda15d07d902fbe735f945c52368"
            ),
        },
    )


@pytest.fixture
def executable_with_nar_hashes(executable_path):
    return (
        executable_path,
        {
            "md5": "9b03233fe39c083ff252735181088c95",
            "sha1": "90655f82ca00760eaf6bd2e4a9c52a6e4d68fff0",
            "sha256": "d29c24cee7dfc0f015b022e9af1c913f165edfaf918fde966d82e2006013a8ce",
            "sha512": (
                "e64305aaaf418cb4fc3aafd5d1ed628f457e81c4b001b65237dfbce26c7d760"
                "22cd8504e549407e676e19943dc24b45e1a1bf43fa2e7d3c7bc2a47f8a7d92a92"
            ),
        },
    )
