# Copyright (C) 2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path

import pytest


def add_files_to_s3_bucket(
    files_path: Path,
    bucket: str,
    prefix: str,
):
    import boto3

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)
    for path in files_path.rglob("**/*"):
        if path.is_file():
            relative_path = path.relative_to(files_path)
            key = os.path.join(prefix, relative_path)
            s3.upload_file(
                Filename=str(path),
                Bucket=bucket,
                Key=key,
                ExtraArgs={
                    "ACL": "public-read",
                },
            )


@pytest.fixture
def s3_bucket_name():
    return "testbucket"


@pytest.fixture
def s3_archives_url(s3_bucket_name):
    return f"s3://{s3_bucket_name}/archives/"


@pytest.fixture
def test_archives_path():
    from swh.core import tests

    return Path(tests.__file__).parent / "data" / "archives"


@pytest.fixture
def mocked_aws(
    test_archives_path,
    s3_bucket_name,
):

    from moto import mock_aws

    with mock_aws():
        add_files_to_s3_bucket(
            test_archives_path,
            s3_bucket_name,
            prefix="archives",
        )
        yield
