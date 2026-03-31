# Copyright (C) 2025-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import math

import pytest

from swh.core.s3.downloader import S3Downloader


@pytest.fixture(autouse=True)
def mocked_aws(mocked_aws):
    return mocked_aws


def check_files_download(files_path, downloaded_files_path):
    for file_path in files_path.rglob("**/*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(files_path)
            downloaded_file_path = downloaded_files_path / relative_path
            assert downloaded_file_path.exists()
            assert downloaded_file_path.stat().st_size == file_path.stat().st_size


def test_s3_downloader_ok(test_archives_path, s3_archives_url, tmp_path):
    """Check files can be successfully downloaded"""
    s3_downloader = S3Downloader(
        local_path=tmp_path,
        s3_url=s3_archives_url,
    )
    assert s3_downloader.download()

    check_files_download(test_archives_path, tmp_path)


def test_s3_downloader_with_chunked_files_ok(
    test_archives_path, s3_archives_url, tmp_path, mocker
):
    """Check files can be successfully downloaded by chunks"""
    chunk_size = 10 * 1024  # 10 Kib
    s3_downloader = S3Downloader(
        local_path=tmp_path,
        s3_url=s3_archives_url,
        multipart_download_chunk_size=chunk_size,
    )

    download_file = mocker.spy(s3_downloader, "_download_file")

    assert s3_downloader.download()

    download_file_calls_kwargs = [call.kwargs for call in download_file.call_args_list]

    check_files_download(test_archives_path, tmp_path)

    for file_path in test_archives_path.rglob("**/*"):
        if file_path.is_file():
            relative_path = "archives/" + str(file_path.relative_to(test_archives_path))
            if file_path.stat().st_size > chunk_size:
                nb_chunks = math.ceil(file_path.stat().st_size / chunk_size)
                assert nb_chunks > 0
                assert (
                    len(
                        [
                            kwargs
                            for kwargs in download_file_calls_kwargs
                            if kwargs["obj"].key == relative_path
                            and kwargs.get("chunk_id", -1) >= 0
                        ]
                    )
                    == nb_chunks
                )


def test_dataset_downloader_resume_download(
    test_archives_path, s3_archives_url, tmp_path
):
    """Check download of files can be successfully resumed when
    a download error happened"""

    # return a patched object.iter_chunks method that raises a ConnectionError
    # after having yielded a few bytes
    def failing_iter_chunks(iter_chunks_orig):
        def iter_chunks(_):
            for i, chunk in enumerate(iter_chunks_orig(4)):
                if i < 3:
                    yield chunk
                else:
                    raise ConnectionError("Remote disconnected")

        return iter_chunks

    # return a patched client.get_object method allowing to mock the iter_chunks
    # method of an s3 object
    def patched_get_object(client_get_object_orig, download_failure_filename):
        def get_object(**kwargs):
            obj = client_get_object_orig(**kwargs)
            if kwargs["Key"].endswith(download_failure_filename):
                obj["Body"].iter_chunks = failing_iter_chunks(obj["Body"].iter_chunks)
            return obj

        return get_object

    s3_downloader = S3Downloader(
        local_path=tmp_path,
        s3_url=s3_archives_url,
    )

    # simulate download failure for a file
    download_failure_filename = "hello.zip"

    orig_client_get_object = s3_downloader.client.get_object
    s3_downloader.client.get_object = patched_get_object(
        orig_client_get_object, download_failure_filename
    )

    # first download attempt should fail
    assert not s3_downloader.download()

    # downloaded files should be incomplete
    with pytest.raises(AssertionError):
        check_files_download(test_archives_path, tmp_path)

    # part file should have been created
    part_file = tmp_path / (download_failure_filename + ".part")
    previous_part_size = part_file.stat().st_size
    assert part_file.exists()

    # second download attempt should still fail
    assert not s3_downloader.download()

    # downloaded files should still be incomplete
    with pytest.raises(AssertionError):
        check_files_download(test_archives_path, tmp_path)

    # part file should have been updated with new bytes
    assert part_file.exists()
    assert part_file.stat().st_size > previous_part_size

    # restore original client.get_object implementation for
    # download to succeed
    s3_downloader.client.get_object = orig_client_get_object

    # third download attempt should succeed
    assert s3_downloader.download()

    # no more part files exist
    assert all(
        not path.name.endswith(".part") for path in test_archives_path.rglob("**/*")
    )

    # downloaded files should be complete
    check_files_download(test_archives_path, tmp_path)


def test_dataset_downloader_keyboard_interrupt(s3_archives_url, tmp_path):
    """Check download of files can be successfully resumed when
    a download error happened"""

    # return a patched object.iter_chunks method that raises a ConnectionError
    # after having yielded a few bytes
    def failing_iter_chunks(iter_chunks_orig):
        def iter_chunks(_):
            for i, chunk in enumerate(iter_chunks_orig(4)):
                if i < 3:
                    yield chunk
                else:
                    raise SystemExit()

        return iter_chunks

    # return a patched client.get_object method allowing to mock the iter_chunks
    # method of an s3 object
    def patched_get_object(client_get_object_orig, download_failure_filename):
        def get_object(**kwargs):
            obj = client_get_object_orig(**kwargs)
            if kwargs["Key"].endswith(download_failure_filename):
                obj["Body"].iter_chunks = failing_iter_chunks(obj["Body"].iter_chunks)
            return obj

        return get_object

    s3_downloader = S3Downloader(
        local_path=tmp_path,
        s3_url=s3_archives_url,
    )

    # simulate keyboard interrupt when downloading a file
    download_failure_filename = "hello.zip"

    orig_client_get_object = s3_downloader.client.get_object
    s3_downloader.client.get_object = patched_get_object(
        orig_client_get_object, download_failure_filename
    )

    with pytest.raises(SystemExit) as exc_info:
        s3_downloader.download()

    assert exc_info.value.code == 1
