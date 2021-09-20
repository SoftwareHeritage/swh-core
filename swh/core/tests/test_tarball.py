# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import os
import shutil

import pytest

from swh.core import tarball


@pytest.fixture
def prepare_shutil_state():
    """Reset any shutil modification in its current state

    """
    import shutil

    registered_formats = [f[0] for f in shutil.get_unpack_formats()]
    for format_id in tarball.ADDITIONAL_ARCHIVE_FORMATS:
        name = format_id[0]
        if name in registered_formats:
            shutil.unregister_unpack_format(name)

    return shutil


def test_compress_uncompress_zip(tmp_path):
    tocompress = tmp_path / "compressme"
    tocompress.mkdir()

    for i in range(10):
        fpath = tocompress / ("file%s.txt" % i)
        fpath.write_text("content of file %s" % i)

    zipfile = tmp_path / "archive.zip"
    tarball.compress(str(zipfile), "zip", str(tocompress))

    destdir = tmp_path / "destdir"
    tarball.uncompress(str(zipfile), str(destdir))

    lsdir = sorted(x.name for x in destdir.iterdir())
    assert ["file%s.txt" % i for i in range(10)] == lsdir


@pytest.mark.xfail(
    reason=(
        "Python's zipfile library doesn't support Info-ZIP's "
        "extension for file permissions."
    )
)
def test_compress_uncompress_zip_modes(tmp_path):
    tocompress = tmp_path / "compressme"
    tocompress.mkdir()

    fpath = tocompress / "text.txt"
    fpath.write_text("echo foo")
    fpath.chmod(0o644)

    fpath = tocompress / "executable.sh"
    fpath.write_text("echo foo")
    fpath.chmod(0o755)

    zipfile = tmp_path / "archive.zip"
    tarball.compress(str(zipfile), "zip", str(tocompress))

    destdir = tmp_path / "destdir"
    tarball.uncompress(str(zipfile), str(destdir))

    (executable_path, text_path) = sorted(destdir.iterdir())
    assert text_path.stat().st_mode == 0o100644  # succeeds, it's the default
    assert executable_path.stat().st_mode == 0o100755  # fails


def test_compress_uncompress_tar(tmp_path):
    tocompress = tmp_path / "compressme"
    tocompress.mkdir()

    for i in range(10):
        fpath = tocompress / ("file%s.txt" % i)
        fpath.write_text("content of file %s" % i)

    tarfile = tmp_path / "archive.tar"
    tarball.compress(str(tarfile), "tar", str(tocompress))

    destdir = tmp_path / "destdir"
    tarball.uncompress(str(tarfile), str(destdir))

    lsdir = sorted(x.name for x in destdir.iterdir())
    assert ["file%s.txt" % i for i in range(10)] == lsdir


def test_compress_uncompress_tar_modes(tmp_path):
    tocompress = tmp_path / "compressme"
    tocompress.mkdir()

    fpath = tocompress / "text.txt"
    fpath.write_text("echo foo")
    fpath.chmod(0o644)

    fpath = tocompress / "executable.sh"
    fpath.write_text("echo foo")
    fpath.chmod(0o755)

    tarfile = tmp_path / "archive.tar"
    tarball.compress(str(tarfile), "tar", str(tocompress))

    destdir = tmp_path / "destdir"
    tarball.uncompress(str(tarfile), str(destdir))

    (executable_path, text_path) = sorted(destdir.iterdir())
    assert text_path.stat().st_mode == 0o100644
    assert executable_path.stat().st_mode == 0o100755


def test_uncompress_tar_failure(tmp_path, datadir):
    """Unpack inexistent tarball should fail

    """
    tarpath = os.path.join(datadir, "archives", "inexistent-archive.tar.Z")

    assert not os.path.exists(tarpath)

    with pytest.raises(ValueError, match="Problem during unpacking"):
        tarball.uncompress(tarpath, tmp_path)


def test_uncompress_tar(tmp_path, datadir):
    """Unpack supported tarball into an existent folder should be ok

    """
    filename = "groff-1.02.tar.Z"
    tarpath = os.path.join(datadir, "archives", filename)

    assert os.path.exists(tarpath)

    extract_dir = os.path.join(tmp_path, filename)

    tarball.uncompress(tarpath, extract_dir)

    assert len(os.listdir(extract_dir)) > 0


def test_register_new_archive_formats(prepare_shutil_state):
    """Registering new archive formats should be fine

    """
    unpack_formats_v1 = [f[0] for f in shutil.get_unpack_formats()]
    for format_id in tarball.ADDITIONAL_ARCHIVE_FORMATS:
        assert format_id[0] not in unpack_formats_v1

    # when
    tarball.register_new_archive_formats()

    # then
    unpack_formats_v2 = [f[0] for f in shutil.get_unpack_formats()]
    for format_id in tarball.ADDITIONAL_ARCHIVE_FORMATS:
        assert format_id[0] in unpack_formats_v2


def test_uncompress_archives(tmp_path, datadir):
    """High level call uncompression on supported archives

    """
    archive_dir = os.path.join(datadir, "archives")
    archive_files = os.listdir(archive_dir)

    for archive_file in archive_files:
        archive_path = os.path.join(archive_dir, archive_file)
        extract_dir = os.path.join(tmp_path, archive_file)
        tarball.uncompress(archive_path, dest=extract_dir)
        assert len(os.listdir(extract_dir)) > 0


def test_normalize_permissions(tmp_path):
    for perms in range(0o1000):
        filename = str(perms)
        file_path = tmp_path / filename
        file_path.touch()
        file_path.chmod(perms)

    for file in tmp_path.iterdir():
        assert file.stat().st_mode == 0o100000 | int(file.name)

    tarball.normalize_permissions(str(tmp_path))

    for file in tmp_path.iterdir():
        if int(file.name) & 0o100:  # original file was executable for its owner
            assert file.stat().st_mode == 0o100755
        else:
            assert file.stat().st_mode == 0o100644


def test_unpcompress_zip_imploded(tmp_path, datadir):
    """Unpack a zip archive with compression type 6 (implode),
    not supported by python zipfile module.

    """
    filename = "msk316src.zip"
    zippath = os.path.join(datadir, "archives", filename)

    assert os.path.exists(zippath)

    extract_dir = os.path.join(tmp_path, filename)

    tarball.uncompress(zippath, extract_dir)

    assert len(os.listdir(extract_dir)) > 0


def test_uncompress_upper_archive_extension(tmp_path, datadir):
    """Copy test archives in a temporary directory but turn their names
    to uppercase, then check they can be successfully extracted.
    """
    archives_path = os.path.join(datadir, "archives")
    archive_files = [
        f
        for f in os.listdir(archives_path)
        if os.path.isfile(os.path.join(archives_path, f))
    ]
    for archive_file in archive_files:
        archive_file_upper = os.path.join(tmp_path, archive_file.upper())
        extract_dir = os.path.join(tmp_path, archive_file)
        shutil.copy(os.path.join(archives_path, archive_file), archive_file_upper)
        tarball.uncompress(archive_file_upper, extract_dir)
        assert len(os.listdir(extract_dir)) > 0


def test_uncompress_archive_no_extension(tmp_path, datadir):
    """Copy test archives in a temporary directory but turn their names
    to their md5 sums, then check they can be successfully extracted.
    """
    archives_path = os.path.join(datadir, "archives")
    archive_files = [
        f
        for f in os.listdir(archives_path)
        if os.path.isfile(os.path.join(archives_path, f))
    ]
    for archive_file in archive_files:
        archive_file_path = os.path.join(archives_path, archive_file)
        with open(archive_file_path, "rb") as f:
            md5sum = hashlib.md5(f.read()).hexdigest()
        archive_file_md5sum = os.path.join(tmp_path, md5sum)
        extract_dir = os.path.join(tmp_path, archive_file)
        shutil.copy(archive_file_path, archive_file_md5sum)
        tarball.uncompress(archive_file_md5sum, extract_dir)
        assert len(os.listdir(extract_dir)) > 0
