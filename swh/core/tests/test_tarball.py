# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

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


def test__unpack_tar_failure(tmp_path, datadir):
    """Unpack inexistent tarball should fail

    """
    tarpath = os.path.join(datadir, "archives", "inexistent-archive.tar.Z")

    assert not os.path.exists(tarpath)

    with pytest.raises(
        shutil.ReadError, match=f"Unable to uncompress {tarpath} to {tmp_path}"
    ):
        tarball._unpack_tar(tarpath, tmp_path)


def test__unpack_tar_failure2(tmp_path, datadir):
    """Unpack Existent tarball into an inexistent folder should fail

    """
    filename = "groff-1.02.tar.Z"
    tarpath = os.path.join(datadir, "archives", filename)

    assert os.path.exists(tarpath)

    extract_dir = os.path.join(tmp_path, "dir", "inexistent")

    with pytest.raises(
        shutil.ReadError, match=f"Unable to uncompress {tarpath} to {tmp_path}"
    ):
        tarball._unpack_tar(tarpath, extract_dir)


def test__unpack_tar_failure3(tmp_path, datadir):
    """Unpack unsupported tarball should fail

    """
    filename = "hello.zip"
    tarpath = os.path.join(datadir, "archives", filename)

    assert os.path.exists(tarpath)

    with pytest.raises(
        shutil.ReadError, match=f"Unable to uncompress {tarpath} to {tmp_path}"
    ):
        tarball._unpack_tar(tarpath, tmp_path)


def test__unpack_tar(tmp_path, datadir):
    """Unpack supported tarball into an existent folder should be ok

    """
    filename = "groff-1.02.tar.Z"
    tarpath = os.path.join(datadir, "archives", filename)

    assert os.path.exists(tarpath)

    extract_dir = os.path.join(tmp_path, filename)
    os.makedirs(extract_dir, exist_ok=True)

    output_directory = tarball._unpack_tar(tarpath, extract_dir)

    assert extract_dir == output_directory
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


def test_uncompress_tarpaths(tmp_path, datadir, prepare_shutil_state):
    """High level call uncompression on un/supported tarballs

    """
    archive_dir = os.path.join(datadir, "archives")
    tarfiles = os.listdir(archive_dir)
    tarpaths = [os.path.join(archive_dir, tarfile) for tarfile in tarfiles]

    unsupported_tarpaths = []
    for t in tarpaths:
        if t.endswith(".Z") or t.endswith(".x") or t.endswith(".lz"):
            unsupported_tarpaths.append(t)

    # not supported yet
    for tarpath in unsupported_tarpaths:
        with pytest.raises(ValueError, match=f"Problem during unpacking {tarpath}."):
            tarball.uncompress(tarpath, dest=tmp_path)

    # register those unsupported formats
    tarball.register_new_archive_formats()

    # unsupported formats are now supported
    for n, tarpath in enumerate(tarpaths, start=1):
        tarball.uncompress(tarpath, dest=tmp_path)

    assert n == len(tarpaths)
