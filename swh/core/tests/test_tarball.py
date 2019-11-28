# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import shutil

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
    tocompress = tmp_path / 'compressme'
    tocompress.mkdir()

    for i in range(10):
        fpath = tocompress / ('file%s.txt' % i)
        fpath.write_text('content of file %s' % i)

    zipfile = tmp_path / 'archive.zip'
    tarball.compress(str(zipfile), 'zip', str(tocompress))

    destdir = tmp_path / 'destdir'
    tarball.uncompress(str(zipfile), str(destdir))

    lsdir = sorted(x.name for x in destdir.iterdir())
    assert ['file%s.txt' % i for i in range(10)] == lsdir


def test_compress_uncompress_tar(tmp_path):
    tocompress = tmp_path / 'compressme'
    tocompress.mkdir()

    for i in range(10):
        fpath = tocompress / ('file%s.txt' % i)
        fpath.write_text('content of file %s' % i)

    tarfile = tmp_path / 'archive.tar'
    tarball.compress(str(tarfile), 'tar', str(tocompress))

    destdir = tmp_path / 'destdir'
    tarball.uncompress(str(tarfile), str(destdir))

    lsdir = sorted(x.name for x in destdir.iterdir())
    assert ['file%s.txt' % i for i in range(10)] == lsdir


def test_unpack_specific_tar_failure(tmp_path, datadir):
    tarpath = os.path.join(datadir, 'archives', 'inexistent-archive.tar.Z')

    assert not os.path.exists(tarpath)

    with pytest.raises(shutil.ReadError,
                       match=f'Unable to uncompress {tarpath} to {tmp_path}'):
        tarball.unpack_specific_tar(tarpath, tmp_path)


def test_unpack_specific_tar(tmp_path, datadir):
    filename = 'groff-1.02.tar.Z'
    tarpath = os.path.join(datadir, 'archives', filename)

    assert os.path.exists(tarpath)

    output_directory = tarball.unpack_specific_tar(tarpath, tmp_path)

    expected_path = os.path.join(tmp_path, filename)

    assert os.path.exists(expected_path)
    assert expected_path == output_directory
    assert len(os.listdir(expected_path)) > 0


def test_register_new_archive_formats(prepare_shutil_state):
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
    archive_dir = os.path.join(datadir, 'archives')
    tarfiles = os.listdir(archive_dir)
    tarpaths = [os.path.join(archive_dir, tarfile) for tarfile in tarfiles]

    unregistered_yet_tarpaths = list(
        filter(lambda t: t.endswith('.Z'), tarpaths))
    for tarpath in unregistered_yet_tarpaths:
        with pytest.raises(ValueError,
                           match=f'File {tarpath} is not a supported archive'):
            tarball.uncompress(tarpath, dest=tmp_path)

    tarball.register_new_archive_formats()

    for n, tarpath in enumerate(tarpaths, start=1):
        tarball.uncompress(tarpath, dest=tmp_path)

    assert n == len(tarpaths)
