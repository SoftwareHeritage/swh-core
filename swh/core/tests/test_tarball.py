# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from zipfile import ZipFile

from swh.core import tarball


def test_is_tarball(tmp_path):

    nozip = tmp_path / 'nozip.zip'
    nozip.write_text('Im no zip')

    assert tarball.is_tarball(str(nozip)) is False

    notar = tmp_path / 'notar.tar'
    notar.write_text('Im no tar')

    assert tarball.is_tarball(str(notar)) is False

    zipfile = tmp_path / 'truezip.zip'
    with ZipFile(str(zipfile), 'w') as myzip:
        myzip.writestr('file1.txt', 'some content')

    assert tarball.is_tarball(str(zipfile)) is True


def test_compress_uncompress_zip(tmp_path):
    tocompress = tmp_path / 'compressme'
    tocompress.mkdir()

    for i in range(10):
        fpath = tocompress / ('file%s.txt' % i)
        fpath.write_text('content of file %s' % i)

    zipfile = tmp_path / 'archive.zip'
    tarball.compress(str(zipfile), 'zip', str(tocompress))

    assert tarball.is_tarball(str(zipfile))

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

    assert tarball.is_tarball(str(tarfile))

    destdir = tmp_path / 'destdir'
    tarball.uncompress(str(tarfile), str(destdir))

    lsdir = sorted(x.name for x in destdir.iterdir())
    assert ['file%s.txt' % i for i in range(10)] == lsdir
