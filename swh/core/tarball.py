# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import stat
import tarfile
import zipfile

from os.path import abspath, realpath, join, dirname
from . import utils


def _canonical_abspath(path):
    """Resolve all paths to an absolute and real one.

    Args:
        path: to resolve

    Returns:
        canonical absolute path to path

    """
    return realpath(abspath(path))


def _badpath(path, basepath):
    """Determine if a path is outside basepath.

    Args:
        path: a relative or absolute path of a file or directory
        basepath: the basepath path must be in

    Returns:
        True if path is outside basepath, false otherwise.

    """
    return not _canonical_abspath(join(basepath, path)).startswith(basepath)


def _badlink(info, basepath):
    """Determine if the tarinfo member is outside basepath.

    Args:
        info: TarInfo member representing a symlink or hardlink of tar archive
        basepath: the basepath the info member must be in

    Returns:
        True if info is outside basepath, false otherwise.

    """
    tippath = _canonical_abspath(join(basepath, dirname(info.name)))
    return _badpath(info.linkname, basepath=tippath)


def is_tarball(filepath):
    """Given a filepath, determine if it represents an archive.

    Args:
        filepath: file to test for tarball property

    Returns:
        Bool, True if it's a tarball, False otherwise

    """
    return tarfile.is_tarfile(filepath) or zipfile.is_zipfile(filepath)


def _uncompress_zip(tarpath, dirpath):
    """Uncompress zip archive safely.

    As per zipfile is concerned
    (cf. note on https://docs.python.org/3.5/library/zipfile.html#zipfile.ZipFile.extract)  # noqa

    Args:
        tarpath: path to the archive
        dirpath: directory to uncompress the archive to

    """
    with zipfile.ZipFile(tarpath) as z:
        z.extractall(path=dirpath)


def _uncompress_tar(tarpath, dirpath):
    """Uncompress tarpath if the tarpath is safe.
    Safe means, no file will be uncompressed outside of dirpath.

    Args:
        tarpath: path to the archive
        dirpath: directory to uncompress the archive to

    Raises:
        ValueError when a member would be extracted outside dirpath.

    """
    def safemembers(tarpath, members, basepath):
        """Given a list of archive members, yield the members (directory,
        file, hard-link) that stays in bounds with basepath.  Note
        that symbolic link are authorized to point outside the
        basepath though.

        Args:
            tarpath: Name of the tarball
            members: Archive members for such tarball
            basepath: the basepath sandbox

        Yields:
            Safe TarInfo member

        Raises:
            ValueError when a member would be extracted outside basepath

        """
        errormsg = 'Archive {} blocked. Illegal path to %s %s'.format(tarpath)

        for finfo in members:
            if finfo.isdir() and _badpath(finfo.name, basepath):
                raise ValueError(errormsg % ('directory', finfo.name))
            elif finfo.isfile() and _badpath(finfo.name, basepath):
                raise ValueError(errormsg % ('file', finfo.name))
            elif finfo.islnk() and _badlink(finfo, basepath):
                raise ValueError(errormsg % ('hard-link', finfo.linkname))
            # Authorize symlinks to point outside basepath
            # elif finfo.issym() and _badlink(finfo, basepath):
            #     raise ValueError(errormsg % ('symlink', finfo.linkname))
            else:
                yield finfo

    with tarfile.open(tarpath) as t:
        members = t.getmembers()
        t.extractall(path=dirpath,
                     members=safemembers(tarpath, members, dirpath))


def uncompress(tarpath, dest):
    """Uncompress tarpath to dest folder if tarball is supported and safe.
       Safe means, no file will be uncompressed outside of dirpath.

       Note that this fixes permissions after successfully
       uncompressing the archive.

    Args:
        tarpath: path to tarball to uncompress
        dest: the destination folder where to uncompress the tarball

    Returns:
        The nature of the tarball, zip or tar.

    Raises:
        ValueError when:
        - an archive member would be extracted outside basepath
        - the archive is not supported

    """
    if tarfile.is_tarfile(tarpath):
        _uncompress_tar(tarpath, dest)
        nature = 'tar'
    elif zipfile.is_zipfile(tarpath):
        _uncompress_zip(tarpath, dest)
        nature = 'zip'
    else:
        raise ValueError('File %s is not a supported archive.' % tarpath)

    # Fix permissions
    for dirpath, _, fnames in os.walk(dest):
        os.chmod(dirpath, 0o755)
        for fname in fnames:
            fpath = os.path.join(dirpath, fname)
            if not os.path.islink(fpath):
                fpath_exec = os.stat(fpath).st_mode & stat.S_IXUSR
                if not fpath_exec:
                    os.chmod(fpath, 0o644)

    return nature


def _ls(rootdir):
    """Generator of filepath, filename from rootdir.

    """
    for dirpath, dirnames, fnames in os.walk(rootdir):
        for fname in (dirnames+fnames):
            fpath = os.path.join(dirpath, fname)
            fname = utils.commonname(rootdir, fpath)
            yield fpath, fname


def _compress_zip(tarpath, files):
    """Compress dirpath's content as tarpath.

    """
    with zipfile.ZipFile(tarpath, 'w') as z:
        for fpath, fname in files:
            z.write(fpath, arcname=fname)


def _compress_tar(tarpath, files):
    """Compress dirpath's content as tarpath.

    """
    with tarfile.open(tarpath, 'w:bz2') as t:
        for fpath, fname in files:
            t.add(fpath, arcname=fname, recursive=False)


def compress(tarpath, nature, dirpath_or_files):
    """Create a tarball tarpath with nature nature.
    The content of the tarball is either dirpath's content (if representing
    a directory path) or dirpath's iterable contents.

    Compress the directory dirpath's content to a tarball.
    The tarball being dumped at tarpath.
    The nature of the tarball is determined by the nature argument.

    """
    if isinstance(dirpath_or_files, str):
        files = _ls(dirpath_or_files)
    else:  # iterable of 'filepath, filename'
        files = dirpath_or_files

    if nature == 'zip':
        _compress_zip(tarpath, files)
    else:
        _compress_tar(tarpath, files)

    return tarpath
