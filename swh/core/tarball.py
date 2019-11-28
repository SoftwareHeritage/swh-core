# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import stat
import tarfile
import zipfile

from . import utils


def unpack_specific_tar(tarpath: str, extract_dir: str) -> str:
    """Unpack specific tarballs (.tar.Z, .tar.lz, .tar.x) file. Returns the
    full path to the uncompressed directory.

    This relies on the `tar` command.

    Raises
        ReadError in case of issue uncompressing the archive.

    """
    try:
        if not os.path.exists(tarpath):
            raise ValueError(f'{tarpath} not found')
        filename = os.path.basename(tarpath)
        output_directory = os.path.join(extract_dir, filename)
        os.makedirs(output_directory, exist_ok=True)
        from subprocess import run
        run(['tar', 'xf', tarpath, '-C', output_directory])
        # data = os.listdir(output_directory)
        # assert len(data) > 0
        return output_directory
    except Exception as e:
        raise shutil.ReadError(
            f'Unable to uncompress {tarpath} to {extract_dir}. Reason: {e}')


def register_new_archive_formats():
    """Register new archive formats to uncompress

    """
    registered_formats = [f[0] for f in shutil.get_unpack_formats()]
    for format_id in ADDITIONAL_ARCHIVE_FORMATS:
        name = format_id[0]
        if name in registered_formats:
            continue
        shutil.register_unpack_format(
            name=format_id[0], extensions=format_id[1], function=format_id[2])


def uncompress(tarpath: str, dest: str):
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
        ValueError when the archive is not supported

    """
    try:
        shutil.unpack_archive(tarpath, extract_dir=dest)
    except shutil.ReadError:
        raise ValueError(f'File {tarpath} is not a supported archive.')

    # Fix permissions
    for dirpath, _, fnames in os.walk(dest):
        os.chmod(dirpath, 0o755)
        for fname in fnames:
            fpath = os.path.join(dirpath, fname)
            if not os.path.islink(fpath):
                fpath_exec = os.stat(fpath).st_mode & stat.S_IXUSR
                if not fpath_exec:
                    os.chmod(fpath, 0o644)


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


# Additional uncompression archive format support
ADDITIONAL_ARCHIVE_FORMATS = [
    # name  , extensions, function
    ('tar.Z|x', ['.tar.Z', '.tar.x'], unpack_specific_tar),
    # FIXME: make this optional depending on the runtime lzip package install
    ('tar.lz', ['.tar.lz'], unpack_specific_tar),
]
