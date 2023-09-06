# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import stat
from subprocess import run
import tarfile
import zipfile

import magic

from . import utils

MIMETYPE_TO_ARCHIVE_FORMAT = {
    "application/x-compress": "tar.Z|x",
    "application/x-tar": "tar",
    "application/x-bzip2": "bztar",
    "application/gzip": "gztar",
    "application/x-gzip": "gztar",
    "application/x-lzip": "tar.lz",
    "application/zip": "zip",
    "application/java-archive": "jar",
    "application/zstd": "tar.zst",
    "application/x-zstd": "tar.zst",
}


def _unpack_tar(tarpath: str, extract_dir: str) -> str:
    """Unpack tarballs unsupported by the standard python library. Examples
    include tar.Z, tar.lz, tar.x, etc....

    As this implementation relies on the `tar` command, this function supports
    the same compression the tar command supports.

    This expects the `extract_dir` to exist.

    Raises:
        shutil.ReadError in case of issue uncompressing the archive (tarpath
        does not exist, extract_dir does not exist, etc...)

    Returns:
        full path to the uncompressed directory.

    """
    try:
        run(["tar", "xf", tarpath, "-C", extract_dir], check=True)
        return extract_dir
    except Exception as e:
        raise shutil.ReadError(
            f"Unable to uncompress {tarpath} to {extract_dir}. Reason: {e}"
        )


def _unpack_zip(zippath: str, extract_dir: str) -> str:
    """Unpack zip files unsupported by the standard python library, for instance
    those with legacy compression type 6 (implode).

    This expects the `extract_dir` to exist.

    Raises:
        shutil.ReadError in case of issue uncompressing the archive (zippath
        does not exist, extract_dir does not exist, etc...)

    Returns:
        full path to the uncompressed directory.

    """
    try:
        run(["unzip", "-q", "-d", extract_dir, zippath], check=True)
        return extract_dir
    except Exception as e:
        raise shutil.ReadError(
            f"Unable to uncompress {zippath} to {extract_dir}. Reason: {e}"
        )


def _unpack_jar(jarpath: str, extract_dir: str) -> str:
    """Unpack jar files using standard Python module zipfile.

    This expects the `extract_dir` to exist.

    Raises:
        shutil.ReadError in case of issue uncompressing the archive (jarpath
        does not exist, extract_dir does not exist, etc...)

    Returns:
        full path to the uncompressed directory.

    """
    try:
        with zipfile.ZipFile(jarpath) as jar:
            jar.extractall(path=extract_dir)
        return extract_dir
    except Exception as e:
        raise shutil.ReadError(
            f"Unable to uncompress {jarpath} to {extract_dir}. Reason: {e}"
        )


def _unpack_zst(zstpath: str, extract_dir: str) -> str:
    """Unpack zst files unsupported by the standard python library. Example
    include tar.zst

    This expects the `extract_dir` to exist.

    Raises:
        shutil.ReadError in case of issue uncompressing the archive (zstpath
    """
    try:
        run(
            ["tar", "--force-local", "-I 'zstd'", "-xf", zstpath, "-C", extract_dir],
            check=True,
        )
        return extract_dir
    except Exception as e:
        raise shutil.ReadError(
            f"Unable to uncompress {zstpath} to {extract_dir}. Reason: {e}"
        )


def register_new_archive_formats():
    """Register new archive formats to uncompress"""
    registered_formats = [f[0] for f in shutil.get_unpack_formats()]
    for name, extensions, function in ADDITIONAL_ARCHIVE_FORMATS:
        if name in registered_formats:
            continue
        shutil.register_unpack_format(name, extensions, function)


def uncompress(tarpath: str, dest: str):
    """Uncompress tarpath to dest folder if tarball is supported.

       Note that this fixes permissions after successfully
       uncompressing the archive.

    Args:
        tarpath: path to tarball to uncompress
        dest: the destination folder where to uncompress the tarball,
            it will be created if it does not exist

    Raises:
        ValueError when a problem occurs during unpacking

    """
    try:
        os.makedirs(dest, exist_ok=True)
        format = None
        # try to get archive format from file mimetype
        m = magic.Magic(mime=True)
        mime = m.from_file(tarpath)
        format = MIMETYPE_TO_ARCHIVE_FORMAT.get(mime)
        if format is None:
            # try to get archive format from extension
            for format_, exts, _ in shutil.get_unpack_formats():
                if any([tarpath.lower().endswith(ext.lower()) for ext in exts]):
                    format = format_
                    break
        shutil.unpack_archive(tarpath, extract_dir=dest, format=format)
    except shutil.ReadError as e:
        raise ValueError(f"Problem during unpacking {tarpath}. Reason: {e}")
    except NotImplementedError:
        if tarpath.lower().endswith(".zip") or format == "zip":
            _unpack_zip(tarpath, dest)
        else:
            raise
    except NotADirectoryError:
        if format and "tar" in format:
            # some old tarballs might fail to be unpacked by shutil.unpack_archive,
            # fallback using the tar command as last resort
            _unpack_tar(tarpath, dest)
        else:
            raise

    normalize_permissions(dest)


def normalize_permissions(path: str):
    """Normalize the permissions of all files and directories under `path`.

    This makes all subdirectories and files with the user executable bit set mode
    0o0755, and all other files mode 0o0644.

    Args:
      path: the path under which permissions should be normalized
    """
    os.chmod(path, 0o0755)
    for dirpath, dnames, fnames in os.walk(path):
        for dname in dnames:
            dpath = os.path.join(dirpath, dname)
            os.chmod(dpath, 0o0755)
        for fname in fnames:
            fpath = os.path.join(dirpath, fname)
            if not os.path.islink(fpath):
                is_executable = os.stat(fpath).st_mode & stat.S_IXUSR
                forced_mode = 0o0755 if is_executable else 0o0644
                os.chmod(fpath, forced_mode)


def _ls(rootdir):
    """Generator of filepath, filename from rootdir."""
    for dirpath, dirnames, fnames in os.walk(rootdir):
        for fname in dirnames + fnames:
            fpath = os.path.join(dirpath, fname)
            fname = utils.commonname(rootdir, fpath)
            yield fpath, fname


def _compress_zip(tarpath, files):
    """Compress dirpath's content as tarpath."""
    with zipfile.ZipFile(tarpath, "w") as z:
        for fpath, fname in files:
            z.write(fpath, arcname=fname)


def _compress_tar(tarpath, files):
    """Compress dirpath's content as tarpath."""
    with tarfile.open(tarpath, "w:bz2") as t:
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

    if nature == "zip":
        _compress_zip(tarpath, files)
    else:
        _compress_tar(tarpath, files)

    return tarpath


# Additional uncompression archive format support
ADDITIONAL_ARCHIVE_FORMATS = [
    # name, extensions, function
    ("tar.Z|x", [".tar.Z", ".tar.x"], _unpack_tar),
    ("jar", [".jar", ".war"], _unpack_jar),
    ("tbz2", [".tbz", "tbz2"], _unpack_tar),
    # FIXME: make this optional depending on the runtime lzip package install
    ("tar.lz", [".tar.lz"], _unpack_tar),
    ("crate", [".crate"], _unpack_tar),
    ("tar.zst", [".tar.zst", ".tar.zstd"], _unpack_zst),
]

register_new_archive_formats()
