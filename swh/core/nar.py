# Copyright (C) 2022-2025 zimoun and the Software Heritage developers
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import hashlib
import io
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, Dict, List, Optional, Union

from swh.core.tarball import uncompress

CHUNK_SIZE = 65536


def _identity(hash: bytes) -> bytes:
    return hash


def _convert_hex(hash: bytes) -> str:
    return hash.hex()


def _convert_b64(hash: bytes) -> str:
    return base64.b64encode(bytes.fromhex(hash.hex())).decode()


def _convert_b32(hash: bytes) -> str:
    return base64.b32encode(bytes.fromhex(hash.hex())).decode().lower()


class Nar:
    """NAR serializer.

    This builds the NAR structure and serializes it as per the phd thesis from Eelco
    Dolstra thesis. See https://edolstra.github.io/pubs/phd-thesis.pdf.

    For example, this tree on a filesystem:

    .. code::

       $ tree foo
       foo
       ├── bar
       │   └── exe
       └── baz

       1 directory, 2 files

    serializes as:

    .. code::

       nix-archive-1(typedirectoryentry(namebarnode(typedirectoryentry(nameexenode(typeregularexecutablecontents<Content of file foo/bar/exe>))))entry(namebaznode(typeregularcontents<Content of file foo/baz>)))

    For reability, the debug mode prints the following:

    .. code::

       nix-archive-1
         (
         type
         directory
           entry
           (
           name
           bar
           node
             (
             type
             directory
               entry
               (
               name
               exe
               node
                 (
                 type
                 regular
                 executable

                 contents
                 <Content of file foo/bar/exe>
                 )
               )
             )
           )
           entry
           (
           name
           baz
           node
             (
             type
             regular
             contents
             <Content of file foo/baz>
            )
          )
        )

    Note: "<Content of file $name>" is a placeholder for the actual file content

    """  # noqa

    def __init__(
        self,
        hash_names: List[str],
        exclude_vcs: bool = False,
        vcs_type: Optional[str] = "git",
        debug: bool = False,
    ):
        self.hash_names = hash_names
        self.updater = {
            hash_name: (
                hashlib.sha256() if hash_name.lower() == "sha256" else hashlib.sha1()
            )
            for hash_name in hash_names
        }
        self.exclude_vcs = exclude_vcs
        self.vcs_type = vcs_type
        self.debug = debug

        self.indent = 0
        self.nar_serialization = b""

    def str_(self, thing: Union[str, io.BufferedReader, list]) -> None:
        """Compute the nar serialization format on 'thing' and compute its hash.

        This is the function named named 'str' in Figure 5.2 p.93 (page 101 of pdf) [1]

        [1] https://edolstra.github.io/pubs/phd-thesis.pdf
        """
        if self.debug and isinstance(thing, (str, io.BufferedReader)):
            indent = "".join(["  " for _ in range(self.indent)])
            if isinstance(thing, io.BufferedReader):
                msg = f"{indent} <Content of file {thing.name}>"
            else:
                msg = f"{indent}{thing}"
            print(msg)

        # named 'int'
        if isinstance(thing, str):
            byte_sequence = thing.encode("utf-8")
            length = len(byte_sequence)
        elif isinstance(thing, io.BufferedReader):
            length = os.stat(thing.name).st_size
        # ease reading of _serialize
        elif isinstance(thing, list):
            for stuff in thing:
                self.str_(stuff)
            return
        else:
            raise ValueError("not string nor file")

        blen = length.to_bytes(8, byteorder="little")  # 64-bit little endian
        self.update(blen)

        # first part of 'pad'
        if isinstance(thing, str):
            self.update(byte_sequence)
        elif isinstance(thing, io.BufferedReader):
            for chunk in iter(lambda: thing.read(CHUNK_SIZE), b""):
                self.update(chunk)

        # second part of 'pad
        m = length % 8
        if m == 0:
            offset = 0
        else:
            offset = 8 - m
        boffset = bytearray(offset)
        self.update(boffset)

    def update(self, chunk: bytes) -> None:
        self.nar_serialization += chunk
        for hash_name in self.hash_names:
            self.updater[hash_name].update(chunk)

    def _serialize_directory(self, fso: Path) -> None:
        """On the first level of the main tree, we may have to skip some paths (e.g.
        .git, ...). Once those are ignored, we can serialize the remaining part of the
        entries.

        """
        path_to_ignore = (
            f"{fso}/.{self.vcs_type}" if self.exclude_vcs and self.vcs_type else None
        )
        for path in sorted(Path(fso).iterdir()):
            if path_to_ignore is None or not path.match(path_to_ignore):
                self._serializeEntry(path)

    def _serialize(self, fso: Path) -> None:
        if self.debug:
            self.indent += 1
        self.str_("(")

        mode = os.lstat(fso).st_mode

        if stat.S_ISREG(mode):
            self.str_(["type", "regular"])
            if mode & 0o111 != 0:
                self.str_(["executable", ""])
            self.str_("contents")
            with open(str(fso), "rb") as f:
                self.str_(f)

        elif stat.S_ISLNK(mode):
            self.str_(["type", "symlink", "target"])
            self.str_(os.readlink(fso))

        elif stat.S_ISDIR(mode):
            self.str_(["type", "directory"])
            self._serialize_directory(fso)
        else:
            raise ValueError("unsupported file type")

        self.str_(")")
        if self.debug:
            self.indent -= 1

    def _serializeEntry(self, fso: Path) -> None:
        if self.debug:
            self.indent += 1
        self.str_(["entry", "(", "name", fso.name, "node"])
        self._serialize(fso)
        self.str_(")")
        if self.debug:
            self.indent -= 1

    def serialize(self, fso: Path) -> bytes:
        self.nar_serialization = b""
        self.str_("nix-archive-1")
        self._serialize(fso)
        return self.nar_serialization

    def _compute_result(self, convert_fn) -> Dict[str, Any]:
        return {
            hash_name: convert_fn(self.updater[hash_name].digest())
            for hash_name in self.hash_names
        }

    def digest(self) -> Dict[str, bytes]:
        """Compute the hash results with bytes format."""
        return self._compute_result(_identity)

    def hexdigest(self) -> Dict[str, str]:
        """Compute the hash results with hex format."""
        return self._compute_result(_convert_hex)

    def b64digest(self) -> Dict[str, str]:
        """Compute the hash results with b64 format."""
        return self._compute_result(_convert_b64)

    def b32digest(self) -> Dict[str, str]:
        """Compute the hash results with b32 format."""
        return self._compute_result(_convert_b32)


def compute_nar_hashes(
    filepath: Path,
    hash_names: List[str] = ["sha256"],
    is_tarball=True,
    top_level=True,
) -> Dict[str, str]:
    """Compute nar checksums dict out of a filepath (tarball or plain file).

    If it's a tarball, this uncompresses the tarball in a temporary directory to compute
    the nar hashes (and then cleans it up).

    Args:
        filepath: The tarball (if is_tarball is True) or a filepath
        hash_names: The list of checksums to compute
        is_tarball: Whether filepath represents a tarball or not
        top_level: Whether we want to compute the top-level directory (of the tarball)
            hashes. This is only useful when used with 'is_tarball' at True.

    Returns:
        The dict of checksums values whose keys are present in hash_names.

    """
    with tempfile.TemporaryDirectory() as tmpdir:
        if is_tarball:
            directory_path = Path(tmpdir)
            directory_path.mkdir(parents=True, exist_ok=True)
            uncompress(str(filepath), dest=str(directory_path))

            if top_level:
                # Default behavior, pass the extracted tarball path root directory
                path_on_disk = directory_path
            else:
                # Pass along the first directory of the tarball
                path_on_disk = next(iter(directory_path.iterdir()))
        else:
            path_on_disk = filepath

        nar = Nar(hash_names)
        nar.serialize(path_on_disk)
        hashes = nar.hexdigest()

    return hashes


def nar_serialize(
    path: Path,
    exclude_vcs: bool = False,
    vcs_type: Optional[str] = "git",
) -> bytes:
    """Return the NAR serialization of a path.

    Args:
        path: The path to NAR serialize, can be a file or a directory.
        exclude_vcs: Whether to exclude VCS related directories (.git for instance).
        vcs_type: The type of VCS to exclude related directories, default to git.

    Returns:
        The NAR serialization of the path.
    """
    nar = Nar(hash_names=["sha256"], exclude_vcs=exclude_vcs, vcs_type=vcs_type)
    return nar.serialize(path)
