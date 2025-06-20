# Copyright (C) 2022-2025 zimoun and the Software Heritage developers
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import contextlib
import hashlib
import io
import os
from pathlib import Path
import stat
import struct
import tempfile
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Protocol, Union

from typing_extensions import Buffer

from swh.core.tarball import uncompress

CHUNK_SIZE = 65536


def _identity(hash: bytes) -> bytes:
    return hash


def _convert_hex(hash: bytes) -> str:
    return hash.hex()


def _convert_b64(hash: bytes) -> str:
    return base64.b64encode(bytes.fromhex(hash.hex())).decode()


_chars = "0123456789abcdfghijklmnpqrsvwxyz"


# base32 digest used in nix-hash, Python implementation found on
# https://bombrary.github.io/blog/posts/nix-impl-digest/
def _convert_b32(hash: bytes) -> str:
    hash_bits = 8 * len(hash)
    nix32_len = (hash_bits - 1) // 5 + 1
    s = ""
    for n in range(nix32_len - 1, -1, -1):
        b = n * 5
        i = b // 8
        j = b % 8
        c = (hash[i] >> j) | (hash[i + 1] << (8 - j) if i + 1 < len(hash) else 0)
        s = s + _chars[c & 0x1F]

    return s


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
        self.nar_serialization = bytearray()

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
        self.nar_serialization.extend(chunk)
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
        self.nar_serialization.clear()
        self.str_("nix-archive-1")
        self._serialize(fso)
        return bytes(self.nar_serialization)

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


# The code below is adapted from the narflinger project (https://github.com/wh0/narflinger)

# MIT License
#
# Copyright (c) 2023 wh0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


class _NARFileReader(Protocol):
    def read1(self, size: int) -> bytes: ...

    def finish(self) -> None: ...

    def close(self) -> None: ...


def _reader_read_limit(input: _NARFileReader, size: int) -> bytes:
    return input.read1(size)


def _reader_read_exact(input: _NARFileReader, size: int) -> bytes:
    piece = input.read1(size)
    piece_len = len(piece)
    if piece_len == size:
        return piece
    remaining = size - piece_len
    pieces = [piece]
    while remaining:
        piece = input.read1(remaining)
        pieces.append(piece)
        remaining -= len(piece)
    return b"".join(pieces)


def _reader_skip_exact(input: _NARFileReader, size: int) -> None:
    remaining = size
    while remaining:
        piece = input.read1(remaining)
        remaining -= len(piece)


def _nar_read_int(input: _NARFileReader) -> int:
    b = _reader_read_exact(input, 8)
    return struct.unpack("<Q", b)[0]


def _nar_skip_padding(input: _NARFileReader, length: int) -> None:
    modulo = length & 7
    if modulo:
        _reader_skip_exact(input, 8 - modulo)


def _nar_read_bytes(input: _NARFileReader) -> bytes:
    length = _nar_read_int(input)
    if not length:
        return b""
    b = _reader_read_exact(input, length)
    _nar_skip_padding(input, length)
    return b


def _nar_generate_binary(input: _NARFileReader) -> Iterator[bytes]:
    length = _nar_read_int(input)
    remaining = length
    while remaining:
        piece = _reader_read_limit(input, remaining)
        yield piece
        remaining -= len(piece)
    _nar_skip_padding(input, length)


def _nar_expect_bytes(input: _NARFileReader, expected: bytes) -> None:
    b = _nar_read_bytes(input)
    if b != expected:
        raise Exception("unexpected %r, expected %r" % (b, expected))


def _nar_generate_pair_keys(input: _NARFileReader) -> Iterator[bytes]:
    _nar_expect_bytes(input, b"(")
    while True:
        k = _nar_read_bytes(input)
        if k == b")":
            break
        yield k


def _nar_unpack_dir_entry(dest_path: str, input: _NARFileReader) -> None:
    name = None
    for k in _nar_generate_pair_keys(input):
        if k == b"name":
            name = _nar_read_bytes(input)
        elif k == b"node":
            assert name is not None
            _nar_unpack_node(os.path.join(dest_path, name.decode("utf-8")), input)
        else:
            raise Exception("dir entry unrecognized key %r" % k)


def _nar_unpack_node(dest_path: str, input: _NARFileReader) -> None:
    type = None
    executable = False
    for k in _nar_generate_pair_keys(input):
        if k == b"type":
            type = _nar_read_bytes(input)
            if type == b"regular":
                pass
            elif type == b"symlink":
                pass
            elif type == b"directory":
                os.mkdir(dest_path)
            else:
                raise Exception("unrecognized type %r" % type)
        elif k == b"executable":
            _nar_expect_bytes(input, b"")
            executable = True
        elif k == b"contents":
            dst_fd = os.open(
                dest_path, os.O_WRONLY | os.O_CREAT, 0o777 if executable else 0o666
            )
            for b in _nar_generate_binary(input):
                os.write(dst_fd, b)
            os.close(dst_fd)
        elif k == b"target":
            target = _nar_read_bytes(input)
            os.symlink(target, dest_path)
        elif k == b"entry":
            _nar_unpack_dir_entry(dest_path, input)
        else:
            raise Exception("node unrecognized key %r" % k)


def _nar_unpack(dest_path: str, reader: _NARFileReader) -> None:
    _nar_expect_bytes(reader, b"nix-archive-1")
    _nar_unpack_node(dest_path, reader)


decompress_empty = b""


class _Decompressor(Protocol):
    def decompress(self, data: Buffer, max_length: int = -1) -> bytes: ...
    @property
    def eof(self) -> bool: ...
    @property
    def needs_input(self) -> bool: ...


class _DecompressReader(_NARFileReader):
    def __init__(self, input: BinaryIO, decompressor: _Decompressor):
        self.input = input
        self.decompressor = decompressor

    def read1(self, size):
        while self.decompressor.needs_input:
            piece_in = self.input.read1(8192)
            piece = self.decompressor.decompress(piece_in, size)
            if piece:
                return piece
        piece = self.decompressor.decompress(decompress_empty, size)
        return piece

    def finish(self):
        piece_in = self.input.read()
        if not self.decompressor.eof:
            self.decompressor.decompress(piece_in)

    def close(self):
        self.input.close()


class _IdentityReader(_NARFileReader):
    def __init__(self, input: BinaryIO):
        self.input = input

    def read1(self, size):
        return self.input.read1(size)

    def finish(self):
        self.input.read()

    def close(self):
        self.input.close()


def _get_nar_reader(nar_archive_path: str) -> _NARFileReader:
    decompressor: Optional[_Decompressor] = None
    nar_reader: _NARFileReader
    if nar_archive_path.endswith(".bz2"):
        import bz2

        decompressor = bz2.BZ2Decompressor()
    elif nar_archive_path.endswith(".xz"):
        import lzma

        decompressor = lzma.LZMADecompressor(lzma.FORMAT_XZ)

    if decompressor is None:
        nar_reader = _IdentityReader(open(nar_archive_path, "rb"))
    else:
        nar_reader = _DecompressReader(open(nar_archive_path, "rb"), decompressor)

    return nar_reader


def nar_unpack(nar_path: str, dest_path: str) -> None:
    """Unpack a NAR archive (possibly compressed with xz or bz2) to a path.

    Please note that a nar archive can contain a single file instead of multiple
    files and directories, in that case ``dest_path`` will target a file after
    the unpacking.

    Args:
        nar_archive_path: A path to a NAR archive.
        dest_path: The destination path where the NAR archive is extracted.
    """
    with contextlib.closing(_get_nar_reader(nar_path)) as nar_reader:
        _nar_unpack(dest_path, nar_reader)
        nar_reader.finish()
