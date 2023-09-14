# Copyright (C) 2016-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import codecs
from contextlib import contextmanager
import itertools
import os
import re
from typing import Iterable, Tuple, TypeVar


@contextmanager
def cwd(path):
    """Contextually change the working directory to do thy bidding.
    Then gets back to the original location.

    """
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def grouper(iterable, n):
    """
    Collect data into fixed-length size iterables. The last block might
    contain less elements as it will hold only the remaining number
    of elements.

    The invariant here is that the number of elements in the input
    iterable and the sum of the number of elements of all iterables
    generated from this function should be equal.

    If ``iterable`` is an iterable of bytes or strings that you need to join
    later, then :func:`iter_chunks`` is preferable, as it avoids this join
    by slicing directly.

    Args:
        iterable (Iterable): an iterable
        n (int): size of block to slice the iterable into

    Yields:
        fixed-length blocks as iterables. As mentioned, the last
        iterable might be less populated.

    """
    args = [iter(iterable)] * n
    stop_value = object()
    for _data in itertools.zip_longest(*args, fillvalue=stop_value):
        yield (d for d in _data if d is not stop_value)


TStr = TypeVar("TStr", bytes, str)


def iter_chunks(
    iterable: Iterable[TStr], chunk_size: int, *, remainder: bool = False
) -> Iterable[TStr]:
    """
    Reads ``bytes`` objects (resp. ``str`` objects) from the ``iterable``,
    and yields them as chunks of exactly ``chunk_size`` bytes (resp. characters).

    ``iterable`` is typically obtained by repeatedly calling a method like
    :meth:`io.RawIOBase.read`; which does only guarantees an upper bound on the size;
    whereas this function returns chunks of exactly the size.

    Args:
        iterable: the input data
        chunk_size: the exact size of chunks to return
        remainder: if True, a last chunk with size strictly smaller than ``chunk_size``
          may be returned, if the data stream from the ``iterable`` had a length that
          is not a multiple of ``chunk_size``
    """
    buf = None
    iterator = iter(iterable)
    while True:
        assert buf is None or len(buf) < chunk_size
        try:
            new_data = next(iterator)
        except StopIteration:
            if remainder and buf:
                yield buf  # may be shorter than ``chunk_size``
            return

        if buf:
            buf += new_data
        else:
            # spares a copy
            buf = new_data

        new_buf = None
        for i in range(0, len(buf), chunk_size):
            chunk = buf[i : i + chunk_size]
            if len(chunk) == chunk_size:
                yield chunk
            else:
                assert not new_buf
                new_buf = chunk
        buf = new_buf


def backslashescape_errors(exception):
    if isinstance(exception, UnicodeDecodeError):
        bad_data = exception.object[exception.start : exception.end]
        escaped = "".join(r"\x%02x" % x for x in bad_data)
        return escaped, exception.end

    return codecs.backslashreplace_errors(exception)


codecs.register_error("backslashescape", backslashescape_errors)


def encode_with_unescape(value):
    """Encode an unicode string containing \\x<hex> backslash escapes"""
    slices = []
    start = 0
    odd_backslashes = False
    i = 0
    while i < len(value):
        if value[i] == "\\":
            odd_backslashes = not odd_backslashes
        else:
            if odd_backslashes:
                if value[i] != "x":
                    raise ValueError(
                        "invalid escape for %r at position %d" % (value, i - 1)
                    )
                slices.append(
                    value[start : i - 1].replace("\\\\", "\\").encode("utf-8")
                )
                slices.append(bytes.fromhex(value[i + 1 : i + 3]))

                odd_backslashes = False
                start = i = i + 3
                continue

        i += 1

    slices.append(value[start:i].replace("\\\\", "\\").encode("utf-8"))

    return b"".join(slices)


def decode_with_escape(value):
    """Decode a bytestring as utf-8, escaping the bytes of invalid utf-8 sequences
    as \\x<hex value>. We also escape NUL bytes as they are invalid in JSON
    strings.
    """
    # escape backslashes
    value = value.replace(b"\\", b"\\\\")
    value = value.replace(b"\x00", b"\\x00")
    return value.decode("utf-8", "backslashescape")


def commonname(path0, path1, as_str=False):
    """Compute the commonname between the path0 and path1."""
    return path1.split(path0)[1]


def numfile_sortkey(fname: str) -> Tuple[int, str]:
    """Simple function to sort filenames of the form:

      nnxxx.ext

    where nn is a number according to the numbers.

    Returns a tuple (order, remaining), where 'order' is the numeric (int)
    value extracted from the file name, and 'remaining' is the remaining part
    of the file name.

    Typically used to sort sql/nn-swh-xxx.sql files.

    Unmatched file names will return 999999 as order value.

    """
    m = re.match(r"(\d*)(.*)", fname)
    assert m is not None
    num, rem = m.groups()
    return (int(num) if num else 999999, rem)


def basename_sortkey(fname: str) -> Tuple[int, str]:
    "like numfile_sortkey but on basenames"
    return numfile_sortkey(os.path.basename(fname))
