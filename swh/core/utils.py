# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import itertools
import codecs
import re

from contextlib import contextmanager


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
    """Collect data into fixed-length chunks or blocks.

    Args:
        iterable: an iterable
        n: size of block
        fillvalue: value to use for the last block

    Returns:
        fixed-length chunks of blocks as iterables

    """
    args = [iter(iterable)] * n
    for _data in itertools.zip_longest(*args, fillvalue=None):
        yield (d for d in _data if d is not None)


def backslashescape_errors(exception):
    if isinstance(exception, UnicodeDecodeError):
        bad_data = exception.object[exception.start:exception.end]
        escaped = ''.join(r'\x%02x' % x for x in bad_data)
        return escaped, exception.end

    return codecs.backslashreplace_errors(exception)


codecs.register_error('backslashescape', backslashescape_errors)


def encode_with_unescape(value):
    """Encode an unicode string containing \\x<hex> backslash escapes"""
    slices = []
    start = 0
    odd_backslashes = False
    i = 0
    while i < len(value):
        if value[i] == '\\':
            odd_backslashes = not odd_backslashes
        else:
            if odd_backslashes:
                if value[i] != 'x':
                    raise ValueError('invalid escape for %r at position %d' %
                                     (value, i-1))
                slices.append(
                    value[start:i-1].replace('\\\\', '\\').encode('utf-8')
                )
                slices.append(bytes.fromhex(value[i+1:i+3]))

                odd_backslashes = False
                start = i = i + 3
                continue

        i += 1

    slices.append(
        value[start:i].replace('\\\\', '\\').encode('utf-8')
    )

    return b''.join(slices)


def decode_with_escape(value):
    """Decode a bytestring as utf-8, escaping the bytes of invalid utf-8 sequences
    as \\x<hex value>. We also escape NUL bytes as they are invalid in JSON
    strings.
    """
    # escape backslashes
    value = value.replace(b'\\', b'\\\\')
    value = value.replace(b'\x00', b'\\x00')
    return value.decode('utf-8', 'backslashescape')


def commonname(path0, path1, as_str=False):
    """Compute the commonname between the path0 and path1.

    """
    return path1.split(path0)[1]


def numfile_sortkey(fname):
    """Simple function to sort filenames of the form:

      nnxxx.ext

    where nn is a number according to the numbers.

    Typically used to sort sql/nn-swh-xxx.sql files.
    """
    num, rem = re.match(r'(\d*)(.*)', fname).groups()
    return (num and int(num) or 99, rem)
