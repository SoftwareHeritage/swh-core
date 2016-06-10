# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import itertools
import codecs


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


def decode_with_escape(value):
    """Decode a bytestring as utf-8, escaping the bytes of invalid utf-8 sequences
    as \\x<hex value>. We also escape NUL bytes as they are invalid in JSON
    strings.
    """
    # escape backslashes
    value = value.replace(b'\\', b'\\\\')
    value = value.replace(b'\x00', b'\\x00')
    return value.decode('utf-8', 'backslashescape')
