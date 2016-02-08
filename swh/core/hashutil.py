# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import functools
import hashlib
import os

from io import BytesIO

# supported hashing algorithms
ALGORITHMS = set(['sha1', 'sha256', 'sha1_git'])

# should be a multiple of 64 (sha1/sha256's block size)
# FWIW coreutils' sha1sum uses 32768
HASH_BLOCK_SIZE = 32768


def _new_git_hash(base_algo, git_type, length):
    """Initialize a digest object (as returned by python's hashlib) for the
    requested algorithm, and feed it with the header for a git object of the
    given type and length.

    The header for hashing a git object consists of:
     - The type of the object (encoded in ASCII)
     - One ASCII space (\x20)
     - The length of the object (decimal encoded in ASCII)
     - One NUL byte

    Args:
        base_algo: a hashlib-supported algorithm
        git_type: the type of the git object (supposedly one of 'blob',
                  'commit', 'tag', 'tree')
        length: the length of the git object you're encoding

    Returns:
        a hashutil.hash object
    """

    h = hashlib.new(base_algo)
    git_header = '%s %d\0' % (git_type, length)
    h.update(git_header.encode('ascii'))

    return h


def _new_hash(algo, length=None):
    """Initialize a digest object (as returned by python's hashlib) for the
    requested algorithm. See the constant ALGORITHMS for the list of supported
    algorithms. If a git-specific hashing algorithm is requested (e.g.,
    "sha1_git"), the hashing object will be pre-fed with the needed header; for
    this to work, length must be given.

    """
    if algo not in ALGORITHMS:
        raise ValueError('unknown hashing algorithm ' + algo)

    h = None
    if algo.endswith('_git'):
        if length is None:
            raise ValueError('missing length for git hashing algorithm')
        base_algo = algo[:-4]
        h = _new_git_hash(base_algo, 'blob', length)
    else:
        h = hashlib.new(algo)

    return h


def _hash_file_obj(f, length, algorithms=ALGORITHMS, chunk_cb=None):
    """hash the content of a file-like object

    If chunk_cb is given, call it on each data chunk after updating the hash

    """
    hashers = {algo: _new_hash(algo, length)
               for algo in algorithms}
    while True:
        chunk = f.read(HASH_BLOCK_SIZE)
        if not chunk:
            break
        for h in hashers.values():
            h.update(chunk)
        if chunk_cb:
            chunk_cb(chunk)

    return {algo: hashers[algo].digest() for algo in hashers}


def _hash_fname(fname, algorithms=ALGORITHMS):
    """hash the content of a file specified by file name

    """
    length = os.path.getsize(fname)
    with open(fname, 'rb') as f:
        return _hash_file_obj(f, length)


def hashfile(f, length=None, algorithms=ALGORITHMS):
    """Hash the content of a given file, given either as a file-like object or a
    file name. All specified hash algorithms will be computed, reading the file
    only once. Returns a dictionary mapping algorithm names to hex-encoded
    checksums.

    When passing a file-like object, content length must be given; when passing
    a file name, content length is ignored.

    """
    if isinstance(f, (str, bytes)):
        return _hash_fname(f, algorithms)
    else:
        return _hash_file_obj(f, length, algorithms)


def hashdata(data, algorithms=ALGORITHMS):
    """Like hashfile, but hashes content passed as a string (of bytes)

    """
    buf = BytesIO(data)
    return _hash_file_obj(buf, len(data), algorithms)


def hash_git_object(git_object, git_type, hash_algo='sha1'):
    """Hash a git_object of git_type using hash_algo.

    Args:
        git_object: a bytestring containing a git object
        git_type: one of ('blob', 'commit', 'tag', 'tree')
        hash_algo: one of BASE_ALGORITHMS
    Returns:
        The resulting hashutil.hash object, fed with all the needed data.
    """

    git_types = ('blob', 'commit', 'tag', 'tree')
    if git_type not in git_types:
        raise ValueError('Unexpected git object type %s. Expected one of %s' %
                         (git_type, ', '.join(git_types)))

    length = len(git_object)

    h = _new_git_hash(hash_algo, git_type, length)
    h.update(git_object)

    return h


@functools.lru_cache()
def hash_to_hex(hash):
    """Converts a hash to its hexadecimal string representation"""
    return hash_to_bytehex(hash).decode('ascii')


@functools.lru_cache()
def hash_to_bytehex(hash):
    """Converts a hash to its hexadecimal bytes representation"""
    return binascii.hexlify(hash)


@functools.lru_cache()
def hex_to_hash(hex):
    """Converts a hexadecimal string representation of a hash to that hash"""
    return bytes.fromhex(hex)


@functools.lru_cache()
def bytehex_to_hash(hex):
    """Converts a hexadecimal bytes representation of a hash to that hash"""
    return hex_to_hash(hex.decode())
