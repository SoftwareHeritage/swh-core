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

# Default algorithms when not mentioned
KNOWN_ALGORITHMS = ALGORITHMS | set(['sha1_blob_git', 'sha1_tree_git',
                                     'sha1_commit_git'])

# should be a multiple of 64 (sha1/sha256's block size)
# FWIW coreutils' sha1sum uses 32768
HASH_BLOCK_SIZE = 32768


def _new_hash(algo, length=None):
    """Initialize a digest object (as returned by python's hashlib) for the
    requested algorithm. See the constant ALGORITHMS for the list of supported
    algorithms. If a git-specific hashing algorithm is requested (e.g.,
    "sha1_git", "sha1_blob_git", "sha1_tree_git", "sha1_commit_git"), the
    hashing object will be pre-fed with the needed header; for
    this to work, length must be given.

    Args:
        algo: List of algorithms in ALGORITHMS
        length: Length of content to hash. Could be None if when hashing
        with sha1 and sha256

    Returns:
        A digest object

    Raises:
        ValueError when on sha1_*git algorithms with length to None
        ValueError when sha1_*git with * not in ('blob', 'commit', 'tree')

    """
    if algo not in KNOWN_ALGORITHMS:
        raise ValueError('unknown hashing algorithm ' + algo)

    h = None
    if algo.endswith('_git'):
        if length is None:
            raise ValueError('missing length for git hashing algorithm')

        algo_hash = algo.split('_')
        h = hashlib.new(algo_hash[0])
        obj_type = 'blob' if algo_hash[1] == 'git' else algo_hash[1]
        if obj_type not in ('blob', 'commit', 'tree'):
            raise ValueError(
                'For `a la git` sha1 computation, the only supported types are'
                ' blob, commit, tree')

        h.update(('%s %d\0' % (obj_type, length)).encode('ascii'))  # git hash header
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
        return _hash_file_obj(f, length, algorithms)


def hashfile(f, length=None, algorithms=ALGORITHMS):
    """Hash the content of a given file, given either as a file-like object or a
    file name. All specified hash algorithms will be computed, reading the file
    only once. Returns a dictionary mapping algorithm names to hex-encoded
    checksums.

    When passing a file-like object, content length must be given; when passing
    a file name, content length is ignored.

    """
    if isinstance(f, str):
        return _hash_fname(f, algorithms)
    else:
        return _hash_file_obj(f, length, algorithms)


def hashdata(data, algorithms=ALGORITHMS):
    """Like hashfile, but hashes content passed as a string (of bytes)

    """
    buf = BytesIO(data)
    return _hash_file_obj(buf, len(data), algorithms)


@functools.lru_cache()
def hash_to_hex(hash):
    """Converts a hash to its hexadecimal string representation"""
    return binascii.hexlify(hash).decode('ascii')


@functools.lru_cache()
def hex_to_hash(hex):
    """Converts a hexadecimal string representation of a hash to that hash"""
    return bytes.fromhex(hex)
