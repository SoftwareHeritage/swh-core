# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest

from nose.tools import istest

from swh.core import hashutil


class Hashlib(unittest.TestCase):

    def setUp(self):
        self.data = b'42\n'
        self.hex_checksums = {
            'sha1':     '34973274ccef6ab4dfaaf86599792fa9c3fe4689',
            'sha1_git': 'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd',
            'sha256':   '084c799cd551dd1d8d5c5f9a5d593b2e931f5e36'
            '122ee5c793c1d08a19839cc0',
            }
        self.checksums = {
            'sha1':     bytes.fromhex('34973274ccef6ab4dfaaf865997'
                                      '92fa9c3fe4689'),
            'sha1_git': bytes.fromhex('d81cc0710eb6cf9efd5b920a845'
                                      '3e1e07157b6cd'),
            'sha256':   bytes.fromhex('084c799cd551dd1d8d5c5f9a5d5'
                                      '93b2e931f5e36122ee5c793c1d0'
                                      '8a19839cc0'),
            }

    @istest
    def hashdata(self):
        checksums = hashutil.hashdata(self.data)
        self.assertEqual(checksums, self.checksums)

    @istest
    def unknown_algo(self):
        with self.assertRaises(ValueError):
            hashutil.hashdata(self.data, algorithms=['does-not-exist'])

    @istest
    def algo_selection(self):
        checksums = hashutil.hashdata(self.data, algorithms=['sha1', 'sha256'])
        self.assertIn('sha1', checksums)
        self.assertIn('sha256', checksums)
        self.assertNotIn('sha1_git', checksums)

    @istest
    def hashfile_by_name(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(self.data)
            f.flush()
            checksums = hashutil.hashfile(f.name)
            self.assertEqual(checksums, self.checksums)

    @istest
    def hashfile_by_obj(self):
        with tempfile.TemporaryFile() as f:
            f.write(self.data)
            f.seek(0)
            checksums = hashutil.hashfile(f, len(self.data))
            self.assertEqual(checksums, self.checksums)

    @istest
    def hex_to_hash(self):
        for algo in self.checksums:
            self.assertEqual(self.checksums[algo],
                             hashutil.hex_to_hash(self.hex_checksums[algo]))

    @istest
    def hash_to_hex(self):
        for algo in self.checksums:
            self.assertEqual(self.hex_checksums[algo],
                             hashutil.hash_to_hex(self.checksums[algo]))
