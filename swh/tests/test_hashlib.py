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
        self.checksums = {
            'sha1':     '34973274ccef6ab4dfaaf86599792fa9c3fe4689',
            'sha1_git': 'd81cc0710eb6cf9efd5b920a8453e1e07157b6cd',
            'sha256':   '084c799cd551dd1d8d5c5f9a5d593b2e931f5e36122ee5c793c1d08a19839cc0',  # NOQA
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
