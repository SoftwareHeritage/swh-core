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
    def hashfile_by_name_as_bytes(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write(self.data)
            f.flush()
            checksums = hashutil.hashfile(f.name.encode('utf-8'))
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


class HashlibGit(unittest.TestCase):

    def setUp(self):
        self.blob_data = b'42\n'

        self.tree_data = b''.join([b'40000 barfoo\0',
                                   bytes.fromhex('c3020f6bf135a38c6df'
                                                 '3afeb5fb38232c5e07087'),
                                   b'100644 blah\0',
                                   bytes.fromhex('63756ef0df5e4f10b6efa'
                                                 '33cfe5c758749615f20'),
                                   b'100644 hello\0',
                                   bytes.fromhex('907b308167f0880fb2a'
                                                 '5c0e1614bb0c7620f9dc3')])

        self.commit_data = """tree 1c61f7259dcb770f46b194d941df4f08ff0a3970
author Antoine R. Dumont (@ardumont) <antoine.romain.dumont@gmail.com> 1444054085 +0200
committer Antoine R. Dumont (@ardumont) <antoine.romain.dumont@gmail.com> 1444054085 +0200

initial
""".encode('utf-8')  # NOQA
        self.tag_data = """object 24d012aaec0bc5a4d2f62c56399053d6cc72a241
type commit
tag 0.0.1
tagger Antoine R. Dumont (@ardumont) <antoine.romain.dumont@gmail.com> 1444225145 +0200

blah
""".encode('utf-8')  # NOQA

        self.checksums = {
            'blob_sha1_git': bytes.fromhex('d81cc0710eb6cf9efd5b920a8453e1'
                                           'e07157b6cd'),
            'tree_sha1_git': bytes.fromhex('ac212302c45eada382b27bfda795db'
                                           '121dacdb1c'),
            'commit_sha1_git': bytes.fromhex('e960570b2e6e2798fa4cfb9af2c399'
                                             'd629189653'),
            'tag_sha1_git': bytes.fromhex('bc2b99ba469987bcf1272c189ed534'
                                          'e9e959f120'),
        }

    @istest
    def unknown_header_type(self):
        with self.assertRaises(ValueError) as cm:
            hashutil.hash_git_object(b'any-data', 'some-unknown-type')

        self.assertIn('Unexpected git object type', cm.exception.args[0])

    @istest
    def hashdata_content(self):
        # when
        hashobj = hashutil.hash_git_object(self.blob_data, 'blob')

        # then
        self.assertEqual(hashobj.digest(),
                         self.checksums['blob_sha1_git'])

    @istest
    def hashdata_tree(self):
        # when
        hashobj = hashutil.hash_git_object(self.tree_data, 'tree')

        # then
        self.assertEqual(hashobj.digest(),
                         self.checksums['tree_sha1_git'])

    @istest
    def hashdata_revision(self):
        # when
        hashobj = hashutil.hash_git_object(self.commit_data, 'commit')

        # then
        self.assertEqual(hashobj.digest(),
                         self.checksums['commit_sha1_git'])

    @istest
    def hashdata_tag(self):
        # when
        hashobj = hashutil.hash_git_object(self.tag_data, 'tag')

        # then
        self.assertEqual(hashobj.digest(),
                         self.checksums['tag_sha1_git'])
