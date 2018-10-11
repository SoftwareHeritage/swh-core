# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile
import unittest

from swh.core import config


class ConfReaderTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # create a temporary folder
        cls.tmpdir = tempfile.mkdtemp(prefix='test-swh-core.')
        cls.conffile = os.path.join(cls.tmpdir, 'config.ini')
        conf_contents = """[main]
a = 1
b = this is a string
c = true
h = false
ls = list, of, strings
li = 1, 2, 3, 4
"""
        with open(cls.conffile, 'w') as conf:
            conf.write(conf_contents)

        cls.non_existing_conffile = os.path.join(cls.tmpdir,
                                                 'config-nonexisting.ini')

        # Create an unreadable, proper configuration file
        cls.perms_broken_file = os.path.join(cls.tmpdir, 'unreadable.ini')
        with open(cls.perms_broken_file, 'w') as conf:
            conf.write(conf_contents)
        os.chmod(cls.perms_broken_file, 0o000)

        # Create a proper configuration file in an unreadable directory
        cls.perms_broken_dir = os.path.join(cls.tmpdir, 'unreadabledir')
        cls.file_in_broken_dir = os.path.join(cls.perms_broken_dir,
                                              'unreadable.ini')
        os.makedirs(cls.perms_broken_dir)
        with open(cls.file_in_broken_dir, 'w') as conf:
            conf.write(conf_contents)
        os.chmod(cls.perms_broken_dir, 0o000)

        cls.empty_conffile = os.path.join(cls.tmpdir, 'empty.ini')
        open(cls.empty_conffile, 'w').close()

        cls.default_conf = {
            'a': ('int', 2),
            'b': ('string', 'default-string'),
            'c': ('bool', True),
            'd': ('int', 10),
            'e': ('int', None),
            'f': ('bool', None),
            'g': ('string', None),
            'h': ('bool', True),
            'i': ('bool', True),
            'ls': ('list[str]', ['a', 'b', 'c']),
            'li': ('list[int]', [42, 43]),
        }

        cls.other_default_conf = {
            'a': ('int', 3),
        }

        cls.full_default_conf = cls.default_conf.copy()
        cls.full_default_conf['a'] = cls.other_default_conf['a']

        cls.parsed_default_conf = {
            key: value
            for key, (type, value)
            in cls.default_conf.items()
        }

        cls.parsed_conffile = {
            'a': 1,
            'b': 'this is a string',
            'c': True,
            'd': 10,
            'e': None,
            'f': None,
            'g': None,
            'h': False,
            'i': True,
            'ls': ['list', 'of', 'strings'],
            'li': [1, 2, 3, 4],
        }

    @classmethod
    def tearDownClass(cls):
        # Make the broken perms items readable again to be able to remove them
        os.chmod(cls.perms_broken_dir, 0o755)
        os.chmod(cls.perms_broken_file, 0o644)
        shutil.rmtree(cls.tmpdir)

    def test_read(self):
        # when
        res = config.read(self.conffile, self.default_conf)

        # then
        self.assertEquals(res, self.parsed_conffile)

    def test_read_empty_file(self):
        # when
        res = config.read(None, self.default_conf)

        # then
        self.assertEquals(res, self.parsed_default_conf)

    def test_support_non_existing_conffile(self):
        # when
        res = config.read(self.non_existing_conffile, self.default_conf)

        # then
        self.assertEquals(res, self.parsed_default_conf)

    def test_support_empty_conffile(self):
        # when
        res = config.read(self.empty_conffile, self.default_conf)

        # then
        self.assertEquals(res, self.parsed_default_conf)

    def test_raise_on_broken_directory_perms(self):
        with self.assertRaises(PermissionError):
            config.read(self.file_in_broken_dir, self.default_conf)

    def test_raise_on_broken_file_perms(self):
        with self.assertRaises(PermissionError):
            config.read(self.perms_broken_file, self.default_conf)

    def test_merge_default_configs(self):
        # when
        res = config.merge_default_configs(self.default_conf,
                                           self.other_default_conf)

        # then
        self.assertEquals(res, self.full_default_conf)

    def test_priority_read_nonexist_conf(self):
        # when
        res = config.priority_read([self.non_existing_conffile, self.conffile],
                                   self.default_conf)

        # then
        self.assertEquals(res, self.parsed_conffile)

    def test_priority_read_conf_nonexist_empty(self):
        # when
        res = config.priority_read([
            self.conffile,
            self.non_existing_conffile,
            self.empty_conffile,
        ], self.default_conf)

        # then
        self.assertEquals(res, self.parsed_conffile)

    def test_priority_read_empty_conf_nonexist(self):
        # when
        res = config.priority_read([
            self.empty_conffile,
            self.conffile,
            self.non_existing_conffile,
        ], self.default_conf)

        # then
        self.assertEquals(res, self.parsed_default_conf)

    def test_swh_config_paths(self):
        res = config.swh_config_paths('foo/bar.ini')

        self.assertEqual(res, [
            '~/.config/swh/foo/bar.ini',
            '~/.swh/foo/bar.ini',
            '/etc/softwareheritage/foo/bar.ini',
        ])

    def test_prepare_folder(self):
        # given
        conf = {'path1': os.path.join(self.tmpdir, 'path1'),
                'path2': os.path.join(self.tmpdir, 'path2', 'depth1')}

        # the folders does not exists
        self.assertFalse(os.path.exists(conf['path1']),
                         "path1 should not exist.")
        self.assertFalse(os.path.exists(conf['path2']),
                         "path2 should not exist.")

        # when
        config.prepare_folders(conf, 'path1')

        # path1 exists but not path2
        self.assertTrue(os.path.exists(conf['path1']),
                        "path1 should now exist!")
        self.assertFalse(os.path.exists(conf['path2']),
                         "path2 should not exist.")

        # path1 already exists, skips it but creates path2
        config.prepare_folders(conf, 'path1', 'path2')

        self.assertTrue(os.path.exists(conf['path1']),
                        "path1 should still exist!")
        self.assertTrue(os.path.exists(conf['path2']),
                        "path2 should now exist.")
