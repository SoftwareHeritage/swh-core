# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest
import os
import shutil

from nose.tools import istest

from swh.core import config


class ConfReaderTest(unittest.TestCase):

    @classmethod
    def setUp(cls):
        # create a temporary folder
        cls.tmpdir = tempfile.mkdtemp(prefix='test-swh-core.')
        cls.conffile = os.path.join(cls.tmpdir, 'config.ini')
        with open(cls.conffile, 'w') as conf:
            conf.write("""[main]
a = 1
b = this is a string
c = true
""")

    @classmethod
    def tearDown(cls):
        shutil.rmtree(cls.tmpdir)

    @istest
    def read(self):
        # given
        default_conf = {
            'a': ('int', 2),
            'b': ('string', 'default-string'),
            'c': ('bool', True),
            'd': ('int', 10),
            'e': ('int', None),
            'f': ('bool', None),
            'g': ('string', None),
        }

        # when
        res = config.read(self.conffile, default_conf)

        # then
        self.assertEquals(res, {
            'a': 1,
            'b': 'this is a string',
            'c': True,
            'd': 10,
            'e': None,
            'f': None,
            'g': None,
        })

    @istest
    def prepare_folder(self):
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
