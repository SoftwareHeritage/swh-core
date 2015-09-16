# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import unittest
import os
import shutil

from nose.tools import istest

from swh.core.conf import reader


def prepare_dummy_conf_file(tmp_dir):
    tmp_conf_file = tempfile.NamedTemporaryFile(mode='w',
                                                suffix="swh-core-test-read-conf",
                                                dir=tmp_dir,
                                                delete=False)
    with open(tmp_conf_file.name, 'w') as f:
        f.write("""[main]
a = 1
b = this is a string
c = true
""")
    return tmp_conf_file.name


class ConfReaderTest(unittest.TestCase):

    @classmethod
    def setUp(self):
        # create a temporary folder
        self.tmp_work_folder = tempfile.mkdtemp(prefix='test-swh-core.',
                                               dir='/tmp')

        self.tmp_conf_file = prepare_dummy_conf_file(self.tmp_work_folder)

    @classmethod
    def teardown(self):
        shutil.rmtree(self.tmp_work_folder)

    @istest
    def test_read(self):
        # given
        default_conf = {'a': ('int', 2),
                        'b': ('string', 'default-string'),
                        'c': ('bool', True),
                        'd': ('int', 10)}

        # when
        res = reader.read(self.tmp_conf_file, default_conf)

        # then
        self.assertEquals(res, {'a': 1,
                                'b': 'this is a string',
                                'c': True,
                                'd': 10})

    @istest
    def test_prepare_folder(self):
        # given
        conf = {'path1': self.tmp_work_folder + 'path1',
                'path2': self.tmp_work_folder + 'path2/depth1'}

        # the folders does not exists
        self.assertFalse(os.path.exists(conf['path1']), "path1 should not exist.")
        self.assertFalse(os.path.exists(conf['path2']), "path2 should not exist.")

        # when
        reader.prepare_folders(conf, 'path1')

        # path1 exists but not path2
        self.assertTrue(os.path.exists(conf['path1']), "path1 should now exist!")
        self.assertFalse(os.path.exists(conf['path2']), "path2 should not exist.")

        # path1 already exists, skips it but creates path2
        reader.prepare_folders(conf, 'path1', 'path2')

        self.assertTrue(os.path.exists(conf['path1']), "path1 should still exist!")
        self.assertTrue(os.path.exists(conf['path2']), "path2 should now exist.")
