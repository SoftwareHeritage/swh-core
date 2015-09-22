# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.core import scheduling


class Scheduling(unittest.TestCase):

    @istest
    def not_implemented_task(self):
        class NotImplementedTask(scheduling.Task):
            pass

        with self.assertRaises(NotImplementedError):
            NotImplementedTask().run()

    @istest
    def add_task(self):
        class AddTask(scheduling.Task):
            def run(self, x, y):
                return x + y

        r = AddTask().apply([2, 3])
        self.assertTrue(r.successful())
        self.assertEqual(r.result, 5)
