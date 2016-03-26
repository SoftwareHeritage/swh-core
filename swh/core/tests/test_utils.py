# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.core import utils


class UtilsTest(unittest.TestCase):
    @istest
    def grouper(self):
        # given
        gen = (d for d in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

        # when
        actual_group = []
        for data in utils.grouper(gen, 3):
            actual_group.append(list(data))

        # then
        self.assertEquals(actual_group, [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])
