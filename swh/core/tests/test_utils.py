# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.core import utils


class UtilsLib(unittest.TestCase):

    @istest
    def grouper(self):
        # given
        actual_data = utils.grouper((i for i in range(0, 9)), 2)

        out = []
        for d in actual_data:
            out.append(list(d))  # force generator resolution for checks

        self.assertEqual(out, [[0, 1], [2, 3], [4, 5], [6, 7], [8]])

        # given
        actual_data = utils.grouper((i for i in range(9, 0, -1)), 4)

        out = []
        for d in actual_data:
            out.append(list(d))  # force generator resolution for checks

        self.assertEqual(out, [[9, 8, 7, 6], [5, 4, 3, 2], [1]])
