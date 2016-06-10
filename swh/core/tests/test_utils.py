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

    @istest
    def backslashescape_errors(self):
        raw_data_err = b'abcd\x80'
        with self.assertRaises(UnicodeDecodeError):
            raw_data_err.decode('utf-8', 'strict')

        self.assertEquals(
            raw_data_err.decode('utf-8', 'backslashescape'),
            'abcd\\x80',
        )

        raw_data_ok = b'abcd\xc3\xa9'
        self.assertEquals(
            raw_data_ok.decode('utf-8', 'backslashescape'),
            raw_data_ok.decode('utf-8', 'strict'),
        )

        unicode_data = 'abcdef\u00a3'
        self.assertEquals(
            unicode_data.encode('ascii', 'backslashescape'),
            b'abcdef\\xa3',
        )

    @istest
    def decode_invalid(self):
        # given
        invalid_str = b'my invalid \xff \xff string'

        # when
        actual_data = utils.decode_with_escape(invalid_str)

        # then
        self.assertEqual(actual_data, 'my invalid \\xff \\xff string')
