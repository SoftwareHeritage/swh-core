# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.core import utils


class UtilsLib(unittest.TestCase):

    def test_grouper(self):
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

    def test_backslashescape_errors(self):
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

    def test_encode_with_unescape(self):
        valid_data = '\\x01020304\\x00'
        valid_data_encoded = b'\x01020304\x00'

        self.assertEquals(
            valid_data_encoded,
            utils.encode_with_unescape(valid_data)
        )

    def test_encode_with_unescape_invalid_escape(self):
        invalid_data = 'test\\abcd'

        with self.assertRaises(ValueError) as exc:
            utils.encode_with_unescape(invalid_data)

        self.assertIn('invalid escape', exc.exception.args[0])
        self.assertIn('position 4', exc.exception.args[0])

    def test_decode_with_escape(self):
        backslashes = b'foo\\bar\\\\baz'
        backslashes_escaped = 'foo\\\\bar\\\\\\\\baz'

        self.assertEquals(
            backslashes_escaped,
            utils.decode_with_escape(backslashes),
        )

        valid_utf8 = b'foo\xc3\xa2'
        valid_utf8_escaped = 'foo\u00e2'

        self.assertEquals(
            valid_utf8_escaped,
            utils.decode_with_escape(valid_utf8),
        )

        invalid_utf8 = b'foo\xa2'
        invalid_utf8_escaped = 'foo\\xa2'

        self.assertEquals(
            invalid_utf8_escaped,
            utils.decode_with_escape(invalid_utf8),
        )

        valid_utf8_nul = b'foo\xc3\xa2\x00'
        valid_utf8_nul_escaped = 'foo\u00e2\\x00'

        self.assertEquals(
            valid_utf8_nul_escaped,
            utils.decode_with_escape(valid_utf8_nul),
        )

    def test_commonname(self):
        # when
        actual_commonname = utils.commonname('/some/where/to/',
                                             '/some/where/to/go/to')
        # then
        self.assertEquals('go/to', actual_commonname)

        # when
        actual_commonname2 = utils.commonname(b'/some/where/to/',
                                              b'/some/where/to/go/to')
        # then
        self.assertEquals(b'go/to', actual_commonname2)
