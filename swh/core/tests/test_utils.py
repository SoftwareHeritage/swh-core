# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core import utils


def test_grouper():
    # given
    actual_data = utils.grouper((i for i in range(0, 9)), 2)

    out = []
    for d in actual_data:
        out.append(list(d))  # force generator resolution for checks

    assert out == [[0, 1], [2, 3], [4, 5], [6, 7], [8]]

    # given
    actual_data = utils.grouper((i for i in range(9, 0, -1)), 4)

    out = []
    for d in actual_data:
        out.append(list(d))  # force generator resolution for checks

    assert out == [[9, 8, 7, 6], [5, 4, 3, 2], [1]]


def test_grouper_with_stop_value():
    # given
    actual_data = utils.grouper(((i, i + 1) for i in range(0, 9)), 2)

    out = []
    for d in actual_data:
        out.append(list(d))  # force generator resolution for checks

    assert out == [
        [(0, 1), (1, 2)],
        [(2, 3), (3, 4)],
        [(4, 5), (5, 6)],
        [(6, 7), (7, 8)],
        [(8, 9)],
    ]

    # given
    actual_data = utils.grouper((i for i in range(9, 0, -1)), 4)

    out = []
    for d in actual_data:
        out.append(list(d))  # force generator resolution for checks

    assert out == [[9, 8, 7, 6], [5, 4, 3, 2], [1]]


def test_iter_chunks():
    def chunks(input_, remainder):
        return list(utils.iter_chunks(input_, 3, remainder=remainder))

    # all even, remainder=False
    assert chunks(["ab", "cd", "ef"], False) == ["abc", "def"]
    assert chunks(["abc", "def"], False) == ["abc", "def"]
    assert chunks(["abcd", "ef"], False) == ["abc", "def"]

    # all even, remainder=True
    assert chunks(["ab", "cd", "ef"], True) == ["abc", "def"]
    assert chunks(["abc", "def"], True) == ["abc", "def"]
    assert chunks(["abcd", "ef"], True) == ["abc", "def"]

    # uneven, remainder=False
    assert chunks([], False) == []
    assert chunks(["ab"], False) == []
    assert chunks(["ab", "cd", "ef", "g"], False) == ["abc", "def"]
    assert chunks(["ab", "cd", "efg"], False) == ["abc", "def"]
    assert chunks(["abc", "def", "g"], False) == ["abc", "def"]
    assert chunks(["abcd", "ef", "g"], False) == ["abc", "def"]

    # uneven, remainder=True
    assert chunks([], True) == []
    assert chunks(["ab"], True) == ["ab"]
    assert chunks(["ab", "cd", "ef", "g"], True) == ["abc", "def", "g"]
    assert chunks(["ab", "cd", "efg"], True) == ["abc", "def", "g"]
    assert chunks(["abc", "def", "g"], True) == ["abc", "def", "g"]
    assert chunks(["abcd", "ef", "g"], True) == ["abc", "def", "g"]


def test_backslashescape_errors():
    raw_data_err = b"abcd\x80"
    with pytest.raises(UnicodeDecodeError):
        raw_data_err.decode("utf-8", "strict")

    assert raw_data_err.decode("utf-8", "backslashescape") == "abcd\\x80"

    raw_data_ok = b"abcd\xc3\xa9"
    assert raw_data_ok.decode("utf-8", "backslashescape") == raw_data_ok.decode(
        "utf-8", "strict"
    )

    unicode_data = "abcdef\u00a3"
    assert unicode_data.encode("ascii", "backslashescape") == b"abcdef\\xa3"


def test_encode_with_unescape():
    valid_data = "\\x01020304\\x00"
    valid_data_encoded = b"\x01020304\x00"

    assert valid_data_encoded == utils.encode_with_unescape(valid_data)


def test_encode_with_unescape_invalid_escape():
    invalid_data = "test\\abcd"

    with pytest.raises(ValueError) as exc:
        utils.encode_with_unescape(invalid_data)

    assert "invalid escape" in exc.value.args[0]
    assert "position 4" in exc.value.args[0]


def test_decode_with_escape():
    backslashes = b"foo\\bar\\\\baz"
    backslashes_escaped = "foo\\\\bar\\\\\\\\baz"

    assert backslashes_escaped == utils.decode_with_escape(backslashes)

    valid_utf8 = b"foo\xc3\xa2"
    valid_utf8_escaped = "foo\u00e2"

    assert valid_utf8_escaped == utils.decode_with_escape(valid_utf8)

    invalid_utf8 = b"foo\xa2"
    invalid_utf8_escaped = "foo\\xa2"

    assert invalid_utf8_escaped == utils.decode_with_escape(invalid_utf8)

    valid_utf8_nul = b"foo\xc3\xa2\x00"
    valid_utf8_nul_escaped = "foo\u00e2\\x00"

    assert valid_utf8_nul_escaped == utils.decode_with_escape(valid_utf8_nul)


def test_commonname():
    # when
    actual_commonname = utils.commonname("/some/where/to/", "/some/where/to/go/to")
    # then
    assert "go/to" == actual_commonname

    # when
    actual_commonname2 = utils.commonname(b"/some/where/to/", b"/some/where/to/go/to")
    # then
    assert b"go/to" == actual_commonname2


def test_numfile_sotkey():
    assert utils.numfile_sortkey("00-xxx.sql") == (0, "-xxx.sql")
    assert utils.numfile_sortkey("01-xxx.sql") == (1, "-xxx.sql")
    assert utils.numfile_sortkey("10-xxx.sql") == (10, "-xxx.sql")
    assert utils.numfile_sortkey("99-xxx.sql") == (99, "-xxx.sql")
    assert utils.numfile_sortkey("100-xxx.sql") == (100, "-xxx.sql")
    assert utils.numfile_sortkey("00100-xxx.sql") == (100, "-xxx.sql")
    assert utils.numfile_sortkey("1.sql") == (1, ".sql")
    assert utils.numfile_sortkey("1") == (1, "")
    assert utils.numfile_sortkey("toto-01.sql") == (999999, "toto-01.sql")


def test_basename_sotkey():
    assert utils.basename_sortkey("00-xxx.sql") == (0, "-xxx.sql")
    assert utils.basename_sortkey("path/to/00-xxx.sql") == (0, "-xxx.sql")
