# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json

from typing import Any, Callable, List, Tuple
from uuid import UUID

import pytest
import arrow
import requests

from swh.core.api.serializers import (
    SWHJSONDecoder,
    SWHJSONEncoder,
    msgpack_dumps,
    msgpack_loads,
    decode_response,
)


class ExtraType:
    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def __repr__(self):
        return f"ExtraType({self.arg1}, {self.arg2})"

    def __eq__(self, other):
        return isinstance(other, ExtraType) and (self.arg1, self.arg2) == (
            other.arg1,
            other.arg2,
        )


extra_encoders: List[Tuple[type, str, Callable[..., Any]]] = [
    (ExtraType, "extratype", lambda o: (o.arg1, o.arg2))
]


extra_decoders = {
    "extratype": lambda o: ExtraType(*o),
}

TZ = datetime.timezone(datetime.timedelta(minutes=118))

DATA = {
    "bytes": b"123456789\x99\xaf\xff\x00\x12",
    "datetime_tz": datetime.datetime(2015, 3, 4, 18, 25, 13, 1234, tzinfo=TZ,),
    "datetime_utc": datetime.datetime(
        2015, 3, 4, 18, 25, 13, 1234, tzinfo=datetime.timezone.utc
    ),
    "datetime_delta": datetime.timedelta(64),
    "arrow_date": arrow.get("2018-04-25T16:17:53.533672+00:00"),
    "swhtype": "fake",
    "swh_dict": {"swhtype": 42, "d": "test"},
    "random_dict": {"swhtype": 43},
    "uuid": UUID("cdd8f804-9db6-40c3-93ab-5955d3836234"),
}

ENCODED_DATA = {
    "bytes": {"swhtype": "bytes", "d": "F)}kWH8wXmIhn8j01^"},
    "datetime_tz": {"swhtype": "datetime", "d": "2015-03-04T18:25:13.001234+01:58",},
    "datetime_utc": {"swhtype": "datetime", "d": "2015-03-04T18:25:13.001234+00:00",},
    "datetime_delta": {
        "swhtype": "timedelta",
        "d": {"days": 64, "seconds": 0, "microseconds": 0},
    },
    "arrow_date": {"swhtype": "arrow", "d": "2018-04-25T16:17:53.533672+00:00"},
    "swhtype": "fake",
    "swh_dict": {"swhtype": 42, "d": "test"},
    "random_dict": {"swhtype": 43},
    "uuid": {"swhtype": "uuid", "d": "cdd8f804-9db6-40c3-93ab-5955d3836234"},
}


def test_serializers_round_trip_json():
    json_data = json.dumps(DATA, cls=SWHJSONEncoder)
    actual_data = json.loads(json_data, cls=SWHJSONDecoder)
    assert actual_data == DATA


def test_serializers_round_trip_json_extra_types():
    expected_original_data = [ExtraType("baz", DATA), "qux"]
    data = json.dumps(
        expected_original_data, cls=SWHJSONEncoder, extra_encoders=extra_encoders
    )
    actual_data = json.loads(data, cls=SWHJSONDecoder, extra_decoders=extra_decoders)
    assert actual_data == expected_original_data


def test_serializers_encode_swh_json():
    json_str = json.dumps(DATA, cls=SWHJSONEncoder)
    actual_data = json.loads(json_str)
    assert actual_data == ENCODED_DATA


def test_serializers_round_trip_msgpack():
    expected_original_data = {
        **DATA,
        "none_dict_key": {None: 42},
        "long_int_is_loooong": 10000000000000000000000000000000,
    }
    data = msgpack_dumps(expected_original_data)
    actual_data = msgpack_loads(data)
    assert actual_data == expected_original_data


def test_serializers_round_trip_msgpack_extra_types():
    original_data = [ExtraType("baz", DATA), "qux"]
    data = msgpack_dumps(original_data, extra_encoders=extra_encoders)
    actual_data = msgpack_loads(data, extra_decoders=extra_decoders)
    assert actual_data == original_data


def test_serializers_generator_json():
    data = json.dumps((i for i in range(5)), cls=SWHJSONEncoder)
    assert json.loads(data, cls=SWHJSONDecoder) == [i for i in range(5)]


def test_serializers_generator_msgpack():
    data = msgpack_dumps((i for i in range(5)))
    assert msgpack_loads(data) == [i for i in range(5)]


def test_serializers_decode_response_json(requests_mock):
    requests_mock.get(
        "https://example.org/test/data",
        json=ENCODED_DATA,
        headers={"content-type": "application/json"},
    )

    response = requests.get("https://example.org/test/data")
    assert decode_response(response) == DATA


def test_serializers_decode_legacy_msgpack():
    legacy_msgpack = {
        "bytes": b"\xc4\x0e123456789\x99\xaf\xff\x00\x12",
        "datetime_tz": (
            b"\x82\xc4\x0c__datetime__\xc3\xc4\x01s\xd9 "
            b"2015-03-04T18:25:13.001234+01:58"
        ),
        "datetime_utc": (
            b"\x82\xc4\x0c__datetime__\xc3\xc4\x01s\xd9 "
            b"2015-03-04T18:25:13.001234+00:00"
        ),
        "datetime_delta": (
            b"\x82\xc4\r__timedelta__\xc3\xc4\x01s\x83\xa4"
            b"days@\xa7seconds\x00\xacmicroseconds\x00"
        ),
        "arrow_date": (
            b"\x82\xc4\t__arrow__\xc3\xc4\x01s\xd9 2018-04-25T16:17:53.533672+00:00"
        ),
        "swhtype": b"\xa4fake",
        "swh_dict": b"\x82\xa7swhtype*\xa1d\xa4test",
        "random_dict": b"\x81\xa7swhtype+",
        "uuid": (
            b"\x82\xc4\x08__uuid__\xc3\xc4\x01s\xd9$"
            b"cdd8f804-9db6-40c3-93ab-5955d3836234"
        ),
    }
    for k, v in legacy_msgpack.items():
        assert msgpack_loads(v) == DATA[k]


def test_serializers_encode_native_datetime():
    dt = datetime.datetime(2015, 1, 1, 12, 4, 42, 231455)
    with pytest.raises(ValueError, match="naive datetime"):
        msgpack_dumps(dt)


def test_serializers_decode_naive_datetime():
    expected_dt = datetime.datetime(2015, 1, 1, 12, 4, 42, 231455)

    # Current encoding
    assert (
        msgpack_loads(
            b"\x82\xc4\x07swhtype\xa8datetime\xc4\x01d\xba"
            b"2015-01-01T12:04:42.231455"
        )
        == expected_dt
    )

    # Legacy encoding
    assert (
        msgpack_loads(
            b"\x82\xc4\x0c__datetime__\xc3\xc4\x01s\xba2015-01-01T12:04:42.231455"
        )
        == expected_dt
    )
