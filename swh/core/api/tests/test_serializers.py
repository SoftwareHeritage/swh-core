# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json
from typing import Any, Callable, List, Tuple, Union
from uuid import UUID

import arrow
from arrow import Arrow
import pytest
import requests
from requests.exceptions import ConnectionError

from swh.core.api.classes import PagedResult
from swh.core.api.serializers import (
    ENCODERS,
    SWHJSONDecoder,
    SWHJSONEncoder,
    decode_response,
    msgpack_dumps,
    msgpack_loads,
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

DATA_BYTES = b"123456789\x99\xaf\xff\x00\x12"
ENCODED_DATA_BYTES = {"swhtype": "bytes", "d": "F)}kWH8wXmIhn8j01^"}

DATA_DATETIME = datetime.datetime(2015, 3, 4, 18, 25, 13, 1234, tzinfo=TZ,)
ENCODED_DATA_DATETIME = {
    "swhtype": "datetime",
    "d": "2015-03-04T18:25:13.001234+01:58",
}

DATA_TIMEDELTA = datetime.timedelta(64)
ENCODED_DATA_TIMEDELTA = {
    "swhtype": "timedelta",
    "d": {"days": 64, "seconds": 0, "microseconds": 0},
}

DATA_ARROW = arrow.get("2018-04-25T16:17:53.533672+00:00")
ENCODED_DATA_ARROW = {"swhtype": "arrow", "d": "2018-04-25T16:17:53.533672+00:00"}

DATA_UUID = UUID("cdd8f804-9db6-40c3-93ab-5955d3836234")
ENCODED_DATA_UUID = {"swhtype": "uuid", "d": "cdd8f804-9db6-40c3-93ab-5955d3836234"}

# For test demonstration purposes
TestPagedResultStr = PagedResult[
    Union[UUID, datetime.datetime, datetime.timedelta], str
]

DATA_PAGED_RESULT = TestPagedResultStr(
    results=[DATA_UUID, DATA_DATETIME, DATA_TIMEDELTA], next_page_token="10",
)

ENCODED_DATA_PAGED_RESULT = {
    "d": {
        "results": [ENCODED_DATA_UUID, ENCODED_DATA_DATETIME, ENCODED_DATA_TIMEDELTA,],
        "next_page_token": "10",
    },
    "swhtype": "paged_result",
}

TestPagedResultTuple = PagedResult[Union[str, bytes, Arrow], List[Union[str, UUID]]]


DATA_PAGED_RESULT2 = TestPagedResultTuple(
    results=["data0", DATA_BYTES, DATA_ARROW], next_page_token=["10", DATA_UUID],
)

ENCODED_DATA_PAGED_RESULT2 = {
    "d": {
        "results": ["data0", ENCODED_DATA_BYTES, ENCODED_DATA_ARROW,],
        "next_page_token": ["10", ENCODED_DATA_UUID],
    },
    "swhtype": "paged_result",
}

DATA = {
    "bytes": DATA_BYTES,
    "datetime_tz": DATA_DATETIME,
    "datetime_utc": datetime.datetime(
        2015, 3, 4, 18, 25, 13, 1234, tzinfo=datetime.timezone.utc
    ),
    "datetime_delta": DATA_TIMEDELTA,
    "arrow_date": DATA_ARROW,
    "swhtype": "fake",
    "swh_dict": {"swhtype": 42, "d": "test"},
    "random_dict": {"swhtype": 43},
    "uuid": DATA_UUID,
    "paged-result": DATA_PAGED_RESULT,
    "paged-result2": DATA_PAGED_RESULT2,
}

ENCODED_DATA = {
    "bytes": ENCODED_DATA_BYTES,
    "datetime_tz": ENCODED_DATA_DATETIME,
    "datetime_utc": {"swhtype": "datetime", "d": "2015-03-04T18:25:13.001234+00:00",},
    "datetime_delta": ENCODED_DATA_TIMEDELTA,
    "arrow_date": ENCODED_DATA_ARROW,
    "swhtype": "fake",
    "swh_dict": {"swhtype": 42, "d": "test"},
    "random_dict": {"swhtype": 43},
    "uuid": ENCODED_DATA_UUID,
    "paged-result": ENCODED_DATA_PAGED_RESULT,
    "paged-result2": ENCODED_DATA_PAGED_RESULT2,
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


def test_exception_serializer_round_trip_json():
    error_message = "unreachable host"
    json_data = json.dumps(
        {"exception": ConnectionError(error_message)}, cls=SWHJSONEncoder
    )
    actual_data = json.loads(json_data, cls=SWHJSONDecoder)
    assert "exception" in actual_data
    assert type(actual_data["exception"]) == ConnectionError
    assert str(actual_data["exception"]) == error_message


def test_serializers_encode_swh_json():
    json_str = json.dumps(DATA, cls=SWHJSONEncoder)
    actual_data = json.loads(json_str)
    assert actual_data == ENCODED_DATA


def test_serializers_round_trip_msgpack():
    expected_original_data = {
        **DATA,
        "none_dict_key": {None: 42},
        "long_int_is_loooong": 10000000000000000000000000000000,
        "long_negative_int_is_loooong": -10000000000000000000000000000000,
    }
    data = msgpack_dumps(expected_original_data)
    actual_data = msgpack_loads(data)
    assert actual_data == expected_original_data


def test_serializers_round_trip_msgpack_extra_types():
    original_data = [ExtraType("baz", DATA), "qux"]
    data = msgpack_dumps(original_data, extra_encoders=extra_encoders)
    actual_data = msgpack_loads(data, extra_decoders=extra_decoders)
    assert actual_data == original_data


def test_exception_serializer_round_trip_msgpack():
    error_message = "unreachable host"
    data = msgpack_dumps({"exception": ConnectionError(error_message)})
    actual_data = msgpack_loads(data)
    assert "exception" in actual_data
    assert type(actual_data["exception"]) == ConnectionError
    assert str(actual_data["exception"]) == error_message


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


def test_msgpack_extra_encoders_mutation():
    data = msgpack_dumps({}, extra_encoders=extra_encoders)
    assert data is not None
    assert ENCODERS[-1][0] != ExtraType


def test_json_extra_encoders_mutation():
    data = json.dumps({}, cls=SWHJSONEncoder, extra_encoders=extra_encoders)
    assert data is not None
    assert ENCODERS[-1][0] != ExtraType
