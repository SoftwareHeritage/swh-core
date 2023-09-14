# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json

import msgpack
import pytest

from swh.core.api.asynchronous import (
    Response,
    RPCServerApp,
    decode_data,
    decode_request,
    encode_msgpack,
)
from swh.core.api.serializers import SWHJSONEncoder, json_dumps, msgpack_dumps

pytest_plugins = ["aiohttp.pytest_plugin", "pytester"]


class TestServerException(Exception):
    pass


class TestClientError(Exception):
    pass


async def root(request):
    return Response("toor")


STRUCT = {
    "txt": "something stupid",
    # 'date': datetime.date(2019, 6, 9),  # not supported
    "datetime": datetime.datetime(2019, 6, 9, 10, 12, tzinfo=datetime.timezone.utc),
    "timedelta": datetime.timedelta(days=-2, hours=3),
    "int": 42,
    "float": 3.14,
    "subdata": {
        "int": 42,
        "datetime": datetime.datetime(
            2019, 6, 10, 11, 12, tzinfo=datetime.timezone.utc
        ),
    },
    "list": [
        42,
        datetime.datetime(2019, 9, 10, 11, 12, tzinfo=datetime.timezone.utc),
        "ok",
    ],
}


async def struct(request):
    return Response(STRUCT)


async def echo(request):
    data = await decode_request(request)
    return Response(data)


async def server_exception(request):
    raise TestServerException()


async def client_error(request):
    raise TestClientError()


async def echo_no_nego(request):
    # let the content negotiation handle the serialization for us...
    data = await decode_request(request)
    ret = encode_msgpack(data)
    return ret


def check_mimetype(src, dst):
    src = src.split(";")[0].strip()
    dst = dst.split(";")[0].strip()
    assert src == dst


@pytest.fixture
def async_app():
    app = RPCServerApp()
    app.client_exception_classes = (TestClientError,)
    app.router.add_route("GET", "/", root)
    app.router.add_route("GET", "/struct", struct)
    app.router.add_route("POST", "/echo", echo)
    app.router.add_route("GET", "/server_exception", server_exception)
    app.router.add_route("GET", "/client_error", client_error)
    app.router.add_route("POST", "/echo-no-nego", echo_no_nego)
    return app


@pytest.fixture
def cli(async_app, aiohttp_client, loop):
    return loop.run_until_complete(aiohttp_client(async_app))


async def test_get_simple(cli) -> None:
    resp = await cli.get("/")
    assert resp.status == 200
    check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
    data = await resp.read()
    value = msgpack.unpackb(data, raw=False)
    assert value == "toor"


async def test_get_server_exception(cli) -> None:
    resp = await cli.get("/server_exception")
    assert resp.status == 500
    data = await resp.read()
    data = msgpack.unpackb(data, raw=False)
    assert data["type"] == "TestServerException"


async def test_get_client_error(cli) -> None:
    resp = await cli.get("/client_error")
    assert resp.status == 400
    data = await resp.read()
    data = msgpack.unpackb(data, raw=False)
    assert data["type"] == "TestClientError"


async def test_get_simple_nego(cli) -> None:
    for ctype in ("x-msgpack", "json"):
        resp = await cli.get("/", headers={"Accept": "application/%s" % ctype})
        assert resp.status == 200
        check_mimetype(resp.headers["Content-Type"], "application/%s" % ctype)
        assert (await decode_request(resp)) == "toor"


async def test_get_struct(cli) -> None:
    """Test returned structured from a simple GET data is OK"""
    resp = await cli.get("/struct")
    assert resp.status == 200
    check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
    assert (await decode_request(resp)) == STRUCT


async def test_get_struct_nego(cli) -> None:
    """Test returned structured from a simple GET data is OK"""
    for ctype in ("x-msgpack", "json"):
        resp = await cli.get("/struct", headers={"Accept": "application/%s" % ctype})
        assert resp.status == 200
        check_mimetype(resp.headers["Content-Type"], "application/%s" % ctype)
        assert (await decode_request(resp)) == STRUCT


async def test_post_struct_msgpack(cli) -> None:
    """Test that msgpack encoded posted struct data is returned as is"""
    # simple struct
    resp = await cli.post(
        "/echo",
        headers={"Content-Type": "application/x-msgpack"},
        data=msgpack_dumps({"toto": 42}),
    )
    assert resp.status == 200
    check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
    assert (await decode_request(resp)) == {"toto": 42}
    # complex struct
    resp = await cli.post(
        "/echo",
        headers={"Content-Type": "application/x-msgpack"},
        data=msgpack_dumps(STRUCT),
    )
    assert resp.status == 200
    check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
    assert (await decode_request(resp)) == STRUCT


async def test_post_struct_json(cli) -> None:
    """Test that json encoded posted struct data is returned as is"""
    resp = await cli.post(
        "/echo",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"toto": 42}, cls=SWHJSONEncoder),
    )
    assert resp.status == 200
    check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
    assert (await decode_request(resp)) == {"toto": 42}

    resp = await cli.post(
        "/echo",
        headers={"Content-Type": "application/json"},
        data=json.dumps(STRUCT, cls=SWHJSONEncoder),
    )
    assert resp.status == 200
    check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
    # assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == STRUCT


async def test_post_struct_nego(cli) -> None:
    """Test that json encoded posted struct data is returned as is

    using content negotiation (accept json or msgpack).
    """
    for ctype in ("x-msgpack", "json"):
        resp = await cli.post(
            "/echo",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/%s" % ctype,
            },
            data=json.dumps(STRUCT, cls=SWHJSONEncoder),
        )
        assert resp.status == 200
        check_mimetype(resp.headers["Content-Type"], "application/%s" % ctype)
        assert (await decode_request(resp)) == STRUCT


async def test_post_struct_no_nego(cli) -> None:
    """Test that json encoded posted struct data is returned as msgpack

    when using non-negotiation-compatible handlers.
    """
    for ctype in ("x-msgpack", "json"):
        resp = await cli.post(
            "/echo-no-nego",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/%s" % ctype,
            },
            data=json.dumps(STRUCT, cls=SWHJSONEncoder),
        )
        assert resp.status == 200
        check_mimetype(resp.headers["Content-Type"], "application/x-msgpack")
        assert (await decode_request(resp)) == STRUCT


def test_async_decode_data_failure():
    with pytest.raises(ValueError, match="Wrong content type"):
        decode_data("some-data", "unknown-content-type")


@pytest.mark.parametrize("data", [None, "", {}, []])
def test_async_decode_data_empty_cases(data):
    assert decode_data(data, "unknown-content-type") == {}


@pytest.mark.parametrize(
    "data,content_type,encode_data_fn",
    [
        ({"a": 1}, "application/json", json_dumps),
        ({"a": 1}, "application/x-msgpack", msgpack_dumps),
    ],
)
def test_async_decode_data_nominal(data, content_type, encode_data_fn):
    actual_data = decode_data(encode_data_fn(data), content_type)
    assert actual_data == data
