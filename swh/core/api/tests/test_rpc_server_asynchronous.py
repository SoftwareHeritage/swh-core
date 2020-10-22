# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.api import remote_api_endpoint
from swh.core.api.asynchronous import RPCServerApp
from swh.core.api.serializers import json_dumps, msgpack_dumps

from .test_serializers import ExtraType, extra_decoders, extra_encoders


class MyRPCServerApp(RPCServerApp):
    extra_type_encoders = extra_encoders
    extra_type_decoders = extra_decoders


class BackendStorageTest:
    """Backend Storage to use as backend class of the rpc server (test only)"""

    @remote_api_endpoint("test_endpoint_url", method="GET")
    def test_endpoint(self, test_data, db=None, cur=None):
        assert test_data == "spam"
        return "egg"

    @remote_api_endpoint("path/to/identity")
    def identity(self, data, db=None, cur=None):
        return data

    @remote_api_endpoint("serializer_test")
    def serializer_test(self, data, db=None, cur=None):
        assert data == ["foo", ExtraType("bar", b"baz")]
        return ExtraType({"spam": "egg"}, "qux")


@pytest.fixture
def async_app():
    return MyRPCServerApp("testapp", backend_class=BackendStorageTest)


def test_api_async_rpc_server_app_ok(async_app):
    assert isinstance(async_app, MyRPCServerApp)

    actual_rpc_server2 = MyRPCServerApp(
        "app2", backend_class=BackendStorageTest, backend_factory=BackendStorageTest
    )
    assert isinstance(actual_rpc_server2, MyRPCServerApp)

    actual_rpc_server3 = MyRPCServerApp("app3")
    assert isinstance(actual_rpc_server3, MyRPCServerApp)


def test_api_async_rpc_server_app_misconfigured():
    expected_error = "backend_factory should only be provided if backend_class is"
    with pytest.raises(ValueError, match=expected_error):
        MyRPCServerApp("failed-app", backend_factory="something-to-make-it-raise")


@pytest.fixture
def cli(loop, aiohttp_client, async_app):
    """aiohttp client fixture to ease testing

    source: https://docs.aiohttp.org/en/stable/testing.html
    """
    loop.set_debug(True)
    return loop.run_until_complete(aiohttp_client(async_app))


async def test_api_async_endpoint(cli, async_app):
    res = await cli.post(
        "/path/to/identity",
        headers=[("Content-Type", "application/json"), ("Accept", "application/json")],
        data=json_dumps({"data": "toto"}),
    )
    assert res.status == 200
    assert res.content_type == "application/json"
    assert await res.read() == json_dumps("toto").encode()


async def test_api_async_nego_default_msgpack(cli):
    res = await cli.post(
        "/path/to/identity",
        headers=[("Content-Type", "application/json")],
        data=json_dumps({"data": "toto"}),
    )
    assert res.status == 200
    assert res.content_type == "application/x-msgpack"
    assert await res.read() == msgpack_dumps("toto")


async def test_api_async_nego_default(cli):
    res = await cli.post(
        "/path/to/identity",
        headers=[
            ("Content-Type", "application/json"),
            ("Accept", "application/x-msgpack"),
        ],
        data=json_dumps({"data": "toto"}),
    )
    assert res.status == 200
    assert res.content_type == "application/x-msgpack"
    assert await res.read() == msgpack_dumps("toto")


async def test_api_async_nego_accept(cli):
    res = await cli.post(
        "/path/to/identity",
        headers=[
            ("Accept", "application/x-msgpack"),
            ("Content-Type", "application/x-msgpack"),
        ],
        data=msgpack_dumps({"data": "toto"}),
    )
    assert res.status == 200
    assert res.content_type == "application/x-msgpack"
    assert await res.read() == msgpack_dumps("toto")


async def test_api_async_rpc_server(cli):
    res = await cli.get(
        "/test_endpoint_url",
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=msgpack_dumps({"test_data": "spam"}),
    )

    assert res.status == 200
    assert res.content_type == "application/x-msgpack"
    assert await res.read() == msgpack_dumps("egg")


async def test_api_async_rpc_server_extra_serializers(cli):
    res = await cli.post(
        "/serializer_test",
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=(
            b"\x81\xa4data\x92\xa3foo\x82\xc4\x07swhtype\xa9extratype"
            b"\xc4\x01d\x92\xa3bar\xc4\x03baz"
        ),
    )

    assert res.status == 200
    assert res.content_type == "application/x-msgpack"
    assert await res.read() == (
        b"\x82\xc4\x07swhtype\xa9extratype\xc4\x01d\x92\x81\xa4spam\xa3egg\xa3qux"
    )
