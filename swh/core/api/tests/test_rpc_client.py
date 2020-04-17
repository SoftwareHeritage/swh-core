# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
import pytest

from swh.core.api import remote_api_endpoint, RPCClient

from .test_serializers import ExtraType, extra_encoders, extra_decoders


@pytest.fixture
def rpc_client(requests_mock):
    class TestStorage:
        @remote_api_endpoint("test_endpoint_url")
        def test_endpoint(self, test_data, db=None, cur=None):
            ...

        @remote_api_endpoint("path/to/endpoint")
        def something(self, data, db=None, cur=None):
            ...

        @remote_api_endpoint("serializer_test")
        def serializer_test(self, data, db=None, cur=None):
            ...

        @remote_api_endpoint("overridden/endpoint")
        def overridden_method(self, data):
            return "foo"

    class Testclient(RPCClient):
        backend_class = TestStorage
        extra_type_encoders = extra_encoders
        extra_type_decoders = extra_decoders

        def overridden_method(self, data):
            return "bar"

    def callback(request, context):
        assert request.headers["Content-Type"] == "application/x-msgpack"
        context.headers["Content-Type"] = "application/x-msgpack"
        if request.path == "/test_endpoint_url":
            context.content = b"\xa3egg"
        elif request.path == "/path/to/endpoint":
            context.content = b"\xa4spam"
        elif request.path == "/serializer_test":
            context.content = (
                b"\x82\xc4\x07swhtype\xa9extratype"
                b"\xc4\x01d\x92\x81\xa4spam\xa3egg\xa3qux"
            )
        else:
            assert False
        return context.content

    requests_mock.post(re.compile("mock://example.com/"), content=callback)

    return Testclient(url="mock://example.com")


def test_client(rpc_client):

    assert hasattr(rpc_client, "test_endpoint")
    assert hasattr(rpc_client, "something")

    res = rpc_client.test_endpoint("spam")
    assert res == "egg"
    res = rpc_client.test_endpoint(test_data="spam")
    assert res == "egg"

    res = rpc_client.something("whatever")
    assert res == "spam"
    res = rpc_client.something(data="whatever")
    assert res == "spam"


def test_client_extra_serializers(rpc_client):
    res = rpc_client.serializer_test(["foo", ExtraType("bar", b"baz")])
    assert res == ExtraType({"spam": "egg"}, "qux")


def test_client_overridden_method(rpc_client):
    res = rpc_client.overridden_method("foo")
    assert res == "bar"
