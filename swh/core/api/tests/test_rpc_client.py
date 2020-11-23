# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re

import pytest
from requests.exceptions import ConnectionError

from swh.core.api import APIError, RemoteException, RPCClient, remote_api_endpoint
from swh.core.api.serializers import exception_to_dict, msgpack_dumps

from .test_serializers import ExtraType, extra_decoders, extra_encoders


class ReraiseException(Exception):
    pass


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
        reraise_exceptions = [ReraiseException]

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


def test_client_connexion_error(rpc_client, requests_mock):
    """
    ConnectionError should be wrapped and raised as an APIError.
    """
    error_message = "unreachable host"
    requests_mock.post(
        re.compile("mock://example.com/connection_error"),
        exc=ConnectionError(error_message),
    )

    with pytest.raises(APIError) as exc_info:
        rpc_client.post("connection_error", data={})

    assert type(exc_info.value.args[0]) == ConnectionError
    assert str(exc_info.value.args[0]) == error_message


def _exception_response(exception, status_code, old_exception_schema=False):
    def callback(request, context):
        assert request.headers["Content-Type"] == "application/x-msgpack"
        context.headers["Content-Type"] = "application/x-msgpack"
        exc_dict = exception_to_dict(exception)
        if old_exception_schema:
            exc_dict = {"exception": exc_dict}
        context.content = msgpack_dumps(exc_dict)
        context.status_code = status_code
        return context.content

    return callback


@pytest.mark.parametrize("old_exception_schema", [False, True])
def test_client_reraise_exception(rpc_client, requests_mock, old_exception_schema):
    """
    Exception caught server-side and whitelisted will be raised again client-side.
    """
    error_message = "something went wrong"
    endpoint = "reraise_exception"

    requests_mock.post(
        re.compile(f"mock://example.com/{endpoint}"),
        content=_exception_response(
            exception=ReraiseException(error_message),
            status_code=400,
            old_exception_schema=old_exception_schema,
        ),
    )

    with pytest.raises(ReraiseException) as exc_info:
        rpc_client.post(endpoint, data={})

    assert str(exc_info.value) == error_message


@pytest.mark.parametrize(
    "status_code, old_exception_schema",
    [(400, False), (500, False), (400, True), (500, True),],
)
def test_client_raise_remote_exception(
    rpc_client, requests_mock, status_code, old_exception_schema
):
    """
    Exception caught server-side and not whitelisted will be wrapped and raised
    as a RemoteException client-side.
    """
    error_message = "something went wrong"
    endpoint = "raise_remote_exception"

    requests_mock.post(
        re.compile(f"mock://example.com/{endpoint}"),
        content=_exception_response(
            exception=Exception(error_message),
            status_code=status_code,
            old_exception_schema=old_exception_schema,
        ),
    )

    with pytest.raises(RemoteException) as exc_info:
        rpc_client.post(endpoint, data={})

    assert str(exc_info.value.args[0]["type"]) == "Exception"
    assert str(exc_info.value.args[0]["message"]) == error_message
