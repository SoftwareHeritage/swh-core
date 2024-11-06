# Copyright (C) 2018-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re

import pytest
from requests.exceptions import ConnectionError

from swh.core.api import (
    RETRY_WAIT_INTERVAL,
    APIError,
    RemoteException,
    RPCClient,
    TransientRemoteException,
    remote_api_endpoint,
)
from swh.core.api.serializers import exception_to_dict, msgpack_dumps
from swh.core.retry import MAX_NUMBER_ATTEMPTS

from .test_serializers import ExtraType, extra_decoders, extra_encoders


class ReraiseException(Exception):
    pass


@pytest.fixture
def rpc_client_class(requests_mock):
    class TestStorage:
        @remote_api_endpoint("test_endpoint_url")
        def test_endpoint(self, test_data, db=None, cur=None): ...

        @remote_api_endpoint("path/to/endpoint")
        def something(self, data, db=None, cur=None): ...

        @remote_api_endpoint("serializer_test")
        def serializer_test(self, data, db=None, cur=None): ...

        @remote_api_endpoint("overridden/endpoint")
        def overridden_method(self, data):
            return "foo"

        @remote_api_endpoint("request_too_large")
        def request_too_large(self, data, db=None, cur=None): ...

    class Testclient(RPCClient):
        backend_class = TestStorage
        extra_type_encoders = extra_encoders
        extra_type_decoders = extra_decoders
        reraise_exceptions = [ReraiseException]
        enable_requests_retry = True

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
        elif request.path == "/request_too_large":
            context.status_code = 413
            context.headers["Content-Type"] = "text/html"
            context.content = b"<h1>413 request entity too large</h1>\r\n"
        else:
            assert False
        return context.content

    requests_mock.post(re.compile("mock://example.com/"), content=callback)

    return Testclient


@pytest.fixture
def rpc_client(rpc_client_class):
    return rpc_client_class(url="mock://example.com")


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


def test_client_request_too_large(rpc_client):
    with pytest.raises(APIError) as exc_info:
        rpc_client.request_too_large("foo")

    assert exc_info.value.args[0] == "<h1>413 request entity too large</h1>\r\n"
    assert exc_info.value.args[1].status_code == 413


def test_client_connexion_error(rpc_client, requests_mock, mocker):
    """
    ConnectionError should be wrapped and raised as an APIError.
    """
    mock_sleep = mocker.patch("time.sleep")
    error_message = "unreachable host"
    requests_mock.post(
        re.compile("mock://example.com/connection_error"),
        exc=ConnectionError(error_message),
    )

    with pytest.raises(APIError) as exc_info:
        rpc_client._post("connection_error", data={})

    assert type(exc_info.value.args[0]) is ConnectionError
    assert str(exc_info.value.args[0]) == error_message

    # check request retries on connection errors
    mock_sleep.assert_has_calls(
        [
            mocker.call(param)
            for param in [RETRY_WAIT_INTERVAL] * (MAX_NUMBER_ATTEMPTS - 1)
        ]
    )


def _exception_response(exception, status_code):
    def callback(request, context):
        assert request.headers["Content-Type"] == "application/x-msgpack"
        context.headers["Content-Type"] = "application/x-msgpack"
        exc_dict = exception_to_dict(exception)
        context.content = msgpack_dumps(exc_dict)
        context.status_code = status_code
        return context.content

    return callback


def test_client_reraise_exception(rpc_client, requests_mock, mocker):
    """
    Exception caught server-side and whitelisted will be raised again client-side.
    """
    mock_sleep = mocker.patch("time.sleep")
    error_message = "something went wrong"
    endpoint = "reraise_exception"

    requests_mock.post(
        re.compile(f"mock://example.com/{endpoint}"),
        content=_exception_response(
            exception=ReraiseException(error_message),
            status_code=400,
        ),
    )

    with pytest.raises(ReraiseException) as exc_info:
        rpc_client._post(endpoint, data={})

    assert str(exc_info.value) == error_message
    # no request retry for such exception
    mock_sleep.assert_not_called()


@pytest.mark.parametrize("status_code", [400, 500, 502, 503])
def test_client_raise_remote_exception(rpc_client, requests_mock, status_code, mocker):
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
        ),
    )
    mock_sleep = mocker.patch("time.sleep")

    with pytest.raises(RemoteException) as exc_info:
        rpc_client._post(endpoint, data={})

    assert str(exc_info.value.args[0]["type"]) == "Exception"
    assert str(exc_info.value.args[0]["message"]) == error_message
    if status_code in (502, 503):
        assert isinstance(exc_info.value, TransientRemoteException)
        # check request retry on transient remote exception
        mock_sleep.assert_has_calls(
            [
                mocker.call(param)
                for param in [RETRY_WAIT_INTERVAL] * (MAX_NUMBER_ATTEMPTS - 1)
            ]
        )
    else:
        assert not isinstance(exc_info.value, TransientRemoteException)
        # no request retry on other remote exceptions
        mock_sleep.assert_not_called()


@pytest.mark.parametrize(
    "timeout_arg,timeout_value",
    [
        pytest.param(None, None, id="default"),
        pytest.param(1.0, 1.0, id="float"),
        pytest.param((1, 2), (1, 2), id="tuple"),
        pytest.param([1, 2], (1, 2), id="list"),
    ],
)
def test_client_timeout_param(rpc_client_class, timeout_arg, timeout_value):
    client = rpc_client_class(url="mock://example.com/", timeout=timeout_arg)
    assert client.timeout == timeout_value


def test_client_timeout_valueerror(rpc_client_class):
    for timeout in ([], [1], [1, 2, 3]):
        with pytest.raises(ValueError) as exc:
            rpc_client_class(url="mock://example.com/", timeout=timeout)
        assert repr(timeout) in str(exc.value)
