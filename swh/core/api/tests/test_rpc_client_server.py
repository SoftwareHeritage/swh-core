# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.api import (
    RemoteException,
    RPCClient,
    RPCServerApp,
    encode_data_server,
    error_handler,
    remote_api_endpoint,
)


class ExpectedException(Exception):
    """Another exception class to distinguish error handlers"""


# this class is used on the server part
class RPCTest:
    @remote_api_endpoint("endpoint_url")
    def endpoint(self, test_data, db=None, cur=None):
        assert test_data == "spam"
        return "egg"

    @remote_api_endpoint("path/to/endpoint")
    def something(self, data, db=None, cur=None):
        return data

    @remote_api_endpoint("raises_typeerror")
    def raise_typeerror(self):
        raise TypeError("Did I pass through?")

    @remote_api_endpoint("raise_exception_exc_arg")
    def raise_exception_exc_arg(self):
        raise Exception(Exception("error"))

    @remote_api_endpoint("raises_expectedexc")
    def raise_expectedexc(self):
        raise ExpectedException("that was expected")


# this class is used on the client part. We cannot inherit from RPCTest
# because the automagic metaclass based code that generates the RPCClient
# proxy class from this does not handle inheritance properly.
# We do add an endpoint on the client side that has no implementation
# server-side to test this very situation (in should generate a 404)
class RPCTest2:
    @remote_api_endpoint("endpoint_url")
    def endpoint(self, test_data, db=None, cur=None):
        assert test_data == "spam"
        return "egg"

    @remote_api_endpoint("path/to/endpoint")
    def something(self, data, db=None, cur=None):
        return data

    @remote_api_endpoint("not_on_server")
    def not_on_server(self, db=None, cur=None):
        return "ok"

    @remote_api_endpoint("raises_typeerror")
    def raise_typeerror(self):
        return "data"

    @remote_api_endpoint("raises_expectedexc")
    def raise_expectedexc(self):
        return "nothing"


class RPCTestClient(RPCClient):
    backend_class = RPCTest2


@pytest.fixture
def app():
    # This fixture is used by the 'swh_rpc_adapter' fixture
    # which is defined in swh/core/pytest_plugin.py
    application = RPCServerApp("testapp", backend_class=RPCTest)

    @application.errorhandler(ExpectedException)
    def my_expected_error_handler(exception):
        return error_handler(exception, encode_data_server, status_code=400)

    @application.errorhandler(Exception)
    def my_error_handler(exception):
        return error_handler(exception, encode_data_server)

    return application


@pytest.fixture
def swh_rpc_client_class():
    # This fixture is used by the 'swh_rpc_client' fixture
    # which is defined in swh/core/pytest_plugin.py
    return RPCTestClient


def test_api_client_endpoint_missing(swh_rpc_client):
    with pytest.raises(AttributeError):
        swh_rpc_client.missing(data="whatever")


def test_api_server_endpoint_missing(swh_rpc_client):
    # A 'missing' endpoint (server-side) should raise an exception
    # due to a 404, since at the end, we do a GET/POST an inexistent URL
    with pytest.raises(Exception, match="404 not found"):
        swh_rpc_client.not_on_server()


def test_api_endpoint_kwargs(swh_rpc_client):
    res = swh_rpc_client.something(data="whatever")
    assert res == "whatever"
    res = swh_rpc_client.endpoint(test_data="spam")
    assert res == "egg"


def test_api_endpoint_args(swh_rpc_client):
    res = swh_rpc_client.something("whatever")
    assert res == "whatever"
    res = swh_rpc_client.endpoint("spam")
    assert res == "egg"


def test_api_typeerror(swh_rpc_client, mocker):
    mocked_capture_exception = mocker.patch("swh.core.api.sentry_sdk.capture_exception")

    with pytest.raises(RemoteException) as exc_info:
        swh_rpc_client.raise_typeerror()

    passed_exc = mocked_capture_exception.call_args[0][0]
    assert isinstance(passed_exc, TypeError)
    assert passed_exc.args[0] == "Did I pass through?"

    assert exc_info.value.args[0]["type"] == "TypeError"
    assert exc_info.value.args[0]["args"] == ["Did I pass through?"]
    assert (
        str(exc_info.value)
        == "<RemoteException 500 TypeError: ['Did I pass through?']>"
    )


def test_api_raise_exception_exc_arg(swh_rpc_client):
    with pytest.raises(RemoteException) as exc_info:
        swh_rpc_client._post("raise_exception_exc_arg", data={})

    assert exc_info.value.args[0]["type"] == "Exception"
    assert type(exc_info.value.args[0]["args"][0]) is Exception
    assert str(exc_info.value.args[0]["args"][0]) == "error"


def test_api_expected_exception_no_sentry_capture(swh_rpc_client, mocker):
    mocked_capture_exception = mocker.patch("swh.core.api.sentry_sdk.capture_exception")

    with pytest.raises(RemoteException) as exc_info:
        swh_rpc_client.raise_expectedexc()

    assert not mocked_capture_exception.called

    assert exc_info.value.args[0]["type"] == "ExpectedException"
    assert exc_info.value.args[0]["args"] == ["that was expected"]
