# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.api import remote_api_endpoint, RPCServerApp, RPCClient
from swh.core.api import error_handler, encode_data_server


# this class is used on the server part
class RPCTest:
    @remote_api_endpoint('endpoint_url')
    def endpoint(self, test_data, db=None, cur=None):
        assert test_data == 'spam'
        return 'egg'

    @remote_api_endpoint('path/to/endpoint')
    def something(self, data, db=None, cur=None):
        return data

    @remote_api_endpoint('raises_typeerror')
    def raise_typeerror(self):
        raise TypeError('Did I pass through?')


# this class is used on the client part. We cannot inherit from RPCTest
# because the automagic metaclass based code that generates the RPCClient
# proxy class from this does not handle inheritance properly.
# We do add an endpoint on the client side that has no implementation
# server-side to test this very situation (in should generate a 404)
class RPCTest2:
    @remote_api_endpoint('endpoint_url')
    def endpoint(self, test_data, db=None, cur=None):
        assert test_data == 'spam'
        return 'egg'

    @remote_api_endpoint('path/to/endpoint')
    def something(self, data, db=None, cur=None):
        return data

    @remote_api_endpoint('not_on_server')
    def not_on_server(self, db=None, cur=None):
        return 'ok'

    @remote_api_endpoint('raises_typeerror')
    def raise_typeerror(self):
        return 'data'


class RPCTestClient(RPCClient):
    backend_class = RPCTest2


@pytest.fixture
def app():
    # This fixture is used by the 'swh_rpc_adapter' fixture
    # which is defined in swh/core/pytest_plugin.py
    application = RPCServerApp('testapp', backend_class=RPCTest)
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
        swh_rpc_client.missing(data='whatever')


def test_api_server_endpoint_missing(swh_rpc_client):
    # A 'missing' endpoint (server-side) should raise an exception
    # due to a 404, since at the end, we do a GET/POST an inexistent URL
    with pytest.raises(Exception, match='404 Not Found'):
        swh_rpc_client.not_on_server()


def test_api_endpoint_kwargs(swh_rpc_client):
    res = swh_rpc_client.something(data='whatever')
    assert res == 'whatever'
    res = swh_rpc_client.endpoint(test_data='spam')
    assert res == 'egg'


def test_api_endpoint_args(swh_rpc_client):
    res = swh_rpc_client.something('whatever')
    assert res == 'whatever'
    res = swh_rpc_client.endpoint('spam')
    assert res == 'egg'


def test_api_typeerror(swh_rpc_client):
    with pytest.raises(TypeError, match='Did I pass through?'):
        swh_rpc_client.raise_typeerror()
