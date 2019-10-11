# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
import pytest

from swh.core.api import remote_api_endpoint, RPCClient


@pytest.fixture
def rpc_client(requests_mock):
    class TestStorage:
        @remote_api_endpoint('test_endpoint_url')
        def test_endpoint(self, test_data, db=None, cur=None):
            return 'egg'

        @remote_api_endpoint('path/to/endpoint')
        def something(self, data, db=None, cur=None):
            return 'spam'

    class Testclient(RPCClient):
        backend_class = TestStorage

    def callback(request, context):
        assert request.headers['Content-Type'] == 'application/x-msgpack'
        context.headers['Content-Type'] = 'application/x-msgpack'
        if request.path == '/test_endpoint_url':
            context.content = b'\xa3egg'
        elif request.path == '/path/to/endpoint':
            context.content = b'\xa4spam'
        else:
            assert False
        return context.content

    requests_mock.post(re.compile('mock://example.com/'),
                       content=callback)

    return Testclient(url='mock://example.com')


def test_client(rpc_client):

    assert hasattr(rpc_client, 'test_endpoint')
    assert hasattr(rpc_client, 'something')

    res = rpc_client.test_endpoint('spam')
    assert res == 'egg'
    res = rpc_client.test_endpoint(test_data='spam')
    assert res == 'egg'

    res = rpc_client.something('whatever')
    assert res == 'spam'
    res = rpc_client.something(data='whatever')
    assert res == 'spam'
