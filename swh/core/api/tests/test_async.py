# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json

import msgpack

import pytest

from swh.core.api.asynchronous import RPCServerApp
from swh.core.api.asynchronous import encode_data_server, decode_request
from swh.core.api.serializers import msgpack_dumps, SWHJSONEncoder


pytest_plugins = ['aiohttp.pytest_plugin', 'pytester']


async def root(request):
    return encode_data_server('toor')

STRUCT = {'txt': 'something stupid',
          # 'date': datetime.date(2019, 6, 9),  # not supported
          'datetime': datetime.datetime(2019, 6, 9, 10, 12),
          'timedelta': datetime.timedelta(days=-2, hours=3),
          'int': 42,
          'float': 3.14,
          'subdata': {'int': 42,
                      'datetime': datetime.datetime(2019, 6, 10, 11, 12),
                      },
          'list': [42, datetime.datetime(2019, 9, 10, 11, 12), 'ok'],
          }


async def struct(request):
    return encode_data_server(STRUCT)


async def echo(request):
    data = await decode_request(request)
    return encode_data_server(data)


@pytest.fixture
def app():
    app = RPCServerApp()
    app.router.add_route('GET', '/', root)
    app.router.add_route('GET', '/struct', struct)
    app.router.add_route('POST', '/echo', echo)
    return app


async def test_get_simple(app,  aiohttp_client) -> None:
    assert app is not None

    cli = await aiohttp_client(app)
    resp = await cli.get('/')
    assert resp.status == 200
    assert resp.headers['Content-Type'] == 'application/x-msgpack'
    data = await resp.read()
    value = msgpack.unpackb(data, raw=False)
    assert value == 'toor'


async def test_get_struct(app,  aiohttp_client) -> None:
    """Test returned structured from a simple GET data is OK"""
    cli = await aiohttp_client(app)
    resp = await cli.get('/struct')
    assert resp.status == 200
    assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == STRUCT


async def test_post_struct_msgpack(app,  aiohttp_client) -> None:
    """Test that msgpack encoded posted struct data is returned as is"""
    cli = await aiohttp_client(app)
    # simple struct
    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/x-msgpack'},
        data=msgpack_dumps({'toto': 42}))
    assert resp.status == 200
    assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == {'toto': 42}
    # complex struct
    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/x-msgpack'},
        data=msgpack_dumps(STRUCT))
    assert resp.status == 200
    assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == STRUCT


async def test_post_struct_json(app,  aiohttp_client) -> None:
    """Test that json encoded posted struct data is returned as is"""
    cli = await aiohttp_client(app)

    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({'toto': 42}, cls=SWHJSONEncoder))
    assert resp.status == 200
    assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == {'toto': 42}

    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/json'},
        data=json.dumps(STRUCT, cls=SWHJSONEncoder))
    assert resp.status == 200
    assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == STRUCT
