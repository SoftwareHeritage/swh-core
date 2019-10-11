# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import msgpack
import json

import pytest

from swh.core.api.asynchronous import RPCServerApp, Response
from swh.core.api.asynchronous import encode_msgpack, decode_request

from swh.core.api.serializers import msgpack_dumps, SWHJSONEncoder


pytest_plugins = ['aiohttp.pytest_plugin', 'pytester']


async def root(request):
    return Response('toor')

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
    return Response(STRUCT)


async def echo(request):
    data = await decode_request(request)
    return Response(data)


async def echo_no_nego(request):
    # let the content negotiation handle the serialization for us...
    data = await decode_request(request)
    ret = encode_msgpack(data)
    return ret


def check_mimetype(src, dst):
    src = src.split(';')[0].strip()
    dst = dst.split(';')[0].strip()
    assert src == dst


@pytest.fixture
def async_app():
    app = RPCServerApp()
    app.router.add_route('GET', '/', root)
    app.router.add_route('GET', '/struct', struct)
    app.router.add_route('POST', '/echo', echo)
    app.router.add_route('POST', '/echo-no-nego', echo_no_nego)
    return app


async def test_get_simple(async_app, aiohttp_client) -> None:
    assert async_app is not None

    cli = await aiohttp_client(async_app)
    resp = await cli.get('/')
    assert resp.status == 200
    check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
    data = await resp.read()
    value = msgpack.unpackb(data, raw=False)
    assert value == 'toor'


async def test_get_simple_nego(async_app, aiohttp_client) -> None:
    cli = await aiohttp_client(async_app)
    for ctype in ('x-msgpack', 'json'):
        resp = await cli.get('/', headers={'Accept': 'application/%s' % ctype})
        assert resp.status == 200
        check_mimetype(resp.headers['Content-Type'], 'application/%s' % ctype)
        assert (await decode_request(resp)) == 'toor'


async def test_get_struct(async_app, aiohttp_client) -> None:
    """Test returned structured from a simple GET data is OK"""
    cli = await aiohttp_client(async_app)
    resp = await cli.get('/struct')
    assert resp.status == 200
    check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
    assert (await decode_request(resp)) == STRUCT


async def test_get_struct_nego(async_app, aiohttp_client) -> None:
    """Test returned structured from a simple GET data is OK"""
    cli = await aiohttp_client(async_app)
    for ctype in ('x-msgpack', 'json'):
        resp = await cli.get('/struct',
                             headers={'Accept': 'application/%s' % ctype})
        assert resp.status == 200
        check_mimetype(resp.headers['Content-Type'], 'application/%s' % ctype)
        assert (await decode_request(resp)) == STRUCT


async def test_post_struct_msgpack(async_app, aiohttp_client) -> None:
    """Test that msgpack encoded posted struct data is returned as is"""
    cli = await aiohttp_client(async_app)
    # simple struct
    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/x-msgpack'},
        data=msgpack_dumps({'toto': 42}))
    assert resp.status == 200
    check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
    assert (await decode_request(resp)) == {'toto': 42}
    # complex struct
    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/x-msgpack'},
        data=msgpack_dumps(STRUCT))
    assert resp.status == 200
    check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
    assert (await decode_request(resp)) == STRUCT


async def test_post_struct_json(async_app, aiohttp_client) -> None:
    """Test that json encoded posted struct data is returned as is"""
    cli = await aiohttp_client(async_app)

    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({'toto': 42}, cls=SWHJSONEncoder))
    assert resp.status == 200
    check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
    assert (await decode_request(resp)) == {'toto': 42}

    resp = await cli.post(
        '/echo',
        headers={'Content-Type': 'application/json'},
        data=json.dumps(STRUCT, cls=SWHJSONEncoder))
    assert resp.status == 200
    check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
    # assert resp.headers['Content-Type'] == 'application/x-msgpack'
    assert (await decode_request(resp)) == STRUCT


async def test_post_struct_nego(async_app, aiohttp_client) -> None:
    """Test that json encoded posted struct data is returned as is

    using content negotiation (accept json or msgpack).
    """
    cli = await aiohttp_client(async_app)

    for ctype in ('x-msgpack', 'json'):
        resp = await cli.post(
            '/echo',
            headers={'Content-Type': 'application/json',
                     'Accept': 'application/%s' % ctype},
            data=json.dumps(STRUCT, cls=SWHJSONEncoder))
        assert resp.status == 200
        check_mimetype(resp.headers['Content-Type'], 'application/%s' % ctype)
        assert (await decode_request(resp)) == STRUCT


async def test_post_struct_no_nego(async_app, aiohttp_client) -> None:
    """Test that json encoded posted struct data is returned as msgpack

    when using non-negotiation-compatible handlers.
    """
    cli = await aiohttp_client(async_app)

    for ctype in ('x-msgpack', 'json'):
        resp = await cli.post(
            '/echo-no-nego',
            headers={'Content-Type': 'application/json',
                     'Accept': 'application/%s' % ctype},
            data=json.dumps(STRUCT, cls=SWHJSONEncoder))
        assert resp.status == 200
        check_mimetype(resp.headers['Content-Type'], 'application/x-msgpack')
        assert (await decode_request(resp)) == STRUCT
