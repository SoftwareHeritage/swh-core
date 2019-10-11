# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import json
import msgpack

from flask import url_for

from swh.core.api import remote_api_endpoint, RPCServerApp


@pytest.fixture
def app():
    class TestStorage:
        @remote_api_endpoint('test_endpoint_url')
        def test_endpoint(self, test_data, db=None, cur=None):
            assert test_data == 'spam'
            return 'egg'

        @remote_api_endpoint('path/to/endpoint')
        def something(self, data, db=None, cur=None):
            return data

    return RPCServerApp('testapp', backend_class=TestStorage)


def test_api_endpoint(flask_app_client):
    res = flask_app_client.post(
        url_for('something'),
        headers=[('Content-Type', 'application/json'),
                 ('Accept', 'application/json')],
        data=json.dumps({'data': 'toto'}),
    )
    assert res.status_code == 200
    assert res.mimetype == 'application/json'


def test_api_nego_default(flask_app_client):
    res = flask_app_client.post(
        url_for('something'),
        headers=[('Content-Type', 'application/json')],
        data=json.dumps({'data': 'toto'}),
    )
    assert res.status_code == 200
    assert res.mimetype == 'application/json'
    assert res.data == b'"toto"'


def test_api_nego_accept(flask_app_client):
    res = flask_app_client.post(
        url_for('something'),
        headers=[('Accept', 'application/x-msgpack'),
                 ('Content-Type', 'application/x-msgpack')],
        data=msgpack.dumps({'data': 'toto'}),
    )
    assert res.status_code == 200
    assert res.mimetype == 'application/x-msgpack'
    assert res.data == b'\xa4toto'


def test_rpc_server(flask_app_client):
    res = flask_app_client.post(
        url_for('test_endpoint'),
        headers=[('Content-Type', 'application/x-msgpack'),
                 ('Accept', 'application/x-msgpack')],
        data=b'\x81\xa9test_data\xa4spam')

    assert res.status_code == 200
    assert res.mimetype == 'application/x-msgpack'
    assert res.data == b'\xa3egg'
