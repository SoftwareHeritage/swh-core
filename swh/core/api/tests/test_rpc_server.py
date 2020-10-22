# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from flask import url_for
import msgpack
import pytest

from swh.core.api import (
    JSONFormatter,
    MsgpackFormatter,
    RPCServerApp,
    negotiate,
    remote_api_endpoint,
)

from .test_serializers import ExtraType, extra_decoders, extra_encoders


class MyRPCServerApp(RPCServerApp):
    extra_type_encoders = extra_encoders
    extra_type_decoders = extra_decoders


class TestStorage:
    @remote_api_endpoint("test_endpoint_url")
    def endpoint_test(self, test_data, db=None, cur=None):
        assert test_data == "spam"
        return "egg"

    @remote_api_endpoint("path/to/endpoint")
    def something(self, data, db=None, cur=None):
        return data

    @remote_api_endpoint("serializer_test")
    def serializer_test(self, data, db=None, cur=None):
        assert data == ["foo", ExtraType("bar", b"baz")]
        return ExtraType({"spam": "egg"}, "qux")


@pytest.fixture
def app():
    return MyRPCServerApp("testapp", backend_class=TestStorage)


def test_api_rpc_server_app_ok(app):
    assert isinstance(app, MyRPCServerApp)

    actual_rpc_server2 = MyRPCServerApp(
        "app2", backend_class=TestStorage, backend_factory=TestStorage
    )
    assert isinstance(actual_rpc_server2, MyRPCServerApp)

    actual_rpc_server3 = MyRPCServerApp("app3")
    assert isinstance(actual_rpc_server3, MyRPCServerApp)


def test_api_rpc_server_app_misconfigured():
    expected_error = "backend_factory should only be provided if backend_class is"
    with pytest.raises(ValueError, match=expected_error):
        MyRPCServerApp("failed-app", backend_factory="something-to-make-it-raise")


def test_api_endpoint(flask_app_client):
    res = flask_app_client.post(
        url_for("something"),
        headers=[("Content-Type", "application/json"), ("Accept", "application/json")],
        data=json.dumps({"data": "toto"}),
    )
    assert res.status_code == 200
    assert res.mimetype == "application/json"


def test_api_nego_default(flask_app_client):
    res = flask_app_client.post(
        url_for("something"),
        headers=[("Content-Type", "application/json")],
        data=json.dumps({"data": "toto"}),
    )
    assert res.status_code == 200
    assert res.mimetype == "application/json"
    assert res.data == b'"toto"'


def test_api_nego_accept(flask_app_client):
    res = flask_app_client.post(
        url_for("something"),
        headers=[
            ("Accept", "application/x-msgpack"),
            ("Content-Type", "application/x-msgpack"),
        ],
        data=msgpack.dumps({"data": "toto"}),
    )
    assert res.status_code == 200
    assert res.mimetype == "application/x-msgpack"
    assert res.data == b"\xa4toto"


def test_rpc_server(flask_app_client):
    res = flask_app_client.post(
        url_for("endpoint_test"),
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=b"\x81\xa9test_data\xa4spam",
    )

    assert res.status_code == 200
    assert res.mimetype == "application/x-msgpack"
    assert res.data == b"\xa3egg"


def test_rpc_server_extra_serializers(flask_app_client):
    res = flask_app_client.post(
        url_for("serializer_test"),
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=b"\x81\xa4data\x92\xa3foo\x82\xc4\x07swhtype\xa9extratype"
        b"\xc4\x01d\x92\xa3bar\xc4\x03baz",
    )

    assert res.status_code == 200
    assert res.mimetype == "application/x-msgpack"
    assert res.data == (
        b"\x82\xc4\x07swhtype\xa9extratype\xc4" b"\x01d\x92\x81\xa4spam\xa3egg\xa3qux"
    )


def test_api_negotiate_no_extra_encoders(app, flask_app_client):
    url = "/test/negotiate/no/extra/encoders"

    @app.route(url, methods=["POST"])
    @negotiate(MsgpackFormatter)
    @negotiate(JSONFormatter)
    def endpoint():
        return "test"

    res = flask_app_client.post(url, headers=[("Content-Type", "application/json")],)
    assert res.status_code == 200
    assert res.mimetype == "application/json"
    assert res.data == b'"test"'
