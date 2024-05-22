# Copyright (C) 2018-2023  The Software Heritage developers
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


class MyCustomException(Exception):
    pass


class MyRPCServerApp(RPCServerApp):
    extra_type_encoders = extra_encoders
    extra_type_decoders = extra_decoders
    exception_status_codes = [
        *RPCServerApp.exception_status_codes,
        (MyCustomException, 503),
    ]


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

    @remote_api_endpoint("crashy/builtin")
    def crashy(self, data, db=None, cur=None):
        raise ValueError("this is an unexpected exception")

    @remote_api_endpoint("crashy/custom")
    def custom_crashy(self, data, db=None, cur=None):
        raise MyCustomException("try again later!")

    @remote_api_endpoint("crashy/adminshutdown")
    def adminshutdown_crash(self, data, db=None, cur=None):
        from psycopg2.errors import AdminShutdown

        raise AdminShutdown("cluster is shutting down")

    @remote_api_endpoint("crashy/querycancelled")
    def querycancelled_crash(self, data, db=None, cur=None):
        from psycopg2.errors import QueryCanceled

        raise QueryCanceled("too big!")


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


def test_rpc_server_exception(flask_app_client):
    res = flask_app_client.post(
        url_for("crashy"),
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=msgpack.dumps({"data": "toto"}),
    )

    assert res.status_code == 500
    assert res.mimetype == "application/x-msgpack", res.data
    data = msgpack.loads(res.data)
    assert data["type"] == "ValueError"
    assert data["module"] == "builtins"
    assert data["args"] == ["this is an unexpected exception"]


def test_rpc_server_custom_exception(flask_app_client):
    res = flask_app_client.post(
        url_for("custom_crashy"),
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=msgpack.dumps({"data": "toto"}),
    )

    assert res.status_code == 503
    assert res.mimetype == "application/x-msgpack", res.data
    data = msgpack.loads(res.data)
    assert data["type"] == "MyCustomException"
    assert data["module"] in (
        "swh.core.api.tests.test_rpc_server",
        "core.api.tests.test_rpc_server",
    )
    assert data["args"] == ["try again later!"]


def test_rpc_server_psycopg2_adminshutdown(flask_app_client):
    pytest.importorskip("psycopg2")

    res = flask_app_client.post(
        url_for("adminshutdown_crash"),
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=msgpack.dumps({"data": "toto"}),
    )

    assert res.status_code == 503
    assert res.mimetype == "application/x-msgpack", res.data
    data = msgpack.loads(res.data)
    assert data["type"] == "AdminShutdown"
    assert data["module"] == "psycopg2.errors"
    assert data["args"] == ["cluster is shutting down"]


def test_rpc_server_psycopg2_querycancelled(flask_app_client):
    pytest.importorskip("psycopg2")

    res = flask_app_client.post(
        url_for("querycancelled_crash"),
        headers=[
            ("Content-Type", "application/x-msgpack"),
            ("Accept", "application/x-msgpack"),
        ],
        data=msgpack.dumps({"data": "toto"}),
    )

    assert res.status_code == 500
    assert res.mimetype == "application/x-msgpack", res.data
    data = msgpack.loads(res.data)
    assert data["type"] == "QueryCanceled"
    assert data["module"] == "psycopg2.errors"
    assert data["args"] == ["too big!"]


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

    res = flask_app_client.post(
        url,
        headers=[("Content-Type", "application/json")],
    )
    assert res.status_code == 200
    assert res.mimetype == "application/json"
    assert res.data == b'"test"'
