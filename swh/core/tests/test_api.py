# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

import requests_mock
from werkzeug.wrappers import BaseResponse
from werkzeug.test import Client as WerkzeugTestClient

from swh.core.api import (
        error_handler, encode_data_server,
        remote_api_endpoint, SWHRemoteAPI, SWHServerAPIApp)


class ApiTest(unittest.TestCase):
    def test_server(self):
        testcase = self
        nb_endpoint_calls = 0

        class TestStorage:
            @remote_api_endpoint('test_endpoint_url')
            def test_endpoint(self, test_data, db=None, cur=None):
                nonlocal nb_endpoint_calls
                nb_endpoint_calls += 1

                testcase.assertEqual(test_data, 'spam')
                return 'egg'

        app = SWHServerAPIApp('testapp',
                              backend_class=TestStorage,
                              backend_factory=lambda: TestStorage())

        @app.errorhandler(Exception)
        def my_error_handler(exception):
            return error_handler(exception, encode_data_server)

        client = WerkzeugTestClient(app, BaseResponse)
        res = client.post('/test_endpoint_url',
                          headers={'Content-Type': 'application/x-msgpack'},
                          data=b'\x81\xa9test_data\xa4spam')

        self.assertEqual(nb_endpoint_calls, 1)
        self.assertEqual(b''.join(res.response), b'\xa3egg')

    def test_client(self):
        class TestStorage:
            @remote_api_endpoint('test_endpoint_url')
            def test_endpoint(self, test_data, db=None, cur=None):
                pass

        nb_http_calls = 0

        def callback(request, context):
            nonlocal nb_http_calls
            nb_http_calls += 1
            self.assertEqual(request.headers['Content-Type'],
                             'application/x-msgpack')
            self.assertEqual(request.body, b'\x81\xa9test_data\xa4spam')
            context.headers['Content-Type'] = 'application/x-msgpack'
            context.content = b'\xa3egg'
            return b'\xa3egg'

        adapter = requests_mock.Adapter()
        adapter.register_uri('POST',
                             'mock://example.com/test_endpoint_url',
                             content=callback)

        class Testclient(SWHRemoteAPI):
            backend_class = TestStorage

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.session.mount('mock', adapter)

        c = Testclient('foo', 'mock://example.com/')
        res = c.test_endpoint('spam')

        self.assertEqual(nb_http_calls, 1)
        self.assertEqual(res, 'egg')
