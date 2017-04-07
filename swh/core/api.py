# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import pickle
import requests

from flask import Flask, Request, Response
from .serializers import (decode_response,
                          encode_data_client as encode_data,
                          msgpack_dumps, msgpack_loads, SWHJSONDecoder)


class SWHRemoteAPI:
    """Proxy to an internal SWH API

    """

    def __init__(self, api_exception, url):
        super().__init__()
        self.api_exception = api_exception
        base_url = url if url.endswith('/') else url + '/'
        self.url = base_url
        self.session = requests.Session()

    def _url(self, endpoint):
        return '%s%s' % (self.url, endpoint)

    def post(self, endpoint, data):
        try:
            response = self.session.post(
                self._url(endpoint),
                data=encode_data(data),
                headers={'content-type': 'application/x-msgpack'},
            )
        except requests.exceptions.ConnectionError as e:
            raise self.api_exception(e)

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        return decode_response(response)

    def get(self, endpoint, data=None):
        try:
            response = self.session.get(
                self._url(endpoint),
                params=data,
            )
        except requests.exceptions.ConnectionError as e:
            raise self.api_exception(e)

        if response.status_code == 404:
            return None

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        else:
            return decode_response(response)


class BytesRequest(Request):
    """Request with proper escaping of arbitrary byte sequences."""
    encoding = 'utf-8'
    encoding_errors = 'surrogateescape'


def encode_data_server(data):
    return Response(
        msgpack_dumps(data),
        mimetype='application/x-msgpack',
    )


def decode_request(request):
    content_type = request.mimetype
    data = request.get_data()

    if content_type == 'application/x-msgpack':
        r = msgpack_loads(data)
    elif content_type == 'application/json':
        r = json.loads(data, cls=SWHJSONDecoder)
    else:
        raise ValueError('Wrong content type `%s` for API request'
                         % content_type)

    return r


def error_handler(exception, encoder):
    # XXX: this breaks language-independence and should be
    # replaced by proper serialization of errors
    logging.exception(exception)
    response = encoder(pickle.dumps(exception))
    response.status_code = 400
    return response


class SWHServerAPIApp(Flask):
    request_class = BytesRequest
