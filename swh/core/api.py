# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import json
import logging
import pickle
import requests

from flask import Flask, Request, Response
from .serializers import (decode_response,
                          encode_data_client as encode_data,
                          msgpack_dumps, msgpack_loads, SWHJSONDecoder)


class RemoteException(Exception):
    pass


class SWHRemoteAPI:
    """Proxy to an internal SWH API

    """

    def __init__(self, api_exception, url, timeout=None):
        super().__init__()
        self.api_exception = api_exception
        base_url = url if url.endswith('/') else url + '/'
        self.url = base_url
        self.session = requests.Session()
        self.timeout = timeout

    def _url(self, endpoint):
        return '%s%s' % (self.url, endpoint)

    def raw_post(self, endpoint, data, **opts):
        if self.timeout and 'timeout' not in opts:
            opts['timeout'] = self.timeout
        try:
            return self.session.post(
                self._url(endpoint),
                data=data,
                **opts
            )
        except requests.exceptions.ConnectionError as e:
            raise self.api_exception(e)

    def raw_get(self, endpoint, params=None, **opts):
        if self.timeout and 'timeout' not in opts:
            opts['timeout'] = self.timeout
        try:
            return self.session.get(
                self._url(endpoint),
                params=params,
                **opts
            )
        except requests.exceptions.ConnectionError as e:
            raise self.api_exception(e)

    def post(self, endpoint, data, params=None):
        data = encode_data(data)
        response = self.raw_post(
            endpoint, data, params=params,
            headers={'content-type': 'application/x-msgpack'})
        return self._decode_response(response)

    def get(self, endpoint, params=None):
        response = self.raw_get(endpoint, params=params)
        return self._decode_response(response)

    def post_stream(self, endpoint, data, params=None):
        if not isinstance(data, collections.Iterable):
            raise ValueError("`data` must be Iterable")
        response = self.raw_post(endpoint, data, params=params)
        return self._decode_response(response)

    def get_stream(self, endpoint, params=None, chunk_size=4096):
        response = self.raw_get(endpoint, params=params, stream=True)
        return response.iter_content(chunk_size)

    def _decode_response(self, response):
        if response.status_code == 404:
            return None
        if response.status_code == 500:
            data = decode_response(response)
            if 'exception_pickled' in data:
                raise pickle.loads(data['exception_pickled'])
            else:
                raise RemoteException(data['exception'])

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))
        elif response.status_code != 200:
            raise RemoteException(
                "Unexpected status code for API request: %s (%s)" % (
                    response.status_code,
                    response.content,
                )
            )
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
