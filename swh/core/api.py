# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import functools
import inspect
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


def remote_api_endpoint(path):
    def dec(f):
        f._endpoint_path = path
        return f
    return dec


class MetaSWHRemoteAPI(type):
    """Metaclass for SWHRemoteAPI, which adds a method for each endpoint
    of the database it is designed to access.

    See for example :class:`swh.indexer.storage.api.client.RemoteStorage`"""
    def __new__(cls, name, bases, attributes):
        # For each method wrapped with @remote_api_endpoint in an API backend
        # (eg. :class:`swh.indexer.storage.IndexerStorage`), add a new
        # method in RemoteStorage, with the same documentation.
        #
        # Note that, despite the usage of decorator magic (eg. functools.wrap),
        # this never actually calls an IndexerStorage method.
        backend_class = attributes.get('backend_class', None)
        for base in bases:
            if backend_class is not None:
                break
            backend_class = getattr(base, 'backend_class', None)
        if backend_class:
            for (meth_name, meth) in backend_class.__dict__.items():
                if hasattr(meth, '_endpoint_path'):
                    cls.__add_endpoint(meth_name, meth, attributes)
        return super().__new__(cls, name, bases, attributes)

    @staticmethod
    def __add_endpoint(meth_name, meth, attributes):
        wrapped_meth = inspect.unwrap(meth)

        @functools.wraps(meth)  # Copy signature and doc
        def meth_(*args, **kwargs):
            # Match arguments and parameters
            post_data = inspect.getcallargs(
                    wrapped_meth, *args, **kwargs)

            # Remove arguments that should not be passed
            self = post_data.pop('self')
            post_data.pop('cur', None)
            post_data.pop('db', None)

            # Send the request.
            return self.post(meth._endpoint_path, post_data)
        attributes[meth_name] = meth_


class SWHRemoteAPI(metaclass=MetaSWHRemoteAPI):
    """Proxy to an internal SWH API

    """

    backend_class = None
    """For each method of `backend_class` decorated with
    :func:`remote_api_endpoint`, a method with the same prototype and
    docstring will be added to this class. Calls to this new method will
    be translated into HTTP requests to a remote server.

    This backend class will never be instantiated, it only serves as
    a template."""

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
    """For each endpoint of the given `backend_class`, tells app.route to call
    a function that decodes the request and sends it to the backend object
    provided by the factory.

    :param Any backend_class: The class of the backend, which will be
                              analyzed to look for API endpoints.
    :param Callable[[], backend_class] backend_factory: A function with no
                                                        argument that returns
                                                        an instance of
                                                        `backend_class`."""
    request_class = BytesRequest

    def __init__(self, *args, backend_class=None, backend_factory=None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        if backend_class is not None:
            if backend_factory is None:
                raise TypeError('Missing argument backend_factory')
            for (meth_name, meth) in backend_class.__dict__.items():
                if hasattr(meth, '_endpoint_path'):
                    self.__add_endpoint(meth_name, meth, backend_factory)

    def __add_endpoint(self, meth_name, meth, backend_factory):
        from flask import request

        @self.route('/'+meth._endpoint_path, methods=['POST'])
        @functools.wraps(meth)  # Copy signature and doc
        def _f():
            # Call the actual code
            obj_meth = getattr(backend_factory(), meth_name)
            return encode_data_server(obj_meth(**decode_request(request)))
