# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import abc
import functools
import inspect
import logging
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from deprecated import deprecated
from flask import Flask, Request, Response, abort, request
import requests
import sentry_sdk
from werkzeug.exceptions import HTTPException

from .negotiation import Formatter as FormatterBase
from .negotiation import Negotiator as NegotiatorBase
from .negotiation import negotiate as _negotiate
from .serializers import (
    exception_to_dict,
    json_dumps,
    json_loads,
    msgpack_dumps,
    msgpack_loads,
)
from .serializers import decode_response
from .serializers import encode_data_client as encode_data

logger = logging.getLogger(__name__)


# support for content negotiation


class Negotiator(NegotiatorBase):
    def best_mimetype(self):
        return request.accept_mimetypes.best_match(
            self.accept_mimetypes, "application/json"
        )

    def _abort(self, status_code, err=None):
        return abort(status_code, err)


def negotiate(formatter_cls, *args, **kwargs):
    return _negotiate(Negotiator, formatter_cls, *args, **kwargs)


class Formatter(FormatterBase):
    def _make_response(self, body, content_type):
        return Response(body, content_type=content_type)

    def configure(self, extra_encoders=None):
        self.extra_encoders = extra_encoders


class JSONFormatter(Formatter):
    format = "json"
    mimetypes = ["application/json"]

    def render(self, obj):
        return json_dumps(obj, extra_encoders=self.extra_encoders)


class MsgpackFormatter(Formatter):
    format = "msgpack"
    mimetypes = ["application/x-msgpack"]

    def render(self, obj):
        return msgpack_dumps(obj, extra_encoders=self.extra_encoders)


# base API classes


class RemoteException(Exception):
    """raised when remote returned an out-of-band failure notification, e.g., as a
    HTTP status code or serialized exception

    Attributes:
        response: HTTP response corresponding to the failure

    """

    def __init__(
        self,
        payload: Optional[Any] = None,
        response: Optional[requests.Response] = None,
    ):
        if payload is not None:
            super().__init__(payload)
        else:
            super().__init__()
        self.response = response

    def __str__(self):
        if (
            self.args
            and isinstance(self.args[0], dict)
            and "type" in self.args[0]
            and "args" in self.args[0]
        ):
            return (
                f"<RemoteException {self.response.status_code} "
                f'{self.args[0]["type"]}: {self.args[0]["args"]}>'
            )
        else:
            return super().__str__()


class TransientRemoteException(RemoteException):
    """Subclass of RemoteException representing errors which are expected
    to be temporary.
    """


F = TypeVar("F", bound=Callable)


def remote_api_endpoint(path: str, method: str = "POST") -> Callable[[F], F]:
    def dec(f: F) -> F:
        f._endpoint_path = path  # type: ignore
        f._method = method  # type: ignore
        return f

    return dec


class APIError(Exception):
    """API Error"""

    def __str__(self):
        return "An unexpected error occurred in the backend: {}".format(self.args)


class MetaRPCClient(type):
    """Metaclass for RPCClient, which adds a method for each endpoint
    of the database it is designed to access.

    See for example :class:`swh.indexer.storage.api.client.RemoteStorage`"""

    def __new__(cls, name, bases, attributes):
        # For each method wrapped with @remote_api_endpoint in an API backend
        # (eg. :class:`swh.indexer.storage.IndexerStorage`), add a new
        # method in RemoteStorage, with the same documentation.
        #
        # Note that, despite the usage of decorator magic (eg. functools.wrap),
        # this never actually calls an IndexerStorage method.
        backend_class = attributes.get("backend_class", None)
        for base in bases:
            if backend_class is not None:
                break
            backend_class = getattr(base, "backend_class", None)
        if backend_class:
            for (meth_name, meth) in backend_class.__dict__.items():
                if hasattr(meth, "_endpoint_path"):
                    cls.__add_endpoint(meth_name, meth, attributes)
        return super().__new__(cls, name, bases, attributes)

    @staticmethod
    def __add_endpoint(meth_name, meth, attributes):
        wrapped_meth = inspect.unwrap(meth)

        @functools.wraps(meth)  # Copy signature and doc
        def meth_(*args, **kwargs):
            # Match arguments and parameters
            post_data = inspect.getcallargs(wrapped_meth, *args, **kwargs)

            # Remove arguments that should not be passed
            self = post_data.pop("self")
            post_data.pop("cur", None)
            post_data.pop("db", None)

            # Send the request.
            return self._post(meth._endpoint_path, post_data)

        if meth_name not in attributes:
            attributes[meth_name] = meth_


class RPCClient(metaclass=MetaRPCClient):
    """Proxy to an internal SWH RPC"""

    backend_class = None  # type: ClassVar[Optional[type]]
    """For each method of `backend_class` decorated with
    :func:`remote_api_endpoint`, a method with the same prototype and
    docstring will be added to this class. Calls to this new method will
    be translated into HTTP requests to a remote server.

    This backend class will never be instantiated, it only serves as
    a template."""

    api_exception = APIError  # type: ClassVar[Type[Exception]]
    """The exception class to raise in case of communication error with
    the server."""

    reraise_exceptions: ClassVar[List[Type[Exception]]] = []
    """On server errors, if any of the exception classes in this list
    has the same name as the error name, then the exception will
    be instantiated and raised instead of a generic RemoteException."""

    extra_type_encoders: List[Tuple[type, str, Callable]] = []
    """Value of `extra_encoders` passed to `json_dumps` or `msgpack_dumps`
    to be able to serialize more object types."""
    extra_type_decoders: Dict[str, Callable] = {}
    """Value of `extra_decoders` passed to `json_loads` or `msgpack_loads`
    to be able to deserialize more object types."""

    def __init__(
        self,
        url,
        api_exception=None,
        timeout=None,
        chunk_size=4096,
        reraise_exceptions=None,
        **kwargs,
    ):
        if api_exception:
            self.api_exception = api_exception
        if reraise_exceptions:
            self.reraise_exceptions = reraise_exceptions
        base_url = url if url.endswith("/") else url + "/"
        self.url = base_url
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            max_retries=kwargs.get("max_retries", 3),
            pool_connections=kwargs.get("pool_connections", 20),
            pool_maxsize=kwargs.get("pool_maxsize", 100),
        )
        self.session.mount(self.url, adapter)

        self.timeout = timeout
        self.chunk_size = chunk_size

    def _url(self, endpoint):
        return "%s%s" % (self.url, endpoint)

    def raw_verb(self, verb, endpoint, **opts):
        if "chunk_size" in opts:
            # if the chunk_size argument has been passed, consider the user
            # also wants stream=True, otherwise, what's the point.
            opts["stream"] = True
        if self.timeout and "timeout" not in opts:
            opts["timeout"] = self.timeout
        try:
            return getattr(self.session, verb)(self._url(endpoint), **opts)
        except requests.exceptions.ConnectionError as e:
            raise self.api_exception(e)

    def _post(self, endpoint, data, **opts):
        if isinstance(data, (abc.Iterator, abc.Generator)):
            data = (self._encode_data(x) for x in data)
        else:
            data = self._encode_data(data)
        chunk_size = opts.pop("chunk_size", self.chunk_size)
        response = self.raw_verb(
            "post",
            endpoint,
            data=data,
            headers={
                "content-type": "application/x-msgpack",
                "accept": "application/x-msgpack",
            },
            **opts,
        )
        if opts.get("stream") or response.headers.get("transfer-encoding") == "chunked":
            self.raise_for_status(response)
            return response.iter_content(chunk_size)
        else:
            return self._decode_response(response)

    def _encode_data(self, data):
        return encode_data(data, extra_encoders=self.extra_type_encoders)

    _post_stream = _post

    @deprecated(version="2.1.0", reason="Use _post instead")
    def post(self, *args, **kwargs):
        return self._post(*args, **kwargs)

    @deprecated(version="2.1.0", reason="Use _post_stream instead")
    def post_stream(self, *args, **kwargs):
        return self._post_stream(*args, **kwargs)

    def _get(self, endpoint, **opts):
        chunk_size = opts.pop("chunk_size", self.chunk_size)
        response = self.raw_verb(
            "get", endpoint, headers={"accept": "application/x-msgpack"}, **opts
        )
        if opts.get("stream") or response.headers.get("transfer-encoding") == "chunked":
            self.raise_for_status(response)
            return response.iter_content(chunk_size)
        else:
            return self._decode_response(response)

    def _get_stream(self, endpoint, **opts):
        return self._get(endpoint, stream=True, **opts)

    @deprecated(version="2.1.0", reason="Use _get instead")
    def get(self, *args, **kwargs):
        return self._get(*args, **kwargs)

    @deprecated(version="2.1.0", reason="Use _get_stream instead")
    def get_stream(self, *args, **kwargs):
        return self._get_stream(*args, **kwargs)

    def raise_for_status(self, response) -> None:
        """check response HTTP status code and raise an exception if it denotes an
        error; do nothing otherwise

        """
        status_code = response.status_code
        status_class = response.status_code // 100

        if status_code == 404:
            raise RemoteException(payload="404 not found", response=response)

        exception = None

        if status_class == 4:
            exc_data = self._decode_response(response, check_status=False)
            for exc_type in self.reraise_exceptions:
                if exc_type.__name__ == exc_data["type"]:
                    exception = exc_type(*exc_data["args"])
                    break
            else:
                exception = RemoteException(payload=exc_data, response=response)

        elif status_class == 5:
            cls: Type[RemoteException]
            if status_code == 503:
                # This isn't a generic HTTP client and we know the server does
                # not support the Retry-After header, so we do not implement
                # it here either.
                cls = TransientRemoteException
            else:
                cls = RemoteException
            exc_data = self._decode_response(response, check_status=False)
            exception = cls(payload=exc_data, response=response)

        if exception:
            raise exception from None

        if status_class != 2:
            raise RemoteException(
                payload=f"API HTTP error: {status_code} {response.content}",
                response=response,
            )

    def _decode_response(self, response, check_status=True):
        if check_status:
            self.raise_for_status(response)
        return decode_response(response, extra_decoders=self.extra_type_decoders)

    def __repr__(self):
        return "<{} url={}>".format(self.__class__.__name__, self.url)


class BytesRequest(Request):
    """Request with proper escaping of arbitrary byte sequences."""

    encoding = "utf-8"
    encoding_errors = "surrogateescape"


ENCODERS: Dict[str, Callable[[Any], Union[bytes, str]]] = {
    "application/x-msgpack": msgpack_dumps,
    "application/json": json_dumps,
}


def encode_data_server(
    data, content_type="application/x-msgpack", extra_type_encoders=None
):
    encoded_data = ENCODERS[content_type](data, extra_encoders=extra_type_encoders)
    return Response(
        encoded_data,
        mimetype=content_type,
    )


def decode_request(request, extra_decoders=None):
    content_type = request.mimetype
    data = request.get_data()
    if not data:
        return {}

    if content_type == "application/x-msgpack":
        r = msgpack_loads(data, extra_decoders=extra_decoders)
    elif content_type == "application/json":
        # XXX this .decode() is needed for py35.
        # Should not be needed any more with py37
        r = json_loads(data.decode("utf-8"), extra_decoders=extra_decoders)
    else:
        raise ValueError("Wrong content type `%s` for API request" % content_type)

    return r


def error_handler(exception, encoder, status_code=500):
    """Error handler to be registered using flask's error-handling decorator
    ``app.errorhandler``.

    This is used for exceptions that are expected in the normal execution flow of the
    RPC-ed API, in which case the status code should be set to a value in the 4xx range,
    as well as for exceptions that are unexpected (generally, a bare
    :class:`Exception`), and for which the status code should be kept in the 5xx class.

    This function only captures exceptions as sentry errors if the status code is in the
    5xx range, as "expected exceptions" in the 4xx range are more likely to be handled
    on the client side.

    """
    status_class = status_code // 100
    if status_class == 5:
        logger.exception(exception)
        sentry_sdk.capture_exception(exception)

    response = encoder(exception_to_dict(exception))
    if isinstance(exception, HTTPException):
        response.status_code = exception.code
    else:
        # TODO: differentiate between server errors and client errors
        response.status_code = status_code
    return response


class RPCServerApp(Flask):
    """For each endpoint of the given `backend_class`, tells app.route to call
    a function that decodes the request and sends it to the backend object
    provided by the factory.

    :param Any backend_class:
        The class of the backend, which will be analyzed to look
        for API endpoints.
    :param Optional[Callable[[], backend_class]] backend_factory:
        A function with no argument that returns an instance of
        `backend_class`. If unset, defaults to calling `backend_class`
        constructor directly.

    For each method 'do_x()' of the ``backend_factory``, subclasses may implement
    two methods: ``pre_do_x(self, kw)`` and ``post_do_x(self, ret, kw)`` that will
    be called respectively before and after ``do_x(**kw)``. ``kw`` is the dict
    of request parameters, and ``ret`` is the return value of ``do_x(**kw)``.
    """

    request_class = BytesRequest

    extra_type_encoders: List[Tuple[type, str, Callable]] = []
    """Value of `extra_encoders` passed to `json_dumps` or `msgpack_dumps`
    to be able to serialize more object types."""
    extra_type_decoders: Dict[str, Callable] = {}
    """Value of `extra_decoders` passed to `json_loads` or `msgpack_loads`
    to be able to deserialize more object types."""

    method_decorators: List[Callable[[Callable], Callable]] = []
    """List of decorators to all methods generated from the ``backend_class``."""

    def __init__(self, *args, backend_class=None, backend_factory=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_backend_class(backend_class, backend_factory)

    def add_backend_class(self, backend_class=None, backend_factory=None):
        if backend_class is None and backend_factory is not None:
            raise ValueError(
                "backend_factory should only be provided if backend_class is"
            )

        if backend_class is not None:
            backend_factory = backend_factory or backend_class
            for (meth_name, meth) in backend_class.__dict__.items():
                if hasattr(meth, "_endpoint_path"):
                    self.__add_endpoint(meth_name, meth, backend_factory)

    def __add_endpoint(self, meth_name, meth, backend_factory):
        from flask import request

        @negotiate(MsgpackFormatter, extra_encoders=self.extra_type_encoders)
        @negotiate(JSONFormatter, extra_encoders=self.extra_type_encoders)
        @functools.wraps(meth)  # Copy signature and doc
        def f():
            # Call the actual code
            pre_hook = getattr(self, f"pre_{meth_name}", None)
            post_hook = getattr(self, f"post_{meth_name}", None)
            obj_meth = getattr(backend_factory(), meth_name)
            kw = decode_request(request, extra_decoders=self.extra_type_decoders)

            if pre_hook is not None:
                pre_hook(kw)

            ret = obj_meth(**kw)

            if post_hook is not None:
                post_hook(ret, kw)

            return ret

        for decorator in self.method_decorators:
            f = decorator(f)

        self.route("/" + meth._endpoint_path, methods=["POST"])(f)
