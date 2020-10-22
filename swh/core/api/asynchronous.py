# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import OrderedDict
import functools
import logging
from typing import Callable, Dict, List, Optional, Tuple, Type, Union

import aiohttp.web
from aiohttp_utils import Response, negotiation
from deprecated import deprecated
import multidict

from .serializers import (
    exception_to_dict,
    json_dumps,
    json_loads,
    msgpack_dumps,
    msgpack_loads,
)


def encode_msgpack(data, **kwargs):
    return aiohttp.web.Response(
        body=msgpack_dumps(data),
        headers=multidict.MultiDict({"Content-Type": "application/x-msgpack"}),
        **kwargs,
    )


encode_data_server = Response


def render_msgpack(request, data, extra_encoders=None):
    return msgpack_dumps(data, extra_encoders=extra_encoders)


def render_json(request, data, extra_encoders=None):
    return json_dumps(data, extra_encoders=extra_encoders)


def decode_data(data, content_type, extra_decoders=None):
    """Decode data according to content type, eventually using some extra decoders.

    """
    if not data:
        return {}
    if content_type == "application/x-msgpack":
        r = msgpack_loads(data, extra_decoders=extra_decoders)
    elif content_type == "application/json":
        r = json_loads(data, extra_decoders=extra_decoders)
    else:
        raise ValueError(f"Wrong content type `{content_type}` for API request")

    return r


async def decode_request(request, extra_decoders=None):
    """Decode asynchronously the request

    """
    data = await request.read()
    return decode_data(data, request.content_type, extra_decoders=extra_decoders)


async def error_middleware(app, handler):
    async def middleware_handler(request):
        try:
            return await handler(request)
        except Exception as e:
            if isinstance(e, aiohttp.web.HTTPException):
                raise
            logging.exception(e)
            res = exception_to_dict(e)
            if isinstance(e, app.client_exception_classes):
                status = 400
            else:
                status = 500
            return encode_data_server(res, status=status)

    return middleware_handler


class RPCServerApp(aiohttp.web.Application):
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
    """

    client_exception_classes: Tuple[Type[Exception], ...] = ()
    """Exceptions that should be handled as a client error (eg. object not
    found, invalid argument)"""
    extra_type_encoders: List[Tuple[type, str, Callable]] = []
    """Value of `extra_encoders` passed to `json_dumps` or `msgpack_dumps`
    to be able to serialize more object types."""
    extra_type_decoders: Dict[str, Callable] = {}
    """Value of `extra_decoders` passed to `json_loads` or `msgpack_loads`
    to be able to deserialize more object types."""

    def __init__(
        self,
        app_name: Optional[str] = None,
        backend_class: Optional[Callable] = None,
        backend_factory: Optional[Union[Callable, str]] = None,
        middlewares=(),
        **kwargs,
    ):
        nego_middleware = negotiation.negotiation_middleware(
            renderers=self._renderers(), force_rendering=True
        )
        middlewares = (nego_middleware, error_middleware,) + middlewares
        super().__init__(middlewares=middlewares, **kwargs)

        # swh decorations starts here
        self.app_name = app_name
        if backend_class is None and backend_factory is not None:
            raise ValueError(
                "backend_factory should only be provided if backend_class is"
            )
        self.backend_class = backend_class
        if backend_class is not None:
            backend_factory = backend_factory or backend_class
            for (meth_name, meth) in backend_class.__dict__.items():
                if hasattr(meth, "_endpoint_path"):
                    path = meth._endpoint_path
                    http_method = meth._method
                    path = path if path.startswith("/") else f"/{path}"
                    self.router.add_route(
                        http_method,
                        path,
                        self._endpoint(meth_name, meth, backend_factory),
                    )

    def _renderers(self):
        """Return an ordered list of renderers in order of increasing desirability (!)
        See mimetype.best_match() docstring

        """
        return OrderedDict(
            [
                (
                    "application/json",
                    lambda request, data: render_json(
                        request, data, extra_encoders=self.extra_type_encoders
                    ),
                ),
                (
                    "application/x-msgpack",
                    lambda request, data: render_msgpack(
                        request, data, extra_encoders=self.extra_type_encoders
                    ),
                ),
            ]
        )

    def _endpoint(self, meth_name, meth, backend_factory):
        """Create endpoint out of the method `meth`.

        """

        @functools.wraps(meth)  # Copy signature and doc
        async def decorated_meth(request, *args, **kwargs):
            obj_meth = getattr(backend_factory(), meth_name)
            data = await request.read()
            kw = decode_data(
                data, request.content_type, extra_decoders=self.extra_type_decoders
            )
            result = obj_meth(**kw)
            return encode_data_server(result)

        return decorated_meth


@deprecated(version="0.0.64", reason="Use the RPCServerApp instead")
class SWHRemoteAPI(RPCServerApp):
    pass
