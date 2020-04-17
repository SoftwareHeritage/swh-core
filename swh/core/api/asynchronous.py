# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import OrderedDict
import logging
from typing import Tuple, Type

import aiohttp.web
from deprecated import deprecated
import multidict

from .serializers import msgpack_dumps, msgpack_loads
from .serializers import json_dumps, json_loads
from .serializers import exception_to_dict

from aiohttp_utils import negotiation, Response


def encode_msgpack(data, **kwargs):
    return aiohttp.web.Response(
        body=msgpack_dumps(data),
        headers=multidict.MultiDict({"Content-Type": "application/x-msgpack"}),
        **kwargs,
    )


encode_data_server = Response


def render_msgpack(request, data):
    return msgpack_dumps(data)


def render_json(request, data):
    return json_dumps(data)


async def decode_request(request):
    content_type = request.headers.get("Content-Type").split(";")[0].strip()
    data = await request.read()
    if not data:
        return {}
    if content_type == "application/x-msgpack":
        r = msgpack_loads(data)
    elif content_type == "application/json":
        r = json_loads(data)
    else:
        raise ValueError("Wrong content type `%s` for API request" % content_type)
    return r


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
    client_exception_classes: Tuple[Type[Exception], ...] = ()
    """Exceptions that should be handled as a client error (eg. object not
    found, invalid argument)"""

    def __init__(self, *args, middlewares=(), **kwargs):
        middlewares = (error_middleware,) + middlewares
        # renderers are sorted in order of increasing desirability (!)
        # see mimeparse.best_match() docstring.
        renderers = OrderedDict(
            [
                ("application/json", render_json),
                ("application/x-msgpack", render_msgpack),
            ]
        )
        nego_middleware = negotiation.negotiation_middleware(
            renderers=renderers, force_rendering=True
        )
        middlewares = (nego_middleware,) + middlewares

        super().__init__(*args, middlewares=middlewares, **kwargs)


@deprecated(version="0.0.64", reason="Use the RPCServerApp instead")
class SWHRemoteAPI(RPCServerApp):
    pass
