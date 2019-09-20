import json
import logging
import pickle
import sys
import traceback
from collections import OrderedDict
import multidict

import aiohttp.web
from deprecated import deprecated

from .serializers import msgpack_dumps, msgpack_loads
from .serializers import SWHJSONDecoder, SWHJSONEncoder

from aiohttp_utils import negotiation, Response


def encode_msgpack(data, **kwargs):
    return aiohttp.web.Response(
        body=msgpack_dumps(data),
        headers=multidict.MultiDict(
            {'Content-Type': 'application/x-msgpack'}),
        **kwargs
    )


encode_data_server = Response


def render_msgpack(request, data):
    return msgpack_dumps(data)


def render_json(request, data):
    return json.dumps(data, cls=SWHJSONEncoder)


async def decode_request(request):
    content_type = request.headers.get('Content-Type').split(';')[0].strip()
    data = await request.read()
    if not data:
        return {}
    if content_type == 'application/x-msgpack':
        r = msgpack_loads(data)
    elif content_type == 'application/json':
        r = json.loads(data.decode(), cls=SWHJSONDecoder)
    else:
        raise ValueError('Wrong content type `%s` for API request'
                         % content_type)
    return r


async def error_middleware(app, handler):
    async def middleware_handler(request):
        try:
            return await handler(request)
        except Exception as e:
            if isinstance(e, aiohttp.web.HTTPException):
                raise
            logging.exception(e)
            exception = traceback.format_exception(*sys.exc_info())
            res = {'exception': exception,
                   'exception_pickled': pickle.dumps(e)}
            return encode_data_server(res, status=500)
    return middleware_handler


class RPCServerApp(aiohttp.web.Application):
    def __init__(self, *args, middlewares=(), **kwargs):
        middlewares = (error_middleware,) + middlewares
        # renderers are sorted in order of increasing desirability (!)
        # see mimeparse.best_match() docstring.
        renderers = OrderedDict([
            ('application/json', render_json),
            ('application/x-msgpack', render_msgpack),
        ])
        nego_middleware = negotiation.negotiation_middleware(
            renderers=renderers,
            force_rendering=True)
        middlewares = (nego_middleware,) + middlewares

        super().__init__(*args, middlewares=middlewares, **kwargs)


@deprecated(version='0.0.64',
            reason='Use the RPCServerApp instead')
class SWHRemoteAPI(RPCServerApp):
    pass
