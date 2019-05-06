import json
import logging
import pickle
import sys
import traceback

import aiohttp.web
import multidict

from .serializers import msgpack_dumps, msgpack_loads, SWHJSONDecoder


def encode_data_server(data, **kwargs):
    return aiohttp.web.Response(
        body=msgpack_dumps(data),
        headers=multidict.MultiDict({'Content-Type': 'application/x-msgpack'}),
        **kwargs
    )


async def decode_request(request):
    content_type = request.headers.get('Content-Type')
    data = await request.read()
    if not data:
        return {}
    if content_type == 'application/x-msgpack':
        r = msgpack_loads(data)
    elif content_type == 'application/json':
        r = json.loads(data, cls=SWHJSONDecoder)
    else:
        raise ValueError('Wrong content type `%s` for API request'
                         % content_type)
    return r


async def error_middleware(app, handler):
    async def middleware_handler(request):
        try:
            return (await handler(request))
        except Exception as e:
            if isinstance(e, aiohttp.web.HTTPException):
                raise
            logging.exception(e)
            exception = traceback.format_exception(*sys.exc_info())
            res = {'exception': exception,
                   'exception_pickled': pickle.dumps(e)}
            return encode_data_server(res, status=500)
    return middleware_handler


class SWHRemoteAPI(aiohttp.web.Application):
    def __init__(self, *args, middlewares=(), **kwargs):
        middlewares = (error_middleware,) + middlewares
        super().__init__(*args, middlewares=middlewares, **kwargs)
