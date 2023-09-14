# This code is a partial and adapted copy of
#   https://github.com/nickstenning/negotiate
#
# Copyright 2012-2013 Nick Stenning
#           2019 The Software Heritage developers
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

from collections import defaultdict
import functools
from inspect import getcallargs
from typing import Any, Callable, DefaultDict, List, NoReturn, Optional, Type

from requests import Response


class FormatterNotFound(Exception):
    pass


class Formatter:
    format: Optional[str] = None
    mimetypes: List[str] = []

    def __init__(self, request_mimetype: Optional[str] = None) -> None:
        if request_mimetype is None or request_mimetype not in self.mimetypes:
            try:
                self.response_mimetype = self.mimetypes[0]
            except IndexError:
                raise NotImplementedError(
                    "%s.mimetypes should be a non-empty list" % self.__class__.__name__
                )
        else:
            self.response_mimetype = request_mimetype

    def configure(self) -> None:
        pass

    def render(self, obj: Any) -> bytes:
        raise NotImplementedError(
            "render() should be implemented by Formatter subclasses"
        )

    def __call__(self, obj: Any) -> Response:
        return self._make_response(
            self.render(obj), content_type=self.response_mimetype
        )

    def _make_response(self, body: bytes, content_type: str) -> Response:
        raise NotImplementedError(
            "_make_response() should be implemented by "
            "framework-specific subclasses of Formatter"
        )


class Negotiator:
    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func
        self._formatters: List[Type[Formatter]] = []
        self._formatters_by_format: DefaultDict = defaultdict(list)
        self._formatters_by_mimetype: DefaultDict = defaultdict(list)

    def __call__(self, *args, **kwargs) -> Response:
        result = self.func(*args, **kwargs)
        format = getcallargs(self.func, *args, **kwargs).get("format")
        mimetype = self.best_mimetype()

        try:
            formatter = self.get_formatter(format, mimetype)
        except FormatterNotFound as e:
            return self._abort(404, str(e))

        return formatter(result)

    def register_formatter(self, formatter: Type[Formatter], *args, **kwargs) -> None:
        self._formatters.append(formatter)
        self._formatters_by_format[formatter.format].append((formatter, args, kwargs))
        for mimetype in formatter.mimetypes:
            self._formatters_by_mimetype[mimetype].append((formatter, args, kwargs))

    def get_formatter(
        self, format: Optional[str] = None, mimetype: Optional[str] = None
    ) -> Formatter:
        if format is None and mimetype is None:
            raise TypeError(
                "get_formatter expects one of the 'format' or 'mimetype' "
                "kwargs to be set"
            )

        if format is not None:
            try:
                # the first added will be the most specific
                formatter_cls, args, kwargs = self._formatters_by_format[format][0]
            except IndexError:
                raise FormatterNotFound("Formatter for format '%s' not found!" % format)
        elif mimetype is not None:
            try:
                # the first added will be the most specific
                formatter_cls, args, kwargs = self._formatters_by_mimetype[mimetype][0]
            except IndexError:
                raise FormatterNotFound(
                    "Formatter for mimetype '%s' not found!" % mimetype
                )

        formatter = formatter_cls(request_mimetype=mimetype)
        formatter.configure(*args, **kwargs)
        return formatter

    @property
    def accept_mimetypes(self) -> List[str]:
        return [m for f in self._formatters for m in f.mimetypes]

    def best_mimetype(self) -> str:
        raise NotImplementedError(
            "best_mimetype() should be implemented in "
            "framework-specific subclasses of Negotiator"
        )

    def _abort(self, status_code: int, err: Optional[str] = None) -> NoReturn:
        raise NotImplementedError(
            "_abort() should be implemented in framework-specific "
            "subclasses of Negotiator"
        )


def negotiate(
    negotiator_cls: Type[Negotiator], formatter_cls: Type[Formatter], *args, **kwargs
) -> Callable:
    def decorator(f):
        if not hasattr(f, "negotiator"):
            f.negotiator = negotiator_cls(f)
        f.negotiator.register_formatter(formatter_cls, *args, **kwargs)

        @functools.wraps(f)
        def newf(*args, **kwargs):
            return f.negotiator(*args, **kwargs)

        return newf

    return decorator
