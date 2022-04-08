# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass, field
import itertools
from typing import Callable, Generic, Iterable, List, Optional, TypeVar

TResult = TypeVar("TResult")
TToken = TypeVar("TToken")


@dataclass(eq=True)
class PagedResult(Generic[TResult, TToken]):
    """Represents a page of results; with a token to get the next page"""

    results: List[TResult] = field(default_factory=list)
    next_page_token: Optional[TToken] = field(default=None)


def _stream_results(f, *args, page_token, **kwargs):
    """Helper for stream_results() and stream_results_optional()"""
    while True:
        page_result = f(*args, page_token=page_token, **kwargs)
        yield from page_result.results
        page_token = page_result.next_page_token
        if page_token is None:
            break


def stream_results(
    f: Callable[..., PagedResult[TResult, TToken]], *args, **kwargs
) -> Iterable[TResult]:
    """Consume the paginated result and stream the page results"""
    if "page_token" in kwargs:
        raise TypeError('stream_results has no argument "page_token".')
    yield from _stream_results(f, *args, page_token=None, **kwargs)


def stream_results_optional(
    f: Callable[..., Optional[PagedResult[TResult, TToken]]], *args, **kwargs
) -> Optional[Iterable[TResult]]:
    """Like stream_results(), but for functions ``f`` that return an Optional."""
    if "page_token" in kwargs:
        raise TypeError('stream_results_optional has no argument "page_token".')
    res = f(*args, page_token=None, **kwargs)
    if res is None:
        return None
    else:
        if res.next_page_token is None:
            return iter(res.results)
        else:
            return itertools.chain(
                res.results,
                _stream_results(f, *args, page_token=res.next_page_token, **kwargs),
            )
