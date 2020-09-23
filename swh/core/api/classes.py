# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass, field
from typing import Callable, Generic, Iterable, List, Optional, TypeVar

TResult = TypeVar("TResult")
TToken = TypeVar("TToken")


@dataclass(eq=True)
class PagedResult(Generic[TResult, TToken]):
    """Represents a page of results; with a token to get the next page"""

    results: List[TResult] = field(default_factory=list)
    next_page_token: Optional[TToken] = field(default=None)


def stream_results(
    f: Callable[..., PagedResult[TResult, TToken]], *args, **kwargs
) -> Iterable[TResult]:
    """Consume the paginated result and stream the page results

    """
    if "page_token" in kwargs:
        raise TypeError('stream_results has no argument "page_token".')
    page_token = None
    while True:
        page_result = f(*args, page_token=page_token, **kwargs)
        yield from page_result.results
        page_token = page_result.next_page_token
        if page_token is None:
            break
