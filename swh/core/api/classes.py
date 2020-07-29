# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass, field

from typing import (
    Generic,
    List,
    Optional,
    TypeVar,
)


TResult = TypeVar("TResult")
TToken = TypeVar("TToken")


@dataclass(eq=True)
class PagedResult(Generic[TResult, TToken]):
    """Represents a page of results; with a token to get the next page"""

    results: List[TResult] = field(default_factory=list)
    next_page_token: Optional[TToken] = field(default=None)
