# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import bisect
import itertools
from typing import Any, Callable, Generic, Iterator, List, Optional, Tuple, TypeVar

SortedListItem = TypeVar("SortedListItem")
SortedListKey = TypeVar("SortedListKey")


class SortedList(Generic[SortedListKey, SortedListItem]):
    data: List[Tuple[SortedListKey, SortedListItem]]

    # https://github.com/python/mypy/issues/708
    # key: Callable[[SortedListItem], SortedListKey]

    def __init__(
        self,
        data: List[SortedListItem] = None,
        key: Optional[Callable[[SortedListItem], SortedListKey]] = None,
    ):
        if key is None:

            def key(item):
                return item

        assert key is not None  # for mypy
        self.data = sorted((key(x), x) for x in data or [])

        self.key: Callable[[SortedListItem], SortedListKey] = key

    def add(self, item: SortedListItem):
        k = self.key(item)
        bisect.insort(self.data, (k, item))

    def __iter__(self) -> Iterator[SortedListItem]:
        for (k, item) in self.data:
            yield item

    def iter_from(self, start_key: Any) -> Iterator[SortedListItem]:
        """Returns an iterator over all the elements whose key is greater
        or equal to `start_key`.
        (This is an efficient equivalent to:
        `(x for x in L if key(x) >= start_key)`)
        """
        from_index = bisect.bisect_left(self.data, (start_key,))
        for (k, item) in itertools.islice(self.data, from_index, None):
            yield item

    def iter_after(self, start_key: Any) -> Iterator[SortedListItem]:
        """Same as iter_from, but using a strict inequality."""
        it = self.iter_from(start_key)
        for item in it:
            if self.key(item) > start_key:
                yield item
                break

        yield from it
