# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import itertools


def grouper(iterable, n):
    """Collect data into fixed-length chunks or blocks.
    The last block is exactly the size of the remaining data.

    Args:
        iterable: an iterable
        n: size of block

    Returns:
        fixed-length chunks of blocks as iterables (except for the last
        one which can be of size < n)

    """
    args = [iter(iterable)] * n
    fv = None
    for _data in itertools.zip_longest(*args, fillvalue=fv):
        yield (d for d in _data if d is not fv)
