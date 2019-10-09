# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import re
import pytest

from functools import partial
from os import path
from typing import Dict, List, Optional
from urllib.parse import urlparse


logger = logging.getLogger(__name__)


# Check get_local_factory function
# Maximum number of iteration checks to generate requests responses
MAX_VISIT_FILES = 10


def get_response_cb(request, context, datadir,
                    ignore_urls: List[str] = [],
                    visits: Optional[Dict] = None):
    """Mount point callback to fetch on disk the request's content.

    This is meant to be used as 'body' argument of the requests_mock.get()
    method.

    It will look for files on the local filesystem based on the requested URL,
    using the following rules:

    - files are searched in the datadir/<hostname> directory

    - the local file name is the path part of the URL with path hierarchy
      markers (aka '/') replaced by '_'

    Eg. if you use the requests_mock fixture in your test file as:

        requests_mock.get('https://nowhere.com', body=get_response_cb)
        # or even
        requests_mock.get(re.compile('https://'), body=get_response_cb)

    then a call requests.get like:

        requests.get('https://nowhere.com/path/to/resource?a=b&c=d')

    will look the content of the response in:

        datadir/nowhere.com/path_to_resource,a=b,c=d

    Args:
        request (requests.Request): Object requests
        context (requests.Context): Object holding response metadata
            information (status_code, headers, etc...)
        ignore_urls: urls whose status response should be 404 even if the local
            file exists
        visits: Dict of url, number of visits. If None, disable multi visit
            support (default)

    Returns:
        Optional[FileDescriptor] on disk file to read from the test context

    """
    logger.debug('get_response_cb(%s, %s)', request, context)
    logger.debug('url: %s', request.url)
    logger.debug('ignore_urls: %s', ignore_urls)
    if request.url in ignore_urls:
        context.status_code = 404
        return None
    url = urlparse(request.url)
    dirname = url.hostname  # pypi.org | files.pythonhosted.org
    # url.path: pypi/<project>/json -> local file: pypi_<project>_json
    filename = url.path[1:]
    if filename.endswith('/'):
        filename = filename[:-1]
    filename = filename.replace('/', '_')
    if url.query:
        filename += ',' + url.query.replace('&', ',')

    filepath = path.join(datadir, dirname, filename)
    if visits is not None:
        visit = visits.get(url, 0)
        visits[url] = visit + 1
        if visit:
            filepath = filepath + '_visit%s' % visit

    if not path.isfile(filepath):
        logger.debug('not found filepath: %s', filepath)
        context.status_code = 404
        return None
    fd = open(filepath, 'rb')
    context.headers['content-length'] = str(path.getsize(filepath))
    return fd


@pytest.fixture
def datadir(request):
    """By default, returns the test directory's data directory.

    This can be overriden on a per arborescence basis. Add an override
    definition in the local conftest, for example:

        import pytest

        from os import path

        @pytest.fixture
        def datadir():
            return path.join(path.abspath(path.dirname(__file__)), 'resources')


    """
    return path.join(path.dirname(str(request.fspath)), 'data')


def requests_mock_datadir_factory(ignore_urls: List[str] = [],
                                  has_multi_visit: bool = False):
    """This factory generates fixture which allow to look for files on the
    local filesystem based on the requested URL, using the following rules:

    - files are searched in the datadir/<hostname> directory

    - the local file name is the path part of the URL with path hierarchy
      markers (aka '/') replaced by '_'

    Multiple implementations are possible, for example:

    - requests_mock_datadir_factory([]):
        This computes the file name from the query and always returns the same
        result.

    - requests_mock_datadir_factory(has_multi_visit=True):
        This computes the file name from the query and returns the content of
        the filename the first time, the next call returning the content of
        files suffixed with _visit1 and so on and so forth. If the file is not
        found, returns a 404.

    - requests_mock_datadir_factory(ignore_urls=['url1', 'url2']):
        This will ignore any files corresponding to url1 and url2, always
        returning 404.

    Args:
        ignore_urls: List of urls to always returns 404 (whether file
            exists or not)
        has_multi_visit: Activate or not the multiple visits behavior

    """
    @pytest.fixture
    def requests_mock_datadir(requests_mock, datadir):
        if not has_multi_visit:
            cb = partial(get_response_cb,
                         ignore_urls=ignore_urls,
                         datadir=datadir)
            requests_mock.get(re.compile('https://'), body=cb)
        else:
            visits = {}
            requests_mock.get(re.compile('https://'), body=partial(
                get_response_cb, ignore_urls=ignore_urls, visits=visits,
                datadir=datadir)
            )

        return requests_mock

    return requests_mock_datadir


# Default `requests_mock_datadir` implementation
requests_mock_datadir = requests_mock_datadir_factory([])

# Implementation for multiple visits behavior:
# - first time, it checks for a file named `filename`
# - second time, it checks for a file named `filename`_visit1
# etc...
requests_mock_datadir_visits = requests_mock_datadir_factory(
    has_multi_visit=True)
