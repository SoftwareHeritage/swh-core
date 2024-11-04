# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import deque
from functools import partial
from importlib import import_module
import logging
from os import path
from pathlib import Path
import re
from typing import Dict, List, Optional
from urllib.parse import unquote, urlparse

import pytest
import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict
from requests.utils import get_encoding_from_headers
import sentry_sdk

logger = logging.getLogger(__name__)


# Check get_local_factory function
# Maximum number of iteration checks to generate requests responses
MAX_VISIT_FILES = 10


def get_response_cb(
    request: requests.Request,
    context,
    datadir,
    ignore_urls: List[str] = [],
    visits: Optional[Dict] = None,
):
    """Mount point callback to fetch on disk the request's content. The request
    urls provided are url decoded first to resolve the associated file on disk.

    This is meant to be used as 'body' argument of the requests_mock.get()
    method.

    It will look for files on the local filesystem based on the requested URL,
    using the following rules:

    - files are searched in the datadir/<hostname> directory

    - the local file name is the path part of the URL with path hierarchy
      markers (aka '/') replaced by '_'

    Eg. if you use the requests_mock fixture in your test file as:

        requests_mock.get('https?://nowhere.com', body=get_response_cb)
        # or even
        requests_mock.get(re.compile('https?://'), body=get_response_cb)

    then a call requests.get like:

        requests.get('https://nowhere.com/path/to/resource?a=b&c=d')

    will look the content of the response in:

        datadir/https_nowhere.com/path_to_resource,a=b,c=d

    or a call requests.get like:

        requests.get('http://nowhere.com/path/to/resource?a=b&c=d')

    will look the content of the response in:

        datadir/http_nowhere.com/path_to_resource,a=b,c=d

    Args:
        request: Object requests
        context (requests.Context): Object holding response metadata
            information (status_code, headers, etc...)
        datadir: Data files path
        ignore_urls: urls whose status response should be 404 even if the local
            file exists
        visits: Dict of url, number of visits. If None, disable multi visit
            support (default)

    Returns:
        Optional[FileDescriptor] on disk file to read from the test context

    """
    logger.debug("get_response_cb(%s, %s)", request, context)
    logger.debug("url: %s", request.url)
    logger.debug("ignore_urls: %s", ignore_urls)
    unquoted_url = unquote(request.url)
    if unquoted_url in ignore_urls:
        context.status_code = 404
        return None
    url = urlparse(unquoted_url)
    # http://pypi.org ~> http_pypi.org
    # https://files.pythonhosted.org ~> https_files.pythonhosted.org
    dirname = "%s_%s" % (url.scheme, url.hostname)
    # url.path: pypi/<project>/json -> local file: pypi_<project>_json
    filename = url.path[1:]
    if filename.endswith("/"):
        filename = filename[:-1]
    filename = filename.replace("/", "_")
    if url.query:
        filename += "," + url.query.replace("&", ",")

    filepath = path.join(datadir, dirname, filename)
    if visits is not None:
        visit = visits.get(url, 0)
        visits[url] = visit + 1
        if visit:
            filepath = filepath + "_visit%s" % visit

    if not path.isfile(filepath):
        logger.debug("not found filepath: %s", filepath)
        context.status_code = 404
        return None
    fd = open(filepath, "rb")
    context.headers["content-length"] = str(path.getsize(filepath))
    return fd


@pytest.fixture
def datadir(request: pytest.FixtureRequest) -> str:
    """By default, returns the test directory's data directory.

    This can be overridden on a per file tree basis. Add an override
    definition in the local conftest, for example::

        import pytest

        from os import path

        @pytest.fixture
        def datadir():
            return path.join(path.abspath(path.dirname(__file__)), 'resources')


    """
    # pytest >= 7 renamed FixtureRequest fspath attribute to path
    path_ = request.path if hasattr(request, "path") else request.fspath  # type: ignore
    return path.join(path.dirname(str(path_)), "data")


def requests_mock_datadir_factory(
    ignore_urls: List[str] = [], has_multi_visit: bool = False
):
    """This factory generates fixtures which allow to look for files on the
    local filesystem based on the requested URL, using the following rules:

    - files are searched in the data/<hostname> directory

    - the local file name is the path part of the URL with path hierarchy
      markers (aka '/') replaced by '_'

    Multiple implementations are possible, for example:

    ``requests_mock_datadir_factory([])``

        This computes the file name from the query and always returns the same
        result.

    ``requests_mock_datadir_factory(has_multi_visit=True)``

        This computes the file name from the query and returns the content of
        the filename the first time, the next call returning the content of
        files suffixed with _visit1 and so on and so forth. If the file is not
        found, returns a 404.

    ``requests_mock_datadir_factory(ignore_urls=['url1', 'url2'])``

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
            cb = partial(get_response_cb, ignore_urls=ignore_urls, datadir=datadir)
            requests_mock.get(re.compile("https?://"), body=cb)
        else:
            visits = {}
            requests_mock.get(
                re.compile("https?://"),
                body=partial(
                    get_response_cb,
                    ignore_urls=ignore_urls,
                    visits=visits,
                    datadir=datadir,
                ),
            )

        return requests_mock

    return requests_mock_datadir


# Default `requests_mock_datadir` implementation
requests_mock_datadir = requests_mock_datadir_factory()
"""
Instance of :py:func:`requests_mock_datadir_factory`,
with the default arguments.
"""

# Implementation for multiple visits behavior:
# - first time, it checks for a file named `filename`
# - second time, it checks for a file named `filename`_visit1
# etc...
requests_mock_datadir_visits = requests_mock_datadir_factory(has_multi_visit=True)
"""
Instance of :py:func:`requests_mock_datadir_factory`,
with the default arguments, but `has_multi_visit=True`.
"""


@pytest.fixture
def swh_rpc_client(swh_rpc_client_class, swh_rpc_adapter):
    """This fixture generates an RPCClient instance that uses the class generated
    by the rpc_client_class fixture as backend.

    Since it uses the swh_rpc_adapter, HTTP queries will be intercepted and
    routed directly to the current Flask app (as provided by the `app`
    fixture).

    So this stack of fixtures allows to test the RPCClient -> RPCServerApp
    communication path using a real RPCClient instance and a real Flask
    (RPCServerApp) app instance.

    To use this fixture:

    - ensure an `app` fixture exists and generate a Flask application,
    - implement an `swh_rpc_client_class` fixtures that returns the
      RPCClient-based class to use as client side for the tests,
    - implement your tests using this `swh_rpc_client` fixture.

    See swh/core/api/tests/test_rpc_client_server.py for an example of usage.
    """
    url = "mock://example.com"
    cli = swh_rpc_client_class(url=url)
    # we need to clear the list of existing adapters here so we ensure we
    # have one and only one adapter which is then used for all the requests.
    cli.session.adapters.clear()
    cli.session.mount("mock://", swh_rpc_adapter)
    return cli


@pytest.fixture
def swh_rpc_adapter(app):
    """Fixture that generates a requests.Adapter instance that
    can be used to test client/servers code based on swh.core.api classes.

    See swh/core/api/tests/test_rpc_client_server.py for an example of usage.

    """
    client = app.test_client()
    yield RPCTestAdapter(client)


class RPCTestAdapter(BaseAdapter):
    def __init__(self, client):
        self._client = client

    def build_response(self, req, resp):
        response = requests.Response()

        # Fallback to None if there's no status_code, for whatever reason.
        response.status_code = resp.status_code

        # Make headers case-insensitive.
        headers = getattr(resp, "headers")
        response.headers = CaseInsensitiveDict(
            headers.to_wsgi_list() if headers else []
        )

        # Set encoding.
        response.encoding = get_encoding_from_headers(response.headers)
        response.raw = resp
        response.reason = response.raw.status

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        # Give the Response some context.
        response.request = req
        response.connection = self
        response._content = resp.data

        return response

    def send(self, request, **kw):
        """
        Overrides ``requests.adapters.BaseAdapter.send``
        """
        resp = self._client.open(
            request.url,
            method=request.method,
            headers=request.headers.items(),
            data=request.body,
        )
        return self.build_response(request, resp)


@pytest.fixture
def flask_app_client(app):
    with app.test_client() as client:
        yield client


# stolen from pytest-flask, required to have url_for() working within tests
# using flask_app_client fixture.
@pytest.fixture(autouse=True)
def _push_request_context(request: pytest.FixtureRequest):
    """During tests execution request context has been pushed, e.g. `url_for`,
    `session`, etc. can be used in tests as is::

        def test_app(app, client):
            assert client.get(url_for('myview')).status_code == 200

    """
    if "app" not in request.fixturenames:
        return
    app = request.getfixturevalue("app")
    ctx = app.test_request_context()
    ctx.push()

    def teardown():
        ctx.pop()

    request.addfinalizer(teardown)


class FakeSocket(object):
    """A fake socket for testing."""

    def __init__(self):
        self.payloads = deque()

    def send(self, payload):
        assert type(payload) is bytes
        self.payloads.append(payload)

    def recv(self):
        try:
            return self.payloads.popleft().decode("utf-8")
        except IndexError:
            return None

    def close(self):
        pass

    def __repr__(self):
        return str(self.payloads)


@pytest.fixture
def statsd():
    """Simple fixture giving a Statsd instance suitable for tests

    The Statsd instance uses a FakeSocket as `.socket` attribute in which one
    can get the accumulated statsd messages in a deque in `.socket.payloads`.
    """

    from swh.core.statsd import Statsd

    statsd = Statsd()
    statsd._socket = FakeSocket()
    yield statsd


@pytest.fixture
def monkeypatch_sentry_transport():
    # Inspired by
    # https://github.com/getsentry/sentry-python/blob/2.0.0/tests/conftest.py#L175-L219

    initialized = False

    def setup_sentry_transport_monkeypatch(*a, **kw):
        nonlocal initialized
        assert not initialized, "already initialized"
        initialized = True
        hub = sentry_sdk.Hub.current
        client = sentry_sdk.Client(*a, **kw)
        hub.bind_client(client)
        client.transport = TestTransport()

    from sentry_sdk.transport import Transport

    class TestTransport(Transport):
        def __init__(self):
            super().__init__()
            self.events = []
            self.envelopes = []

        def capture_envelope(self, envelope):
            for item in envelope:
                if item.headers.get("type") in ("event", "transaction"):
                    self.events.append(item.payload.json)
            self.envelopes.append(envelope)

    with sentry_sdk.Hub(None):
        yield setup_sentry_transport_monkeypatch


@pytest.fixture
def sentry_events(monkeypatch_sentry_transport):
    monkeypatch_sentry_transport()
    return sentry_sdk.Hub.current.client.transport.events


@pytest.fixture(autouse=True)
def clean_scopes():
    # https://github.com/getsentry/sentry-python/blob/2.0.0/tests/conftest.py#L61-L68
    from sentry_sdk import scope

    scope._global_scope = None
    scope._isolation_scope.set(None)
    scope._current_scope.set(None)


@pytest.fixture()
def mock_import_module(request, mocker, datadir):
    mock = mocker.MagicMock

    def import_module_mocker(name, package=None):
        if not name.startswith("swh.test"):
            return import_module(name, package)

        m = request.node.get_closest_marker("init_version")
        if m:
            version = m.kwargs.get("version", 1)
        else:
            version = 3
        if name.startswith("swh."):
            name = name[4:]
        modpath = name.split(".")

        def get_datastore(*args, **kw):
            return mock(current_version=version)

        return mock(
            __name__=name.split(".")[-1],
            __file__=str(Path(datadir, *modpath, "__init__.py")),
            get_datastore=get_datastore,
        )

    return mocker.patch("swh.core.db.db_utils.import_module", import_module_mocker)


@pytest.fixture()
def mock_get_entry_points(request, mocker, datadir, mock_import_module):
    mock = mocker.MagicMock

    def get_entry_points_mocker(group):
        m = request.node.get_closest_marker("init_version")
        if m:
            version = m.kwargs.get("version", 1)
        else:
            version = 3

        class EntryPoints(dict):
            def __iter__(self):
                return iter(self.values())

        package = group[4:-8]  # remove 'swh.' and '.classes'
        entrypoints = EntryPoints()
        pkgdir = Path(datadir) / package
        if pkgdir.is_dir():
            for entry in pkgdir.iterdir():
                if not entry.name.startswith("_") and entry.is_dir():
                    ep = mock(
                        module=f"swh.{package}.{entry.name}",
                        load=lambda: mock(
                            current_version=version,
                            __doc__="A mockup backend for tests",
                        ),
                    )
                    # needed to overwrite the Mock's name argument, see
                    # https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
                    ep.name = entry.name
                    entrypoints[entry.name] = ep
        return entrypoints

    return mocker.patch("swh.core.config.get_entry_points", get_entry_points_mocker)


# for bw compat
mock_get_swh_backend_module = mock_get_entry_points
