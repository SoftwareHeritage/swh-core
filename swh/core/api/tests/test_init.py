# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.core.api import remote_api_endpoint


def test_remote_api_endpoint():
    @remote_api_endpoint("hello_route")
    def hello():
        pass

    assert hasattr(hello, "_endpoint_path")
    assert hello._endpoint_path == "hello_route"
    assert hasattr(hello, "_method")
    assert hello._method == "POST"


def test_remote_api_endpoint_2():
    @remote_api_endpoint("another_route", method="GET")
    def hello2():
        pass

    assert hasattr(hello2, "_endpoint_path")
    assert hello2._endpoint_path == "another_route"
    assert hasattr(hello2, "_method")
    assert hello2._method == "GET"
