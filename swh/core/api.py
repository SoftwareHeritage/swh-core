# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pickle
import requests

from .serializers import (decode_response,
                          encode_data_client as encode_data)


class SWHRemoteAPI:
    """Proxy to an internal SWH API

    """

    def __init__(self, api_exception, url):
        super().__init__()
        self.api_exception = api_exception
        base_url = url if url.endswith('/') else url + '/'
        self.url = base_url
        self.session = requests.Session()

    def _url(self, endpoint):
        return '%s%s' % (self.url, endpoint)

    def post(self, endpoint, data):
        try:
            response = self.session.post(
                self._url(endpoint),
                data=encode_data(data),
                headers={'content-type': 'application/x-msgpack'},
            )
        except ConnectionError as e:
            print(str(e))
            raise self.api_exception(e)

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        return decode_response(response)

    def get(self, endpoint, data=None):
        try:
            response = self.session.get(
                self._url(endpoint),
                params=data,
            )
        except ConnectionError as e:
            print(str(e))
            raise self.api_exception(e)

        if response.status_code == 404:
            return None

        # XXX: this breaks language-independence and should be
        # replaced by proper unserialization
        if response.status_code == 400:
            raise pickle.loads(decode_response(response))

        else:
            return decode_response(response)
