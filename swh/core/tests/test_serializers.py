# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json
import unittest
from uuid import UUID

import arrow

from swh.core.serializers import (
    SWHJSONDecoder,
    SWHJSONEncoder,
    msgpack_dumps,
    msgpack_loads
)


class Serializers(unittest.TestCase):
    def setUp(self):
        self.tz = datetime.timezone(datetime.timedelta(minutes=118))

        self.data = {
            'bytes': b'123456789\x99\xaf\xff\x00\x12',
            'datetime_naive': datetime.datetime(2015, 1, 1, 12, 4, 42, 231455),
            'datetime_tz': datetime.datetime(2015, 3, 4, 18, 25, 13, 1234,
                                             tzinfo=self.tz),
            'datetime_utc': datetime.datetime(2015, 3, 4, 18, 25, 13, 1234,
                                              tzinfo=datetime.timezone.utc),
            'datetime_delta': datetime.timedelta(64),
            'arrow_date': arrow.get('2018-04-25T16:17:53.533672+00:00'),
            'swhtype': 'fake',
            'swh_dict': {'swhtype': 42, 'd': 'test'},
            'random_dict': {'swhtype': 43},
            'uuid': UUID('cdd8f804-9db6-40c3-93ab-5955d3836234'),
        }

        self.encoded_data = {
            'bytes': {'swhtype': 'bytes', 'd': 'F)}kWH8wXmIhn8j01^'},
            'datetime_naive': {'swhtype': 'datetime',
                               'd': '2015-01-01T12:04:42.231455'},
            'datetime_tz': {'swhtype': 'datetime',
                            'd': '2015-03-04T18:25:13.001234+01:58'},
            'datetime_utc': {'swhtype': 'datetime',
                             'd': '2015-03-04T18:25:13.001234+00:00'},
            'datetime_delta': {'swhtype': 'timedelta',
                               'd': 'datetime.timedelta(64)'},
            'arrow_date': {'swhtype': 'arrow',
                           'd': '2018-04-25T16:17:53.533672+00:00'},
            'swhtype': 'fake',
            'swh_dict': {'swhtype': 42, 'd': 'test'},
            'random_dict': {'swhtype': 43},
            'uuid': {'swhtype': 'uuid',
                     'd': 'cdd8f804-9db6-40c3-93ab-5955d3836234'},
        }

        self.generator = (i for i in range(5))
        self.gen_lst = list(range(5))

    def test_round_trip_json(self):
        data = json.dumps(self.data, cls=SWHJSONEncoder)
        self.assertEqual(self.data, json.loads(data, cls=SWHJSONDecoder))

    def test_encode_swh_json(self):
        data = json.dumps(self.data, cls=SWHJSONEncoder)
        self.assertEqual(self.encoded_data, json.loads(data))

    def test_round_trip_msgpack(self):
        data = msgpack_dumps(self.data)
        self.assertEqual(self.data, msgpack_loads(data))

    def test_generator_json(self):
        data = json.dumps(self.generator, cls=SWHJSONEncoder)
        self.assertEqual(self.gen_lst, json.loads(data, cls=SWHJSONDecoder))

    def test_generator_msgpack(self):
        data = msgpack_dumps(self.generator)
        self.assertEqual(self.gen_lst, msgpack_loads(data))
