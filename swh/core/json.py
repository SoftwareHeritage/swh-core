# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import datetime
from json import JSONDecoder, JSONEncoder

import dateutil.parser


class SWHJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return {
                'swhtype': 'bytes',
                'd': base64.b85encode(o).decode('ascii'),
            }
        elif isinstance(o, datetime.datetime):
            return {
                'swhtype': 'datetime',
                'd': o.isoformat(),
            }
        try:
            return super().default(o)
        except TypeError as e:
            try:
                iterable = iter(o)
            except TypeError:
                raise e from None
            else:
                return list(iterable)


class SWHJSONDecoder(JSONDecoder):
    def decode_data(self, o):
        if isinstance(o, dict):
            if set(o.keys()) == {'d', 'swhtype'}:
                datatype = o['swhtype']
                if datatype == 'bytes':
                    return base64.b85decode(o['d'])
                elif datatype == 'datetime':
                    return dateutil.parser.parse(o['d'])
            return {key: self.decode_data(value) for key, value in o.items()}
        if isinstance(o, list):
            return [self.decode_data(value) for value in o]
        else:
            return o

    def raw_decode(self, s, idx=0):
        data, index = super().raw_decode(s, idx)
        return self.decode_data(data), index
