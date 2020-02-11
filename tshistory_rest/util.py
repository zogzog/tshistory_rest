import json
import zlib

import pandas as pd
from tshistory import util


def has_formula():
    try:
        import tshistory_formula.api
    except ImportError:
        return False
    return True


def utcdt(dtstr):
    return pd.Timestamp(dtstr)


def todict(dictstr):
    if dictstr is None:
        return None
    return json.loads(dictstr)


def enum(*enum):
    " an enum input type "

    def _str(val):
        if val not in enum:
            raise ValueError(f'Possible choices are in {enum}')
        return val
    _str.__schema__ = {'type': 'enum'}
    return _str


def binary_pack_meta_data(meta, series):
    index, values = util.numpy_serialize(
        series,
        meta['value_type'] == 'object'
    )
    bmeta = json.dumps(meta).encode('utf-8')
    return zlib.compress(
        util.nary_pack(bmeta, index, values)
    )
