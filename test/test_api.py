import json
import pandas as pd
import zlib

from tshistory import util
from tshistory.testutil import utcdt, genserie, assert_df


def test_no_series(client):
    res = client.get('/series/state?name=no-such-series')
    assert res.status_code == 404
    assert res.json == {
        'message': '`no-such-series` does not exists'
    }

    res = client.get('/series/metadata?name=no-such-series')
    assert res.status_code == 404
    assert res.json == {
        'message': '`no-such-series` does not exists'
    }

    res = client.get('/series/history?name=no-such-series')
    assert res.status_code == 404
    assert res.json == {
        'message': '`no-such-series` does not exists'
    }

    res = client.get('/series/staircase', params={
        'name': 'no-such-series',
        'delta': pd.Timedelta(hours=3)
    })

    assert res.status_code == 404
    assert res.json == {
        'message': '`no-such-series` does not exists'
    }


def test_base(client):
    # insert
    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    res = client.patch('/series/state', params={
        'name': 'test',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 10),
        'tzaware': util.tzaware_serie(series_in)
    })

    assert res.status_code == 201

    # catalog
    res = client.get('/series/catalog')
    assert res.status_code == 200
    assert res.json == {'test': 'primary'}

    # metadata
    res = client.get('/series/metadata?name=test')
    meta = res.json
    assert meta == {}

    res = client.get('/series/metadata?name=test&all=1')
    meta = res.json
    assert meta == {
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    res = client.put('/series/metadata', params={
        'metadata': json.dumps({
            'freq': 'D',
            'description': 'banana spot price'
        }),
        'name': 'test'
    })
    assert res.status_code == 200
    res = client.get('/series/metadata?name=test')
    meta2 = res.json
    assert meta2 == {
        'freq': 'D',
        'description': 'banana spot price'
    }

    # metadata: delete by uploading an empty dict
    res = client.put('/series/metadata', params={
        'metadata': json.dumps({}),
        'name': 'test'
    })
    assert res.status_code == 200
    res = client.get('/series/metadata?name=test')
    meta2 = res.json
    assert meta2 == {}

    # get
    res = client.get('/series/state?name=test')
    series = util.fromjson(res.body, 'test', meta['tzaware'])
    assert_df("""
2018-01-01 00:00:00+00:00    0.0
2018-01-01 01:00:00+00:00    1.0
2018-01-01 02:00:00+00:00    2.0
""", series)


    # reinsert
    series_in = genserie(utcdt(2018, 1, 1, 3), 'H', 1, [3])
    res = client.patch('/series/state', params={
        'name': 'test',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 13),
        'tzaware': util.tzaware_serie(series_in)
    })

    assert res.status_code == 200

    res = client.get('/series/state?name=test')
    series = util.fromjson(res.body, 'test', meta['tzaware'])
    assert_df("""
2018-01-01 00:00:00+00:00    0.0
2018-01-01 01:00:00+00:00    1.0
2018-01-01 02:00:00+00:00    2.0
2018-01-01 03:00:00+00:00    3.0
""", series)

    # checkout a past state
    res = client.get('/series/state', params={
        'name': 'test',
        'insertion_date': utcdt(2018, 1, 1, 10)
    })
    series = util.fromjson(res.body, 'test', meta['tzaware'])
    assert_df("""
2018-01-01 00:00:00+00:00    0.0
2018-01-01 01:00:00+00:00    1.0
2018-01-01 02:00:00+00:00    2.0
""", series)

    # checkout too far in the past
    res = client.get('/series/state', params={
        'name': 'test',
        'insertion_date': utcdt(2018, 1, 1, 0)
    })
    assert res.json is None

    # history
    res = client.get('/series/history?name=test')
    df = pd.read_json(res.body)

    # we real client would need to handle timestamp
    # tz-awareness
    assert_df("""
2018-01-01 10:00:00  2018-01-01 13:00:00
2018-01-01 00:00:00                  0.0                    0
2018-01-01 01:00:00                  1.0                    1
2018-01-01 02:00:00                  2.0                    2
2018-01-01 03:00:00                  NaN                    3
""", df)

    # diff mode
    res = client.get('/series/history', params={
        'name': 'test',
        'diffmode': True
    })
    df = pd.read_json(res.body)

    assert_df("""
2018-01-01 10:00:00  2018-01-01 13:00:00
2018-01-01 00:00:00                  0.0                  NaN
2018-01-01 01:00:00                  1.0                  NaN
2018-01-01 02:00:00                  2.0                  NaN
2018-01-01 03:00:00                  NaN                  3.0
""", df)

    # empty range
    res = client.get('/series/history', params={
        'name': 'test',
        'from_insertion_date': utcdt(2018, 1, 1, 11),
        'to_insertion_date': utcdt(2018, 1, 1, 12),
    })
    df = pd.read_json(res.body)
    assert len(df) == 0

    # insertion dates subset
    res = client.get('/series/history', params={
        'name': 'test',
        'from_insertion_date': utcdt(2018, 1, 1, 10),
        'to_insertion_date': utcdt(2018, 1, 1, 12),
    })
    df = pd.read_json(res.body)

    assert_df("""
                     2018-01-01 10:00:00
2018-01-01 00:00:00                    0
2018-01-01 01:00:00                    1
2018-01-01 02:00:00                    2
""", df)

    # value dates subset
    res = client.get('/series/history', params={
        'name': 'test',
        'from_value_date': utcdt(2018, 1, 1, 2),
        'to_value_date': utcdt(2018, 1, 1, 3),
    })
    df = pd.read_json(res.body)

    assert_df("""
                     2018-01-01 10:00:00  2018-01-01 13:00:00
2018-01-01 02:00:00                  2.0                    2
2018-01-01 03:00:00                  NaN                    3
""", df)


    # state/get from from/to value date restriction
    res = client.get('/series/state', params={
        'name': 'test',
        'from_value_date': utcdt(2018, 1, 1, 1),
        'to_value_date': utcdt(2018, 1, 1, 2)
    })
    assert res.json == {
        '2018-01-01T01:00:00.000Z': 1.0,
        '2018-01-01T02:00:00.000Z': 2.0
    }


def test_delete(client):
    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    res = client.patch('/series/state', params={
        'name': 'test',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 10),
        'tzaware': util.tzaware_serie(series_in)
    })

    res = client.delete('/series/state', params={
        'name': 'no-such-series'
    })
    assert res.status_code == 404
    res = client.delete('/series/state', params={
        'name': 'test'
    })
    assert res.status_code == 200
    res = client.get('/series/catalog')
    assert 'test' not in res.json


def test_rename(client):
    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    res = client.patch('/series/state', params={
        'name': 'test',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 10),
        'tzaware': util.tzaware_serie(series_in)
    })
    res = client.put('/series/state', params={
        'name': 'no-such-series',
        'newname': 'no-better'
    })
    assert res.status_code == 404
    res = client.put('/series/state', params={
        'name': 'test',
        'newname': 'test2'
    })
    assert res.status_code == 200
    res = client.get('/series/catalog')
    assert res.json == {
        'test2': 'primary'
    }

    res = client.patch('/series/state', params={
        'name': 'test3',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 10),
        'tzaware': util.tzaware_serie(series_in)
    })
    res = client.put('/series/state', params={
        'name': 'test2',
        'newname': 'test3'
    })
    assert res.status_code == 409
    assert res.json == {
        'message': '`test3` does exists'
    }


def test_staircase(client):
    # each days we insert 7 data points
    for idx, idate in enumerate(pd.date_range(start=utcdt(2015, 1, 1),
                                              end=utcdt(2015, 1, 4),
                                              freq='D')):
        series = genserie(start=idate, freq='H', repeat=7)
        client.patch('/series/state', params={
            'name': 'staircase',
            'series': util.tojson(series),
            'author': 'Babar',
            'insertion_date': idate,
            'tzaware': util.tzaware_serie(series)
    })

    res = client.get('/series/staircase', params={
        'name': 'staircase',
        'delta': pd.Timedelta(hours=3),
        'from_value_date': utcdt(2015, 1, 1, 4),
        'to_value_date': utcdt(2015, 1, 2, 5),
    })
    series = util.fromjson(res.body, 'test', True)

    assert_df("""
2015-01-01 04:00:00+00:00    4.0
2015-01-01 05:00:00+00:00    5.0
2015-01-01 06:00:00+00:00    6.0
2015-01-02 03:00:00+00:00    3.0
2015-01-02 04:00:00+00:00    4.0
2015-01-02 05:00:00+00:00    5.0
""", series)


def test_get_fast_path(client):
    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    res = client.patch('/series/state', params={
        'name': 'test_fast',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 10),
        'tzaware': util.tzaware_serie(series_in)
    })

    assert res.status_code == 201

    out = client.get('/series/state', params={
        'name': 'test_fast',
        'mode': 'numpy'
    })
    meta, data = util.binary_unpack(zlib.decompress(out.body))
    meta = json.loads(meta)
    index, values = util.binary_unpack(data)
    index, values = util.numpy_deserialize(index, values, meta)
    series = pd.Series(values, index=index)
    series = series.tz_localize('UTC')

    assert_df("""
2018-01-01 00:00:00+00:00    0.0
2018-01-01 01:00:00+00:00    1.0
2018-01-01 02:00:00+00:00    2.0
""", series)

    assert meta == {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'value_dtype': '<f8'
    }
