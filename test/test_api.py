import json
import pandas as pd

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

    # metadata
    res = client.get('/series/metadata?name=test')
    meta = res.json
    assert meta == {}

    res = client.get('/series/metadata?name=test&all=1')
    meta = res.json
    assert meta == {
        'index_dtype': '|M8[ns]',
        'index_names': [],
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


def test_staircase(client):
    # each days we insert 7 data points
    for idx, idate in enumerate(pd.DatetimeIndex(start=utcdt(2015, 1, 1),
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
