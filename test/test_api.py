from tshistory import util
from tshistory.testutil import utcdt, genserie, assert_df


def test_api(client):
    series_in = genserie(utcdt(2018, 1, 1), 'H', 3)
    res = client.patch('/series/state', params={
        'name': 'test',
        'series': util.tojson(series_in),
        'author': 'Babar',
        'insertion_date': utcdt(2018, 1, 1, 10),
        'tzaware': util.tzaware_serie(series_in)
    })

    assert res.status_code == 201

    res = client.get('/series/metadata?name=test')
    meta = res.json
    assert meta == {
        'index_dtype': '|M8[ns]',
        'index_names': [],
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

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
