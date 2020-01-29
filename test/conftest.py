from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine
import pytest
from pytest_sa_pg import db
import webtest

from tshistory import schema, api
from tshistory_rest import app


DATADIR = Path(__file__).parent / 'data'
DBURI = 'postgresql://localhost:5433/postgres'


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, 5433, {
        'timezone': 'UTC',
        'log_timezone': 'UTC'}
    )
    e = create_engine(DBURI)
    sch = schema.tsschema()
    sch.create(e)
    sch = schema.tsschema(ns='other')
    sch.create(e)
    return e


# Error-displaying web tester

class WebTester(webtest.TestApp):

    def _check_status(self, status, res):
        try:
            super(WebTester, self)._check_status(status, res)
        except:
            print('ERRORS', res.errors)
            # raise <- default behaviour on 4xx is silly


@pytest.fixture(scope='session')
def client(engine):
    wsgi = app.make_app(
        api.timeseries(
            str(engine.url),
            namespace='tsh',
            sources=[(DBURI, 'other')]
        )
    )
    yield WebTester(wsgi)
