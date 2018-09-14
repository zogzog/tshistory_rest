from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, MetaData
import pytest
from pytest_sa_pg import db
import webtest

from tshistory import schema
from tshistory_rest import app


DATADIR = Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def engine(request):
    db.setup_local_pg_cluster(request, DATADIR, 5433, {
        'timezone': 'UTC',
        'log_timezone': 'UTC'}
    )
    e = create_engine('postgresql://localhost:5433/postgres')
    schema.reset(e)
    schema.init(e, MetaData())
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
    wsgi = app.make_app(engine)
    yield WebTester(wsgi)
