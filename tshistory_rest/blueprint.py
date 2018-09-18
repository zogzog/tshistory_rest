import pandas as pd

from flask import Blueprint, request, make_response
from flask_restplus import Api, Resource, reqparse

from tshistory import tsio, util


def utcdt(dtstr):
    return pd.Timestamp(dtstr)


bp = Blueprint(
    'tshistory_rest',
    __name__,
    template_folder='tshr_templates',
    static_folder='tshr_static',
)

api = Api(
    bp,
    version='1.0',
    title='tshistory api',
    description='tshistory timeseries store rest api'
)
api.namespaces.pop(0)  # wipe the default namespace

ns = api.namespace(
    'series',
    description='Time Series Operations'
)

base = reqparse.RequestParser()

base.add_argument(
    'namespace', type=str, default='tsh',
    help='timeseries store namespace'
)

insert = base.copy()
insert.add_argument(
    'name', type=str, required=True,
    help='timeseries name'
)
insert.add_argument(
    'series', type=str, required=True,
    help='json representation of the series'
)
insert.add_argument(
    'author', type=str, required=True,
    help='author of the insertion'
)
insert.add_argument(
    'insertion_date', type=utcdt, default=None,
    help='insertion date can be forced'
)
insert.add_argument(
    'tzaware', type=bool, default=True,
    help='tzaware series'
)
insert.add_argument(
    'metadata', type=str, default=None,
    help='metadata associated with this insertion'
)

metadata = base.copy()
metadata.add_argument(
    'name', type=str, required=True,
    help='series name'
)

get = base.copy()
get.add_argument(
    'name', type=str, required=True,
    help='timeseries name'
)
get.add_argument(
    'insertion_date', type=utcdt, default=None,
    help='insertion date can be forced'
)

history = base.copy()
history.add_argument(
    'name', type=str, required=True,
    help='timeseries name'
)
history.add_argument(
    'from_insertion_date', type=utcdt, default=None
)
history.add_argument(
    'to_insertion_date', type=utcdt, default=None
)
history.add_argument(
    'from_value_date', type=utcdt, default=None
)
history.add_argument(
    'to_value_date', type=utcdt, default=None
)
history.add_argument(
    'diffmode', type=bool, default=False
)

staircase = base.copy()
staircase.add_argument(
    'name', type=str, required=True,
    help='timeseries name'
)
staircase.add_argument(
    'delta', type=pd.Timedelta, required=True,
    help='time delta in iso 8601 duration'
)
staircase.add_argument(
    'from_value_date', type=utcdt, default=None
)
staircase.add_argument(
    'to_value_date', type=utcdt, default=None
)



def blueprint(engine, tshclass=tsio.TimeSerie):

    @ns.route('/metadata')
    class timeseries_metadata(Resource):

        @api.doc(parser=metadata)
        def get(self):
            args = metadata.parse_args()
            tsh = tshclass(namespace=args.namespace)
            with engine.begin() as cn:
                meta = tsh.metadata(cn, args.name)
            return meta, 200

    @ns.route('/state')
    class timeseries_state(Resource):

        @api.doc(parser=insert)
        def patch(self):
            args = insert.parse_args()
            tsh = tshclass(namespace=args.namespace)
            series = util.fromjson(args.series, args.name)
            if args.tzaware:
                # pandas to_json converted to utc
                # and dropped the offset
                series.index = series.index.tz_localize('utc')

            with engine.begin() as cn:
                exists = tsh.exists(cn, args.name)
                tsh.insert(
                    cn, series, args.name, args.author,
                    metadata=args.metadata,
                    _insertion_date=args.insertion_date
                )

            return '', 200 if exists else 201

        @api.doc(parser=get)
        def get(self):
            args = get.parse_args()
            tsh = tshclass(namespace=args.namespace)
            with engine.begin() as cn:
                series = tsh.get(
                    cn, args.name,
                    revision_date=args.insertion_date
                )

            if series is not None:
                response = make_response(
                    series.to_json(orient='index',
                                   date_format='iso')
                )
            else:
                response = make_response('null')
            response.headers['Content-Type'] = 'text/json'
            return response

    @ns.route('/history')
    class timeseries_history(Resource):

        @api.doc(parser=history)
        def get(self):
            args = history.parse_args()
            tsh = tshclass(namespace=args.namespace)
            with engine.begin() as cn:
                hist = tsh.get_history(
                    cn, args.name,
                    from_insertion_date=args.from_insertion_date,
                    to_insertion_date=args.to_insertion_date,
                    from_value_date=args.from_value_date,
                    to_value_date=args.to_value_date,
                    diffmode=args.diffmode
                )

            if hist is not None:
                response = make_response(
                    pd.DataFrame(hist).to_json()
                )
            else:
                response = make_response('null')
            response.headers['Content-Type'] = 'text/json'
            return response

    @ns.route('/staircase')
    class timeseries_staircase(Resource):

        @api.doc(parser=staircase)
        def get(self):
            args = staircase.parse_args()
            tsh = tshclass(namespace=args.namespace)
            with engine.begin() as cn:
                series = tsh.get_delta(
                    cn, args.name, delta=args.delta,
                    from_value_date=args.from_value_date,
                    to_value_date=args.to_value_date,
                )

            if series is not None:
                response = make_response(
                    series.to_json(orient='index', date_format='iso')
                )
            else:
                response = make_response('null')
            response.headers['Content-Type'] = 'text/json'
            return response

    return bp
