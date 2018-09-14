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


def blueprint(engine):

    @ns.route('/metadata')
    class timeseries_metadata(Resource):

        @api.doc(parser=metadata)
        def get(self):
            args = metadata.parse_args()
            tsh = tsio.TimeSerie(namespace=args.namespace)
            with engine.begin() as cn:
                meta = tsh.metadata(cn, args.name)
            return meta, 200

    @ns.route('/state')
    class timeseries_state(Resource):

        @api.doc(parser=insert)
        def patch(self):
            args = insert.parse_args()
            tsh = tsio.TimeSerie(namespace=args.namespace)
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
            tsh = tsio.TimeSerie(namespace=args.namespace)
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

    return bp
