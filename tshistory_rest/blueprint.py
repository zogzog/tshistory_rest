import json
import io

import numpy as np
import pandas as pd

from flask import Blueprint, request, make_response
from flask_restplus import (
    Api as baseapi,
    inputs,
    Resource,
    reqparse
)

from tshistory import tsio, util


def utcdt(dtstr):
    return pd.Timestamp(dtstr)


def enum(*enum):
    " an enum input type "

    def _str(val):
        if val not in enum:
            raise ValueError(f'Possible choices are in {enum}')
        return val
    _str.__schema__ = {'type': 'enum'}
    return _str


bp = Blueprint(
    'tshistory_rest',
    __name__,
    template_folder='tshr_templates',
    static_folder='tshr_static',
)

class Api(baseapi):

    # see https://github.com/flask-restful/flask-restful/issues/67
    def _help_on_404(self, message=None):
        return message or 'No such thing.'


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
base.add_argument(
    'name', type=str, required=True,
    help='timeseries name'
)

insert = base.copy()
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
    'tzaware', type=inputs.boolean, default=True,
    help='tzaware series'
)
insert.add_argument(
    'metadata', type=str, default=None,
    help='metadata associated with this insertion'
)

rename = base.copy()
rename.add_argument(
    'newname', type=str, required=True,
    help='new name of the series'
)

metadata = base.copy()
metadata.add_argument(
    'all', type=inputs.boolean, default=False,
    help='get all metadata, including internal'
)
put_metadata = base.copy()
put_metadata.add_argument(
    'metadata', type=str, required=True,
    help='set new metadata for a series'
)

get = base.copy()
get.add_argument(
    'insertion_date', type=utcdt, default=None,
    help='insertion date can be forced'
)
get.add_argument(
    'from_value_date', type=utcdt, default=None
)
get.add_argument(
    'to_value_date', type=utcdt, default=None
)
get.add_argument(
    'mode', type=enum('json', 'numpy'), default='json'
)

delete = base.copy()

history = base.copy()
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
    'diffmode', type=inputs.boolean, default=False
)

staircase = base.copy()
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


catalog = reqparse.RequestParser()
catalog.add_argument(
    'namespace', type=str, default='tsh',
    help='timeseries store namespace'
)



def blueprint(engine, tshclass=tsio.timeseries):

    @ns.route('/metadata')
    class timeseries_metadata(Resource):

        @api.doc(parser=metadata)
        def get(self):
            args = metadata.parse_args()
            tsh = tshclass(namespace=args.namespace)

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')

            with engine.begin() as cn:
                meta = tsh.metadata(cn, args.name)

            # remove internal if asked
            if not args.all:
                meta = {
                    key: val for key, val in meta.items()
                    if key not in tsh.metakeys
                }

            return meta, 200

        @api.doc(parser=put_metadata)
        def put(self):
            args = put_metadata.parse_args()
            tsh = tshclass(namespace=args.namespace)

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')

            metadata = json.loads(args.metadata)
            with engine.begin() as cn:
                tsh.update_metadata(cn, args.name, metadata)

            return '', 200


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
                    insertion_date=args.insertion_date
                )

            return '', 200 if exists else 201

        @api.doc(parser=rename)
        def put(self):
            args = rename.parse_args()
            tsh = tshclass(namespace=args.namespace)

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')
            if tsh.exists(engine, args.newname):
                api.abort(409, f'`{args.newname}` does exists')

            with engine.begin() as cn:
                tsh.rename(cn, args.name, args.newname)

            # should be a 204 but https://github.com/flask-restful/flask-restful/issues/736
            return '', 200

        @api.doc(parser=get)
        def get(self):
            args = get.parse_args()
            tsh = tshclass(namespace=args.namespace)

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')

            with engine.begin() as cn:
                series = tsh.get(
                    cn, args.name,
                    revision_date=args.insertion_date,
                    from_value_date=args.from_value_date,
                    to_value_date=args.to_value_date
                )
                # the fast path will need it
                # also it is read from a cache filled at get time
                # so very cheap call
                metadata = tsh.metadata(cn, args.name)

            if args.mode == 'json':
                if series is not None:
                    response = make_response(
                        series.to_json(orient='index',
                                       date_format='iso')
                    )
                else:
                    response = make_response('null')
                response.headers['Content-Type'] = 'text/json'
                return response

            # let's wrapp the numpy arrays to be efficient
            stream = io.BytesIO()
            index, values = util.numpy_serialize(
                series,
                metadata['value_type'] == 'object'
            )
            stream.write(
                util.binary_pack(index, values)
            )
            response = make_response(
                stream.getvalue()
            )
            response.headers['Content-Type'] = 'application/octet-stream'
            return response

        @api.doc(parser=delete)
        def delete(self):
            args = delete.parse_args()
            tsh = tshclass(namespace=args.namespace)

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')

            with engine.begin() as cn:
                tsh.delete(cn, args.name)

            # should be a 204 but https://github.com/flask-restful/flask-restful/issues/736
            return args.name, 200

    @ns.route('/history')
    class timeseries_history(Resource):

        @api.doc(parser=history)
        def get(self):
            args = history.parse_args()
            tsh = tshclass(namespace=args.namespace)

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')

            with engine.begin() as cn:
                hist = tsh.history(
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

            if not tsh.exists(engine, args.name):
                api.abort(404, f'`{args.name}` does not exists')

            with engine.begin() as cn:
                series = tsh.staircase(
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

    @ns.route('/catalog')
    class timeseries_catalog(Resource):

        @api.doc(parser=catalog)
        def get(self):
            args = catalog.parse_args()
            tsh = tshclass(namespace=args.namespace)

            with engine.begin() as cn:
                return tsh.list_series(cn)

    return bp

