import json
import io
import zlib
from array import array

import numpy as np
import pandas as pd

from flask import Blueprint, request, make_response
from flask_restplus import (
    Api as baseapi,
    inputs,
    Resource,
    reqparse
)

from tshistory import api as tsapi, util, tsio


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
    'name', type=str, required=True,
    help='timeseries name'
)

update = base.copy()
update.add_argument(
    'series', type=str, required=True,
    help='json representation of the series'
)
update.add_argument(
    'author', type=str, required=True,
    help='author of the insertion'
)
update.add_argument(
    'insertion_date', type=utcdt, default=None,
    help='insertion date can be forced'
)
update.add_argument(
    'tzaware', type=inputs.boolean, default=True,
    help='tzaware series'
)
update.add_argument(
    'metadata', type=str, default=None,
    help='metadata associated with this insertion'
)
update.add_argument(
    'replace', type=bool, default=False,
    help='replace the current series entirely with the provided series '
    '(no update semantics)'
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
    'format', type=enum('json', 'tshpack'), default='json'
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
history.add_argument(
    'format', type=enum('json', 'tshpack'), default='json'
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
staircase.add_argument(
    'format', type=enum('json', 'tshpack'), default='json'
)


catalog = reqparse.RequestParser()


def binary_pack_meta_data(meta, series):
    stream = io.BytesIO()
    index, values = util.numpy_serialize(
        series,
        meta['value_type'] == 'object'
    )
    bmeta = json.dumps(meta).encode('utf-8')
    stream.write(
        zlib.compress(
            util.nary_pack(bmeta, index, values)
        )
    )
    return stream.getvalue()


def blueprint(uri,
              namespace='tsh',
              apiclass=tsapi.multisourcetimeseries,
              tshclass=tsio.timeseries,
              sources=()):

    tsa = apiclass(uri, namespace, tshclass)
    for sourceuri, sourcens in sources:
        tsa.addsource(sourceuri, sourcens)

    @ns.route('/metadata')
    class timeseries_metadata(Resource):

        @api.doc(parser=metadata)
        def get(self):
            args = metadata.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            meta = tsa.metadata(args.name, all=args.all)
            return meta, 200

        @api.doc(parser=put_metadata)
        def put(self):
            args = put_metadata.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            metadata = json.loads(args.metadata)
            try:
                tsa.update_metadata(args.name, metadata)
            except ValueError as err:
                if err.args[0].startswith('not allowed to'):
                    api.abort(405, err.args[0])
                raise

            return '', 200


    @ns.route('/state')
    class timeseries_state(Resource):

        @api.doc(parser=update)
        def patch(self):
            args = update.parse_args()
            series = util.fromjson(args.series, args.name)
            if args.tzaware:
                # pandas to_json converted to utc
                # and dropped the offset
                series.index = series.index.tz_localize('utc')

            exists = tsa.exists(args.name)
            try:
                if args.replace:
                    tsa.replace(
                        args.name, series, args.author,
                        metadata=args.metadata,
                        insertion_date=args.insertion_date
                    )
                else:
                    tsa.update(
                        args.name, series, args.author,
                        metadata=args.metadata,
                        insertion_date=args.insertion_date
                    )
            except ValueError as err:
                if err.args[0].startswith('not allowed to'):
                    api.abort(405, err.args[0])
                raise

            return '', 200 if exists else 201

        @api.doc(parser=rename)
        def put(self):
            args = rename.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')
            if tsa.exists(args.newname):
                api.abort(409, f'`{args.newname}` does exists')

            try:
                tsa.rename(args.name, args.newname)
            except ValueError as err:
                if err.args[0].startswith('not allowed to'):
                    api.abort(405, err.args[0])
                raise

            # should be a 204 but https://github.com/flask-restful/flask-restful/issues/736
            return '', 200

        @api.doc(parser=get)
        def get(self):
            args = get.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            series = tsa.get(
                args.name,
                revision_date=args.insertion_date,
                from_value_date=args.from_value_date,
                to_value_date=args.to_value_date
            )
            # the fast path will need it
            # also it is read from a cache filled at get time
            # so very cheap call
            metadata = tsa.metadata(args.name, all=True)

            if args.format == 'json':
                if series is not None:
                    response = make_response(
                        series.to_json(orient='index',
                                       date_format='iso')
                    )
                else:
                    response = make_response('null')
                response.headers['Content-Type'] = 'text/json'
                return response

            response = make_response(
                binary_pack_meta_data(metadata, series)
            )
            response.headers['Content-Type'] = 'application/octet-stream'
            return response

        @api.doc(parser=delete)
        def delete(self):
            args = delete.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            try:
                tsa.delete(args.name)
            except ValueError as err:
                if err.args[0].startswith('not allowed to'):
                    api.abort(405, err.args[0])
                raise

            # should be a 204 but https://github.com/flask-restful/flask-restful/issues/736
            return args.name, 200

    @ns.route('/history')
    class timeseries_history(Resource):

        @api.doc(parser=history)
        def get(self):
            args = history.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            hist = tsa.history(
                args.name,
                from_insertion_date=args.from_insertion_date,
                to_insertion_date=args.to_insertion_date,
                from_value_date=args.from_value_date,
                to_value_date=args.to_value_date,
                diffmode=args.diffmode
            )
            metadata = tsa.metadata(args.name, all=True)

            if args.format == 'json':
                if hist is not None:
                    response = make_response(
                        pd.DataFrame(hist).to_json()
                    )
                else:
                    response = make_response('null')
                response.headers['Content-Type'] = 'text/json'
                return response

            response = make_response(
                util.pack_history(metadata, hist)
            )
            response.headers['Content-Type'] = 'application/octet-stream'
            return response

    @ns.route('/staircase')
    class timeseries_staircase(Resource):

        @api.doc(parser=staircase)
        def get(self):
            args = staircase.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            series = tsa.staircase(
                args.name, delta=args.delta,
                from_value_date=args.from_value_date,
                to_value_date=args.to_value_date,
            )
            metadata = tsa.metadata(args.name, all=True)

            if args.format == 'json':
                if series is not None:
                    response = make_response(
                        series.to_json(orient='index', date_format='iso')
                    )
                else:
                    response = make_response('null')
                response.headers['Content-Type'] = 'text/json'
                return response

            response = make_response(
                binary_pack_meta_data(metadata, series)
            )
            response.headers['Content-Type'] = 'application/octet-stream'
            return response

    @ns.route('/catalog')
    class timeseries_catalog(Resource):

        @api.doc(parser=catalog)
        def get(self):
            args = catalog.parse_args()
            return tsa.catalog()

    return bp

