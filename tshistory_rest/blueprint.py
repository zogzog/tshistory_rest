import json
import zlib
from array import array

import numpy as np
import pandas as pd

from flask import (
    Blueprint,
    make_response,
    request
)
from flask_restplus import (
    Api as baseapi,
    inputs,
    Resource,
    reqparse
)

from tshistory import api as tsapi, util, tsio


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


def no_content():
    # see https://github.com/flask-restful/flask-restful/issues/736
    resp = make_response('', 204)
    resp.headers.clear()
    return resp


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
    'metadata', type=todict, default=None,
    help='metadata associated with this insertion'
)
update.add_argument(
    'replace', type=inputs.boolean, default=False,
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
metadata.add_argument(
    'type', type=enum('standard', 'type', 'interval'),
    default='standard',
    help='specify the kind of needed metadata'
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
    '_keep_nans', type=inputs.boolean, default=False
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
catalog.add_argument(
    'allsources', type=inputs.boolean, default=True
)


def blueprint(tsa):

    # warn against playing proxy games
    assert isinstance(tsa, tsapi.dbtimeseries)

    bp = Blueprint(
        'tshistory_rest',
        __name__,
        template_folder='tshr_templates',
        static_folder='tshr_static',
    )

    # api & ns

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


    # routes

    @ns.route('/metadata')
    class timeseries_metadata(Resource):

        @api.expect(metadata)
        def get(self):
            args = metadata.parse_args()
            if not tsa.exists(args.name):
                api.abort(404, f'`{args.name}` does not exists')

            if args.type == 'standard':
                meta = tsa.metadata(args.name, all=args.all)
                return meta, 200
            elif args.type == 'type':
                stype = tsa.type(args.name)
                return stype, 200
            else:
                assert args.type == 'interval'
                try:
                    ival = tsa.interval(args.name)
                except ValueError as err:
                    return no_content()
                tzaware = tsa.metadata(args.name, all=True).get('tzaware', False)
                return (tzaware,
                        ival.left.isoformat(),
                        ival.right.isoformat()), 200

        @api.expect(put_metadata)
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

        @api.expect(update)
        def patch(self):
            args = update.parse_args()
            series = util.fromjson(args.series, args.name, args.tzaware)
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

        @api.expect(rename)
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

            return no_content()

        @api.expect(get)
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

        @api.expect(delete)
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

            return no_content()

    @ns.route('/history')
    class timeseries_history(Resource):

        @api.expect(history)
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
                diffmode=args.diffmode,
                _keep_nans=args._keep_nans
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

        @api.expect(staircase)
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

        @api.expect(catalog)
        def get(self):
            args = catalog.parse_args()
            cat = {
                f'{uri}!{ns}': series
                for (uri, ns), series in tsa.catalog(allsources=args.allsources).items()
            }
            return cat

    return bp

