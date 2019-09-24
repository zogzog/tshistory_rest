from flask import Flask

from tshistory_rest.blueprint import blueprint


def make_app(dburi, sources=()):
    app = Flask(__name__)
    app.register_blueprint(
        blueprint(
            dburi,
            namespace='tsh',
            sources=sources
        )
    )
    return app


