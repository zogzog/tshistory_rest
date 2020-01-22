from flask import Flask

from tshistory_rest.blueprint import blueprint


def make_app(dburi, namespace='tsh', sources=()):
    app = Flask(__name__)
    app.register_blueprint(
        blueprint(
            dburi,
            namespace=namespace,
            sources=sources
        )
    )
    return app

