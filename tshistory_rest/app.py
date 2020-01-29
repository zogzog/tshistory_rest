from flask import Flask

from tshistory_rest.blueprint import blueprint


def make_app(tsa):
    app = Flask(__name__)
    app.register_blueprint(
        blueprint(tsa)
    )
    return app

