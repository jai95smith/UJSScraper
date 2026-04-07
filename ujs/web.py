"""Web UI for Lehigh Valley Court AI."""

import os
from flask import Flask, Blueprint, render_template, request

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def landing():
    api_url = os.environ.get('API_URL', 'http://localhost:8100')
    return render_template('landing.html', api_url=api_url)


@main_bp.route('/chat')
def chat():
    api_url = os.environ.get('API_URL', 'http://localhost:8100')
    initial_query = request.args.get('q', '')
    return render_template('chat.html', api_url=api_url, initial_query=initial_query)


def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.register_blueprint(main_bp)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(port=8000, debug=True)
