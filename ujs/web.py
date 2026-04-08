"""Web UI for Lehigh Valley Court AI."""

import os
from flask import Flask, Blueprint, render_template, request

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def landing():
    api_url = os.environ.get('API_URL', 'http://localhost:8100')
    case_count = 0
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cases")
            case_count = cur.fetchone()[0]
    except Exception:
        pass
    return render_template('landing.html', api_url=api_url, case_count=case_count)


@main_bp.route('/chat')
@main_bp.route('/chat/<conversation_id>')
def chat(conversation_id=None):
    api_url = os.environ.get('API_URL', 'http://localhost:8100')
    initial_query = request.args.get('q', '')
    return render_template('chat.html', api_url=api_url, initial_query=initial_query,
                           conversation_id=conversation_id or '')


def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.register_blueprint(main_bp)

    @app.after_request
    def no_cache(response):
        if 'text/html' in response.content_type:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(port=8000, debug=True)
