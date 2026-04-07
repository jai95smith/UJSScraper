"""Chat UI for PA Court Search."""

import os
from flask import Flask, Blueprint, render_template

chat_bp = Blueprint('chat', __name__)

SUGGESTIONS = [
    "What hearings are in Lehigh tomorrow?",
    "Show me Smith cases in Northampton",
    "What is Kelli Murphy charged with?",
    "How many criminal cases were filed this week?",
]


@chat_bp.route('/')
def index():
    api_url = os.environ.get('API_URL', 'http://localhost:8100')
    return render_template('chat.html', api_url=api_url, suggestions=SUGGESTIONS)


def create_app():
    app = Flask(__name__, template_folder='templates')
    app.register_blueprint(chat_bp)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(port=8101, debug=True)
