"""Web UI for Lehigh Valley Court AI."""

import os, secrets
from datetime import timedelta
from functools import wraps
from urllib.parse import urlparse
from flask import Flask, Blueprint, render_template, request, session, redirect, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from authlib.integrations.flask_client import OAuth
from ujs.auth import create_user_token, revoke_user_tokens

main_bp = Blueprint('main', __name__)
oauth = OAuth()


def _api_url():
    return os.environ.get('API_URL', 'http://localhost:8100')


def _safe_redirect_url(url):
    """Only allow relative paths. Reject absolute URLs / protocol-relative URLs."""
    if not url or not url.startswith('/') or url.startswith('//'):
        return '/chat'
    parsed = urlparse(url)
    if parsed.netloc or parsed.scheme:
        return '/chat'
    return url


def _user_context():
    """Return template context for the current user (or empty)."""
    user = session.get('user')
    ctx = {'user': user, 'user_token': ''}
    if user:
        ctx['user_token'] = create_user_token(user['sub'], user['email'], user.get('name', ''))
    return ctx


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            return redirect(url_for('main.login', next=request.path))
        return f(*args, **kwargs)
    return decorated


@main_bp.route('/')
def landing():
    api_url = _api_url()
    case_count = 0
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cases")
            case_count = cur.fetchone()[0]
    except Exception:
        pass
    return render_template('landing.html', api_url=api_url, case_count=case_count, **_user_context())


@main_bp.route('/login')
def login():
    if session.get('user'):
        return redirect('/chat')
    next_url = _safe_redirect_url(request.args.get('next', '/chat'))
    return render_template('login.html', api_url=_api_url(), next=next_url)


@main_bp.route('/login/google')
def login_google():
    base = os.environ.get('SITE_URL', request.host_url.rstrip('/'))
    redirect_uri = base + '/auth/callback'
    return oauth.google.authorize_redirect(redirect_uri)


@main_bp.route('/auth/callback')
def auth_callback():
    try:
        token = oauth.google.authorize_access_token()
    except Exception:
        return redirect(url_for('main.login', error='auth_failed'))
    userinfo = token.get('userinfo')
    if not userinfo:
        try:
            userinfo = oauth.google.userinfo()
        except Exception:
            return redirect(url_for('main.login', error='auth_failed'))
    # Validate audience — reject tokens issued for other apps
    id_token = token.get('id_token') or {}
    if hasattr(id_token, 'get'):
        aud = id_token.get('aud')
        expected_client_id = os.environ.get('GOOGLE_CLIENT_ID')
        if aud and expected_client_id and aud != expected_client_id:
            return redirect(url_for('main.login', error='auth_failed'))
    # Clear old session to prevent session fixation
    session.clear()
    session.permanent = True
    session['user'] = {
        'sub': userinfo['sub'],
        'email': userinfo['email'],
        'name': userinfo.get('name', ''),
        'picture': userinfo.get('picture', ''),
    }
    next_url = _safe_redirect_url(request.args.get('next', '/chat'))
    return redirect(next_url)


@main_bp.route('/logout')
def logout():
    user = session.get('user')
    if user:
        revoke_user_tokens(user['sub'])
    session.clear()
    return redirect('/')


@main_bp.route('/chat')
@main_bp.route('/chat/<conversation_id>')
@login_required
def chat(conversation_id=None):
    api_url = _api_url()
    initial_query = request.args.get('q', '')
    return render_template('chat.html', api_url=api_url, initial_query=initial_query,
                           conversation_id=conversation_id or '', **_user_context())


def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config['PREFERRED_URL_SCHEME'] = 'https'
    app.secret_key = os.environ.get('FLASK_SECRET_KEY')
    if not app.secret_key:
        if os.environ.get('FLASK_ENV') == 'development':
            app.secret_key = 'dev-only-insecure-key'
        else:
            raise RuntimeError("FLASK_SECRET_KEY env var is required")

    app.config['MAX_CONTENT_LENGTH'] = 1 * 1024 * 1024  # 1MB

    # Session cookie security
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_PERMANENT'] = True
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
    if os.environ.get('FLASK_ENV') != 'development':
        app.config['SESSION_COOKIE_SECURE'] = True

    # Google OAuth
    oauth.init_app(app)
    client_id = os.environ.get('GOOGLE_CLIENT_ID')
    client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
    if not client_id or not client_secret:
        import warnings
        warnings.warn("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET not set — OAuth login will fail")
    oauth.register(
        name='google',
        client_id=client_id or 'not-configured',
        client_secret=client_secret or 'not-configured',
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )

    app.register_blueprint(main_bp)

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html', api_url=_api_url()), 404

    @app.after_request
    def security_headers(response):
        if any(t in response.content_type for t in ('text/html', 'javascript', 'text/css')):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        response.headers['Content-Security-Policy'] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com; "
            "img-src 'self' https://*.googleusercontent.com data:; "
            "connect-src 'self' https://api.gavelsearch.com; "
            "font-src 'self'; "
            "frame-ancestors 'none'"
        )
        return response

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(port=8000, debug=True)
