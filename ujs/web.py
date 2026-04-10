"""Web UI for Lehigh Valley Court AI."""

import os, secrets, time
from datetime import timedelta
from functools import wraps
from urllib.parse import urlparse
from flask import Flask, Blueprint, Response, render_template, request, session, redirect, url_for, send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix
from authlib.integrations.flask_client import OAuth
from ujs.auth import create_user_token, revoke_user_tokens

main_bp = Blueprint('main', __name__)
oauth = OAuth()


def _api_url():
    return os.environ.get('API_URL', 'http://localhost:8100')


_case_count_cache = {'value': 0, 'expires': 0}


def _get_case_count():
    now = time.time()
    if now < _case_count_cache['expires']:
        return _case_count_cache['value']
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cases")
            count = cur.fetchone()[0]
        _case_count_cache['value'] = count
        _case_count_cache['expires'] = now + 3600  # 1 hour
        return count
    except Exception:
        return _case_count_cache['value']


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


@main_bp.route('/robots.txt')
def robots():
    return send_from_directory(os.path.join(os.path.dirname(__file__), 'static'), 'robots.txt', mimetype='text/plain')


@main_bp.route('/sitemap.xml')
def sitemap():
    pages = [
        ('/', 'weekly', '1.0'),
        ('/privacy', 'yearly', '0.3'),
        ('/disclaimer', 'yearly', '0.3'),
    ]
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    for loc, freq, priority in pages:
        xml += f'  <url><loc>https://gavelsearch.com{loc}</loc><changefreq>{freq}</changefreq><priority>{priority}</priority></url>\n'
    xml += '</urlset>'
    return Response(xml, mimetype='application/xml')


@main_bp.route('/privacy')
def privacy():
    return render_template('privacy.html', api_url=_api_url())


@main_bp.route('/disclaimer')
def disclaimer():
    return render_template('disclaimer.html', api_url=_api_url())


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            return redirect(url_for('main.login', next=request.path))
        return f(*args, **kwargs)
    return decorated


@main_bp.route('/')
def landing():
    return render_template('landing.html', api_url=_api_url(), case_count=_get_case_count(), **_user_context())


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


@main_bp.route('/unsubscribe/<token>')
def unsubscribe(token):
    """Public one-click unsubscribe — no login required (CAN-SPAM compliance).
    Always returns success to prevent token enumeration."""
    from ujs import db as _db
    try:
        with _db.connect() as conn:
            prefs = _db.get_preferences_by_token(conn, token)
            if prefs:
                _db.update_preferences(conn, prefs["user_id"], email_alerts=False)
    except Exception:
        pass
    # Always show success — prevents information leakage about valid tokens
    return render_template('unsubscribe.html', api_url=_api_url(), success=True)


@main_bp.route('/logout')
def logout():
    user = session.get('user')
    if user:
        revoke_user_tokens(user['sub'])
    session.clear()
    return redirect('/')


@main_bp.route('/settings')
@login_required
def settings():
    return render_template('settings.html', api_url=_api_url(), **_user_context())


@main_bp.route('/chat')
@main_bp.route('/chat/<conversation_id>')
@login_required
def chat(conversation_id=None):
    api_url = _api_url()
    initial_query = request.args.get('q', '')
    return render_template('chat.html', api_url=api_url, initial_query=initial_query,
                           conversation_id=conversation_id or '', case_count=_get_case_count(), **_user_context())


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
            "script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://cdn.jsdelivr.net https://static.cloudflareinsights.com https://www.googletagmanager.com; "
            "style-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com; "
            "img-src 'self' https://*.googleusercontent.com https://www.google-analytics.com data:; "
            "connect-src 'self' https://api.gavelsearch.com https://cdn.jsdelivr.net https://www.google-analytics.com https://*.google-analytics.com https://*.analytics.google.com; "
            "font-src 'self'; "
            "frame-ancestors 'none'"
        )
        return response

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(port=8000, debug=True)
