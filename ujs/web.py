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


_STATE_NAMES = {
    "PA": "pennsylvania", "NJ": "new-jersey", "NY": "new-york", "OH": "ohio",
    "DE": "delaware", "MD": "maryland", "CT": "connecticut", "VA": "virginia",
    "FL": "florida", "TX": "texas", "CA": "california", "IL": "illinois",
}
_STATE_CODES = {v: k for k, v in _STATE_NAMES.items()}


def _state_slug(code):
    return _STATE_NAMES.get(code.upper(), code.lower())


def _state_code(slug):
    return _STATE_CODES.get(slug.lower())


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
    from ujs import db
    pages = [
        ('/', 'weekly', '1.0'),
        ('/privacy', 'yearly', '0.3'),
        ('/disclaimer', 'yearly', '0.3'),
    ]
    for c in db.get_active_counties():
        state = _state_slug(c.get("state", "PA"))
        slug = c["county"].lower().replace(' ', '-')
        pages.append((f'/{state}/{slug}/', 'daily', '0.8'))
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
            return redirect(url_for('main.login', next=request.full_path))
        return f(*args, **kwargs)
    return decorated


@main_bp.route('/')
def landing():
    from ujs import db
    return render_template('landing.html', api_url=_api_url(), case_count=_get_case_count(),
                           counties=db.get_active_counties(), **_user_context())


@main_bp.route('/<state_slug>/<county_slug>/')
def county_lander(state_slug, county_slug):
    from ujs import db
    state_code = _state_code(state_slug)
    if not state_code:
        return render_template('404.html', api_url=_api_url()), 404
    county_name = county_slug.replace('-', ' ').title()
    active = db.get_active_counties()
    match = next((c for c in active if c["county"].lower() == county_name.lower()
                  and c.get("state", "PA").upper() == state_code), None)
    if not match:
        return render_template('404.html', api_url=_api_url()), 404
    with db.connect() as conn:
        charge_stats = db.get_charge_stats(conn, county=county_name, limit=10)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT description) FROM charges ch JOIN cases c ON ch.docket_number = c.docket_number WHERE c.county ILIKE %s", (county_name,))
        unique_charges = cur.fetchone()[0]
    return render_template('county_lander.html',
        api_url=_api_url(), county=county_name, state_code=state_code,
        state_slug=state_slug, county_slug=county_slug,
        case_count=match["case_count"], total_case_count=_get_case_count(),
        charge_stats=charge_stats, unique_charges=unique_charges,
        counties=active, **_user_context())


@main_bp.route('/login')
def login():
    if session.get('user'):
        return redirect('/chat')
    next_url = _safe_redirect_url(request.args.get('next', '/chat'))
    return render_template('login.html', api_url=_api_url(), next=next_url)


@main_bp.route('/login/google')
def login_google():
    # Stash next URL in session so it survives the OAuth round-trip
    next_url = request.args.get('next')
    if next_url:
        session['login_next'] = _safe_redirect_url(next_url)
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
    # Grab next URL before clearing session
    login_next = session.get('login_next')
    # Clear old session to prevent session fixation
    session.clear()
    session.permanent = True
    session['user'] = {
        'sub': userinfo['sub'],
        'email': userinfo['email'],
        'name': userinfo.get('name', ''),
        'picture': userinfo.get('picture', ''),
    }
    next_url = _safe_redirect_url(login_next or request.args.get('next', '/chat'))
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


_ADMIN_EMAILS = {"jai95smith@gmail.com", "jsmith@lehighdaily.com"}


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        user = session.get('user')
        if not user or user.get('email') not in _ADMIN_EMAILS:
            return redirect('/')
        return f(*args, **kwargs)
    return decorated


@main_bp.route('/admin')
@admin_required
def admin_panel():
    return render_template('admin.html', api_url=_api_url(), **_user_context())


@main_bp.route('/admin/api/status')
@admin_required
def admin_status():
    """API endpoint for admin panel to fetch live status."""
    import subprocess
    # Cron jobs
    try:
        crontab = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=5).stdout
    except Exception:
        crontab = ""
    crons = []
    for line in crontab.strip().split('\n'):
        if not line or line.startswith('#') and 'HALTED' not in line:
            continue
        halted = line.startswith('# HALTED:')
        clean = line.replace('# HALTED: ', '') if halted else line
        # Extract name from command
        if 'watchdog' in clean:
            name = 'Watchdog'
            schedule = 'Every 2 min'
            desc = 'Checks watched dockets for changes'
        elif 'notify' in clean:
            name = 'Notifications'
            schedule = 'Daily 12pm UTC'
            desc = 'Sends email alerts for detected changes'
        else:
            name = clean[:40]
            schedule = clean.split(' ', 5)[:5]
            desc = ''
        crons.append({'name': name, 'schedule': schedule, 'desc': desc, 'active': not halted, 'raw': clean})
    # Systemd services
    services = []
    for svc in ['ujs-api', 'ujs-web', 'ujs-worker', 'ujs-ingest']:
        try:
            r = subprocess.run(['systemctl', 'is-active', svc], capture_output=True, text=True, timeout=5)
            status = r.stdout.strip()
        except Exception:
            status = 'unknown'
        services.append({'name': svc, 'status': status})
    # DB stats
    from ujs import db
    with db.connect() as conn:
        stats = db.get_stats(conn)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM change_log")
        change_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ingest_queue WHERE status = 'pending'")
        queue_pending = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ingest_queue WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'")
        analyzed_24h = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM analyses")
        total_analyzed = cur.fetchone()[0]
    return {
        'crons': crons,
        'services': services,
        'stats': {**stats, 'changes': change_count, 'queue_pending': queue_pending,
                  'analyzed_24h': analyzed_24h, 'total_analyzed': total_analyzed},
    }


@main_bp.route('/admin/api/cron/<action>/<name>')
@admin_required
def admin_cron_toggle(action, name):
    """Toggle a cron job on/off."""
    import subprocess
    try:
        crontab = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return {'error': 'Failed to read crontab'}, 500
    lines = crontab.strip().split('\n')
    new_lines = []
    found = False
    for line in lines:
        if name.lower() in line.lower():
            found = True
            if action == 'stop' and not line.startswith('# HALTED:'):
                new_lines.append('# HALTED: ' + line)
            elif action == 'start' and line.startswith('# HALTED:'):
                new_lines.append(line.replace('# HALTED: ', ''))
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
    if not found:
        return {'error': f'Cron {name} not found'}, 404
    try:
        subprocess.run(['crontab', '-'], input='\n'.join(new_lines) + '\n', capture_output=True, text=True, timeout=5)
    except Exception:
        return {'error': 'Failed to update crontab'}, 500
    return {'status': 'ok', 'action': action, 'name': name}


@main_bp.route('/admin/api/service/<action>/<name>')
@admin_required
def admin_service_toggle(action, name):
    """Start/stop/restart a systemd service."""
    import subprocess
    allowed = {'ujs-api', 'ujs-web', 'ujs-worker', 'ujs-ingest'}
    if name not in allowed:
        return {'error': f'Unknown service {name}'}, 404
    if action not in ('start', 'stop', 'restart'):
        return {'error': f'Unknown action {action}'}, 400
    try:
        subprocess.run(['systemctl', action, name], capture_output=True, text=True, timeout=10)
    except Exception as e:
        return {'error': str(e)}, 500
    return {'status': 'ok', 'action': action, 'name': name}


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
    from ujs import db
    return render_template('chat.html', api_url=api_url, initial_query=initial_query,
                           conversation_id=conversation_id or '', case_count=_get_case_count(),
                           counties=db.get_active_counties(), **_user_context())


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
