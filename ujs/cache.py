"""Redis cache layer — response caching, rate limits, news cache."""

import json, hashlib, re, time, logging
import redis

logger = logging.getLogger("ujs.cache")

_STOP_WORDS = {'a', 'an', 'the', 'is', 'are', 'was', 'were', 'what', 'which', 'who', 'whom',
               'this', 'that', 'these', 'those', 'am', 'be', 'been', 'being', 'have', 'has',
               'had', 'do', 'does', 'did', 'will', 'would', 'shall', 'should', 'may', 'might',
               'can', 'could', 'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from',
               'about', 'into', 'through', 'during', 'before', 'after', 'above', 'below',
               'between', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once',
               'here', 'there', 'when', 'where', 'why', 'how', 'all', 'each', 'every', 'both',
               'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only',
               'own', 'same', 'so', 'than', 'too', 'very', 'just', 'because', 'as', 'until',
               'while', 'and', 'but', 'or', 'if', 'because', 'it', 'its', 'me', 'my', 'i',
               'tell', 'show', 'give', 'get', 'find', 'look', 'up', 'many', 'much', 'how'}

# Redis connection (lazy init)
_redis = None


def _get_redis():
    global _redis
    if _redis is None:
        try:
            _redis = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
            _redis.ping()
            logger.info("Redis connected")
        except Exception as e:
            logger.warning("Redis unavailable: %s", e)
            _redis = None
    return _redis


def normalize_query(question):
    """Normalize a question to a canonical cache key.
    'tell me how many cases today' → 'cases today'
    'What hearings are in Lehigh tomorrow?' → 'hearings lehigh tomorrow'"""
    q = question.lower().strip()
    q = re.sub(r'[^\w\s]', '', q)  # Remove punctuation
    words = [w for w in q.split() if w not in _STOP_WORDS and len(w) > 1]
    return ' '.join(sorted(words))


def _cache_key(prefix, normalized):
    """Short hash-based cache key."""
    h = hashlib.md5(normalized.encode()).hexdigest()[:12]
    return f"ujs:{prefix}:{h}"


# ---------------------------------------------------------------------------
# Response cache (full job responses)
# ---------------------------------------------------------------------------

_RESPONSE_TTL = 3600  # 1 hour

# Queries containing these patterns are person-specific — don't cache
_PERSON_PATTERNS = re.compile(r'[A-Z][a-z]+[\s,]+[A-Z][a-z]+|rapsheet|record for', re.IGNORECASE)


def get_cached_response(question):
    """Check if this question has a cached response. Returns response text or None."""
    r = _get_redis()
    if not r:
        return None
    if _PERSON_PATTERNS.search(question):
        return None  # Don't cache person-specific queries
    key = _cache_key("resp", normalize_query(question))
    try:
        return r.get(key)
    except Exception:
        return None


def set_cached_response(question, response):
    """Cache a response for this question."""
    r = _get_redis()
    if not r:
        return
    if _PERSON_PATTERNS.search(question):
        return
    key = _cache_key("resp", normalize_query(question))
    try:
        r.setex(key, _RESPONSE_TTL, response)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# News cache (replaces in-memory _news_cache)
# ---------------------------------------------------------------------------

_NEWS_TTL = 86400  # 24 hours


def get_cached_news(cache_key):
    """Get cached news for a person query."""
    r = _get_redis()
    if not r:
        return None
    try:
        return r.get(f"ujs:news:{cache_key}")
    except Exception:
        return None


def set_cached_news(cache_key, text):
    """Cache news text."""
    r = _get_redis()
    if not r:
        return
    try:
        r.setex(f"ujs:news:{cache_key}", _NEWS_TTL, text)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Rate limiting (replaces in-memory dicts)
# ---------------------------------------------------------------------------

def check_rate(key, limit, window=60):
    """Check if key has exceeded limit in window seconds. Returns True if limited."""
    r = _get_redis()
    if not r:
        return False  # Fail open if Redis is down
    rkey = f"ujs:rate:{key}"
    try:
        pipe = r.pipeline()
        pipe.incr(rkey)
        pipe.expire(rkey, window)
        count, _ = pipe.execute()
        return count > limit
    except Exception:
        return False
