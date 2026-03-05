from .base import *
import os

DEBUG = os.getenv("DEBUG", "False") == "True"

# =============================================================================
# Sentry Error Tracking (set SENTRY_DSN env var to enable)
# =============================================================================
SENTRY_DSN = os.getenv("SENTRY_DSN", "")
if SENTRY_DSN:
    import sentry_sdk
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
        send_default_pii=False,
    )

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-change-this-in-production")

# SECURITY WARNING: define the correct hosts in production!
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "localhost").split(",")

# WhiteNoise for serving static files in production
MIDDLEWARE.insert(1, "whitenoise.middleware.WhiteNoiseMiddleware")

# ManifestStaticFilesStorage is recommended in production, to prevent
# outdated JavaScript / CSS assets being served from cache
# (e.g. after a Wagtail upgrade).
# See https://docs.djangoproject.com/en/5.2/ref/contrib/staticfiles/#manifeststaticfilesstorage
# Temporarily using CompressedStaticFilesStorage instead of CompressedManifestStaticFilesStorage
# to avoid manifest issues
STORAGES["staticfiles"]["BACKEND"] = "whitenoise.storage.CompressedStaticFilesStorage"

# WhiteNoise cache headers - 1 year for static files
WHITENOISE_MAX_AGE = 31536000

# Wagtail admin base URL
WAGTAILADMIN_BASE_URL = os.getenv("SITE_URL", "https://malla-ts.com")

# =============================================================================
# Security Settings
# =============================================================================
SECURE_SSL_REDIRECT = True
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = 'DENY'

# HSTS - enforce HTTPS for 1 year, include subdomains, allow preload list
SECURE_HSTS_SECONDS = 31536000
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True

# django-ratelimit: use X-Real-IP set by nginx (single IP, not comma-separated)
RATELIMIT_IP_META_KEY = "HTTP_X_REAL_IP"

# =============================================================================
# Database Connection Pooling
# =============================================================================
DATABASES["default"]["CONN_MAX_AGE"] = 600  # 10-minute persistent connections

# =============================================================================
# Cache Configuration (Redis)
# =============================================================================
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.dummy.DummyCache",
    }
}

# Use database sessions while cache is disabled
SESSION_ENGINE = "django.contrib.sessions.backends.db"

# =============================================================================
# Structured Logging
# =============================================================================
LOG_DIR = os.path.join(BASE_DIR, "logs")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[{asctime}] {levelname} {name} {module}.{funcName}:{lineno} | {message}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "[{asctime}] {levelname} {message}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "filters": {
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "app_file": {
            "class": "logging.FileHandler",
            "filename": os.path.join(LOG_DIR, "app.log"),
            "formatter": "verbose",
        },
        "error_file": {
            "class": "logging.FileHandler",
            "filename": os.path.join(LOG_DIR, "error.log"),
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
    "loggers": {
        "django": {
            "handlers": ["console", "error_file"],
            "level": "WARNING",
            "propagate": False,
        },
        "django.request": {
            "handlers": ["console", "error_file"],
            "level": "ERROR",
            "propagate": False,
        },
        "django.security": {
            "handlers": ["console", "error_file"],
            "level": "WARNING",
            "propagate": False,
        },
        "home": {
            "handlers": ["console", "app_file"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

# =============================================================================
# Email Configuration (Amazon SES API)
# =============================================================================
EMAIL_BACKEND = os.getenv('EMAIL_BACKEND', 'django_ses.SESBackend')
DEFAULT_FROM_EMAIL = os.getenv('DEFAULT_FROM_EMAIL', 'info@malla-ts.com')

# AWS SES Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
AWS_SES_REGION_NAME = os.getenv('AWS_SES_REGION_NAME', 'us-east-1')
AWS_SES_REGION_ENDPOINT = os.getenv('AWS_SES_REGION_ENDPOINT', 'email.us-east-1.amazonaws.com')

# Disable rate limiting check (requires GetSendQuota permission)
AWS_SES_AUTO_THROTTLE = None

try:
    from .local import *
except ImportError:
    pass
