import logging
import time
import uuid

from django.db import connection
from django.http import HttpResponsePermanentRedirect

logger = logging.getLogger(__name__)


class RequestIDMiddleware:
    """Attach a unique request ID to every request for tracing.

    - Reads X-Request-ID from incoming headers (e.g. from a load balancer)
    - Generates one if missing
    - Sets it on the response as X-Request-ID
    - Stores it on request.id for use in logging
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
        request.id = request_id

        response = self.get_response(request)
        response["X-Request-ID"] = request_id
        return response


class WwwRedirectMiddleware:
    """Redirect non-www requests to www.malla-ts.com (301)."""

    CANONICAL_HOST = "www.malla-ts.com"

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        host = request.get_host().split(":")[0]
        if host != self.CANONICAL_HOST and host.endswith("malla-ts.com"):
            return HttpResponsePermanentRedirect(
                f"https://{self.CANONICAL_HOST}{request.get_full_path()}"
            )
        return self.get_response(request)


class ServerTimingMiddleware:
    """Add Server-Timing header with DB query stats.

    Visible in browser DevTools > Network > Timing tab.
    Only active when DEBUG=False (production) or always — it's low overhead.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start = time.monotonic()

        # Reset query log for this request
        initial_queries = len(connection.queries)

        response = self.get_response(request)

        duration_ms = (time.monotonic() - start) * 1000
        db_queries = len(connection.queries) - initial_queries

        response["Server-Timing"] = (
            f'total;dur={duration_ms:.1f};desc="Total", '
            f'db;desc="DB queries: {db_queries}"'
        )

        # Log slow requests
        if duration_ms > 500:
            logger.warning(
                "Slow request: %s %s took %.0fms (%d queries)",
                request.method, request.path, duration_ms, db_queries,
            )

        return response
