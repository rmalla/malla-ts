class ProcurementError(Exception):
    """Base exception for catalog app."""


class HigherGovAPIError(ProcurementError):
    """Error communicating with HigherGov API."""

    def __init__(self, message, status_code=None, response_body=None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


class RateLimitExceeded(HigherGovAPIError):
    """HigherGov rate limit exceeded."""


class MonthlyQuotaExceeded(HigherGovAPIError):
    """Monthly API record quota exceeded."""


class ImportError(ProcurementError):
    """Error during data import."""


class AnalysisError(ProcurementError):
    """Error during opportunity analysis."""
