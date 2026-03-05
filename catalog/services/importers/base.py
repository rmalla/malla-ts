import logging
import traceback

from catalog.constants import LogLevel
from catalog.models import ImportJob, ImportJobLog
from .parsers import parse_decimal, parse_int, parse_date

logger = logging.getLogger(__name__)


class BaseImporter:
    """
    Abstract base for all importers.

    Wraps ImportJob lifecycle: mark_running → run → mark_completed/failed.
    Provides dual logging to Python logger + ImportJobLog table.
    """

    job_type = None  # Subclasses must set this

    def __init__(self, client, stdout=None):
        """
        Args:
            client: HigherGovClient instance
            stdout: Optional management command stdout for CLI output
        """
        self.client = client
        self.stdout = stdout
        self.job = None
        self.filter_service = None

    def log(self, message, level=LogLevel.INFO, context=None):
        """Dual log to Python logger and ImportJobLog."""
        log_fn = {
            LogLevel.INFO: logger.info,
            LogLevel.WARNING: logger.warning,
            LogLevel.ERROR: logger.error,
        }.get(level, logger.info)
        log_fn(message)

        if self.job:
            ImportJobLog.objects.create(
                job=self.job,
                level=level,
                message=message,
                context=context or {},
            )

        if self.stdout:
            from django.core.management.base import OutputWrapper

            style_fn = None
            if hasattr(self.stdout, "style"):
                if level == LogLevel.ERROR:
                    style_fn = self.stdout.style.ERROR
                elif level == LogLevel.WARNING:
                    style_fn = self.stdout.style.WARNING
                else:
                    style_fn = self.stdout.style.SUCCESS

            if style_fn:
                self.stdout.write(style_fn(message))
            else:
                self.stdout.write(message)

    def write(self, message):
        """Write to stdout if available."""
        if self.stdout:
            self.stdout.write(message)

    def create_job(self, parameters=None):
        """Create a new ImportJob."""
        self.job = ImportJob.objects.create(
            job_type=self.job_type,
            parameters=parameters or {},
        )
        return self.job

    # Shared parse helpers (delegated to parsers module)
    _parse_decimal = staticmethod(parse_decimal)
    _parse_int = staticmethod(parse_int)
    _parse_date = staticmethod(parse_date)

    def run(self, **kwargs):
        """Override in subclasses. Implement the actual import logic."""
        raise NotImplementedError

    def safe_run(self, **kwargs):
        """Run the importer with full job lifecycle management."""
        parameters = {k: str(v) for k, v in kwargs.items() if v is not None}
        self.create_job(parameters=parameters)
        self.log(f"Starting {self.job.get_job_type_display()}")

        try:
            self.job.mark_running()

            # Initialize filter service for this pipeline stage
            from catalog.services.filter_service import FilterService
            self.filter_service = FilterService(stage=self.job_type)
            self.log(
                f"Loaded {self.filter_service.rule_count} active pipeline filter(s)"
            )

            self.run(**kwargs)

            # Persist filter stats
            self.job.api_calls_made = self.client.api_calls_made
            self.job.records_filtered = sum(
                self.filter_service._match_counts.values()
            )
            self.job.save(update_fields=["api_calls_made", "records_filtered"])

            # Log filter summary
            summary = self.filter_service.get_summary()
            if any(v > 0 for v in summary.values()):
                for rule_str, count in summary.items():
                    if count > 0:
                        self.log(f"Filtered {count} record(s) by: {rule_str}")

            self.job.mark_completed()
            self.log(
                f"Completed: {self.job.records_created} created, "
                f"{self.job.records_updated} updated, "
                f"{self.job.records_errored} errored, "
                f"{self.job.records_filtered} filtered "
                f"(API calls: {self.job.api_calls_made})"
            )
        except Exception as e:
            self.job.api_calls_made = self.client.api_calls_made
            self.job.save(update_fields=["api_calls_made"])
            self.job.mark_failed(str(e))
            self.log(
                f"Failed: {e}\n{traceback.format_exc()}",
                level=LogLevel.ERROR,
                context={"traceback": traceback.format_exc()},
            )
            raise

        return self.job
