from django.db import models
from django.utils import timezone

from catalog.constants import JobType, JobStatus, LogLevel


class ImportJob(models.Model):
    """Audit trail for each sync/import run."""

    job_type = models.CharField(
        max_length=30, choices=JobType.choices, db_index=True
    )
    status = models.CharField(
        max_length=20, choices=JobStatus.choices, default=JobStatus.PENDING,
        db_index=True,
    )
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    records_fetched = models.PositiveIntegerField(default=0)
    records_created = models.PositiveIntegerField(default=0)
    records_updated = models.PositiveIntegerField(default=0)
    records_errored = models.PositiveIntegerField(default=0)
    records_filtered = models.PositiveIntegerField(default=0)
    api_calls_made = models.PositiveIntegerField(default=0)
    parameters = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Import Job"
        verbose_name_plural = "Import Jobs"
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.get_job_type_display()} — {self.get_status_display()} ({self.created_at:%Y-%m-%d %H:%M})"

    @property
    def duration(self):
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    def mark_running(self):
        self.status = JobStatus.RUNNING
        self.started_at = timezone.now()
        self.save(update_fields=["status", "started_at"])

    def mark_completed(self):
        self.status = JobStatus.COMPLETED
        self.completed_at = timezone.now()
        self.save(update_fields=["status", "completed_at"])

    def mark_failed(self, error_message=""):
        self.status = JobStatus.FAILED
        self.completed_at = timezone.now()
        self.error_message = str(error_message)[:5000]
        self.save(update_fields=["status", "completed_at", "error_message"])

    def increment(self, field, amount=1):
        """Atomically increment a counter field."""
        from django.db.models import F

        setattr(self, field, F(field) + amount)
        self.save(update_fields=[field])
        self.refresh_from_db(fields=[field])


class ImportJobLog(models.Model):
    """Detailed log entries for an import job."""

    job = models.ForeignKey(
        ImportJob, on_delete=models.CASCADE, related_name="logs"
    )
    level = models.CharField(
        max_length=10, choices=LogLevel.choices, default=LogLevel.INFO
    )
    message = models.TextField()
    context = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Import Job Log"
        verbose_name_plural = "Import Job Logs"
        ordering = ["created_at"]

    def __str__(self):
        return f"[{self.level}] {self.message[:80]}"
