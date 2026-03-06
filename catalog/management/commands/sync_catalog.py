import sys

from django.core.management.base import BaseCommand, CommandError

from catalog.constants import JobStatus
from catalog.models import ImportJob, Product, Manufacturer
from catalog.models.catalog import ProductSpecification
from catalog.services.importers import (
    FLISHistoryImporter,
    PUBLOGImporter,
    FOIAImporter,
    FLISVImporter,
)


class StyledStdout:
    """Wrapper that attaches Django style to stdout for importers."""

    def __init__(self, stdout, style):
        self.stdout = stdout
        self.style = style

    def write(self, msg):
        self.stdout.write(msg)


class Command(BaseCommand):
    help = "Product catalog import pipeline — import products from DLA data files"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", help="Sub-command")

        # flis-history
        flis_parser = subparsers.add_parser(
            "flis-history", help="Import FLIS HISTORY.zip (manufacturers + products)"
        )
        flis_parser.add_argument(
            "--skip-management", action="store_true",
            help="Skip V_MANAGEMENT_HISTORY price import",
        )
        flis_parser.add_argument(
            "--skip-references", action="store_true",
            help="Skip V_REFERENCE_NUMBER_HISTORY part number import",
        )
        flis_parser.add_argument(
            "--limit", type=int, default=None,
            help="Stop after creating N products (for test runs)",
        )

        # publog
        publog_parser = subparsers.add_parser(
            "publog", help="Import PUB LOG product catalog (MANAGEMENT/CAGE/IDENTIFICATION/REFERENCE)"
        )
        publog_parser.add_argument(
            "--batch-size", type=int, default=5000,
            help="NIINs to process per round (default: 5000)",
        )
        publog_parser.add_argument(
            "--rounds", type=int, default=1,
            help="Number of rounds to run (0 = unlimited). Default: 1",
        )
        publog_parser.add_argument(
            "--skip-rows", type=int, default=0,
            help="Skip this many rows in MANAGEMENT CSV before scanning (resume point).",
        )
        publog_parser.add_argument(
            "--cooldown", type=int, default=30,
            help="Seconds to pause between rounds (default: 30)",
        )

        # foia
        foia_parser = subparsers.add_parser(
            "foia", help="Import products from FOIA Excel reports"
        )
        foia_parser.add_argument(
            "--file", type=str, default=None,
            help="Import a specific FOIA file (e.g. FOIA_Report_Jan2026.xlsx)",
        )
        foia_parser.add_argument(
            "--limit", type=int, default=None,
            help="Stop after processing N rows (across all files)",
        )

        # flis-chars
        flisv_parser = subparsers.add_parser(
            "flis-chars", help="Enrich products with FLISV characteristics"
        )
        flisv_parser.add_argument(
            "--batch-size", type=int, default=10000,
            help="Number of NIINs to process per batch (default: 10000)",
        )

        # status
        subparsers.add_parser("status", help="Show import stats and record counts")

    def handle(self, *args, **options):
        subcommand = options.get("subcommand")
        if not subcommand:
            self.stderr.write(self.style.ERROR(
                "Please specify a subcommand: flis-history, publog, foia, flis-chars, status"
            ))
            sys.exit(1)

        handler = {
            "flis-history": self.handle_flis_history,
            "publog": self.handle_publog,
            "foia": self.handle_foia,
            "flis-chars": self.handle_flis_chars,
            "status": self.handle_status,
        }.get(subcommand)

        if handler:
            handler(options)
        else:
            raise CommandError(f"Unknown subcommand: {subcommand}")

    def _make_stdout(self):
        return StyledStdout(self.stdout, self.style)

    def handle_flis_history(self, options):
        skip_management = options.get("skip_management", False)
        skip_references = options.get("skip_references", False)
        limit = options.get("limit")
        self.stdout.write(self.style.MIGRATE_HEADING("=== FLIS History Import ==="))

        importer = FLISHistoryImporter(stdout=self._make_stdout())
        job = importer.safe_run(
            skip_management=skip_management,
            skip_references=skip_references,
            limit=limit,
        )
        self._print_job_summary(job)

    def handle_publog(self, options):
        import gc
        import time
        import resource

        batch_size = options.get("batch_size", 5000)
        rounds = options.get("rounds", 1)
        cooldown = options.get("cooldown", 30)
        skip_rows = options.get("skip_rows", 0)
        unlimited = rounds == 0

        self.stdout.write(self.style.MIGRATE_HEADING("=== PUB LOG Product Import ==="))
        self.stdout.write(f"Batch size: {batch_size:,} NIINs per round")
        self.stdout.write(f"Rounds: {'unlimited (until done)' if unlimited else rounds}")
        if skip_rows:
            self.stdout.write(f"Skipping first {skip_rows:,} MANAGEMENT rows")

        total_created = 0
        total_errored = 0
        round_num = 0
        mgmt_offset = skip_rows

        while True:
            round_num += 1
            if not unlimited and round_num > rounds:
                break

            self.stdout.write("")
            self.stdout.write(self.style.MIGRATE_HEADING(
                f"--- Round {round_num}{'' if unlimited else f'/{rounds}'} ---"
            ))

            mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            self.stdout.write(f"Memory (RSS peak): {mem_mb:.0f} MB")

            importer = PUBLOGImporter(stdout=self._make_stdout())
            job = importer.safe_run(limit=batch_size, skip_rows=mgmt_offset)
            mgmt_offset = importer.last_mgmt_row
            self._print_job_summary(job)

            total_created += job.records_created
            total_errored += job.records_errored

            if job.records_created == 0:
                self.stdout.write(self.style.SUCCESS(
                    "\nAll eligible NIINs have been imported!"
                ))
                break

            del importer
            gc.collect()

            if not unlimited and round_num >= rounds:
                break

            self.stdout.write(f"Cooling down {cooldown}s before next round...")
            time.sleep(cooldown)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"=== PUBLOG Import Complete: {round_num} rounds, "
            f"{total_created:,} created, {total_errored:,} errored ==="
        ))

    def handle_foia(self, options):
        file = options.get("file")
        limit = options.get("limit")
        self.stdout.write(self.style.MIGRATE_HEADING("=== FOIA Product Import ==="))
        if file:
            self.stdout.write(f"File: {file}")
        if limit:
            self.stdout.write(f"Limit: {limit} rows")

        importer = FOIAImporter(stdout=self._make_stdout())
        job = importer.safe_run(file=file, limit=limit)
        self._print_job_summary(job)

    def handle_flis_chars(self, options):
        batch_size = options.get("batch_size", 10000)
        self.stdout.write(self.style.MIGRATE_HEADING("=== FLISV Characteristics Enrichment ==="))

        importer = FLISVImporter(stdout=self._make_stdout())
        job = importer.safe_run(batch_size=batch_size)
        self._print_job_summary(job)

    def handle_status(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("=== Catalog Status ==="))

        # Recent jobs
        self.stdout.write("Recent jobs:")
        for job in ImportJob.objects.order_by("-created_at")[:10]:
            duration = ""
            if job.duration:
                duration = f" ({job.duration.total_seconds():.1f}s)"
            self.stdout.write(
                f"  {job.get_job_type_display():25s} | {job.get_status_display():10s} | "
                f"Created: {job.records_created}, Updated: {job.records_updated}, "
                f"Errored: {job.records_errored}{duration}"
            )
            if job.error_message:
                self.stdout.write(self.style.ERROR(f"    Error: {job.error_message[:200]}"))

        self.stdout.write("")

        # Record counts
        self.stdout.write("Record counts:")
        self.stdout.write(f"  Products:          {Product.objects.count():,}")
        self.stdout.write(f"  Manufacturers:     {Manufacturer.objects.count():,}")
        self.stdout.write(f"  Specifications:    {ProductSpecification.objects.count():,}")

        with_price = Product.objects.filter(price__isnull=False).count()
        with_nsn = Product.objects.exclude(nsn="").count()
        self.stdout.write(f"  Products w/ price: {with_price:,}")
        self.stdout.write(f"  Products w/ NSN:   {with_nsn:,}")

    def _print_job_summary(self, job):
        job.refresh_from_db()
        status_style = self.style.SUCCESS if job.status == JobStatus.COMPLETED else self.style.ERROR
        self.stdout.write(
            f"Job #{job.pk} [{status_style(job.get_status_display())}] — "
            f"Fetched: {job.records_fetched}, Created: {job.records_created}, "
            f"Updated: {job.records_updated}, Errored: {job.records_errored}"
        )
        if job.duration:
            self.stdout.write(f"Duration: {job.duration.total_seconds():.1f}s")
        if job.error_message:
            self.stdout.write(self.style.ERROR(f"Error: {job.error_message[:200]}"))
