import sys
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from catalog.constants import JobStatus, SourceType
from catalog.models import ImportJob
from catalog.services.highergov_client import HigherGovClient
from catalog.services.importers import (
    OpportunityImporter,
    DIBBSImporter,
    NSNEnricher,
    CAGEResolver,
    FOIAImporter,
    FLISHistoryImporter,
    FLISVImporter,
    PUBLOGImporter,
)
from catalog.services.analyzers import OpportunityAnalyzer
from catalog.services.sam_client import SAMGovClient


class StyledStdout:
    """Wrapper that attaches Django style to stdout for importers."""

    def __init__(self, stdout, style):
        self.stdout = stdout
        self.style = style

    def write(self, msg):
        self.stdout.write(msg)


class Command(BaseCommand):
    help = "Product catalog pipeline — import data, sync opportunities, enrich NSNs, resolve CAGE codes, analyze markets"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", help="Sub-command")

        # dibbs
        dibbs_parser = subparsers.add_parser("dibbs", help="Sync DIBBS opportunities")
        dibbs_parser.add_argument(
            "--since", type=str, default=None,
            help="Sync opportunities posted since date (YYYY-MM-DD)",
        )

        # sam
        sam_parser = subparsers.add_parser("sam", help="Sync SAM.gov opportunities")
        sam_parser.add_argument(
            "--since", type=str, default=None,
            help="Sync opportunities posted since date (YYYY-MM-DD)",
        )

        # sled
        sled_parser = subparsers.add_parser("sled", help="Sync SLED opportunities")
        sled_parser.add_argument(
            "--since", type=str, default=None,
            help="Sync opportunities posted since date (YYYY-MM-DD)",
        )

        # enrich
        enrich_parser = subparsers.add_parser("enrich", help="Enrich NSNs from opportunity data")
        enrich_parser.add_argument(
            "--batch-size", type=int, default=50,
            help="Number of NSNs to enrich per run (default: 50)",
        )

        # resolve
        resolve_parser = subparsers.add_parser("resolve", help="Resolve unresolved CAGE codes")
        resolve_parser.add_argument(
            "--batch-size", type=int, default=50,
            help="Number of CAGE codes to resolve per run (default: 50)",
        )
        resolve_parser.add_argument(
            "--retry-failed", action="store_true",
            help="Re-attempt codes previously resolved with no company name",
        )

        # analyze
        subparsers.add_parser("analyze", help="Compute market opportunities")

        # pipeline
        pipeline_parser = subparsers.add_parser("pipeline", help="Run full pipeline")
        pipeline_parser.add_argument(
            "--since", type=str, default=None,
            help="Sync opportunities posted since date (YYYY-MM-DD)",
        )
        pipeline_parser.add_argument(
            "--batch-size", type=int, default=50,
            help="Batch size for enrichment and resolution (default: 50)",
        )
        pipeline_parser.add_argument(
            "--source", type=str, default="all",
            choices=["dibbs", "sam", "sled", "all"],
            help="Which source(s) to sync (default: all)",
        )

        # foia
        foia_parser = subparsers.add_parser("foia", help="Import FOIA purchase transactions")
        foia_parser.add_argument(
            "--file", type=str, default=None,
            help="Import a specific FOIA file (e.g. FOIA_Report_Jan2026.xlsx)",
        )
        foia_parser.add_argument(
            "--limit", type=int, default=None,
            help="Stop after processing N rows (across all files)",
        )

        # flis-history
        flis_parser = subparsers.add_parser("flis-history", help="Import FLIS HISTORY.zip catalog data")
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
            help="Stop after creating N catalog items (for test runs)",
        )

        # flis-chars
        flisv_parser = subparsers.add_parser("flis-chars", help="Enrich NSN catalog with FLISV characteristics")
        flisv_parser.add_argument(
            "--batch-size", type=int, default=10000,
            help="Number of NIINs to process per batch (default: 10000)",
        )

        # publog
        publog_parser = subparsers.add_parser("publog", help="Import PUB LOG product catalog (MANAGEMENT/CAGE/IDENTIFICATION/REFERENCE)")
        publog_parser.add_argument(
            "--batch-size", type=int, default=5000,
            help="NIINs to process per round (default: 5000)",
        )
        publog_parser.add_argument(
            "--rounds", type=int, default=1,
            help="Number of rounds to run (0 = unlimited, until all done). Default: 1",
        )
        parser.add_argument(
            "--skip-rows", type=int, default=0,
            help="Skip this many rows in MANAGEMENT CSV before scanning (resume point).",
        )
        publog_parser.add_argument(
            "--skip-suppliers", action="store_true",
            help="Skip REFERENCE/CAGE scan — only create catalog entries (fast test mode)",
        )
        publog_parser.add_argument(
            "--cooldown", type=int, default=30,
            help="Seconds to pause between rounds (default: 30)",
        )

        # flis-link
        subparsers.add_parser("flis-link", help="Cross-reference and link FOIA transactions to catalog")

        # status
        subparsers.add_parser("status", help="Show API usage and last job stats")

    def handle(self, *args, **options):
        subcommand = options.get("subcommand")
        if not subcommand:
            self.stderr.write(self.style.ERROR(
                "Please specify a subcommand: dibbs, sam, sled, enrich, resolve, analyze, pipeline, "
                "foia, flis-history, flis-chars, publog, flis-link, status"
            ))
            sys.exit(1)

        handler = {
            "dibbs": self.handle_dibbs,
            "sam": self.handle_sam,
            "sled": self.handle_sled,
            "enrich": self.handle_enrich,
            "resolve": self.handle_resolve,
            "analyze": self.handle_analyze,
            "pipeline": self.handle_pipeline,
            "foia": self.handle_foia,
            "flis-history": self.handle_flis_history,
            "flis-chars": self.handle_flis_chars,
            "publog": self.handle_publog,
            "flis-link": self.handle_flis_link,
            "status": self.handle_status,
        }.get(subcommand)

        if handler:
            handler(options)
        else:
            raise CommandError(f"Unknown subcommand: {subcommand}")

    def _make_stdout(self):
        return StyledStdout(self.stdout, self.style)

    def _get_sam_client(self):
        """Get a SAM.gov client if configured, otherwise None."""
        try:
            if SAMGovClient.is_configured():
                return SAMGovClient()
        except Exception:
            pass
        return None

    def _sync_source(self, client, source_type, since, heading, sam_client=None):
        """Run an OpportunityImporter for the given source type."""
        self.stdout.write(self.style.MIGRATE_HEADING(heading))
        importer = OpportunityImporter(
            client, source_type=source_type, stdout=self._make_stdout(),
            sam_client=sam_client if source_type == SourceType.SAM else None,
        )
        job = importer.safe_run(since=since)
        self._print_job_summary(job)
        return job

    def handle_dibbs(self, options):
        since = options.get("since")
        self.stdout.write(self.style.MIGRATE_HEADING("=== DIBBS Sync ==="))

        with HigherGovClient() as client:
            importer = DIBBSImporter(client, stdout=self._make_stdout())
            job = importer.safe_run(since=since)

        self._print_job_summary(job)

    def handle_sam(self, options):
        since = options.get("since")
        self.stdout.write(self.style.MIGRATE_HEADING("=== SAM Sync ==="))

        sam_client = self._get_sam_client()
        with HigherGovClient() as client:
            importer = OpportunityImporter(
                client, source_type=SourceType.SAM, stdout=self._make_stdout(),
                sam_client=sam_client,
            )
            job = importer.safe_run(since=since)

        if sam_client:
            sam_client.session.close()
        self._print_job_summary(job)

    def handle_sled(self, options):
        since = options.get("since")
        self.stdout.write(self.style.MIGRATE_HEADING("=== SLED Sync ==="))

        with HigherGovClient() as client:
            importer = OpportunityImporter(
                client, source_type=SourceType.SLED, stdout=self._make_stdout()
            )
            job = importer.safe_run(since=since)

        self._print_job_summary(job)

    def handle_enrich(self, options):
        batch_size = options.get("batch_size", 50)
        self.stdout.write(self.style.MIGRATE_HEADING("=== NSN Enrichment ==="))

        with HigherGovClient() as client:
            enricher = NSNEnricher(client, stdout=self._make_stdout())
            job = enricher.safe_run(batch_size=batch_size)

        self._print_job_summary(job)

    def handle_resolve(self, options):
        batch_size = options.get("batch_size", 50)
        retry_failed = options.get("retry_failed", False)
        self.stdout.write(self.style.MIGRATE_HEADING("=== CAGE Resolution ==="))
        if retry_failed:
            self.stdout.write("Mode: retrying previously-failed codes")

        with HigherGovClient() as client:
            resolver = CAGEResolver(client, stdout=self._make_stdout())
            job = resolver.safe_run(batch_size=batch_size, retry_failed=retry_failed)

        self._print_job_summary(job)

    def handle_analyze(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("=== Opportunity Analysis ==="))

        analyzer = OpportunityAnalyzer(stdout=self._make_stdout())
        job = analyzer.analyze()

        self._print_job_summary(job)

    def handle_pipeline(self, options):
        since = options.get("since")
        batch_size = options.get("batch_size", 50)
        source = options.get("source", "all")

        self.stdout.write(self.style.MIGRATE_HEADING("=== Full Pipeline ==="))
        self.stdout.write("")

        # Determine which sources to sync
        if source == "all":
            sources = [
                (SourceType.DIBBS, "--- Step 1a: DIBBS Sync ---"),
                (SourceType.SAM, "--- Step 1b: SAM Sync ---"),
                (SourceType.SLED, "--- Step 1c: SLED Sync ---"),
            ]
        else:
            source_map = {
                "dibbs": SourceType.DIBBS,
                "sam": SourceType.SAM,
                "sled": SourceType.SLED,
            }
            st = source_map[source]
            sources = [(st, f"--- Step 1: {source.upper()} Sync ---")]

        sam_client = self._get_sam_client()

        with HigherGovClient() as client:
            # Step 1: Sync opportunities from selected sources
            for source_type, heading in sources:
                self._sync_source(client, source_type, since, heading, sam_client=sam_client)
                self.stdout.write("")

            # Step 2: NSN Enrichment
            self.stdout.write(self.style.MIGRATE_HEADING("--- Step 2: NSN Enrichment ---"))
            enricher = NSNEnricher(client, stdout=self._make_stdout())
            enrich_job = enricher.safe_run(batch_size=batch_size)
            self._print_job_summary(enrich_job)
            self.stdout.write("")

            # Step 3: CAGE Resolution
            self.stdout.write(self.style.MIGRATE_HEADING("--- Step 3: CAGE Resolution ---"))
            resolver = CAGEResolver(client, stdout=self._make_stdout())
            resolve_job = resolver.safe_run(batch_size=batch_size)
            self._print_job_summary(resolve_job)
            self.stdout.write("")

        if sam_client:
            sam_client.session.close()

        # Step 4: Opportunity Analysis
        self.stdout.write(self.style.MIGRATE_HEADING("--- Step 4: Opportunity Analysis ---"))
        analyzer = OpportunityAnalyzer(stdout=self._make_stdout())
        analyze_job = analyzer.analyze()
        self._print_job_summary(analyze_job)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Pipeline complete."))

    def handle_foia(self, options):
        file = options.get("file")
        limit = options.get("limit")
        self.stdout.write(self.style.MIGRATE_HEADING("=== FOIA Transaction Import ==="))
        if file:
            self.stdout.write(f"File: {file}")
        if limit:
            self.stdout.write(f"Limit: {limit} rows")

        importer = FOIAImporter(stdout=self._make_stdout())
        job = importer.safe_run(file=file, limit=limit)
        self._print_job_summary(job)

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

    def handle_flis_chars(self, options):
        batch_size = options.get("batch_size", 10000)
        self.stdout.write(self.style.MIGRATE_HEADING("=== FLISV Characteristics Enrichment ==="))

        importer = FLISVImporter(stdout=self._make_stdout())
        job = importer.safe_run(batch_size=batch_size)
        self._print_job_summary(job)

    def handle_publog(self, options):
        import gc
        import time
        import resource

        batch_size = options.get("batch_size", 5000)
        rounds = options.get("rounds", 1)
        skip_suppliers = options.get("skip_suppliers", False)
        cooldown = options.get("cooldown", 30)
        skip_rows = options.get("skip_rows", 0)
        unlimited = rounds == 0

        self.stdout.write(self.style.MIGRATE_HEADING("=== PUB LOG Product Catalog Import ==="))
        self.stdout.write(f"Batch size: {batch_size:,} NIINs per round")
        self.stdout.write(f"Rounds: {'unlimited (until done)' if unlimited else rounds}")
        if skip_rows:
            self.stdout.write(f"Skipping first {skip_rows:,} MANAGEMENT rows")
        if skip_suppliers:
            self.stdout.write("Mode: skip suppliers (fast test)")

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

            # Log memory usage
            mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            self.stdout.write(f"Memory (RSS peak): {mem_mb:.0f} MB")

            importer = PUBLOGImporter(stdout=self._make_stdout())
            job = importer.safe_run(limit=batch_size, skip_suppliers=skip_suppliers, skip_rows=mgmt_offset)
            mgmt_offset = importer.last_mgmt_row
            self._print_job_summary(job)

            total_created += job.records_created
            total_errored += job.records_errored

            # If nothing was created this round, all eligible NIINs are imported
            if job.records_created == 0:
                self.stdout.write(self.style.SUCCESS(
                    "\nAll eligible NIINs have been imported!"
                ))
                break

            # Clean up before next round
            del importer
            gc.collect()

            # Don't sleep after the last round
            if not unlimited and round_num >= rounds:
                break

            self.stdout.write(f"Cooling down {cooldown}s before next round...")
            time.sleep(cooldown)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"=== PUBLOG Import Complete: {round_num} rounds, "
            f"{total_created:,} created, {total_errored:,} errored ==="
        ))

    def handle_flis_link(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("=== FLIS Cross-Reference Link ==="))
        self._run_flis_link()

    def _run_flis_link(self):
        """Cross-reference: link transactions to catalog and update counts."""
        from catalog.constants import JobType, JobStatus
        from catalog.models import (
            CatalogItem, Manufacturer, ImportJob,
        )
        from catalog.models.transactions import PurchaseTransaction
        from catalog.models.catalog import SupplierLink

        job = ImportJob.objects.create(
            job_type=JobType.FLIS_LINK,
            parameters={},
        )
        job.mark_running()

        try:
            updated = 0

            # 1. Link PurchaseTransaction.catalog_item where NSN exists in catalog
            self.stdout.write("  Linking transactions to catalog...")
            unlinked = PurchaseTransaction.objects.filter(
                catalog_item__isnull=True,
            ).exclude(nsn="")

            nsn_map = dict(CatalogItem.objects.values_list("nsn", "pk"))

            batch = []
            for txn in unlinked.iterator(chunk_size=5000):
                pk = nsn_map.get(txn.nsn)
                if pk:
                    txn.catalog_item_id = pk
                    batch.append(txn)

                if len(batch) >= 5000:
                    PurchaseTransaction.objects.bulk_update(batch, ["catalog_item_id"], batch_size=5000)
                    updated += len(batch)
                    batch.clear()

            if batch:
                PurchaseTransaction.objects.bulk_update(batch, ["catalog_item_id"], batch_size=5000)
                updated += len(batch)
                batch.clear()

            self.stdout.write(f"  Linked {updated} transactions to catalog")

            # 2. Link PurchaseTransaction.manufacturer by name matching
            self.stdout.write("  Linking transactions to manufacturers...")
            mfr_name_map = {}
            for m in Manufacturer.objects.all().only("pk", "company_name"):
                if m.company_name:
                    mfr_name_map[m.company_name.strip().upper()] = m.pk

            unlinked_mfr = PurchaseTransaction.objects.filter(
                manufacturer__isnull=True,
            ).exclude(manufacturer_name="")

            mfr_linked = 0
            batch = []
            for txn in unlinked_mfr.iterator(chunk_size=5000):
                pk = mfr_name_map.get(txn.manufacturer_name.strip().upper())
                if pk:
                    txn.manufacturer_id = pk
                    batch.append(txn)

                if len(batch) >= 5000:
                    PurchaseTransaction.objects.bulk_update(batch, ["manufacturer_id"], batch_size=5000)
                    mfr_linked += len(batch)
                    batch.clear()

            if batch:
                PurchaseTransaction.objects.bulk_update(batch, ["manufacturer_id"], batch_size=5000)
                mfr_linked += len(batch)

            self.stdout.write(f"  Linked {mfr_linked} transactions to manufacturers")

            # 3. Update denormalized counts on CatalogItem
            self.stdout.write("  Updating catalog counts...")
            from django.db.models import Count
            items_with_counts = CatalogItem.objects.annotate(
                _supplier_count=Count("supplier_links", distinct=True),
                _award_count=Count("awards", distinct=True),
                _txn_count=Count("transactions", distinct=True),
            )

            count_updated = 0
            batch = []
            for item in items_with_counts.iterator(chunk_size=5000):
                changed = False
                if item.supplier_count != item._supplier_count:
                    item.supplier_count = item._supplier_count
                    changed = True
                if item.award_count != (item._award_count + item._txn_count):
                    item.award_count = item._award_count + item._txn_count
                    changed = True
                if changed:
                    batch.append(item)

                if len(batch) >= 5000:
                    CatalogItem.objects.bulk_update(batch, ["supplier_count", "award_count"], batch_size=5000)
                    count_updated += len(batch)
                    batch.clear()

            if batch:
                CatalogItem.objects.bulk_update(batch, ["supplier_count", "award_count"], batch_size=5000)
                count_updated += len(batch)

            self.stdout.write(f"  Updated counts for {count_updated} catalog entries")

            job.records_updated = updated + mfr_linked + count_updated
            job.save(update_fields=["records_updated"])
            job.mark_completed()

            self.stdout.write(self.style.SUCCESS("Cross-reference complete."))
            self._print_job_summary(job)

        except Exception as e:
            job.mark_failed(str(e))
            self.stdout.write(self.style.ERROR(f"Cross-reference failed: {e}"))
            raise

    def handle_status(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("=== Catalog Status ==="))

        # Monthly API usage
        try:
            with HigherGovClient() as client:
                usage = client.get_monthly_usage()
                if usage >= 0:
                    self.stdout.write(f"Monthly API records used: {usage:,}/10,000")
                else:
                    self.stdout.write("Monthly API usage: unavailable (Redis not connected)")
        except Exception:
            self.stdout.write("API client not configured or unavailable.")

        self.stdout.write("")

        # Last jobs by type
        self.stdout.write("Recent jobs:")
        for job in ImportJob.objects.order_by("-created_at")[:10]:
            duration = ""
            if job.duration:
                duration = f" ({job.duration.total_seconds():.1f}s)"
            self.stdout.write(
                f"  {job.get_job_type_display():25s} | {job.get_status_display():10s} | "
                f"Created: {job.records_created}, Updated: {job.records_updated}, "
                f"Errored: {job.records_errored} | "
                f"API calls: {job.api_calls_made}{duration}"
            )

        self.stdout.write("")

        # Record counts — per source
        from catalog.models import (
            Opportunity, CatalogItem, Manufacturer, MarketOpportunity,
            SupplierLink, AwardHistory, PurchaseTransaction, Product,
            CatalogSpecifications,
        )

        self.stdout.write("Record counts:")
        total_opps = Opportunity.objects.count()
        self.stdout.write(f"  Opportunities (total): {total_opps:,}")
        for st in SourceType:
            count = Opportunity.objects.filter(source_type=st.value).count()
            self.stdout.write(f"    -> {st.label}: {count:,}")
        self.stdout.write(f"  Catalog Items:        {CatalogItem.objects.count():,}")
        self.stdout.write(f"  Products:             {Product.objects.count():,}")
        self.stdout.write(f"  Supplier Links:       {SupplierLink.objects.count():,}")
        self.stdout.write(f"  Award Histories:      {AwardHistory.objects.count():,}")
        self.stdout.write(f"  Purchase Transactions: {PurchaseTransaction.objects.count():,}")
        self.stdout.write(f"  Manufacturers:        {Manufacturer.objects.count():,}")
        self.stdout.write(f"  Market Opportunities: {MarketOpportunity.objects.count():,}")

        reseller_count = MarketOpportunity.objects.filter(has_reseller_wins=True).count()
        self.stdout.write(f"    -> with reseller wins: {reseller_count:,}")

        enriched = CatalogSpecifications.objects.count()
        self.stdout.write(f"  Catalog items with specs: {enriched:,}")

    def _print_job_summary(self, job):
        job.refresh_from_db()
        status_style = self.style.SUCCESS if job.status == JobStatus.COMPLETED else self.style.ERROR
        self.stdout.write(
            f"Job #{job.pk} [{status_style(job.get_status_display())}] — "
            f"Fetched: {job.records_fetched}, Created: {job.records_created}, "
            f"Updated: {job.records_updated}, Errored: {job.records_errored}, "
            f"API calls: {job.api_calls_made}"
        )
        if job.duration:
            self.stdout.write(f"Duration: {job.duration.total_seconds():.1f}s")
        if job.error_message:
            self.stdout.write(self.style.ERROR(f"Error: {job.error_message[:200]}"))
