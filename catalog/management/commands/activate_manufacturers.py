"""
Gradually activate NEUTRAL manufacturers to publish their products.

Usage:
    python manage.py activate_manufacturers --stats
    python manage.py activate_manufacturers --batch 10          # dry-run (default)
    python manage.py activate_manufacturers --batch 10 --execute
    python manage.py activate_manufacturers --all --execute
    python manage.py activate_manufacturers --strategy volume   # old ranking
"""

import math

from django.core.management.base import BaseCommand
from django.db.models import Count, Q, Subquery, OuterRef

from catalog.models.catalog import Product, ProductSpecification
from catalog.models.entities import Manufacturer, ManufacturerProfile


class Command(BaseCommand):
    help = "Activate top NEUTRAL manufacturers by niche-first or volume scoring"

    # FSC-count → (label, multiplier)
    _SPECIALIZATION_TIERS = [
        (0,  "unknown",     0.5),
        (1,  "specialist",  3.0),
        (3,  "focused",     2.5),
        (6,  "generalist",  1.5),
        (9,  "generalist",  1.0),
    ]
    _DISTRIBUTOR = ("distributor", 0.6)

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true", default=True,
            help="Preview which manufacturers would be activated (default)",
        )
        parser.add_argument(
            "--execute", action="store_true",
            help="Actually activate manufacturers",
        )
        parser.add_argument(
            "--batch", type=int, default=10,
            help="Enable top N manufacturers by ranking score (default: 10)",
        )
        parser.add_argument(
            "--all", action="store_true",
            help="Enable all NEUTRAL manufacturers at once",
        )
        parser.add_argument(
            "--stats", action="store_true",
            help="Show current activation status and exit",
        )
        parser.add_argument(
            "--strategy", choices=["niche", "volume"], default="niche",
            help="Ranking strategy: niche (default) or volume (legacy)",
        )

    def handle(self, **options):
        if options["stats"]:
            self._show_stats()
            return

        execute = options["execute"]
        batch_all = options["all"]
        batch_size = options["batch"]
        strategy = options["strategy"]

        # Current counts
        enabled_count = ManufacturerProfile.objects.filter(
            status=Manufacturer.ENABLED
        ).count()
        published_count = Product.objects.published().count()

        self.stdout.write(
            f"\nCurrent: {enabled_count:,} enabled manufacturers, "
            f"{published_count:,} published products\n"
        )

        # Build ranked list of NEUTRAL manufacturers with products
        candidates = self._ranked_candidates(strategy)

        if not candidates:
            self.stdout.write("No NEUTRAL manufacturers with products to activate.")
            return

        if not batch_all:
            candidates = candidates[:batch_size]

        label = "all" if batch_all else f"top {batch_size}"
        self.stdout.write(
            f"Batch: {label} by activation score (strategy: {strategy})\n"
        )

        if strategy == "niche":
            self.stdout.write(
                f"  {'#':>3}  {'Name':<40} {'CAGE':<6} {'Type':<12} "
                f"{'FSCs':>4}  {'Products':>8}  {'Specs%':>6}  {'Score':>8}\n"
            )
            self.stdout.write(
                f"  {'—' * 3}  {'—' * 40} {'—' * 5}  {'—' * 12} "
                f"{'—' * 4}  {'—' * 8}  {'—' * 6}  {'—' * 8}\n"
            )
        else:
            self.stdout.write(
                f"  {'#':>3}  {'Name':<40} {'CAGE':<6} {'Products':>8}  "
                f"{'Specs%':>6}  {'Score':>10}\n"
            )
            self.stdout.write(
                f"  {'—' * 3}  {'—' * 40} {'—' * 5}  {'—' * 8}  "
                f"{'—' * 6}  {'—' * 10}\n"
            )

        total_new_products = 0
        for i, c in enumerate(candidates, 1):
            if strategy == "niche":
                self.stdout.write(
                    f"  {i:>3}  {c['name']:<40} {c['cage']:<6} {c['type']:<12} "
                    f"{c['fsc_count']:>4}  {c['product_count']:>8,}  "
                    f"{c['spec_pct']:>5.1f}%  {c['score']:>8.1f}\n"
                )
            else:
                self.stdout.write(
                    f"  {i:>3}  {c['name']:<40} {c['cage']:<6} "
                    f"{c['product_count']:>8,}  "
                    f"{c['spec_pct']:>5.1f}%  {c['score']:>10,.0f}\n"
                )
            total_new_products += c["product_count"]

        new_enabled = enabled_count + len(candidates)
        new_published = published_count + total_new_products
        self.stdout.write(
            f"\nAfter activation: {new_enabled:,} enabled manufacturers, "
            f"~{new_published:,} published products\n"
        )

        if not execute:
            self.stdout.write(
                self.style.WARNING(
                    "\nDry run — no changes made. Use --execute to apply.\n"
                )
            )
            return

        # Execute activation
        profile_ids = [c["profile_id"] for c in candidates]
        updated = ManufacturerProfile.objects.filter(id__in=profile_ids).update(
            status=Manufacturer.ENABLED
        )

        actual_published = Product.objects.published().count()
        self.stdout.write(
            self.style.SUCCESS(
                f"\nActivated {updated:,} manufacturers. "
                f"Published products: {published_count:,} → {actual_published:,} "
                f"(+{actual_published - published_count:,})\n"
            )
        )

    def _classify(self, fsc_count):
        """Return (label, multiplier) for a given FSC count."""
        for threshold, label, mult in self._SPECIALIZATION_TIERS:
            if fsc_count <= threshold:
                return label, mult
        return self._DISTRIBUTOR

    def _show_stats(self):
        enabled = ManufacturerProfile.objects.filter(
            status=Manufacturer.ENABLED
        ).count()
        neutral = ManufacturerProfile.objects.filter(
            status=Manufacturer.NEUTRAL
        ).count()
        disabled = ManufacturerProfile.objects.filter(
            status=Manufacturer.DISABLED
        ).count()
        published = Product.objects.published().count()
        total = Product.objects.count()

        self.stdout.write(f"\nManufacturer status:")
        self.stdout.write(f"  ENABLED:  {enabled:>6,}")
        self.stdout.write(f"  NEUTRAL:  {neutral:>6,}")
        self.stdout.write(f"  DISABLED: {disabled:>6,}")
        self.stdout.write(f"\nProducts:")
        self.stdout.write(
            f"  Published: {published:>8,} / {total:,} "
            f"({published / total * 100:.1f}%)\n"
        )

        # Classification breakdown of NEUTRAL manufacturers with products
        fsc_sub = (
            Product.objects.filter(
                manufacturer=OuterRef("organization"),
                is_active=Manufacturer.NEUTRAL,
                fsc__isnull=False,
            )
            .values("manufacturer")
            .annotate(cnt=Count("fsc", distinct=True))
            .values("cnt")
        )

        profiles = (
            ManufacturerProfile.objects.filter(status=Manufacturer.NEUTRAL)
            .annotate(
                product_count=Count(
                    "organization__products",
                    filter=Q(
                        organization__products__is_active=Manufacturer.NEUTRAL
                    ),
                ),
                fsc_count=Subquery(fsc_sub),
            )
            .filter(product_count__gt=0)
        )

        buckets = {}
        for p in profiles:
            fc = p.fsc_count or 0
            label, _ = self._classify(fc)
            if label not in buckets:
                buckets[label] = {"count": 0, "products": 0}
            buckets[label]["count"] += 1
            buckets[label]["products"] += p.product_count

        self.stdout.write(f"\nNEUTRAL classification (with products):")
        order = ["specialist", "focused", "generalist", "unknown", "distributor"]
        for label in order:
            if label in buckets:
                b = buckets[label]
                avg = b["products"] / b["count"] if b["count"] else 0
                self.stdout.write(
                    f"  {label:<12} {b['count']:>6,} manufacturers, "
                    f"{b['products']:>8,} products (avg {avg:.0f})"
                )
        self.stdout.write("")

    def _ranked_candidates(self, strategy="niche"):
        """Return NEUTRAL manufacturers ranked by the chosen strategy."""
        # Subquery: count of products with ≥1 spec for each manufacturer
        specs_sub = (
            Product.objects.filter(
                manufacturer=OuterRef("organization"),
                is_active=Manufacturer.NEUTRAL,
            )
            .filter(specs__isnull=False)
            .values("manufacturer")
            .annotate(cnt=Count("id", distinct=True))
            .values("cnt")
        )

        annotations = {
            "product_count": Count(
                "organization__products",
                filter=Q(
                    organization__products__is_active=Manufacturer.NEUTRAL
                ),
            ),
            "specs_count": Subquery(specs_sub),
        }

        if strategy == "niche":
            # Subquery: distinct FSC count per manufacturer (non-null FSCs only)
            fsc_sub = (
                Product.objects.filter(
                    manufacturer=OuterRef("organization"),
                    is_active=Manufacturer.NEUTRAL,
                    fsc__isnull=False,
                )
                .values("manufacturer")
                .annotate(cnt=Count("fsc", distinct=True))
                .values("cnt")
            )
            annotations["fsc_count"] = Subquery(fsc_sub)

        profiles = (
            ManufacturerProfile.objects.filter(status=Manufacturer.NEUTRAL)
            .annotate(**annotations)
            .filter(product_count__gt=0)
            .select_related("organization")
            .order_by("-product_count")
        )

        results = []
        for p in profiles:
            pc = p.product_count
            sc = p.specs_count or 0
            spec_coverage = sc / pc if pc else 0
            spec_pct = spec_coverage * 100
            name = (
                p.display_name or p.organization.company_name or "(unnamed)"
            )

            if strategy == "niche":
                fc = p.fsc_count or 0
                label, multiplier = self._classify(fc)
                score = multiplier * spec_coverage * math.log2(pc + 1)
            else:
                fc = 0
                label = ""
                score = pc * spec_coverage

            results.append({
                "profile_id": p.id,
                "name": name[:40],
                "cage": p.organization.cage_code or "",
                "product_count": pc,
                "specs_count": sc,
                "spec_pct": spec_pct,
                "score": score,
                "fsc_count": fc,
                "type": label,
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results
