from django.contrib import admin, messages
from django.urls import path, reverse
from django.utils.html import format_html

from .models import (
    Organization,
    OrganizationProfile,
    DistributorStats,
    PipelineFilter,
    ImportJob,
    ImportJobLog,
    CatalogItem,
    CatalogPricing,
    CatalogSpecifications,
    Product,
    ProductSpecification,
    SupplierLink,
    AwardHistory,
    Opportunity,
    MarketOpportunity,
    PurchaseTransaction,
    DataProvenance,
)


# =============================================================================
# Inlines
# =============================================================================

class ImportJobLogInline(admin.TabularInline):
    model = ImportJobLog
    extra = 0
    readonly_fields = ("level", "message", "context", "created_at")
    ordering = ("created_at",)

    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class SupplierLinkInline(admin.TabularInline):
    model = SupplierLink
    extra = 0
    raw_id_fields = ("organization",)
    readonly_fields = ("organization", "part_number", "source", "created_at")

    def has_add_permission(self, request, obj=None):
        return False


class AwardHistoryInline(admin.TabularInline):
    model = AwardHistory
    extra = 0
    raw_id_fields = ("awardee",)
    readonly_fields = (
        "awardee", "contract_number", "quantity", "unit_cost",
        "award_date", "surplus", "part_number", "created_at",
    )

    def has_add_permission(self, request, obj=None):
        return False


class CatalogPricingInline(admin.StackedInline):
    model = CatalogPricing
    extra = 0
    max_num = 1

    def has_add_permission(self, request, obj=None):
        return obj is not None and not hasattr(obj, 'pricing')


class CatalogSpecificationsInline(admin.StackedInline):
    model = CatalogSpecifications
    extra = 0
    max_num = 1
    fieldsets = (
        ("Tier 1 Fields", {
            "fields": (
                "material", "overall_length", "overall_width", "overall_height",
                "overall_diameter", "weight", "color", "end_item_identification",
                "special_features",
            ),
        }),
        ("Full Data", {
            "fields": ("specifications_json", "characteristics_json", "source"),
            "classes": ("collapse",),
        }),
    )


class ProductSpecificationInline(admin.TabularInline):
    model = ProductSpecification
    extra = 0
    readonly_fields = ("group", "label", "value", "sort_order")

    def has_add_permission(self, request, obj=None):
        return False


class OrganizationProfileInline(admin.StackedInline):
    model = OrganizationProfile
    extra = 0
    max_num = 1
    raw_id_fields = ("logo",)


# =============================================================================
# Pipeline Filters
# =============================================================================

@admin.register(PipelineFilter)
class PipelineFilterAdmin(admin.ModelAdmin):
    list_display = (
        "field_type", "field_value", "action", "stage",
        "is_active", "reason", "created_by", "updated_at",
    )
    list_filter = ("field_type", "stage", "is_active")
    list_editable = ("is_active",)
    search_fields = ("field_value", "reason")
    ordering = ("field_type", "field_value")
    list_per_page = 50
    readonly_fields = ("created_at", "updated_at")

    fieldsets = (
        (None, {
            "fields": (
                "field_type", "field_value", "action", "stage",
                "is_active", "reason",
            ),
        }),
        ("Audit", {
            "fields": ("created_by", "created_at", "updated_at"),
        }),
    )

    def save_model(self, request, obj, form, change):
        if not change:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)


# =============================================================================
# Import Jobs
# =============================================================================

@admin.register(ImportJob)
class ImportJobAdmin(admin.ModelAdmin):
    list_display = (
        "id", "job_type", "status", "duration_display",
        "records_fetched", "records_created", "records_updated",
        "records_errored", "records_filtered", "api_calls_made", "created_at",
    )
    list_filter = ("job_type", "status")
    readonly_fields = (
        "job_type", "status", "started_at", "completed_at",
        "records_fetched", "records_created", "records_updated",
        "records_errored", "records_filtered", "api_calls_made", "parameters",
        "error_message", "created_at",
    )
    ordering = ("-created_at",)
    list_per_page = 25
    inlines = [ImportJobLogInline]

    def duration_display(self, obj):
        d = obj.duration
        if d:
            return f"{d.total_seconds():.1f}s"
        return "--"
    duration_display.short_description = "Duration"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


# =============================================================================
# Organizations (unified model)
# =============================================================================

@admin.register(Organization)
class OrganizationAdmin(admin.ModelAdmin):
    change_list_template = "admin/catalog/organization/change_list.html"
    list_display = (
        "display_name_col", "cage_code", "slug",
        "city", "state", "country",
        "is_manufacturer", "is_awardee",
    )
    list_filter = ("is_manufacturer", "is_awardee", "is_distributor", "country", "resolution_status")
    search_fields = ("cage_code", "company_name", "slug", "uei")
    ordering = ("company_name",)
    list_per_page = 50
    inlines = [OrganizationProfileInline]

    readonly_fields = ("profile_display_name",)

    fieldsets = (
        ("Identification", {
            "fields": (
                "cage_code", "company_name", "profile_display_name", "slug", "website", "uei",
            ),
        }),
        ("Location", {
            "fields": ("address", "city", "state", "zip_code", "country"),
        }),
        ("Flags", {
            "fields": ("is_manufacturer", "is_distributor", "is_awardee",
                       "resolution_status", "resolution_source", "resolved_from_api"),
        }),
    )

    def profile_display_name(self, obj):
        try:
            return obj.profile.display_name or "(not set)"
        except OrganizationProfile.DoesNotExist:
            return "(no profile)"
    profile_display_name.short_description = "Display Name"

    def get_urls(self):
        custom_urls = [
            path(
                "apply-name-filters/",
                self.admin_site.admin_view(self.apply_name_filters_view),
                name="catalog_organization_apply_name_filters",
            ),
        ]
        return custom_urls + super().get_urls()

    def apply_name_filters_view(self, request):
        from django.http import HttpResponseRedirect, HttpResponseNotAllowed
        from catalog.constants import FilterFieldType, PipelineStage
        from catalog.models import PipelineFilter

        if request.method != "POST":
            return HttpResponseNotAllowed(["POST"])

        # Disable nameless organizations
        nameless = Organization.objects.filter(company_name="")
        nameless_pks = list(nameless.values_list("pk", flat=True))
        nameless_count = len(nameless_pks)
        if nameless_pks:
            OrganizationProfile.objects.filter(
                organization_id__in=nameless_pks
            ).update(status=Organization.DISABLED)

        # Disable organizations matching name filters
        rules = PipelineFilter.objects.filter(
            is_active=True,
            field_type=FilterFieldType.MANUFACTURER_NAME,
            stage__in=[PipelineStage.ALL, PipelineStage.NSN_ENRICH],
        )

        name_count = 0
        if rules.exists():
            qs = Organization.objects.exclude(company_name="")
            for org in qs.iterator():
                for rule in rules:
                    if rule.matches(org.company_name):
                        OrganizationProfile.objects.update_or_create(
                            organization=org,
                            defaults={"status": Organization.DISABLED},
                        )
                        name_count += 1
                        break

        total = nameless_count + name_count
        if total:
            parts = []
            if nameless_count:
                parts.append(f"{nameless_count} with no name")
            if name_count:
                parts.append(f"{name_count} matching name filters")
            self.message_user(request, f"Disabled {total} org(s): {', '.join(parts)}.", messages.SUCCESS)
        else:
            self.message_user(request, "No new organizations to disable.", messages.INFO)
        return HttpResponseRedirect(reverse("admin:catalog_organization_changelist"))

    def display_name_col(self, obj):
        return obj.display_name
    display_name_col.short_description = "Name"
    display_name_col.admin_order_field = "company_name"


# =============================================================================
# Distributor Stats
# =============================================================================

@admin.register(DistributorStats)
class DistributorStatsAdmin(admin.ModelAdmin):
    list_display = (
        "org_display", "award_count", "total_award_value", "nsn_count", "is_active",
    )
    list_filter = ("is_active",)
    search_fields = ("organization__company_name", "organization__cage_code")
    raw_id_fields = ("organization",)
    ordering = ("-total_award_value",)
    list_per_page = 50

    def org_display(self, obj):
        return f"{obj.organization.cage_code or '--'} - {obj.organization.company_name}"
    org_display.short_description = "Organization"
    org_display.admin_order_field = "organization__company_name"


# =============================================================================
# Catalog Items
# =============================================================================

@admin.register(CatalogItem)
class CatalogItemAdmin(admin.ModelAdmin):
    list_display = (
        "nsn", "nomenclature", "part_numbers_short", "price_display",
        "distributor_use", "supplier_count", "award_count", "opportunity_count",
    )
    list_filter = ("distributor_use", "fsc")
    search_fields = ("nsn", "nomenclature", "part_numbers")
    raw_id_fields = ("fsc",)
    ordering = ("nsn",)
    list_per_page = 50
    inlines = [CatalogPricingInline, CatalogSpecificationsInline, SupplierLinkInline, AwardHistoryInline]

    fieldsets = (
        ("Identification", {
            "fields": ("nsn", "niin", "nomenclature", "part_numbers", "fsc"),
        }),
        ("Flags & Counts", {
            "fields": (
                "distributor_use", "is_active",
                "supplier_count", "product_count", "award_count", "opportunity_count",
            ),
        }),
        ("Raw Data", {
            "fields": ("raw_api_response",),
            "classes": ("collapse",),
        }),
    )

    def part_numbers_short(self, obj):
        pn = obj.part_numbers
        if len(pn) > 60:
            return pn[:60] + "..."
        return pn
    part_numbers_short.short_description = "Part Numbers"

    def price_display(self, obj):
        try:
            pricing = obj.pricing
            if pricing.unit_price:
                return f"${pricing.unit_price:,.2f}"
        except CatalogPricing.DoesNotExist:
            pass
        return "--"
    price_display.short_description = "Unit Price"


# =============================================================================
# Products
# =============================================================================

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = (
        "display_name", "nsn_display", "manufacturer_display",
        "part_number", "price_display", "source", "is_active", "view_on_site_link",
    )
    list_filter = ("source", "is_active", "catalog_item__fsc")
    search_fields = (
        "catalog_item__nsn", "catalog_item__nomenclature",
        "part_number", "name",
        "manufacturer__company_name", "manufacturer__cage_code",
    )
    raw_id_fields = ("catalog_item", "manufacturer")
    ordering = ("-created_at",)
    list_per_page = 50
    list_select_related = ("catalog_item", "catalog_item__pricing", "manufacturer", "manufacturer__profile")
    inlines = [ProductSpecificationInline]

    fieldsets = (
        ("Product Info", {
            "fields": (
                "name", "description", "part_number", "source", "is_active",
            ),
        }),
        ("Links", {
            "fields": ("catalog_item", "manufacturer"),
        }),
    )

    def nsn_display(self, obj):
        return obj.catalog_item.nsn if obj.catalog_item else "--"
    nsn_display.short_description = "NSN"

    def price_display(self, obj):
        if obj.catalog_item:
            try:
                pricing = obj.catalog_item.pricing
                if pricing.unit_price:
                    return f"${pricing.unit_price:,.2f}"
            except CatalogPricing.DoesNotExist:
                pass
        return "--"
    price_display.short_description = "Price"

    def manufacturer_display(self, obj):
        name = obj.manufacturer.display_name
        url = reverse("admin:catalog_organization_change", args=[obj.manufacturer.pk])
        return format_html('<a href="{}">{}</a>', url, name)
    manufacturer_display.short_description = "Manufacturer"

    def view_on_site_link(self, obj):
        url = f"/products/{obj.manufacturer.slug}/{obj.part_number_slug}/"
        return format_html('<a href="{}" target="_blank">View</a>', url)
    view_on_site_link.short_description = "Public Page"


# =============================================================================
# Purchase Transactions (FOIA data)
# =============================================================================

@admin.register(PurchaseTransaction)
class PurchaseTransactionAdmin(admin.ModelAdmin):
    list_display = (
        "nsn", "item_name_short", "manufacturer_name", "supplier_name",
        "quantity", "unit_price", "extended_price", "transaction_date",
        "department", "agency",
    )
    list_filter = ("department", "source_file")
    search_fields = (
        "nsn", "item_name", "manufacturer_name", "supplier_name",
        "part_number_raw", "manufacturer_part_number",
    )
    raw_id_fields = ("catalog_item", "manufacturer")
    ordering = ("-transaction_date",)
    list_per_page = 50
    date_hierarchy = "transaction_date"

    readonly_fields = (
        "nsn", "catalog_item", "item_name", "manufacturer_name",
        "manufacturer_part_number", "manufacturer", "supplier_name",
        "quantity", "unit_of_measure", "unit_price", "extended_price",
        "transaction_date", "department", "agency", "source_of_supply",
        "part_number_raw", "source_file", "created_at",
    )

    def item_name_short(self, obj):
        name = obj.item_name
        if len(name) > 50:
            return name[:50] + "..."
        return name
    item_name_short.short_description = "Item"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


# =============================================================================
# Opportunities
# =============================================================================

SOURCE_BADGE_COLORS = {
    "dibbs": "#28a745",
    "sam": "#007bff",
    "sled": "#fd7e14",
}


@admin.register(Opportunity)
class OpportunityAdmin(admin.ModelAdmin):
    list_display = (
        "source_badge", "source_id", "title", "nsn",
        "estimated_value", "posted_date", "agency",
    )
    list_filter = ("source_type", "status", "agency")
    search_fields = ("source_id", "opp_key", "title", "nsn")
    list_select_related = ("catalog_item",)
    raw_id_fields = ("catalog_item",)
    ordering = ("-posted_date",)
    list_per_page = 50
    date_hierarchy = "posted_date"

    def source_badge(self, obj):
        color = SOURCE_BADGE_COLORS.get(obj.source_type, "#6c757d")
        label = obj.get_source_type_display()
        return format_html(
            '<span style="background:{}; color:#fff; padding:2px 8px; '
            'border-radius:4px; font-size:11px; font-weight:bold;">{}</span>',
            color, label,
        )
    source_badge.short_description = "Source"
    source_badge.admin_order_field = "source_type"


# =============================================================================
# Market Opportunities
# =============================================================================

@admin.register(MarketOpportunity)
class MarketOpportunityAdmin(admin.ModelAdmin):
    list_display = (
        "nsn_display", "rating", "has_reseller_wins", "distributor_use",
        "has_active_opportunity", "total_reseller_awards", "total_award_value",
        "score_display",
    )
    list_filter = ("rating", "has_reseller_wins", "distributor_use", "has_active_opportunity")
    search_fields = ("catalog_item__nsn", "catalog_item__nomenclature")
    raw_id_fields = ("catalog_item",)
    ordering = ("-total_award_value",)
    list_per_page = 50

    readonly_fields = (
        "catalog_item", "rating", "manufacturer_cage_codes", "reseller_cage_codes",
        "has_reseller_wins", "distributor_use", "has_active_opportunity",
        "total_reseller_awards", "total_award_value",
        "latest_reseller_award_date", "avg_reseller_unit_cost",
        "analysis_notes",
    )

    def nsn_display(self, obj):
        return f"{obj.catalog_item.nsn} -- {obj.catalog_item.nomenclature[:40]}"
    nsn_display.short_description = "NSN"

    def score_display(self, obj):
        score = obj.opportunity_score
        if score >= 70:
            color = "#28a745"
        elif score >= 40:
            color = "#ffc107"
        else:
            color = "#dc3545"
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}/100</span>',
            color, score,
        )
    score_display.short_description = "Score"

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False
