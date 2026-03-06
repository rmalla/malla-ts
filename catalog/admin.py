from django.contrib import admin, messages
from django.db.models import Count
from django.http import JsonResponse
from django.urls import path, reverse
from django.utils.html import format_html

from .constants import FilterFieldType, FilterAction, PipelineStage
from .models import (
    Manufacturer,
    ManufacturerProfile,
    PipelineFilter,
    ImportJob,
    ImportJobLog,
    Product,
    ProductSpecification,
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


class ProductSpecificationInline(admin.TabularInline):
    model = ProductSpecification
    extra = 0
    readonly_fields = ("group", "label", "value", "sort_order")

    def has_add_permission(self, request, obj=None):
        return False


class ManufacturerProfileInline(admin.StackedInline):
    model = ManufacturerProfile
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

class ProfileStatusFilter(admin.SimpleListFilter):
    title = "profile status"
    parameter_name = "profile_status"

    def lookups(self, request, model_admin):
        return [
            ("1", "Enabled"),
            ("0", "Neutral"),
            ("-1", "Disabled"),
            ("none", "No profile"),
        ]

    def queryset(self, request, queryset):
        val = self.value()
        if val is None:
            return queryset
        if val == "none":
            return queryset.filter(profile__isnull=True)
        return queryset.filter(profile__status=int(val))


@admin.register(Manufacturer)
class ManufacturerAdmin(admin.ModelAdmin):
    change_list_template = "admin/catalog/manufacturer/change_list.html"
    list_display = (
        "display_name_col", "product_count_col", "cage_code", "slug",
        "website",
        "city", "state", "country",
        "is_manufacturer", "is_awardee",
        "status_toggle",
    )
    list_filter = (
        ProfileStatusFilter,
        "is_manufacturer", "is_awardee", "is_distributor", "country", "resolution_status",
    )
    list_select_related = ("profile",)
    search_fields = ("cage_code", "company_name", "slug", "uei")
    ordering = ("company_name",)
    list_per_page = 50
    inlines = [ManufacturerProfileInline]

    class Media:
        css = {"all": ("catalog/css/toggle.css",)}
        js = ("catalog/js/toggle.js",)

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

    def status_toggle(self, obj):
        try:
            val = obj.profile.status
        except ManufacturerProfile.DoesNotExist:
            val = 0
        return format_html(
            '<div class="tri-toggle" data-val="{val}" data-pk="{pk}">'
            '<input type="hidden" name="_status_{pk}" value="{val}">'
            '<div class="tri-toggle__track">'
            '<div class="tri-toggle__seg">&#x2212;</div>'
            '<div class="tri-toggle__seg">&#x25CF;</div>'
            '<div class="tri-toggle__seg">&#x2713;</div>'
            '<div class="tri-toggle__thumb"></div>'
            '</div></div>',
            val=val, pk=obj.pk,
        )
    status_toggle.short_description = "Status"

    def profile_display_name(self, obj):
        try:
            return obj.profile.display_name or "(not set)"
        except ManufacturerProfile.DoesNotExist:
            return "(no profile)"
    profile_display_name.short_description = "Display Name"

    def get_urls(self):
        custom_urls = [
            path(
                "set-status/<int:pk>/",
                self.admin_site.admin_view(self.set_status_view),
                name="catalog_manufacturer_set_status",
            ),
            path(
                "apply-name-filters/",
                self.admin_site.admin_view(self.apply_name_filters_view),
                name="catalog_manufacturer_apply_name_filters",
            ),
            path(
                "purge-filtered/",
                self.admin_site.admin_view(self.purge_filtered_view),
                name="catalog_manufacturer_purge_filtered",
            ),
        ]
        return custom_urls + super().get_urls()

    def set_status_view(self, request, pk):
        if request.method != "POST":
            return JsonResponse({"ok": False, "error": "POST required"}, status=405)

        try:
            status = int(request.POST.get("status", ""))
        except (ValueError, TypeError):
            return JsonResponse({"ok": False, "error": "Invalid status"}, status=400)

        if status not in (-1, 0, 1):
            return JsonResponse({"ok": False, "error": "Status must be -1, 0, or 1"}, status=400)

        try:
            org = Manufacturer.objects.get(pk=pk)
        except Manufacturer.DoesNotExist:
            return JsonResponse({"ok": False, "error": "Not found"}, status=404)

        old_status = None
        try:
            old_status = org.profile.status
        except ManufacturerProfile.DoesNotExist:
            pass

        ManufacturerProfile.objects.update_or_create(
            organization=org,
            defaults={"status": status},
        )

        if org.cage_code:
            if status == Manufacturer.DISABLED:
                pf, created = PipelineFilter.objects.get_or_create(
                    field_type=FilterFieldType.CAGE_CODE,
                    field_value=org.cage_code,
                    stage=PipelineStage.ALL,
                    defaults={
                        "action": FilterAction.EXCLUDE,
                        "is_active": True,
                        "reason": f"Auto-disabled via admin toggle for {org.company_name}",
                        "created_by": request.user,
                    },
                )
                if not created and not pf.is_active:
                    pf.is_active = True
                    pf.reason = f"Re-disabled via admin toggle for {org.company_name}"
                    pf.save(update_fields=["is_active", "reason", "updated_at"])
            elif old_status == Manufacturer.DISABLED:
                PipelineFilter.objects.filter(
                    field_type=FilterFieldType.CAGE_CODE,
                    field_value=org.cage_code,
                    stage=PipelineStage.ALL,
                    is_active=True,
                ).update(is_active=False)

        return JsonResponse({"ok": True})

    def apply_name_filters_view(self, request):
        from django.db.models import Q
        from django.http import HttpResponseRedirect, HttpResponseNotAllowed

        if request.method != "POST":
            return HttpResponseNotAllowed(["POST"])

        # 1. Disable nameless manufacturers
        nameless_pks = list(
            Manufacturer.objects.filter(company_name="").values_list("pk", flat=True)
        )
        nameless_count = 0
        if nameless_pks:
            nameless_count = ManufacturerProfile.objects.filter(
                organization_id__in=nameless_pks
            ).update(status=Manufacturer.DISABLED)
            # Create profiles for nameless orgs that don't have one
            existing = set(
                ManufacturerProfile.objects.filter(
                    organization_id__in=nameless_pks
                ).values_list("organization_id", flat=True)
            )
            missing = [pk for pk in nameless_pks if pk not in existing]
            if missing:
                ManufacturerProfile.objects.bulk_create(
                    [ManufacturerProfile(organization_id=pk, status=Manufacturer.DISABLED) for pk in missing],
                    ignore_conflicts=True,
                )
                nameless_count += len(missing)

        # 2. Build a single Q filter from all active name rules
        rules = PipelineFilter.objects.filter(
            is_active=True,
            field_type=FilterFieldType.MANUFACTURER_NAME,
            stage__in=[PipelineStage.ALL, PipelineStage.NSN_ENRICH],
        )

        name_count = 0
        if rules.exists():
            q = Q()
            for rule in rules:
                q |= Q(company_name__icontains=rule.field_value)

            matched_pks = list(
                Manufacturer.objects.exclude(company_name="")
                .filter(q)
                .values_list("pk", flat=True)
            )

            if matched_pks:
                # Update existing profiles
                name_count = ManufacturerProfile.objects.filter(
                    organization_id__in=matched_pks
                ).exclude(status=Manufacturer.DISABLED).update(status=Manufacturer.DISABLED)

                # Create profiles for orgs that don't have one
                existing = set(
                    ManufacturerProfile.objects.filter(
                        organization_id__in=matched_pks
                    ).values_list("organization_id", flat=True)
                )
                missing = [pk for pk in matched_pks if pk not in existing]
                if missing:
                    ManufacturerProfile.objects.bulk_create(
                        [ManufacturerProfile(organization_id=pk, status=Manufacturer.DISABLED) for pk in missing],
                        ignore_conflicts=True,
                    )
                    name_count += len(missing)

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
        return HttpResponseRedirect(reverse("admin:catalog_manufacturer_changelist"))

    def purge_filtered_view(self, request):
        from django.db.models import Q
        from django.http import HttpResponseRedirect, HttpResponseNotAllowed

        if request.method != "POST":
            return HttpResponseNotAllowed(["POST"])

        rules = PipelineFilter.objects.filter(
            is_active=True,
            field_type=FilterFieldType.MANUFACTURER_NAME,
        )
        if not rules.exists():
            self.message_user(request, "No active manufacturer name filters found.", messages.INFO)
            return HttpResponseRedirect(reverse("admin:catalog_manufacturer_changelist"))

        q = Q()
        for rule in rules:
            q |= Q(company_name__icontains=rule.field_value)

        matched_qs = Manufacturer.objects.filter(q)
        match_count = matched_qs.count()

        if not match_count:
            self.message_user(request, "No matching organizations to purge.", messages.INFO)
            return HttpResponseRedirect(reverse("admin:catalog_manufacturer_changelist"))

        deleted_count, deleted_detail = matched_qs.delete()

        parts = [f"{model}: {count}" for model, count in deleted_detail.items() if count]
        self.message_user(
            request,
            f"Purged {match_count} org(s) ({deleted_count} objects total: {', '.join(parts)}).",
            messages.SUCCESS,
        )
        return HttpResponseRedirect(reverse("admin:catalog_manufacturer_changelist"))

    def get_queryset(self, request):
        return super().get_queryset(request).annotate(product_count=Count("products"))

    def display_name_col(self, obj):
        return obj.display_name
    display_name_col.short_description = "Name"
    display_name_col.admin_order_field = "company_name"

    def product_count_col(self, obj):
        count = obj.product_count
        url = reverse("admin:catalog_product_changelist") + f"?manufacturer__id__exact={obj.pk}"
        return format_html('<a href="{}">{}</a>', url, count)
    product_count_col.short_description = "Products"
    product_count_col.admin_order_field = "product_count"


# =============================================================================
# Products
# =============================================================================

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = (
        "display_name", "nsn", "manufacturer_display",
        "part_number", "price_display", "source", "is_active", "google_search_link", "view_on_site_link",
    )
    list_filter = ("source", "is_active", "fsc")
    search_fields = (
        "nsn", "nomenclature",
        "part_number", "name",
        "manufacturer__company_name", "manufacturer__cage_code",
    )
    raw_id_fields = ("manufacturer", "fsc")
    ordering = ("-created_at",)
    list_per_page = 50
    list_select_related = ("manufacturer", "manufacturer__profile")
    inlines = [ProductSpecificationInline]

    fieldsets = (
        ("Product Info", {
            "fields": (
                "name", "description", "part_number",
                "nsn", "nomenclature", "price", "fsc", "unit_of_issue",
                "source", "is_active",
            ),
        }),
        ("Links", {
            "fields": ("manufacturer",),
        }),
    )

    def price_display(self, obj):
        if obj.price:
            return f"${obj.price:,.2f}"
        return "--"
    price_display.short_description = "Price"

    def manufacturer_display(self, obj):
        name = obj.manufacturer.display_name
        url = reverse("admin:catalog_manufacturer_change", args=[obj.manufacturer.pk])
        return format_html('<a href="{}">{}</a>', url, name)
    manufacturer_display.short_description = "Manufacturer"

    def google_search_link(self, obj):
        from urllib.parse import quote
        query = f"{obj.manufacturer.display_name} {obj.part_number}"
        url = f"https://www.google.com/search?q={quote(query)}"
        return format_html('<a href="{}" target="_blank">Search</a>', url)
    google_search_link.short_description = "Google"

    def view_on_site_link(self, obj):
        url = f"/products/{obj.manufacturer.slug}/{obj.part_number_slug}/"
        return format_html('<a href="{}" target="_blank">View</a>', url)
    view_on_site_link.short_description = "Public Page"
