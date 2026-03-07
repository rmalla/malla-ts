from django.contrib import admin, messages
from django.db.models import Count
from django.http import JsonResponse
from django.shortcuts import redirect
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
# Helpers
# =============================================================================

def _prefixed_search(queryset, search_term, prefix_map):
    """Parse 'prefix:value' and filter on a single field. Returns (qs, use_default)."""
    if ":" in search_term:
        prefix, _, value = search_term.partition(":")
        value = value.strip()
        field = prefix_map.get(prefix.strip().lower())
        if field and value:
            return queryset.filter(**{f"{field}__icontains": value}), False
    return queryset, True


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
    readonly_fields = ("logo_preview",)

    def logo_preview(self, obj):
        if obj.logo:
            return format_html(
                '<img src="{}" alt="Logo" style="max-width:120px; max-height:120px; '
                'border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.1); background:#fff;" />',
                obj.logo.file.url,
            )
        return "(no logo)"
    logo_preview.short_description = "Logo Preview"


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


class ProductCountFilter(admin.SimpleListFilter):
    title = "product count"
    parameter_name = "product_count"

    def lookups(self, request, model_admin):
        return [
            ("0", "0"),
            ("1", "1"),
            ("2-5", "2–5"),
            ("6-20", "6–20"),
            ("20-100", "20–100"),
            ("100+", "> 100"),
        ]

    def queryset(self, request, queryset):
        val = self.value()
        if val is None:
            return queryset
        ranges = {
            "0": (0, 0),
            "1": (1, 1),
            "2-5": (2, 5),
            "6-20": (6, 20),
            "20-100": (20, 100),
        }
        if val in ranges:
            lo, hi = ranges[val]
            return queryset.filter(product_count__gte=lo, product_count__lte=hi)
        if val == "100+":
            return queryset.filter(product_count__gt=100)
        return queryset


class CountryGroupFilter(admin.SimpleListFilter):
    title = "country"
    parameter_name = "country_group"

    def lookups(self, request, model_admin):
        return [
            ("us", "United States"),
            ("non-us", "Non-US"),
        ]

    def queryset(self, request, queryset):
        val = self.value()
        if val == "us":
            return queryset.filter(country="UNITED STATES")
        if val == "non-us":
            return queryset.exclude(country="UNITED STATES").exclude(country="")
        return queryset


@admin.register(Manufacturer)
class ManufacturerAdmin(admin.ModelAdmin):
    change_list_template = "admin/catalog/manufacturer/change_list.html"
    list_display = (
        "logo_thumb", "display_name_col", "product_count_col", "cage_code", "slug",
        "website",
        "city", "state", "country",
        "manufacturer_toggle",
        "status_toggle",
        "view_on_site_link",
    )
    list_filter = (
        ProfileStatusFilter, ProductCountFilter, CountryGroupFilter,
        "is_manufacturer", "resolution_status",
    )
    list_select_related = ("profile", "profile__logo")
    search_fields = ("cage_code", "company_name", "slug", "uei")
    search_help_text = "Prefixes: cage: name: slug: uei: product: — or search all fields"
    ordering = ("profile__display_name",)

    _PREFIX_MAP = {"cage": "cage_code", "name": "company_name", "slug": "slug", "uei": "uei"}

    def get_search_results(self, request, queryset, search_term):
        qs, use_default = _prefixed_search(queryset, search_term, self._PREFIX_MAP)
        if not use_default:
            return qs, False
        # product: prefix — find manufacturers by product nomenclature/name
        if search_term.startswith("product:"):
            value = search_term.partition(":")[2].strip()
            if value:
                from django.db.models import Q
                return queryset.filter(
                    Q(products__nomenclature__icontains=value) |
                    Q(products__name__icontains=value)
                ).distinct(), False
            return queryset, False
        return super().get_search_results(request, queryset, search_term)
    list_per_page = 50
    inlines = [ManufacturerProfileInline]

    class Media:
        css = {"all": ("catalog/css/toggle.css",)}
        js = ("catalog/js/toggle.js",)

    readonly_fields = ("profile_display_name", "manufacturer_toggle_detail", "status_toggle_detail", "view_on_site_detail", "logo_preview_detail")

    fieldsets = (
        (None, {
            "fields": ("logo_preview_detail", "view_on_site_detail", "manufacturer_toggle_detail", "status_toggle_detail"),
        }),
        ("Identification", {
            "fields": (
                "cage_code", "company_name", "profile_display_name", "slug", "website", "uei",
            ),
        }),
        ("Location", {
            "fields": ("address", "city", "state", "zip_code", "country"),
        }),
        ("Flags", {
            "fields": ("is_manufacturer",
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

    def manufacturer_toggle(self, obj):
        val = obj.is_manufacturer
        return format_html(
            '<div class="tri-toggle" data-val="{val}" data-pk="{pk}" data-field="is_manufacturer">'
            '<input type="hidden" name="_mfr_{pk}" value="{val}">'
            '<div class="tri-toggle__track">'
            '<div class="tri-toggle__seg">&#x2212;</div>'
            '<div class="tri-toggle__seg">&#x25CF;</div>'
            '<div class="tri-toggle__seg">&#x2713;</div>'
            '<div class="tri-toggle__thumb"></div>'
            '</div></div>',
            val=val, pk=obj.pk,
        )
    manufacturer_toggle.short_description = "Manufacturer"

    def view_on_site_detail(self, obj):
        try:
            status = obj.profile.status
        except ManufacturerProfile.DoesNotExist:
            status = 0
        if obj.pk and obj.slug and status == Manufacturer.ENABLED:
            url = reverse("manufacturer_detail", args=[obj.slug])
            return format_html('<a href="{}" target="_blank" style="font-size:14px">View on site &rarr;</a>', url)
        return "-"
    view_on_site_detail.short_description = "Frontend"

    def manufacturer_toggle_detail(self, obj):
        if not obj.pk:
            return "--"
        val = obj.is_manufacturer
        return format_html(
            '<div class="tri-toggle" data-val="{val}" data-pk="{pk}" data-field="is_manufacturer">'
            '<input type="hidden" name="_mfr_{pk}" value="{val}">'
            '<div class="tri-toggle__track">'
            '<div class="tri-toggle__seg">&#x2212;</div>'
            '<div class="tri-toggle__seg">&#x25CF;</div>'
            '<div class="tri-toggle__seg">&#x2713;</div>'
            '<div class="tri-toggle__thumb"></div>'
            '</div></div>',
            val=val, pk=obj.pk,
        )
    manufacturer_toggle_detail.short_description = "Is Confirmed Manufacturer"

    def status_toggle_detail(self, obj):
        if not obj.pk:
            return "--"
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
    status_toggle_detail.short_description = "Status"

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
                "set-field/<int:pk>/",
                self.admin_site.admin_view(self.set_field_view),
                name="catalog_manufacturer_set_field",
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
            path(
                "refresh-display-names/",
                self.admin_site.admin_view(self.refresh_display_names_view),
                name="catalog_manufacturer_refresh_display_names",
            ),
        ]
        return custom_urls + super().get_urls()

    def refresh_display_names_view(self, request):
        if request.method != "POST":
            return JsonResponse({"ok": False, "error": "POST required"}, status=405)
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        refresh_slugs = "refresh_slugs" in request.POST
        call_command("refresh_display_names", refresh_slugs=refresh_slugs, stdout=out)
        messages.success(request, out.getvalue().strip().split("\n")[-1])
        return redirect(reverse("admin:catalog_manufacturer_changelist"))

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

    def set_field_view(self, request, pk):
        """Generic tri-toggle endpoint for SmallIntegerField tri-state fields."""
        if request.method != "POST":
            return JsonResponse({"ok": False, "error": "POST required"}, status=405)

        field = request.POST.get("field", "")
        ALLOWED_FIELDS = {"is_manufacturer"}
        if field not in ALLOWED_FIELDS:
            return JsonResponse({"ok": False, "error": f"Field '{field}' not allowed"}, status=400)

        try:
            value = int(request.POST.get("value", ""))
        except (ValueError, TypeError):
            return JsonResponse({"ok": False, "error": "Invalid value"}, status=400)

        if value not in (-1, 0, 1):
            return JsonResponse({"ok": False, "error": "Value must be -1, 0, or 1"}, status=400)

        try:
            mfr = Manufacturer.objects.get(pk=pk)
        except Manufacturer.DoesNotExist:
            return JsonResponse({"ok": False, "error": "Not found"}, status=404)

        setattr(mfr, field, value)
        mfr.save(update_fields=[field])
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

    def view_on_site_link(self, obj):
        try:
            status = obj.profile.status
        except ManufacturerProfile.DoesNotExist:
            status = 0
        if obj.slug and status == Manufacturer.ENABLED:
            url = reverse("manufacturer_detail", args=[obj.slug])
            return format_html('<a href="{}" target="_blank">View</a>', url)
        return "-"
    view_on_site_link.short_description = "Site"

    def logo_thumb(self, obj):
        try:
            logo = obj.profile.logo
        except ManufacturerProfile.DoesNotExist:
            logo = None
        if logo:
            return format_html(
                '<img src="{}" width="28" height="28" style="border-radius:4px; '
                'object-fit:contain; background:#fff; box-shadow:0 1px 3px rgba(0,0,0,0.1);" />',
                logo.file.url,
            )
        return ""
    logo_thumb.short_description = ""

    def logo_preview_detail(self, obj):
        try:
            logo = obj.profile.logo
        except ManufacturerProfile.DoesNotExist:
            logo = None
        if logo:
            return format_html(
                '<img src="{}" style="max-width:120px; max-height:120px; border-radius:8px; '
                'box-shadow:0 2px 8px rgba(0,0,0,0.1); background:#fff;" />',
                logo.file.url,
            )
        return "(no logo)"
    logo_preview_detail.short_description = "Logo"

    def display_name_col(self, obj):
        return obj.display_name
    display_name_col.short_description = "Name"
    display_name_col.admin_order_field = "profile__display_name"

    def product_count_col(self, obj):
        count = obj.product_count
        url = reverse("admin:catalog_product_changelist") + f"?manufacturer__id__exact={obj.pk}"
        return format_html('<a href="{}">{}</a>', url, count)
    product_count_col.short_description = "Products"
    product_count_col.admin_order_field = "product_count"


# =============================================================================
# Products
# =============================================================================

class ManufacturerVerifiedFilter(admin.SimpleListFilter):
    title = "manufacturer verified"
    parameter_name = "mfr_verified"

    def lookups(self, request, model_admin):
        return [
            ("1", "Yes"),
            ("0", "Neutral"),
            ("-1", "No"),
        ]

    def queryset(self, request, queryset):
        val = self.value()
        if val is not None:
            return queryset.filter(manufacturer__is_manufacturer=int(val))
        return queryset


class PublishedFilter(admin.SimpleListFilter):
    title = "published"
    parameter_name = "published"

    def lookups(self, request, model_admin):
        return [
            ("1", "Published"),
            ("0", "Not published"),
        ]

    def queryset(self, request, queryset):
        val = self.value()
        if val == "1":
            return queryset.published()
        if val == "0":
            return queryset.exclude(
                pk__in=queryset.model.objects.published().values("pk")
            )
        return queryset


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    change_list_template = "admin/catalog/product/change_list.html"
    list_display = (
        "display_name_col", "nsn", "filter_manufacturer_link", "manufacturer_display",
        "part_number", "price_display", "source", "status_toggle", "google_search_link", "view_on_site_link",
    )
    list_filter = ("source", "is_active", PublishedFilter, ManufacturerVerifiedFilter)
    search_fields = (
        "nsn", "nomenclature",
        "part_number", "name", "display_name",
        "manufacturer__company_name", "manufacturer__cage_code",
    )
    search_help_text = "Prefixes: nsn: pn: name: mfr: cage: — or search all fields"
    raw_id_fields = ("manufacturer", "fsc")

    _PREFIX_MAP = {
        "nsn": "nsn", "pn": "part_number", "name": "name",
        "mfr": "manufacturer__company_name", "cage": "manufacturer__cage_code",
    }

    def get_search_results(self, request, queryset, search_term):
        qs, use_default = _prefixed_search(queryset, search_term, self._PREFIX_MAP)
        if not use_default:
            return qs, False
        return super().get_search_results(request, queryset, search_term)
    ordering = ("-created_at",)
    list_per_page = 50
    list_select_related = ("manufacturer", "manufacturer__profile")
    inlines = [ProductSpecificationInline]

    class Media:
        css = {"all": ("catalog/css/toggle.css",)}
        js = ("catalog/js/toggle.js",)

    readonly_fields = ("status_toggle_detail",)

    fieldsets = (
        (None, {
            "fields": ("status_toggle_detail",),
        }),
        ("Product Info", {
            "fields": (
                "display_name", "name", "description", "part_number",
                "nsn", "nomenclature", "price", "fsc", "unit_of_issue",
                "source",
            ),
        }),
        ("Links", {
            "fields": ("manufacturer",),
        }),
    )

    def get_urls(self):
        custom_urls = [
            path(
                "set-field/<int:pk>/",
                self.admin_site.admin_view(self.set_field_view),
                name="catalog_product_set_field",
            ),
            path(
                "naturalize-names/",
                self.admin_site.admin_view(self.naturalize_names_view),
                name="catalog_product_naturalize_names",
            ),
        ]
        return custom_urls + super().get_urls()

    def naturalize_names_view(self, request):
        if request.method != "POST":
            return JsonResponse({"ok": False, "error": "POST required"}, status=405)
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        force = "force" in request.POST
        call_command("naturalize_product_names", force=force, stdout=out)
        messages.success(request, out.getvalue().strip().split("\n")[-1])
        return redirect(reverse("admin:catalog_product_changelist"))

    def set_field_view(self, request, pk):
        if request.method != "POST":
            return JsonResponse({"ok": False, "error": "POST required"}, status=405)
        field = request.POST.get("field", "")
        if field not in {"is_active"}:
            return JsonResponse({"ok": False, "error": f"Field '{field}' not allowed"}, status=400)
        try:
            value = int(request.POST.get("value", ""))
        except (ValueError, TypeError):
            return JsonResponse({"ok": False, "error": "Invalid value"}, status=400)
        if value not in (-1, 0, 1):
            return JsonResponse({"ok": False, "error": "Value must be -1, 0, or 1"}, status=400)
        try:
            product = Product.objects.get(pk=pk)
        except Product.DoesNotExist:
            return JsonResponse({"ok": False, "error": "Not found"}, status=404)
        setattr(product, field, value)
        product.save(update_fields=[field])
        return JsonResponse({"ok": True})

    def display_name_col(self, obj):
        return obj.get_display_name()
    display_name_col.short_description = "Name"
    display_name_col.admin_order_field = "display_name"

    def price_display(self, obj):
        if obj.price:
            return f"${obj.price:,.2f}"
        return "--"
    price_display.short_description = "Price"

    def filter_manufacturer_link(self, obj):
        url = reverse("admin:catalog_product_changelist") + f"?manufacturer__id__exact={obj.manufacturer_id}"
        return format_html('<a href="{}">F</a>', url)
    filter_manufacturer_link.short_description = "F"

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

    def status_toggle(self, obj):
        val = obj.is_active
        return format_html(
            '<div class="tri-toggle" data-val="{val}" data-pk="{pk}" data-field="is_active">'
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

    def status_toggle_detail(self, obj):
        if not obj.pk:
            return "--"
        val = obj.is_active
        return format_html(
            '<div class="tri-toggle" data-val="{val}" data-pk="{pk}" data-field="is_active">'
            '<input type="hidden" name="_status_{pk}" value="{val}">'
            '<div class="tri-toggle__track">'
            '<div class="tri-toggle__seg">&#x2212;</div>'
            '<div class="tri-toggle__seg">&#x25CF;</div>'
            '<div class="tri-toggle__seg">&#x2713;</div>'
            '<div class="tri-toggle__thumb"></div>'
            '</div></div>',
            val=val, pk=obj.pk,
        )
    status_toggle_detail.short_description = "Status"

    def view_on_site_link(self, obj):
        if obj.is_active == Product.DISABLED:
            return "-"
        if obj.is_active == Product.NEUTRAL:
            try:
                if obj.manufacturer.profile.status != Manufacturer.ENABLED:
                    return "-"
            except ManufacturerProfile.DoesNotExist:
                return "-"
        url = f"/products/{obj.manufacturer.slug}/{obj.part_number_slug}/"
        return format_html('<a href="{}" target="_blank">View</a>', url)
    view_on_site_link.short_description = "Public Page"
