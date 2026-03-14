from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html

from .models import (
    HomePage, StandardPage, EquipmentCategoryPage, EquipmentPage,
    IndustryPage, ContactPage, ContactSubmission,
    FederalSupplyClass,
)


# =============================================================================
# Wagtail Page Models (direct DB access alongside Wagtail admin)
# =============================================================================

@admin.register(HomePage)
class HomePageAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug', 'hero_title', 'live', 'first_published_at')
    search_fields = ('title', 'hero_title', 'hero_subtitle')
    readonly_fields = ('path', 'depth', 'url_path', 'content_type', 'first_published_at', 'last_published_at')


@admin.register(StandardPage)
class StandardPageAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug', 'intro', 'live', 'first_published_at')
    search_fields = ('title', 'intro', 'body')
    readonly_fields = ('path', 'depth', 'url_path', 'content_type', 'first_published_at', 'last_published_at')


@admin.register(EquipmentCategoryPage)
class EquipmentCategoryPageAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug', 'intro', 'live', 'first_published_at')
    search_fields = ('title', 'intro', 'description')
    readonly_fields = ('path', 'depth', 'url_path', 'content_type', 'first_published_at', 'last_published_at')


@admin.register(EquipmentPage)
class EquipmentPageAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug', 'subtitle', 'live', 'first_published_at')
    search_fields = ('title', 'subtitle', 'description')
    readonly_fields = ('path', 'depth', 'url_path', 'content_type', 'first_published_at', 'last_published_at')


@admin.register(IndustryPage)
class IndustryPageAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug', 'intro', 'live', 'first_published_at')
    search_fields = ('title', 'intro', 'description')
    readonly_fields = ('path', 'depth', 'url_path', 'content_type', 'first_published_at', 'last_published_at')


@admin.register(ContactPage)
class ContactPageAdmin(admin.ModelAdmin):
    list_display = ('title', 'slug', 'phone', 'email', 'live', 'first_published_at')
    search_fields = ('title', 'intro', 'phone', 'email')
    readonly_fields = ('path', 'depth', 'url_path', 'content_type', 'first_published_at', 'last_published_at')


# =============================================================================
# Business Models
# =============================================================================

@admin.register(ContactSubmission)
class ContactSubmissionAdmin(admin.ModelAdmin):
    list_display = ('name', 'email', 'phone', 'organization', 'submitted_at')
    list_filter = ('submitted_at',)
    search_fields = ('name', 'email', 'organization', 'message')
    readonly_fields = ('name', 'email', 'phone', 'organization', 'message', 'submitted_at')
    ordering = ('-submitted_at',)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


# =============================================================================
# NSN Models
# =============================================================================

@admin.register(FederalSupplyClass)
class FederalSupplyClassAdmin(admin.ModelAdmin):
    list_display = ('code', 'name', 'group', 'group_name', 'nsn_count', 'product_count')
    list_filter = ('group',)
    search_fields = ('code', 'name', 'group_name')
    ordering = ('code',)

    def get_queryset(self, request):
        from django.db.models import Count
        return super().get_queryset(request).annotate(
            _nsn_count=Count("nsns", distinct=True),
            _product_count=Count("nsns__products", distinct=True),
        )

    def nsn_count(self, obj):
        url = reverse("admin:catalog_nationalstocknumber_changelist") + f"?fsc={obj.code}"
        return format_html('<a href="{}">{}</a>', url, obj._nsn_count)
    nsn_count.short_description = 'NSNs'
    nsn_count.admin_order_field = '_nsn_count'

    def product_count(self, obj):
        url = reverse("admin:catalog_product_changelist") + f"?fsc={obj.code}"
        return format_html('<a href="{}">{}</a>', url, obj._product_count)
    product_count.short_description = 'Products'
    product_count.admin_order_field = '_product_count'


