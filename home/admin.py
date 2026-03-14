from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html

from .models import (
    HomePage, StandardPage, EquipmentCategoryPage, EquipmentPage,
    IndustryPage, ContactPage, ContactSubmission,
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


# FederalSupplyClass admin is registered in catalog/admin.py (as a proxy)
# so it appears under "Product Catalog" in the sidebar.


