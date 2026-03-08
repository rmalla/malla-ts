from django.conf import settings
from django.urls import include, path
from django.contrib import admin
from django.http import HttpResponse, JsonResponse
from django.views.decorators.cache import cache_page
from django.views.generic import RedirectView
from django.db import connection

from wagtail.admin import urls as wagtailadmin_urls
from wagtail import urls as wagtail_urls
from wagtail.documents import urls as wagtaildocs_urls
from django.contrib.sitemaps.views import sitemap
from wagtail.contrib.sitemaps import Sitemap as WagtailSitemap

from catalog.sitemaps import (
    ManufacturerSitemap,
    ProductSitemap,
    StaticCatalogSitemap,
)

sitemaps = {
    "wagtail": WagtailSitemap,
    "products": ProductSitemap,
    "manufacturers": ManufacturerSitemap,
    "catalog_static": StaticCatalogSitemap,
}

from search import views as search_views
from home import views as home_views

import time


def robots_txt(request):
    """Serve robots.txt for search engine crawlers"""
    lines = [
        "User-agent: *",
        "Allow: /",
        "Disallow: /admin/",
        "Disallow: /django-admin/",
        "Disallow: /documents/",
        "Disallow: /search/",
        "",
        "Sitemap: https://www.malla-ts.com/sitemap.xml",
    ]
    return HttpResponse("\n".join(lines), content_type="text/plain")


def health_check(request):
    """Health check endpoint for monitoring"""
    checks = {
        "status": "ok",
        "timestamp": time.time(),
    }

    # Check database
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        checks["db"] = True
    except Exception:
        checks["db"] = False
        checks["status"] = "degraded"

    # Check Redis cache
    try:
        from django.core.cache import cache
        cache.set("_health", "1", 10)
        checks["cache"] = cache.get("_health") == "1"
        if not checks["cache"]:
            checks["status"] = "degraded"
    except Exception:
        checks["cache"] = False
        checks["status"] = "degraded"

    status_code = 200 if checks["status"] == "ok" else 503
    return JsonResponse(checks, status=status_code)


urlpatterns = [
    path("robots.txt", robots_txt, name="robots_txt"),
    path("sitemap.xml", cache_page(3600)(sitemap), {"sitemaps": sitemaps}, name="sitemap"),
    path("health/", health_check, name="health_check"),
    # Redirect old home.Manufacturer admin → new ManufacturerProfile admin
    path("django-admin/home/manufacturer/", RedirectView.as_view(
        url="/django-admin/catalog/manufacturer/", permanent=True,
    )),
    path("django-admin/", admin.site.urls),
    path("admin/", include(wagtailadmin_urls)),
    path("documents/", include(wagtaildocs_urls)),
    path("search/", search_views.search, name="search"),
    path("contact/submit/", home_views.contact_form_submit, name="contact_submit"),

    # Products catalog
    path("products/", home_views.product_list, name="product_list"),
    path("products/<slug:manufacturer_slug>/<slug:part_slug>/", home_views.product_detail, name="product_detail"),
    path("products/<str:nsn>/", home_views.product_redirect, name="product_redirect"),

    # Manufacturer pages
    path("manufacturers/", home_views.manufacturer_list, name="manufacturer_list"),
    path("manufacturers/<slug:slug>/", home_views.manufacturer_detail, name="manufacturer_detail"),

]


if settings.DEBUG:
    from django.conf.urls.static import static
    from django.contrib.staticfiles.urls import staticfiles_urlpatterns

    # Serve static and media files from development server
    urlpatterns += staticfiles_urlpatterns()
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

urlpatterns = urlpatterns + [
    # For anything not caught by a more specific rule above, hand over to
    # Wagtail's page serving mechanism. This should be the last pattern in
    # the list:
    path("", include(wagtail_urls)),
]
