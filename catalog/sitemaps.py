from django.contrib.sitemaps import Sitemap
from django.urls import reverse

from .models import Manufacturer, Product

SITE_DOMAIN = "www.malla-ts.com"


class CatalogSitemap(Sitemap):
    """Base sitemap that forces www.malla-ts.com domain."""
    protocol = "https"

    def get_urls(self, page=1, site=None, protocol=None):
        # Override to inject the correct domain regardless of request host
        from django.contrib.sites.requests import RequestSite

        class FakeSite:
            domain = SITE_DOMAIN
            name = SITE_DOMAIN

        return super().get_urls(page=page, site=FakeSite(), protocol=self.protocol)


class ProductSitemap(CatalogSitemap):
    changefreq = "weekly"
    priority = 0.7
    limit = 5000

    def items(self):
        return (
            Product.objects.published()
            .select_related("manufacturer")
            .order_by("pk")
        )

    def location(self, obj):
        return reverse(
            "product_detail",
            kwargs={
                "manufacturer_slug": obj.manufacturer.slug,
                "part_slug": obj.part_number_slug,
            },
        )


class ManufacturerSitemap(CatalogSitemap):
    changefreq = "weekly"
    priority = 0.8

    def items(self):
        return Manufacturer.objects.filter(profile__status=Manufacturer.ENABLED)

    def location(self, obj):
        return reverse("manufacturer_detail", kwargs={"slug": obj.slug})


class StaticCatalogSitemap(CatalogSitemap):
    changefreq = "daily"
    priority = 0.9

    def items(self):
        return ["product_list", "manufacturer_list"]

    def location(self, item):
        return reverse(item)
