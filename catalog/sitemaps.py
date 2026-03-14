from django.contrib.sitemaps import Sitemap
from django.urls import reverse
from wagtail.models import Page

SITE_DOMAIN = "www.malla-ts.com"


class MallatsSitemap(Sitemap):
    """Single flat sitemap with all root-level pages."""
    protocol = "https"
    changefreq = "daily"
    priority = 0.9

    def get_urls(self, page=1, site=None, protocol=None):
        class FakeSite:
            domain = SITE_DOMAIN
            name = SITE_DOMAIN

        return super().get_urls(page=page, site=FakeSite(), protocol=self.protocol)

    def items(self):
        catalog_urls = ["product_list", "manufacturer_list", "nsn_search"]
        wagtail_pages = list(
            Page.objects.live().public().filter(depth__gte=2).specific()
        )
        return catalog_urls + wagtail_pages

    def location(self, item):
        if isinstance(item, str):
            return reverse(item)
        return item.get_url()
