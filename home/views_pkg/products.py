import re

from django.core.paginator import Paginator
from django.db import models
from django.db.models import Q
from django.http import Http404
from django.shortcuts import redirect, render, get_object_or_404
from django.views.decorators.cache import cache_page
from django_ratelimit.decorators import ratelimit

from catalog.models import Manufacturer, Product, ManufacturerProfile


def format_nsn(raw):
    """Convert raw NSN '2530001928932' to dashed '2530-00-192-8932'."""
    raw = re.sub(r"[^0-9]", "", raw)
    if len(raw) == 13:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:9]}-{raw[9:13]}"
    return raw


def normalize_nsn(dashed):
    """Convert dashed NSN '2530-00-192-8932' to raw '2530001928932'."""
    return re.sub(r"[^0-9]", "", dashed)


@cache_page(60 * 60)
@ratelimit(key="ip", rate="60/m", method="GET", block=True)
def product_list(request):
    """Browse products."""
    query = request.GET.get("q", "").strip()
    page_number = request.GET.get("page", 1)

    products = Product.objects.published().select_related(
        "nsn", "nsn__fsc", "manufacturer", "manufacturer__profile", "manufacturer__profile__logo"
    ).order_by("name", "manufacturer__company_name")

    if query:
        raw_query = re.sub(r"[^0-9A-Za-z ]", "", query)
        products = products.filter(
            Q(nsn__nomenclature__icontains=query)
            | Q(nsn__nsn__icontains=raw_query)
            | Q(name__icontains=query)
            | Q(part_number__icontains=query)
            | Q(manufacturer__profile__display_name__icontains=query)
            | Q(manufacturer__company_name__icontains=query)
            | Q(manufacturer__cage_code__icontains=query)
        ).distinct()

    total_count = products.count()
    paginator = Paginator(products, 25)
    products_page = paginator.get_page(page_number)

    context = {
        "products": products_page,
        "query": query,
        "total_count": total_count,
    }
    return render(request, "home/product_list.html", context)


@cache_page(60 * 60 * 24)
def product_detail(request, manufacturer_slug, part_slug):
    """Detail view for a single product."""
    product = get_object_or_404(
        Product.objects.published().select_related("nsn", "nsn__fsc", "manufacturer", "manufacturer__profile", "manufacturer__profile__logo"),
        manufacturer__slug=manufacturer_slug,
        part_number_slug=part_slug,
    )

    formatted_nsn = product.nsn.nsn if product.nsn else ""

    # Product specifications (key-value pairs)
    specs = list(product.specs.all().order_by("group", "sort_order", "label"))

    # Related products: same FSC first, then same manufacturer as fallback
    related = []
    fsc = product.nsn.fsc if product.nsn else None
    if fsc:
        related = list(
            Product.objects.published().filter(nsn__fsc=fsc)
            .exclude(pk=product.pk)
            .select_related("manufacturer", "manufacturer__profile", "manufacturer__profile__logo")
            .order_by("name")[:12]
        )
    if len(related) < 12:
        already = {p.pk for p in related} | {product.pk}
        more = (
            Product.objects.published()
            .filter(manufacturer=product.manufacturer)
            .exclude(pk__in=already)
            .select_related("manufacturer", "manufacturer__profile", "manufacturer__profile__logo")
            .order_by("name")[:12 - len(related)]
        )
        related.extend(more)

    context = {
        "supplier": product,  # backward compat for templates
        "product": product,
        "cage": product.manufacturer,  # backward compat
        "manufacturer": product.manufacturer,
        "nsn_obj": product.nsn,
        "formatted_nsn": formatted_nsn,
        "specifications": specs,
        "related_products": related,
        "format_nsn": format_nsn,
    }
    return render(request, "home/product_detail.html", context)


@cache_page(60 * 60 * 24)
def manufacturer_detail(request, slug):
    """Manufacturer page with company info and product listings."""
    org = get_object_or_404(
        Manufacturer.objects.select_related("profile", "profile__logo"),
        slug=slug,
        profile__status=Manufacturer.ENABLED,
    )

    products = (
        Product.objects.published().filter(manufacturer=org)
        .select_related("nsn", "nsn__fsc")
        .order_by("name")
    )

    context = {
        "cage": org,  # backward compat
        "manufacturer": org,
        "products": products,
        "product_count": products.count(),
        "format_nsn": format_nsn,
    }
    return render(request, "home/manufacturer_detail.html", context)


@cache_page(60 * 60)
@ratelimit(key="ip", rate="60/m", method="GET", block=True)
def manufacturer_list(request):
    """Browse manufacturers."""
    query = request.GET.get("q", "").strip()
    country_filter = request.GET.get("country", "").strip()
    page_number = request.GET.get("page", 1)

    manufacturers = Manufacturer.objects.select_related("profile", "profile__logo").filter(
        profile__status=Manufacturer.ENABLED,
        products__is_active__gte=0,
    ).distinct().annotate(
        product_count=models.Count("products", filter=models.Q(products__is_active__gte=0))
    ).order_by("profile__display_name")

    if country_filter == "us":
        manufacturers = manufacturers.filter(country="UNITED STATES")
    elif country_filter == "non-us":
        manufacturers = manufacturers.exclude(country="UNITED STATES").exclude(country="")

    if query:
        manufacturers = manufacturers.filter(
            models.Q(profile__display_name__icontains=query)
            | models.Q(company_name__icontains=query)
            | models.Q(cage_code__icontains=query)
        )

    total_count = manufacturers.count()
    paginator = Paginator(manufacturers, 25)
    manufacturers_page = paginator.get_page(page_number)

    context = {
        "manufacturers": manufacturers_page,
        "query": query,
        "country_filter": country_filter,
        "total_count": total_count,
    }
    return render(request, "home/manufacturer_list.html", context)


def product_redirect(request, nsn):
    """Redirect /products/<nsn>/ to /nsn/<nsn>/."""
    from catalog.models import NationalStockNumber
    dashed = format_nsn(normalize_nsn(nsn))
    if not NationalStockNumber.objects.filter(nsn=dashed).exists():
        raise Http404
    return redirect("nsn_detail", nsn=dashed)
