import json
import re

from django.core.paginator import Paginator
from django.db.models import Count, Min, Max, Q
from django.http import Http404
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.cache import cache_page

from catalog.models import Product, NationalStockNumber
from home.models import FederalSupplyClass

from .products import format_nsn, normalize_nsn


@cache_page(60 * 60 * 24)
def nsn_detail(request, nsn):
    """Detail page for a single NSN — shows all products with that NSN."""
    raw = normalize_nsn(nsn)
    dashed = format_nsn(raw)

    # Redirect dashless to canonical dashed URL
    if nsn != dashed:
        return redirect("nsn_detail", nsn=dashed, permanent=True)

    nsn_obj = (
        NationalStockNumber.objects
        .select_related("fsc")
        .filter(nsn=dashed)
        .first()
    )

    if not nsn_obj:
        raise Http404

    products = (
        Product.objects.published()
        .filter(nsn=nsn_obj)
        .select_related("nsn", "nsn__fsc", "manufacturer", "manufacturer__profile", "manufacturer__profile__logo")
        .order_by("manufacturer__company_name")
    )

    product_count = products.count()
    if product_count == 0:
        raise Http404

    fsc = nsn_obj.fsc
    nomenclature = nsn_obj.nomenclature

    # Related NSNs from same FSC
    related_nsns = []
    if fsc:
        related_nsns = list(
            NationalStockNumber.objects
            .filter(fsc=fsc)
            .exclude(pk=nsn_obj.pk)
            .filter(products__in=Product.objects.published())
            .values("nsn", "nomenclature")
            .order_by("nsn")
            .distinct()[:12]
        )

    # JSON-LD
    json_ld_items = []
    for i, p in enumerate(products[:50], 1):
        item = {
            "@type": "Product",
            "position": i,
            "name": p.get_display_name(),
            "url": request.build_absolute_uri(
                reverse("product_detail", kwargs={
                    "manufacturer_slug": p.manufacturer.slug,
                    "part_slug": p.part_number_slug,
                })
            ),
        }
        if p.manufacturer:
            item["manufacturer"] = {
                "@type": "Organization",
                "name": p.manufacturer.display_name,
            }
        if p.price:
            item["offers"] = {
                "@type": "Offer",
                "price": str(p.price),
                "priceCurrency": "USD",
            }
        json_ld_items.append(item)

    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": f"NSN {dashed} — {nomenclature}",
        "numberOfItems": len(json_ld_items),
        "itemListElement": json_ld_items,
    })

    breadcrumb_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://www.malla-ts.com/"},
            {"@type": "ListItem", "position": 2, "name": "NSN Search", "item": "https://www.malla-ts.com/nsn/"},
            *(
                [{"@type": "ListItem", "position": 3, "name": f"FSC {fsc.code}", "item": f"https://www.malla-ts.com/nsn/fsc/{fsc.code}/"},
                 {"@type": "ListItem", "position": 4, "name": f"NSN {dashed}", "item": f"https://www.malla-ts.com/nsn/{dashed}/"}]
                if fsc else
                [{"@type": "ListItem", "position": 3, "name": f"NSN {dashed}", "item": f"https://www.malla-ts.com/nsn/{dashed}/"}]
            ),
        ],
    })

    context = {
        "nsn": dashed,
        "niin": nsn_obj.niin,
        "fsc": fsc,
        "nomenclature": nomenclature,
        "unit_of_issue": nsn_obj.unit_of_issue,
        "products": products,
        "product_count": product_count,
        "related_nsns": related_nsns,
        "json_ld": json_ld,
        "breadcrumb_ld": breadcrumb_ld,
        "format_nsn": format_nsn,
    }
    return render(request, "home/nsn_detail.html", context)


@cache_page(60 * 60)
def nsn_search(request):
    """NSN search and browse page."""
    query = request.GET.get("q", "").strip()
    page_number = request.GET.get("page", 1)

    results = None
    total_count = 0

    if query:
        raw_query = re.sub(r"[^0-9A-Za-z ]", "", query)
        # Search NationalStockNumber directly, but only those with published products
        nsns = (
            NationalStockNumber.objects
            .filter(
                Q(nsn__icontains=raw_query)
                | Q(nomenclature__icontains=query)
                | Q(niin__icontains=raw_query)
            )
            .filter(products__in=Product.objects.published())
            .annotate(
                min_price=Min("products__price"),
                max_price=Max("products__price"),
                product_count=Count("products", filter=Q(
                    products__in=Product.objects.published()
                )),
            )
            .order_by("nsn")
            .distinct()
        )
        total_count = nsns.count()
        paginator = Paginator(nsns, 25)
        results = paginator.get_page(page_number)

    # FSC categories for browsing
    fsc_list = (
        FederalSupplyClass.objects.annotate(
            nsn_count=Count("nsns", filter=Q(
                nsns__products__in=Product.objects.published(),
            ))
        )
        .filter(nsn_count__gt=0)
        .order_by("code")
    )

    search_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "WebSite",
        "url": "https://www.malla-ts.com/nsn/",
        "name": "NSN Lookup - Malla Technical Services",
        "potentialAction": {
            "@type": "SearchAction",
            "target": "https://www.malla-ts.com/nsn/?q={search_term_string}",
            "query-input": "required name=search_term_string",
        },
    })

    context = {
        "query": query,
        "results": results,
        "total_count": total_count,
        "fsc_list": fsc_list,
        "search_ld": search_ld,
    }
    return render(request, "home/nsn_search.html", context)


@cache_page(60 * 60)
def nsn_fsc_list(request, fsc_code):
    """List distinct NSNs within a Federal Supply Class."""
    fsc = get_object_or_404(FederalSupplyClass, code=fsc_code)
    page_number = request.GET.get("page", 1)

    nsns = (
        NationalStockNumber.objects
        .filter(fsc=fsc)
        .filter(products__in=Product.objects.published())
        .annotate(
            min_price=Min("products__price"),
            max_price=Max("products__price"),
            product_count=Count("products", filter=Q(
                products__in=Product.objects.published()
            )),
        )
        .order_by("nsn")
        .distinct()
    )

    total_count = nsns.count()
    paginator = Paginator(nsns, 25)
    nsns_page = paginator.get_page(page_number)

    breadcrumb_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://www.malla-ts.com/"},
            {"@type": "ListItem", "position": 2, "name": "NSN Search", "item": "https://www.malla-ts.com/nsn/"},
            {"@type": "ListItem", "position": 3, "name": f"FSC {fsc.code} — {fsc.name}", "item": f"https://www.malla-ts.com/nsn/fsc/{fsc.code}/"},
        ],
    })

    context = {
        "fsc": fsc,
        "nsns": nsns_page,
        "total_count": total_count,
        "breadcrumb_ld": breadcrumb_ld,
    }
    return render(request, "home/nsn_fsc_list.html", context)
