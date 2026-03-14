import json
import re

from django.core.paginator import Paginator
from django.db.models import Count, Min, Max, Q
from django.http import Http404
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.core.cache import cache as django_cache

from catalog.models import Manufacturer, Product, NationalStockNumber
from home.models import FederalSupplyClass

from .products import format_nsn, normalize_nsn

NSN_SIDEBAR_CACHE_KEY = "nsn_sidebar_data_v3"


def _enabled_manufacturer_ids():
    """Return IDs of manufacturers with an enabled profile. Tiny query (~4 rows)."""
    return list(
        Manufacturer.objects.filter(profile__status=Manufacturer.ENABLED)
        .values_list("id", flat=True)
    )


def _published_product_filter():
    """Q filter equivalent to Product.objects.published() but using manufacturer IDs."""
    mfr_ids = _enabled_manufacturer_ids()
    return Q(is_active=1) | Q(is_active=0, manufacturer_id__in=mfr_ids)


def _get_sidebar_data():
    """Build FSC sidebar grouped by category → FSG → FSC, with NSN counts. Cached 6 hours."""
    sidebar = django_cache.get(NSN_SIDEBAR_CACHE_KEY)
    if sidebar is not None:
        return sidebar

    mfr_ids = _enabled_manufacturer_ids()

    fsc_qs = (
        FederalSupplyClass.objects
        .filter(
            nsns__products__is_active__gte=0,
            nsns__products__manufacturer_id__in=mfr_ids,
        )
        .annotate(nsn_count=Count("nsns", distinct=True))
        .filter(nsn_count__gt=0)
        .order_by("code")
    )

    # Group by category → FSG → FSC
    categories = {}
    for fsc in fsc_qs:
        cat_code = fsc.category or "miscellaneous"
        cat_name = fsc.category_name or "Miscellaneous"
        fsg = fsc.group or fsc.code[:2]

        if cat_code not in categories:
            categories[cat_code] = {
                "code": cat_code,
                "name": cat_name,
                "groups": {},
                "total": 0,
            }

        cat = categories[cat_code]
        if fsg not in cat["groups"]:
            cat["groups"][fsg] = {
                "code": fsg,
                "name": fsc.group_name or f"Group {fsg}",
                "classes": [],
                "total": 0,
            }

        cat["groups"][fsg]["classes"].append({
            "code": fsc.code,
            "name": fsc.name,
            "count": fsc.nsn_count,
        })
        cat["groups"][fsg]["total"] += fsc.nsn_count
        cat["total"] += fsc.nsn_count

    # Convert groups dicts to sorted lists
    for cat in categories.values():
        cat["groups"] = sorted(cat["groups"].values(), key=lambda g: g["code"])

    sidebar = sorted(categories.values(), key=lambda c: -c["total"])
    django_cache.set(NSN_SIDEBAR_CACHE_KEY, sidebar, 60 * 60 * 6)
    return sidebar


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

    pub_q = _published_product_filter()

    products = (
        Product.objects.filter(pub_q, nsn=nsn_obj)
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
            .filter(products__in=Product.objects.filter(pub_q))
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
                [{"@type": "ListItem", "position": 3, "name": f"FSC {fsc.code}", "item": f"https://www.malla-ts.com/nsn/?fsc={fsc.code}"},
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


def nsn_search(request):
    """NSN browse and search page — always shows paginated results."""
    query = request.GET.get("q", "").strip()
    fsc_code = request.GET.get("fsc", "").strip()
    page_number = request.GET.get("page", 1)

    pub_q = _published_product_filter()
    published_products = Product.objects.filter(pub_q)

    # Base queryset: all NSNs with published products
    nsns = (
        NationalStockNumber.objects
        .filter(products__in=published_products)
    )

    # Apply FSC filter
    active_fsc = None
    if fsc_code:
        active_fsc = FederalSupplyClass.objects.filter(code=fsc_code).first()
        if active_fsc:
            nsns = nsns.filter(fsc=active_fsc)

    # Apply text search
    if query:
        raw_query = re.sub(r"[^0-9A-Za-z ]", "", query)
        nsns = nsns.filter(
            Q(nsn__icontains=raw_query)
            | Q(nomenclature__icontains=query)
            | Q(niin__icontains=raw_query)
        )

    nsns = (
        nsns
        .select_related("fsc")
        .annotate(
            min_price=Min("products__price"),
            max_price=Max("products__price"),
        )
        .order_by("nsn")
        .distinct()
    )

    total_count = nsns.count()
    paginator = Paginator(nsns, 50)
    results = paginator.get_page(page_number)

    # Sidebar data
    sidebar_groups = _get_sidebar_data()

    # Build query string for pagination links (without page param)
    qs_parts = []
    if query:
        qs_parts.append(f"q={query}")
    if fsc_code:
        qs_parts.append(f"fsc={fsc_code}")
    pagination_qs = "&".join(qs_parts)

    # Dynamic title/description
    if active_fsc and query:
        page_title = f"FSC {active_fsc.code} '{query}' — NSN Lookup"
        meta_desc = f"Search results for '{query}' in FSC {active_fsc.code} {active_fsc.name}. Browse National Stock Numbers with pricing and supplier data."
    elif active_fsc:
        page_title = f"FSC {active_fsc.code} {active_fsc.name} — NSN Lookup"
        meta_desc = f"Browse {total_count:,} National Stock Numbers in FSC {active_fsc.code} {active_fsc.name}. Find NSN pricing, nomenclature, and suppliers."
    elif query:
        page_title = f"'{query}' — NSN Search Results"
        meta_desc = f"Search results for '{query}' — {total_count:,} National Stock Numbers found. Browse NSN pricing and supplier data."
    else:
        page_title = "NSN Lookup — National Stock Number Search"
        meta_desc = f"Browse {total_count:,} National Stock Numbers. Search by NSN, nomenclature, or part number. Filter by Federal Supply Class."

    if results.number > 1:
        page_title = f"{page_title} — Page {results.number}"

    # JSON-LD: SearchAction + BreadcrumbList
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

    breadcrumb_items = [
        {"@type": "ListItem", "position": 1, "name": "Home", "item": "https://www.malla-ts.com/"},
        {"@type": "ListItem", "position": 2, "name": "NSN Lookup", "item": "https://www.malla-ts.com/nsn/"},
    ]
    if active_fsc:
        breadcrumb_items.append({
            "@type": "ListItem", "position": 3,
            "name": f"FSC {active_fsc.code} {active_fsc.name}",
            "item": f"https://www.malla-ts.com/nsn/?fsc={active_fsc.code}",
        })

    breadcrumb_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": breadcrumb_items,
    })

    # ItemList JSON-LD for current page
    item_list_items = []
    for i, nsn_item in enumerate(results, 1):
        item_list_items.append({
            "@type": "ListItem",
            "position": i,
            "name": f"NSN {nsn_item.nsn} — {nsn_item.nomenclature or 'N/A'}",
            "url": f"https://www.malla-ts.com/nsn/{nsn_item.nsn}/",
        })

    item_list_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": page_title,
        "numberOfItems": total_count,
        "itemListElement": item_list_items,
    })

    context = {
        "query": query,
        "fsc_code": fsc_code,
        "active_fsc": active_fsc,
        "results": results,
        "total_count": total_count,
        "sidebar_groups": sidebar_groups,
        "pagination_qs": pagination_qs,
        "page_title": page_title,
        "meta_desc": meta_desc,
        "search_ld": search_ld,
        "breadcrumb_ld": breadcrumb_ld,
        "item_list_ld": item_list_ld,
    }
    return render(request, "home/nsn_search.html", context)


def nsn_fsc_list(request, fsc_code):
    """Redirect FSC detail pages to the browse page with FSC filter."""
    # Preserve any query params
    page = request.GET.get("page", "")
    q = request.GET.get("q", "")
    url = f"/nsn/?fsc={fsc_code}"
    if q:
        url += f"&q={q}"
    if page and page != "1":
        url += f"&page={page}"
    return redirect(url, permanent=True)
