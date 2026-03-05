from django.shortcuts import render, get_object_or_404
from django.core.cache import cache
from django.views.decorators.cache import cache_page
from django.core.paginator import Paginator
from django.db.models import Q, Count
from django_ratelimit.decorators import ratelimit
from catalog.models import CatalogItem
from ..models import FederalSupplyClass
import logging

logger = logging.getLogger(__name__)


def _get_fsc_list():
    """Get FSC list with item counts, cached for 1 hour."""
    cache_key = "fsc_list_with_counts"
    fsc_list = cache.get(cache_key)
    if fsc_list is None:
        fsc_list = list(
            FederalSupplyClass.objects.annotate(
                item_count=Count('catalog_items')
            ).filter(item_count__gt=0).order_by('code')
        )
        cache.set(cache_key, fsc_list, 3600)
    return fsc_list


@ratelimit(key='ip', rate='60/m', method='GET', block=True)
def nsn_search(request):
    """NSN search landing page and search results"""
    query = request.GET.get('q', '').strip()
    page_number = request.GET.get('page', 1)

    items = None
    total_count = 0

    if query:
        clean_query = query.replace('-', '')

        items = CatalogItem.objects.filter(
            Q(nsn__icontains=query) |
            Q(niin__icontains=clean_query) |
            Q(nomenclature__icontains=query)
        ).select_related('fsc').order_by('nsn')

        total_count = items.count()

        paginator = Paginator(items, 25)
        items = paginator.get_page(page_number)

    context = {
        'query': query,
        'items': items,
        'total_count': total_count,
        'fsc_list': _get_fsc_list(),
    }

    return render(request, 'home/nsn_search.html', context)


@cache_page(60 * 60 * 24)
def nsn_detail(request, nsn):
    """NSN detail page"""
    item = get_object_or_404(CatalogItem.objects.select_related('fsc'), nsn=nsn)

    related_items = CatalogItem.objects.filter(
        fsc=item.fsc
    ).exclude(pk=item.pk).select_related('fsc').order_by('nomenclature')[:4]

    # Get pricing
    pricing = None
    try:
        pricing = item.pricing
    except Exception:
        pass

    # Get specifications
    specs = None
    try:
        specs = item.specifications
    except Exception:
        pass

    context = {
        'item': item,
        'related_items': related_items,
        'pricing': pricing,
        'specs': specs,
    }

    return render(request, 'home/nsn_detail.html', context)


@cache_page(60 * 60)
def nsn_fsc_list(request, fsc_code):
    """Browse NSN items by FSC code"""
    fsc = get_object_or_404(FederalSupplyClass, code=fsc_code)
    page_number = request.GET.get('page', 1)

    items = CatalogItem.objects.filter(fsc=fsc).order_by('nomenclature')
    total_count = items.count()

    paginator = Paginator(items, 25)
    items = paginator.get_page(page_number)

    context = {
        'fsc': fsc,
        'items': items,
        'total_count': total_count,
    }

    return render(request, 'home/nsn_fsc_list.html', context)
