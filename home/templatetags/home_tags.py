from django import template
from catalog.models import Manufacturer

register = template.Library()


@register.simple_tag
def get_manufacturers():
    """Get enabled manufacturers for homepage display."""
    return Manufacturer.objects.filter(
        status=Manufacturer.ENABLED
    ).select_related("logo")
