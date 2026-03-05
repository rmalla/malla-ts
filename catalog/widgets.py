from django.forms import Widget


class TriToggleWidget(Widget):
    """Three-position toggle switch: Disabled / Neutral / Enabled."""

    template_name = "catalog/widgets/tri_toggle.html"

    class Media:
        css = {"all": ("catalog/css/toggle.css",)}
        js = ("catalog/js/toggle.js",)

    def get_context(self, name, value, attrs):
        ctx = super().get_context(name, value, attrs)
        ctx["widget"]["val"] = str(value if value is not None else 0)
        return ctx
