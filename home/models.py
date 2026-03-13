from django.db import models
from wagtail.models import Page
from wagtail.fields import RichTextField, StreamField
from wagtail import blocks
from wagtail.admin.panels import FieldPanel
from wagtail.images.blocks import ImageChooserBlock
from wagtail.images.models import Image


class HomePage(Page):
    """Main homepage for Malla Technical Services"""

    hero_title = models.CharField(
        max_length=255,
        default="Supply Chain Engineering Excellence",
        help_text="Main hero title"
    )
    hero_subtitle = models.CharField(
        max_length=500,
        default="20+ Years of Supply Chain Expertise",
        help_text="Hero subtitle"
    )
    hero_description = RichTextField(
        default="Delivering quality equipment solutions to government facilities, national parks, energy sectors, and municipal services nationwide.",
        help_text="Hero description text"
    )

    intro_section = RichTextField(
        blank=True,
        help_text="Introduction section below hero"
    )

    hero_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Hero background image (1920x900px recommended)"
    )

    content_panels = Page.content_panels + [
        FieldPanel('hero_title'),
        FieldPanel('hero_subtitle'),
        FieldPanel('hero_description'),
        FieldPanel('hero_image'),
        FieldPanel('intro_section'),
    ]

    max_count = 1  # Only one homepage


class StandardPage(Page):
    """Standard content page for general content"""

    intro = models.CharField(
        max_length=500,
        blank=True,
        help_text="Brief introduction or tagline"
    )

    body = RichTextField(
        blank=True,
        help_text="Main page content"
    )

    hero_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Hero background image (1920x600px recommended)"
    )

    content_panels = Page.content_panels + [
        FieldPanel('intro'),
        FieldPanel('body'),
        FieldPanel('hero_image'),
    ]

    def get_template(self, request, *args, **kwargs):
        if self.slug == 'about':
            return 'home/about_page.html'
        if self.slug == 'industries':
            return 'home/industries_landing.html'
        if self.slug == 'equipment':
            return 'home/equipment_landing.html'
        return super().get_template(request, *args, **kwargs)

    def get_context(self, request, *args, **kwargs):
        context = super().get_context(request, *args, **kwargs)
        if self.slug == 'equipment':
            from catalog.models import Product, Manufacturer
            from django.db.models import Count, Min, Max

            manufacturers = Manufacturer.objects.select_related(
                'profile', 'profile__logo'
            ).filter(profile__status=Manufacturer.ENABLED).annotate(
                product_count=Count('products', filter=models.Q(products__is_active__gte=0))
            ).order_by('-product_count')

            stats = Product.objects.published().aggregate(
                total=Count('id'),
                min_price=Min('price'),
                max_price=Max('price'),
            )

            featured_products = list(
                Product.objects.published()
                .filter(price__isnull=False)
                .select_related('manufacturer', 'manufacturer__profile', 'manufacturer__profile__logo')
                .order_by('?')[:8]
            )

            context['manufacturers'] = manufacturers
            context['stats'] = stats
            context['featured_products'] = featured_products
            context['total_manufacturers'] = Manufacturer.objects.count()
        return context


class EquipmentCategoryPage(Page):
    """Page for equipment categories"""

    intro = models.CharField(
        max_length=500,
        blank=True,
        help_text="Brief introduction to this equipment category"
    )

    description = RichTextField(
        blank=True,
        help_text="Detailed description of equipment category"
    )

    key_features = RichTextField(
        blank=True,
        help_text="Key features and benefits"
    )

    typical_applications = RichTextField(
        blank=True,
        help_text="Typical applications and use cases"
    )

    hero_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Hero background image (1920x600px recommended)"
    )

    content_panels = Page.content_panels + [
        FieldPanel('intro'),
        FieldPanel('description'),
        FieldPanel('key_features'),
        FieldPanel('typical_applications'),
        FieldPanel('hero_image'),
    ]

    subpage_types = ['home.EquipmentPage']
    parent_page_types = ['home.HomePage', 'home.StandardPage']


class EquipmentPage(Page):
    """Individual equipment item page"""

    subtitle = models.CharField(
        max_length=255,
        blank=True,
        help_text="Equipment subtitle or model info"
    )

    description = RichTextField(
        help_text="Full equipment description"
    )

    specifications = RichTextField(
        blank=True,
        help_text="Technical specifications"
    )

    applications = RichTextField(
        blank=True,
        help_text="Applications and use cases"
    )

    industries = RichTextField(
        blank=True,
        help_text="Industries this equipment serves"
    )

    hero_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Hero background image (1920x600px recommended)"
    )

    content_panels = Page.content_panels + [
        FieldPanel('subtitle'),
        FieldPanel('description'),
        FieldPanel('specifications'),
        FieldPanel('applications'),
        FieldPanel('industries'),
        FieldPanel('hero_image'),
    ]

    parent_page_types = ['home.EquipmentCategoryPage', 'home.StandardPage']


class IndustryPage(Page):
    """Page for industries served"""

    intro = models.CharField(
        max_length=500,
        blank=True,
        help_text="Brief industry introduction"
    )

    description = RichTextField(
        help_text="Detailed industry description"
    )

    expertise = RichTextField(
        blank=True,
        help_text="Our expertise in this industry"
    )

    equipment_provided = RichTextField(
        blank=True,
        help_text="Types of equipment we provide to this industry"
    )

    client_examples = RichTextField(
        blank=True,
        help_text="Example clients or projects"
    )

    hero_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Hero background image (1920x600px recommended)"
    )

    content_panels = Page.content_panels + [
        FieldPanel('intro'),
        FieldPanel('description'),
        FieldPanel('expertise'),
        FieldPanel('equipment_provided'),
        FieldPanel('client_examples'),
        FieldPanel('hero_image'),
    ]

    parent_page_types = ['home.HomePage', 'home.StandardPage']


class ContactPage(Page):
    """Contact page with simple form"""

    intro = models.CharField(
        max_length=500,
        default="Get in touch with our supply chain engineering team",
        help_text="Contact page introduction"
    )

    body = RichTextField(
        blank=True,
        help_text="Additional contact information"
    )

    phone = models.CharField(
        max_length=50,
        blank=True,
        help_text="Contact phone number"
    )

    email = models.EmailField(
        blank=True,
        help_text="Contact email"
    )

    address = RichTextField(
        blank=True,
        help_text="Physical address"
    )

    hero_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text="Hero background image (1920x600px recommended)"
    )

    content_panels = Page.content_panels + [
        FieldPanel('intro'),
        FieldPanel('body'),
        FieldPanel('phone'),
        FieldPanel('email'),
        FieldPanel('address'),
        FieldPanel('hero_image'),
    ]

    max_count = 1
    parent_page_types = ['home.HomePage']


class ContactSubmission(models.Model):
    """Store contact form submissions"""
    name = models.CharField(max_length=255)
    email = models.EmailField()
    phone = models.CharField(max_length=50, blank=True)
    organization = models.CharField(max_length=255, blank=True)
    message = models.TextField()
    submitted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-submitted_at']
        verbose_name = 'Contact Submission'
        verbose_name_plural = 'Contact Submissions'

    def __str__(self):
        return f"{self.name} - {self.email} ({self.submitted_at.strftime('%Y-%m-%d %H:%M')})"


# =============================================================================
# NSN (National Stock Number) Models
# =============================================================================

class FederalSupplyClass(models.Model):
    """4-digit Federal Supply Classification (FSC)"""
    code = models.CharField(max_length=4, unique=True, db_index=True)
    name = models.CharField(max_length=255)
    group = models.CharField(max_length=2, db_index=True)  # First 2 digits (FSG)
    group_name = models.CharField(max_length=255, blank=True)

    class Meta:
        verbose_name = "Federal Supply Class"
        verbose_name_plural = "Federal Supply Classes"
        ordering = ['code']

    def __str__(self):
        return f"{self.code} - {self.name}"
