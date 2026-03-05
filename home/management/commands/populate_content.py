from django.core.management.base import BaseCommand
from wagtail.models import Page, Site
from home.models import (
    HomePage, StandardPage, EquipmentCategoryPage,
    EquipmentPage, IndustryPage, ContactPage
)


class Command(BaseCommand):
    help = 'Populates the Malla Technical Services website with initial content'

    def handle(self, *args, **options):
        self.stdout.write('Populating content...')

        # Get the root page
        root_page = Page.objects.get(id=1)

        # Delete ALL existing pages under root (except root itself)
        for page in root_page.get_children():
            self.stdout.write(f'Deleting existing page: {page.title}')
            page.delete()

        # Create Homepage
        home = HomePage(
            title="Malla Technical Services",
            slug="home",
            hero_title="Procurement Engineering Excellence",
            hero_subtitle="20+ Years of Supply Chain Expertise",
            hero_description="<p>Providing quality equipment procurement solutions to government facilities, national parks, energy sectors, and municipal services nationwide.</p>",
            intro_section="<p>At Malla Technical Services, we specialize exclusively in equipment procurement—connecting public sector organizations with the right equipment at competitive prices. We don't install or provide ancillary services; we focus on what we do best: sourcing and delivering quality equipment that meets your specifications and budget.</p>",
            show_in_menus=True,
            draft_title="Malla Technical Services"
        )
        root_page.add_child(instance=home)
        revision = home.save_revision()
        revision.publish()

        # Create or update site
        site = Site.objects.first()
        if site:
            site.root_page = home
            site.save()
        else:
            Site.objects.create(
                hostname='www.malla-ts.com',
                site_name='Malla Technical Services',
                root_page=home,
                is_default_site=True
            )

        self.stdout.write(self.style.SUCCESS('Created Homepage and Site'))

        # Create About Page
        about = StandardPage(
            title="About Us",
            slug="about",
            intro="Over 20 years of procurement engineering expertise serving the public sector",
            body="""
            <h2>Our Story</h2>
            <p>Malla Technical Services was founded with a singular focus: to provide exceptional equipment procurement services to government agencies, national parks, energy companies, and municipal organizations. With over two decades of experience in supply chain management and procurement engineering, we've built a reputation for reliability, competitive pricing, and deep industry knowledge.</p>

            <h2>What We Do</h2>
            <p>We specialize in <strong>equipment procurement only</strong>. Unlike other vendors who bundle installation and service contracts, we focus exclusively on sourcing and delivering the right equipment for your needs. This specialization allows us to:</p>
            <ul>
                <li>Maintain extensive supplier networks across all equipment categories</li>
                <li>Negotiate competitive pricing through volume relationships</li>
                <li>Provide expert guidance on equipment specifications and compatibility</li>
                <li>Navigate complex government procurement requirements with ease</li>
                <li>Ensure timely delivery and proper documentation</li>
            </ul>

            <h2>Our Expertise</h2>
            <p>Our team of procurement engineers brings deep technical knowledge across multiple industries. We understand the unique requirements of government facilities, the rugged demands of national parks equipment, the safety-critical nature of energy sector equipment, and the reliability requirements of municipal infrastructure.</p>

            <h2>Why Equipment Only?</h2>
            <p>By focusing solely on equipment procurement, we avoid conflicts of interest and can provide objective recommendations. We're not trying to upsell installation services or maintenance contracts—we simply want to ensure you get the right equipment at the best price.</p>

            <h2>Our Commitment</h2>
            <p>We're committed to transparency, competitive pricing, and exceptional service. Every project receives the attention of experienced procurement engineers who understand both the technical requirements and the procurement process.</p>
            """,
            show_in_menus=True
        )
        home.add_child(instance=about)
        about.save_revision().publish()

        self.stdout.write(self.style.SUCCESS('Created About Page'))

        # Create Equipment Category Page
        equipment = StandardPage(
            title="Equipment",
            slug="equipment",
            intro="Browse our equipment procurement capabilities across multiple categories",
            body="""
            <h2>Equipment Procurement Services</h2>
            <p>Malla Technical Services provides comprehensive equipment procurement across a wide range of categories. Our procurement engineers work with trusted suppliers nationwide to ensure you receive quality equipment at competitive prices.</p>

            <h3>What We Provide</h3>
            <ul>
                <li><strong>Industrial & Manufacturing Equipment</strong> - Machinery, tools, safety equipment</li>
                <li><strong>Facility & Maintenance Equipment</strong> - HVAC, electrical, plumbing supplies</li>
                <li><strong>Grounds & Parks Equipment</strong> - Landscaping, maintenance, visitor services</li>
                <li><strong>Energy Sector Equipment</strong> - Safety systems, monitoring equipment, tools</li>
                <li><strong>Municipal Infrastructure Equipment</strong> - Water/wastewater, transportation, public works</li>
                <li><strong>Office & Administrative Equipment</strong> - Furniture, technology, supplies</li>
                <li><strong>Safety & Security Equipment</strong> - PPE, monitoring systems, access control</li>
            </ul>

            <h3>Our Process</h3>
            <ol>
                <li><strong>Requirements Analysis</strong> - We review your specifications and requirements</li>
                <li><strong>Supplier Sourcing</strong> - We identify qualified suppliers and obtain competitive quotes</li>
                <li><strong>Recommendation</strong> - We present options with technical analysis</li>
                <li><strong>Procurement</strong> - We handle purchasing, documentation, and logistics</li>
                <li><strong>Delivery Coordination</strong> - We ensure timely delivery to your location</li>
            </ol>

            <p><a href="/contact/">Contact us</a> to discuss your equipment procurement needs.</p>
            """,
            show_in_menus=True
        )
        home.add_child(instance=equipment)
        equipment.save_revision().publish()

        self.stdout.write(self.style.SUCCESS('Created Equipment Page'))

        # Create Industries Parent Page
        industries = StandardPage(
            title="Industries",
            slug="industries",
            intro="Specialized procurement expertise across multiple sectors",
            body="""
            <h2>Industries We Serve</h2>
            <p>Our procurement engineers have deep experience serving diverse industries, each with unique requirements and regulations. We understand your industry's challenges and can navigate complex procurement processes efficiently.</p>
            """,
            show_in_menus=True
        )
        home.add_child(instance=industries)
        industries.save_revision().publish()

        self.stdout.write(self.style.SUCCESS('Created Industries Page'))

        # Create Industry Pages
        govt = IndustryPage(
            title="Government Facilities",
            slug="government",
            intro="Comprehensive equipment procurement for federal, state, and local government operations",
            description="""
            <p>We serve government facilities at all levels—federal, state, and local. Our procurement engineers understand the unique requirements of government procurement, including compliance, documentation, and competitive bidding processes.</p>
            """,
            expertise="""
            <h3>Our Government Procurement Expertise</h3>
            <ul>
                <li>Compliance with FAR and agency-specific regulations</li>
                <li>GSA Schedule and cooperative purchasing agreements</li>
                <li>Competitive bid analysis and vendor selection</li>
                <li>Documentation and audit trail requirements</li>
                <li>Small business and diversity supplier programs</li>
            </ul>
            """,
            equipment_provided="""
            <h3>Equipment Categories</h3>
            <ul>
                <li>Office furniture and equipment</li>
                <li>Facility maintenance equipment</li>
                <li>Security and access control systems</li>
                <li>HVAC and building systems</li>
                <li>IT and telecommunications equipment</li>
                <li>Specialized mission equipment</li>
            </ul>
            """,
            client_examples="""
            <p>We've successfully served federal agencies, state departments, county facilities, and municipal governments across the United States.</p>
            """
        )
        industries.add_child(instance=govt)
        govt.save_revision().publish()

        parks = IndustryPage(
            title="National Parks Service",
            slug="national-parks",
            intro="Specialized equipment for parks maintenance, conservation, and visitor services",
            description="""
            <p>National parks require durable, reliable equipment that can withstand challenging environments while minimizing environmental impact. We understand these unique requirements and source equipment specifically suited for parks operations.</p>
            """,
            expertise="""
            <h3>Parks-Specific Knowledge</h3>
            <ul>
                <li>Rugged equipment for remote locations</li>
                <li>Environmentally responsible product selection</li>
                <li>Seasonal equipment needs and procurement timing</li>
                <li>Visitor safety and experience equipment</li>
                <li>Conservation and wildlife management tools</li>
            </ul>
            """,
            equipment_provided="""
            <h3>Equipment We Provide</h3>
            <ul>
                <li>Grounds maintenance and landscaping equipment</li>
                <li>Trail maintenance tools and supplies</li>
                <li>Visitor services equipment</li>
                <li>Wildlife monitoring and conservation equipment</li>
                <li>Facility maintenance supplies</li>
                <li>Safety and emergency equipment</li>
            </ul>
            """,
            client_examples="""
            <p>We've equipped national parks, state parks, and recreation areas with reliable equipment that stands up to heavy use in challenging conditions.</p>
            """
        )
        industries.add_child(instance=parks)
        parks.save_revision().publish()

        energy = IndustryPage(
            title="Energy & Oil/Gas",
            slug="energy",
            intro="Industrial-grade equipment for energy production, distribution, and safety operations",
            description="""
            <p>The energy sector demands reliable, safety-critical equipment that meets rigorous standards. Our procurement engineers understand these requirements and work with suppliers who specialize in energy sector equipment.</p>
            """,
            expertise="""
            <h3>Energy Sector Expertise</h3>
            <ul>
                <li>Safety-critical equipment specifications</li>
                <li>Industry certification requirements (API, ASME, etc.)</li>
                <li>Hazardous environment equipment selection</li>
                <li>Compliance with energy regulations</li>
                <li>Emergency response equipment procurement</li>
            </ul>
            """,
            equipment_provided="""
            <h3>Equipment Categories</h3>
            <ul>
                <li>Safety and PPE equipment</li>
                <li>Monitoring and measurement instruments</li>
                <li>Valves, fittings, and components</li>
                <li>Maintenance and repair tools</li>
                <li>Emergency response equipment</li>
                <li>Facility equipment and supplies</li>
            </ul>
            """,
            client_examples="""
            <p>We serve oil and gas companies, power generation facilities, renewable energy projects, and transmission/distribution operations.</p>
            """
        )
        industries.add_child(instance=energy)
        energy.save_revision().publish()

        municipal = IndustryPage(
            title="Municipal Services",
            slug="municipal",
            intro="Equipment procurement for cities, counties, and local utility services",
            description="""
            <p>Municipal operations require dependable equipment to serve communities effectively. From water treatment to public works, we understand the diverse equipment needs of local government services.</p>
            """,
            expertise="""
            <h3>Municipal Procurement Experience</h3>
            <ul>
                <li>Local government procurement processes</li>
                <li>Multi-department equipment coordination</li>
                <li>Budget-conscious sourcing strategies</li>
                <li>Emergency procurement capabilities</li>
                <li>Equipment standardization programs</li>
            </ul>
            """,
            equipment_provided="""
            <h3>Municipal Equipment</h3>
            <ul>
                <li>Water and wastewater treatment equipment</li>
                <li>Public works and infrastructure equipment</li>
                <li>Parks and recreation equipment</li>
                <li>Transportation and fleet equipment</li>
                <li>Public safety equipment</li>
                <li>Facility maintenance supplies</li>
            </ul>
            """,
            client_examples="""
            <p>We've served cities, counties, special districts, and utility authorities throughout the United States.</p>
            """
        )
        industries.add_child(instance=municipal)
        municipal.save_revision().publish()

        self.stdout.write(self.style.SUCCESS('Created Industry Pages'))

        # Create Contact Page
        contact = ContactPage(
            title="Contact Us",
            slug="contact",
            intro="Get in touch with our procurement engineering team",
            body="""
            <p>Ready to discuss your equipment procurement needs? Our team of experienced procurement engineers is here to help.</p>
            <p>Whether you have a specific equipment requirement, need assistance with specifications, or want to discuss a larger procurement project, we're ready to assist.</p>
            """,
            phone="(555) 123-4567",
            email="info@malla-ts.com",
            address="<p>Malla Technical Services<br>123 Procurement Way<br>Suite 100<br>Business City, ST 12345</p>",
            show_in_menus=True
        )
        home.add_child(instance=contact)
        contact.save_revision().publish()

        self.stdout.write(self.style.SUCCESS('Created Contact Page'))

        self.stdout.write(self.style.SUCCESS('✓ Content population complete!'))
        self.stdout.write(self.style.SUCCESS('Visit https://www.malla-ts.com to see your site'))
