from django.shortcuts import redirect
from django.contrib import messages
from django.core.mail import send_mail
from django.conf import settings
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_protect
from django_ratelimit.decorators import ratelimit
from ..models import ContactSubmission
import logging

logger = logging.getLogger(__name__)


@ratelimit(key='ip', rate='5/h', method='POST', block=True)
@csrf_protect
@require_POST
def contact_form_submit(request):
    """Handle contact form submission"""

    name = request.POST.get('name', '').strip()
    email = request.POST.get('email', '').strip()
    phone = request.POST.get('phone', '').strip()
    organization = request.POST.get('organization', '').strip()
    message = request.POST.get('message', '').strip()

    nsn = request.POST.get('nsn', '').strip()
    item_name = request.POST.get('item_name', '').strip()

    if not name or not email:
        messages.error(request, 'Please fill in all required fields.')
        return redirect(request.META.get('HTTP_REFERER', '/contact/'))

    try:
        submission = ContactSubmission.objects.create(
            name=name,
            email=email,
            phone=phone,
            organization=organization,
            message=message
        )
        logger.info("Contact submission saved: %s (%s) [id=%d]", name, email, submission.pk)

        if nsn:
            subject = f'NSN Quote Request - {nsn} - {name}'
            email_message = f"""
New NSN Quote Request from malla-ts.com:

NSN: {nsn}
Item: {item_name}

---

Name: {name}
Email: {email}
Phone: {phone}
Organization: {organization}

Message:
{message if message else '(No additional message)'}

---
Submitted at: {submission.submitted_at.strftime('%Y-%m-%d %H:%M:%S')}
View item: https://www.malla-ts.com/nsn/{nsn}/
"""
        else:
            subject = f'New Contact Form Submission - {name}'
            email_message = f"""
New contact form submission from malla-ts.com:

Name: {name}
Email: {email}
Phone: {phone}
Organization: {organization}

Message:
{message}

---
Submitted at: {submission.submitted_at.strftime('%Y-%m-%d %H:%M:%S')}
"""

        try:
            logger.info("Sending email: to=info@malla-ts.com from=%s subject='%s'",
                        settings.DEFAULT_FROM_EMAIL, subject)
            send_mail(
                subject=subject,
                message=email_message,
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=['info@malla-ts.com'],
                fail_silently=False,
            )
            logger.info("Email sent successfully for submission id=%d", submission.pk)
        except Exception as email_error:
            logger.error("Email sending failed for submission id=%d: %s",
                         submission.pk, email_error, exc_info=True)
            messages.success(request, 'Thank you for contacting us! We will get back to you shortly.')
            return redirect(request.META.get('HTTP_REFERER', '/contact/'))

        messages.success(request, 'Thank you for contacting us! We will get back to you shortly.')

    except Exception as e:
        logger.error("Contact form error: %s", e, exc_info=True)
        messages.error(request, 'An error occurred. Please try again later.')

    return redirect(request.META.get('HTTP_REFERER', '/contact/'))
