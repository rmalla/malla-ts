from django import forms


class ContactForm(forms.Form):
    """Contact form with server-side validation."""
    name = forms.CharField(
        max_length=255,
        widget=forms.TextInput(attrs={
            'placeholder': 'Your full name',
            'id': 'name',
        })
    )
    email = forms.EmailField(
        widget=forms.EmailInput(attrs={
            'placeholder': 'your@email.com',
            'id': 'email',
        })
    )
    phone = forms.CharField(
        max_length=50,
        required=False,
        widget=forms.TextInput(attrs={
            'placeholder': 'Phone number',
            'id': 'phone',
        })
    )
    organization = forms.CharField(
        max_length=255,
        required=False,
        widget=forms.TextInput(attrs={
            'placeholder': 'Company / Organization',
            'id': 'organization',
        })
    )
    subject = forms.CharField(
        max_length=255,
        required=False,
        widget=forms.Select(choices=[
            ('', 'Select a subject...'),
            ('quote', 'Request a Quote'),
            ('equipment', 'Equipment Inquiry'),
            ('nsn', 'NSN / Part Number Inquiry'),
            ('partnership', 'Partnership Opportunity'),
            ('general', 'General Inquiry'),
        ], attrs={'id': 'subject'})
    )
    message = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'rows': 5,
            'placeholder': 'How can we help?',
            'id': 'message',
        })
    )

    # Hidden fields for NSN quote requests
    nsn = forms.CharField(max_length=16, required=False, widget=forms.HiddenInput())
    item_name = forms.CharField(max_length=255, required=False, widget=forms.HiddenInput())
