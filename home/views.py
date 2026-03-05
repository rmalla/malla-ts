# Views are organized into modules under home/views_pkg/
# This file re-exports them for backward compatibility with urls.py imports.
from .views_pkg import contact_form_submit, nsn_search, nsn_detail, nsn_fsc_list  # noqa: F401
from .views_pkg import product_list, product_detail, product_redirect, manufacturer_list, manufacturer_detail  # noqa: F401
