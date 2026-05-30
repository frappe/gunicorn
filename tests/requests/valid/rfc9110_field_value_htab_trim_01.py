#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9110 section 5.5: OWS around field-value is optional and not part
# of the value; leading and trailing HTAB must be stripped.
request = {
    "method": "GET",
    "uri": uri("/foo"),
    "version": (1, 1),
    "headers": [
        ("HOST", "example.com"),
        ("X-VALUE", "abc"),
    ],
    "body": b"",
}
