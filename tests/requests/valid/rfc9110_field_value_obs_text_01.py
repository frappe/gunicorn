#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9110 section 5.5: field-vchar = VCHAR / obs-text (0x80-0xFF).
# Value carries two obs-text bytes 0xC3 0xA9 (UTF-8 "e"-acute), stored
# as latin-1 per the WSGI environ convention.
request = {
    "method": "GET",
    "uri": uri("/foo"),
    "version": (1, 1),
    "headers": [
        ("HOST", "example.com"),
        ("X-VALUE", "caf\u00c3\u00a9"),
    ],
    "body": b"",
}
