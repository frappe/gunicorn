#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9110 section 5.6.2: token = 1*tchar; tchar includes !#$%&'*+-.^_`|~
# and alphanumerics. Dot, pipe, and other specials are legal in field-names.
request = {
    "method": "GET",
    "uri": uri("/foo"),
    "version": (1, 1),
    "headers": [
        ("HOST", "example.com"),
        ("X.CUSTOM|PIPE", "ok"),
    ],
    "body": b"",
}
