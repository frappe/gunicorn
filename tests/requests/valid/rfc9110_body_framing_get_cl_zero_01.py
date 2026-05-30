#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9110 section 8.6: Content-Length: 0 on GET is valid.
request = {
    "method": "GET",
    "uri": uri("/foo"),
    "version": (1, 1),
    "headers": [
        ("HOST", "example.com"),
        ("CONTENT-LENGTH", "0"),
    ],
    "body": b"",
}
