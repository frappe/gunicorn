#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9110 section 8.6: a GET with a non-zero Content-Length is
# "discouraged" but not forbidden; the body must be preserved.
request = {
    "method": "GET",
    "uri": uri("/foo"),
    "version": (1, 1),
    "headers": [
        ("HOST", "example.com"),
        ("CONTENT-LENGTH", "5"),
    ],
    "body": b"hello",
}
