#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9112 section 6.1: Content-Length is the only framing option for
# HTTP/1.0 bodies (chunked was added in HTTP/1.1).
request = {
    "method": "POST",
    "uri": uri("/foo"),
    "version": (1, 0),
    "headers": [
        ("HOST", "example.com"),
        ("CONTENT-LENGTH", "5"),
    ],
    "body": b"hello",
}
