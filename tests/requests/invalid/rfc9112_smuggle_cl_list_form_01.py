#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

# RFC 9112 section 6.3 allows Content-Length list form when all values
# match, but gunicorn takes the safer strict view and rejects any list
# form outright to avoid proxy/origin desync. PortSwigger HTTP Desync,
# CL list variant.
from gunicorn.http.errors import InvalidHeader
request = InvalidHeader
