#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

"""ASGI PROXY protocol parser tests.

Covers the validation gaps that the WSGI parser already enforces:
- v1 TCP4/TCP6 addresses must be valid IP addresses (inet_pton).
- v2 must reject non-STREAM (UDP) protocols when family is INET/INET6.
"""

import struct

import pytest

from gunicorn.asgi.parser import (
    PythonProtocol,
    PP_V2_SIGNATURE,
    InvalidProxyLine,
    InvalidProxyHeader,
)


class TestProxyV1AddressValidation:
    """v1 must validate IPv4/IPv6 source/destination addresses."""

    def test_v1_invalid_ipv4_source_rejected(self):
        parser = PythonProtocol(proxy_protocol='v1')
        with pytest.raises(InvalidProxyLine):
            parser.feed(b"PROXY TCP4 not-an-ip 192.168.0.1 1 2\r\n")

    def test_v1_invalid_ipv4_destination_rejected(self):
        parser = PythonProtocol(proxy_protocol='v1')
        with pytest.raises(InvalidProxyLine):
            parser.feed(b"PROXY TCP4 192.168.0.1 999.999.999.999 1 2\r\n")

    def test_v1_invalid_ipv6_source_rejected(self):
        parser = PythonProtocol(proxy_protocol='v1')
        with pytest.raises(InvalidProxyLine):
            parser.feed(b"PROXY TCP6 not::an::ip ::1 1 2\r\n")

    def test_v1_valid_ipv4_accepted(self):
        parser = PythonProtocol(proxy_protocol='v1')
        parser.feed(b"PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n")
        assert parser.proxy_protocol_info['client_addr'] == '192.168.0.1'
        assert parser.proxy_protocol_info['proxy_protocol'] == 'TCP4'


class TestProxyV2NonStreamRejected:
    """v2 must reject DGRAM (UDP) when family is INET or INET6."""

    @staticmethod
    def _v2_header(fam_proto, addr_payload):
        ver_cmd = 0x21  # version 2, command PROXY
        length = len(addr_payload)
        header = struct.pack('>BBH', ver_cmd, fam_proto, length)
        return PP_V2_SIGNATURE + header + addr_payload

    def test_v2_inet_dgram_rejected(self):
        # family=0x10 (INET), protocol=0x02 (DGRAM)
        fam_proto = 0x12
        addr_payload = b'\x01\x02\x03\x04\x05\x06\x07\x08' + b'\x00\x50\x01\xbb'
        data = self._v2_header(fam_proto, addr_payload)
        parser = PythonProtocol(proxy_protocol='v2')
        with pytest.raises(InvalidProxyHeader):
            parser.feed(data)

    def test_v2_inet6_dgram_rejected(self):
        # family=0x20 (INET6), protocol=0x02 (DGRAM)
        fam_proto = 0x22
        addr_payload = b'\x00' * 32 + b'\x00\x50\x01\xbb'
        data = self._v2_header(fam_proto, addr_payload)
        parser = PythonProtocol(proxy_protocol='v2')
        with pytest.raises(InvalidProxyHeader):
            parser.feed(data)

    def test_v2_inet_stream_accepted(self):
        # family=0x10 (INET), protocol=0x01 (STREAM)
        fam_proto = 0x11
        addr_payload = b'\x01\x02\x03\x04\x05\x06\x07\x08' + b'\x00\x50\x01\xbb'
        data = self._v2_header(fam_proto, addr_payload)
        parser = PythonProtocol(proxy_protocol='v2')
        # Followed by an HTTP request so the parser can transition out of
        # the proxy_protocol state without hanging on more data.
        parser.feed(data + b"GET / HTTP/1.1\r\nHost: e\r\n\r\n")
        assert parser.proxy_protocol_info['proxy_protocol'] == 'TCP4'
