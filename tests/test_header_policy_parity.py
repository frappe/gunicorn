#
# This file is part of gunicorn released under the MIT license.
# See the NOTICE for more information.

"""Parity tests for WSGI header policy across Python and fast parsers.

These checks ensure that Expect, secure_scheme_headers, forwarder_headers,
and the forwarded_allow_ips trust gate are enforced identically regardless
of the parser implementation selected by ``http_parser``.
"""

import sys

import pytest

from gunicorn.config import Config
from gunicorn.http.parser import RequestParser
from gunicorn.http.errors import (
    ExpectationFailed,
    InvalidHeaderName,
    InvalidSchemeHeaders,
)


def _parse(raw, cfg, peer_addr):
    parser = RequestParser(cfg, iter([raw]), peer_addr)
    return next(iter(parser))


def _cfg(http_parser, **overrides):
    cfg = Config()
    cfg.set("http_parser", http_parser)
    for k, v in overrides.items():
        cfg.set(k, v)
    return cfg


@pytest.fixture(params=["python", "fast"])
def parser_name(request):
    if request.param == "fast":
        if hasattr(sys, "pypy_version_info"):
            pytest.skip("gunicorn_h1c not supported on PyPy")
        gunicorn_h1c = pytest.importorskip("gunicorn_h1c")
        if not hasattr(gunicorn_h1c.H1CProtocol, "asgi_headers"):
            pytest.skip("gunicorn_h1c >= 0.6.2 required")
    return request.param


class TestExpectPolicy:
    def test_expect_100_continue_sets_flag(self, parser_name):
        cfg = _cfg(parser_name)
        raw = (
            b"POST / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"Content-Length: 0\r\n"
            b"Expect: 100-continue\r\n"
            b"\r\n"
        )
        req = _parse(raw, cfg, ("127.0.0.1", 1234))
        assert req._expected_100_continue is True

    def test_expect_unknown_value_rejected(self, parser_name):
        cfg = _cfg(parser_name)
        raw = (
            b"POST / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"Content-Length: 0\r\n"
            b"Expect: bogus-extension\r\n"
            b"\r\n"
        )
        with pytest.raises(ExpectationFailed):
            _parse(raw, cfg, ("127.0.0.1", 1234))

    def test_expect_ignored_in_http10(self, parser_name):
        cfg = _cfg(parser_name)
        raw = (
            b"POST / HTTP/1.0\r\n"
            b"Host: example.com\r\n"
            b"Content-Length: 0\r\n"
            b"Expect: 100-continue\r\n"
            b"\r\n"
        )
        req = _parse(raw, cfg, ("127.0.0.1", 1234))
        assert req._expected_100_continue is False


class TestSecureSchemeHeaders:
    def test_trusted_peer_promotes_https(self, parser_name):
        cfg = _cfg(
            parser_name,
            forwarded_allow_ips="127.0.0.1",
            secure_scheme_headers={"X-FORWARDED-PROTO": "https"},
        )
        raw = (
            b"GET / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"X-Forwarded-Proto: https\r\n"
            b"\r\n"
        )
        req = _parse(raw, cfg, ("127.0.0.1", 1234))
        assert req.scheme == "https"

    def test_untrusted_peer_keeps_http(self, parser_name):
        cfg = _cfg(
            parser_name,
            forwarded_allow_ips="127.0.0.1",
            secure_scheme_headers={"X-FORWARDED-PROTO": "https"},
        )
        raw = (
            b"GET / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"X-Forwarded-Proto: https\r\n"
            b"\r\n"
        )
        req = _parse(raw, cfg, ("203.0.113.5", 1234))
        assert req.scheme == "http"

    def test_conflicting_scheme_headers_rejected(self, parser_name):
        cfg = _cfg(
            parser_name,
            forwarded_allow_ips="127.0.0.1",
            secure_scheme_headers={
                "X-FORWARDED-PROTO": "https",
                "X-FORWARDED-SSL": "on",
            },
        )
        raw = (
            b"GET / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"X-Forwarded-Proto: https\r\n"
            b"X-Forwarded-Ssl: off\r\n"
            b"\r\n"
        )
        with pytest.raises(InvalidSchemeHeaders):
            _parse(raw, cfg, ("127.0.0.1", 1234))


class TestForwarderTrustGate:
    def test_untrusted_peer_underscore_header_rejected(self, parser_name):
        cfg = _cfg(
            parser_name,
            forwarded_allow_ips="127.0.0.1",
            forwarder_headers="SCRIPT_NAME",
            header_map="refuse",
        )
        raw = (
            b"GET / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"Script_Name: /evil\r\n"
            b"\r\n"
        )
        with pytest.raises(InvalidHeaderName):
            _parse(raw, cfg, ("203.0.113.5", 1234))

    def test_trusted_peer_underscore_header_accepted(self, parser_name):
        cfg = _cfg(
            parser_name,
            forwarded_allow_ips="127.0.0.1",
            forwarder_headers="SCRIPT_NAME",
        )
        raw = (
            b"GET / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"Script_Name: /api\r\n"
            b"\r\n"
        )
        req = _parse(raw, cfg, ("127.0.0.1", 1234))
        names = {n for n, _ in req.headers}
        assert "SCRIPT_NAME" in names

    def test_header_map_drop_silences_underscore(self, parser_name):
        cfg = _cfg(
            parser_name,
            forwarded_allow_ips="127.0.0.1",
            header_map="drop",
        )
        raw = (
            b"GET / HTTP/1.1\r\n"
            b"Host: example.com\r\n"
            b"Stray_Name: x\r\n"
            b"\r\n"
        )
        req = _parse(raw, cfg, ("203.0.113.5", 1234))
        names = {n for n, _ in req.headers}
        assert "STRAY_NAME" not in names
