"""Bounded public HTTPS page read: pinned DNS, no redirects or authentication."""
import asyncio
from html.parser import HTMLParser
import http.client
import ipaddress
import socket
import ssl
from urllib.parse import urlsplit


class PageUnavailable(RuntimeError):
    pass


class _Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.hidden = 0
        self.published = None

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'noscript'}:
            self.hidden += 1
        if tag == 'meta':
            a = dict(attrs)
            if a.get('property') == 'article:published_time':
                self.published = a.get('content', '')[:40]

    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'noscript'}:
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden and data.strip():
            self.parts.append(data.strip())


class _PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self, host, address):
        super().__init__(host, timeout=12, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        raw = socket.create_connection((self.address, 443), timeout=self.timeout)
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


def _read(url):
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
        or parsed.port not in (None, 443) or len(url) > 1500):
        raise PageUnavailable('invalid_url')
    host = parsed.hostname.encode('idna').decode()
    addresses = {r[4][0] for r in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    if not addresses or any(not ipaddress.ip_address(a).is_global for a in addresses):
        raise PageUnavailable('non_public_host')
    connection = _PinnedHTTPS(host, sorted(addresses)[0])
    try:
        path = parsed.path or '/'
        if parsed.query:
            path += '?' + parsed.query
        connection.request('GET', path, headers={'Accept': 'text/html,text/plain', 'User-Agent': 'ANODHAS-LeadResearch/1.0'})
        response = connection.getresponse()
        if response.status != 200:
            raise PageUnavailable('http_' + str(response.status))
        if response.getheader('Content-Type', '').split(';')[0] not in {'text/html', 'text/plain', 'application/xhtml+xml'}:
            raise PageUnavailable('content_type')
        data = response.read(500001)
        if len(data) > 500000:
            raise PageUnavailable('page_too_large')
        parser = _Text()
        parser.feed(data.decode('utf-8', errors='replace'))
        return '\n'.join(parser.parts)[:16000], parser.published
    finally:
        connection.close()


async def read_public_page(url):
    return await asyncio.to_thread(_read, url)
