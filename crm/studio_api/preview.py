"""Inert, owner-only preview of allowlisted public build documents.

No fetch/proxy of arbitrary URLs. Scripts, forms, navigation and API calls never
enter the preview document. Browser sandbox and CSP add independent boundaries.
"""
from html import escape
from html.parser import HTMLParser
from pathlib import Path
import re
from .service import Rejected

PAGES = {'/':'index.html', '/cases/studio-assistant/':'cases/studio-assistant/index.html',
    '/cases/master-crm/':'cases/master-crm/index.html', '/cases/owner-music/':'cases/owner-music/index.html'}
TAGS = frozenset('div span main header footer section article aside nav h1 h2 h3 h4 h5 h6 p strong b em i small br hr ul ol li dl dt dd figure figcaption img button input textarea label details summary progress time table thead tbody tr th td svg path circle rect line polyline polygon g'.split())
VOID = {'br','hr','img','input'}
SKIP = {'script','style','iframe','object','embed','noscript','template'}
ATTRS = frozenset('class id title width height viewbox d fill stroke stroke-width stroke-linecap stroke-linejoin cx cy r x y x1 x2 y1 y2 rx points transform opacity role alt colspan rowspan datetime max value'.split())

def asset(value):
    return value in {'/brand.png','/assistant.png','/music.png','/portrait.png','/portrait-light.png',
        '/assets/master-crm-illustration.png','/assets/personal-vpn.png','/assets/vpn-protocols.png','/assets/ai-documents.png'} or bool(re.fullmatch(r'/_next/static/css/[A-Za-z0-9_.-]+\.css',value or ''))

class InertHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts=[]
        self.skip=[]
        self.body=False

    def handle_starttag(self,tag,attrs):
        if self.skip:
            if tag in SKIP and tag not in {'embed'}:
                self.skip.append(tag)
            return
        if tag in SKIP:
            if tag!='embed':
                self.skip.append(tag)
            return
        attributes=dict(attrs)
        if tag=='body':
            self.body=True
            return
        if tag=='link' and attributes.get('rel')=='stylesheet' and asset(attributes.get('href')) and attributes['href'].endswith('.css'):
            self.parts.append('<link rel="stylesheet" href="'+escape(attributes['href'],quote=True)+'">')
            return
        if not self.body:
            return
        if tag=='a':
            tag='span'
        if tag not in TAGS:
            return
        safe=[]
        for key,value in attrs:
            if value is None:
                continue
            if key in ATTRS or key.startswith('aria-') or key=='data-slot':
                safe.append((key,value))
            elif tag=='img' and key=='src' and asset(value) and not value.endswith('.css'):
                safe.append((key,value))
            elif key=='style' and not re.search(r'url|@|expression|\\',value,re.I):
                safe.append((key,value))
        if tag in {'button','input','textarea'}:
            safe.append(('disabled',''))
        self.parts.append('<'+tag+''.join(' '+key+'="'+escape(value,quote=True)+'"' for key,value in safe)+'>')

    def handle_endtag(self,tag):
        if self.skip:
            if tag==self.skip[-1]:
                self.skip.pop()
            return
        if tag=='body':
            self.body=False
            return
        if tag=='a':
            tag='span'
        if self.body and tag in TAGS and tag not in VOID:
            self.parts.append('</'+tag+'>')

    def handle_data(self,data):
        if self.body and not self.skip:
            self.parts.append(escape(data))

def render_preview(root, page, theme):
    if page not in PAGES or theme not in {'light','dark'}:
        raise Rejected('preview_route_not_allowed',422)
    base=Path(root).resolve()
    target=(base / PAGES[page]).resolve()
    if not target.is_relative_to(base) or not target.is_file():
        raise Rejected('preview_build_unavailable',503)
    if target.stat().st_size>2_000_000:
        raise Rejected('preview_build_unavailable',503)
    parser=InertHTML()
    parser.feed(target.read_text(encoding='utf-8'))
    return '<!doctype html><html lang="ru" data-theme="'+theme+'" class="'+('dark' if theme=='dark' else '')+'"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>ANODHAS — предпросмотр</title></head><body>'+''.join(parser.parts)+'</body></html>'
