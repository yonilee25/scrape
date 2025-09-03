import requests
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree as ET
from libs.contracts.models import DiscoveryItem
from datetime import datetime

# Very minimal sitemap crawler: for a given domain, fetch /sitemap.xml and read <url><loc> entries.

def fetch_sitemap_urls(domain: str):
    urls = []
    base = f"https://{domain}"
    for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml"]:
        try:
            resp = requests.get(urljoin(base, path), timeout=15)
            if resp.status_code == 200 and resp.text.strip().startswith("<"):
                urls.append(urljoin(base, path))
                break
        except Exception:
            continue
    return urls

def parse_sitemap(url: str, person: str, limit: int = 50):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        items = []
        # If it's a sitemapindex
        if root.tag.endswith("sitemapindex"):
            for sm in root.findall(".//sm:sitemap", ns):
                loc = sm.findtext("sm:loc", default="", namespaces=ns)
                if loc:
                    items.extend(parse_sitemap(loc, person, limit=limit))
                    if len(items) >= limit:
                        break
        else:
            # urlset
            for u in root.findall(".//sm:url", ns):
                loc = u.findtext("sm:loc", default="", namespaces=ns)
                if not loc:
                    continue
                if person.lower() in loc.lower():
                    lastmod = u.findtext("sm:lastmod", default="", namespaces=ns)
                    items.append(DiscoveryItem(url=loc, kind="webpage", source="sitemap", title=loc, published_at=lastmod or None, confidence=0.55))
                if len(items) >= limit:
                    break
        return items[:limit]
    except Exception:
        return []

def discover(person: str, domains: list[str], max_results: int = 25):
    out = []
    for d in domains:
        sm_urls = fetch_sitemap_urls(d)
        for sm in sm_urls:
            out.extend(parse_sitemap(sm, person, limit=max_results))
            if len(out) >= max_results:
                return out[:max_results]
    return out[:max_results]
