import requests
from urllib.parse import urljoin
from libs.contracts.models import DiscoveryItem

# Query WordPress JSON API if enabled on a site (seed domains required).

def discover(person: str, domains: list[str], max_results: int = 20):
    out = []
    for d in domains:
        base = f"https://{d}"
        try:
            r = requests.get(urljoin(base, "/wp-json/wp/v2/search"), params={"search": person, "per_page": 10}, timeout=15)
            if r.status_code == 200:
                for it in r.json():
                    url = it.get("url") or it.get("link")
                    if not url:
                        continue
                    out.append(DiscoveryItem(url=url, kind="webpage", source="wordpress", title=it.get("title") or url, confidence=0.6))
                    if len(out) >= max_results:
                        return out
        except Exception:
            continue
    return out[:max_results]
