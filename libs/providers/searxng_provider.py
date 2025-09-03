import requests
from libs.contracts.models import DiscoveryItem
from libs.common.config import Settings

settings = Settings()

def discover(person: str, max_results: int = 20):
    if not settings.SEARXNG_URL:
        return []
    try:
        r = requests.get(f"{settings.SEARXNG_URL}/search",
                         params={"q": f'"{person}"', "format": "json", "time_range": "year", "safesearch": 1},
                         timeout=20)
        r.raise_for_status()
        data = r.json()
        items = []
        for res in data.get("results", [])[:max_results]:
            url = res.get("url")
            if not url:
                continue
            items.append(DiscoveryItem(
                url=url,
                kind="webpage",
                source="searxng",
                title=res.get("title") or url,
                published_at=res.get("publishedDate"),
                confidence=0.6
            ))
        return items
    except Exception as e:
        return []
