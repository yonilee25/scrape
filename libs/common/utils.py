from urllib.parse import urlparse
from urllib import robotparser
import time, random
import requests

# trusted domains you don't want to skip if robots parsing is flaky/over-strict
ALWAYS_ALLOW = {
    "en.wikipedia.org", "www.reuters.com", "apnews.com",
    "www.cnn.com", "www.bbc.com", "www.nytimes.com", "www.wired.com",
    "www.cnbc.com", "www.theguardian.com", "www.axios.com"
}

# (optional) be nice: tiny per-domain throttle
DOMAIN_DELAY = {
    "en.wikipedia.org": 0.8,
    "www.reuters.com": 1.0,
    "apnews.com":       0.8,
}

def format_status(status: str) -> str:
    badge = f"<span class='badge'>{status}</span>"
    return badge

def robots_allows(url: str, user_agent: str = "DeepResearchBot") -> bool:
    """
    Return True if we're allowed to fetch `url` for `user_agent`.
    For a small allowlist of high-signal domains, override to True,
    but still throttle politely.
    """
    try:
        host = urlparse(url).netloc.lower()

        # 1) Trusted allowlist override (prevents 'false negative' blocks)
        if host in ALWAYS_ALLOW:
            # polite, tiny delay to avoid hammering
            if host in DOMAIN_DELAY:
                time.sleep(DOMAIN_DELAY[host] + random.uniform(0, 0.2))
            return True

        # 2) Normal robots.txt check
        scheme = urlparse(url).scheme or "https"
        robots_url = f"{scheme}://{host}/robots.txt"
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()

        # If robots fetch returns something weird and robotparser has no rules,
        # treat it as allow to avoid accidental blanket blocks.
        # (robotparser can leave all flags False/None on some error codes)
        if getattr(rp, "default_entry", None) is None and not rp.allow_all and not rp.disallow_all:
            return True

        return rp.can_fetch(user_agent, url)

    except Exception:
        # Be permissive on network/parse errors (your code already chose this)
        return True
    
#def robots_allows(url: str, user_agent: str = "DeepResearchBot"):
#    try:
#        parsed = urlparse(url)
#        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
#        rp = robotparser.RobotFileParser()
#        rp.set_url(robots_url)
#        rp.read()
#        return rp.can_fetch(user_agent, url)
#    except Exception:
#        return True  # be permissive if robots is unreachable, you can flip this to False
