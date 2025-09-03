from urllib.parse import urlparse
from urllib import robotparser
import requests

def format_status(status: str) -> str:
    badge = f"<span class='badge'>{status}</span>"
    return badge

def robots_allows(url: str, user_agent: str = "DeepResearchBot"):
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(user_agent, url)
    except Exception:
        return True  # be permissive if robots is unreachable, you can flip this to False
