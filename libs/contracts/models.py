from pydantic import BaseModel, Field
from typing import Optional, Literal

class ResearchRequest(BaseModel):
    person: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    languages: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)

class DiscoveryItem(BaseModel):
    url: str
    kind: Literal["webpage","pdf","podcast","video","filing","rss"] = "webpage"
    source: str = "unknown"
    title: Optional[str] = None
    published_at: Optional[str] = None
    confidence: float = 0.5
