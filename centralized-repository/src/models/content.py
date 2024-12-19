from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime

@dataclass
class UnifiedContent:
    title: str
    authors: List[str]
    date: Optional[datetime]
    abstract: str
    url: str
    source: str  # 'website', 'knowhub', or 'orcid'
    content_type: str
    keywords: List[str]
    doi: str = ''
    handle: str = ''
    journal: str = ''
    external_ids: Dict[str, str] = None
    affiliations: List[str] = None
    full_text: str = ''
    image_url: str = ''
    orcid_id: str = ''
    document_type: str = ''  # Added this field