
import csv
import os
from pathlib import Path
from typing import List, Dict
from ..models.content import UnifiedContent
from datetime import datetime
import logging

class ContentExporter:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def export_to_csv(self, items: List[UnifiedContent], filename: str) -> Path:
        filepath = self.output_dir / filename
        
        fieldnames = [
            'source', 'content_type', 'title', 'authors', 'date',
            'abstract', 'keywords', 'doi', 'url', 'handle', 'journal',
            'external_ids', 'affiliations', 'full_text', 'image_url',
            'orcid_id'
        ]
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for item in items:
                    row = {
                        'source': item.source,
                        'content_type': item.content_type,
                        'title': item.title,
                        'authors': '; '.join(item.authors),
                        'date': item.date.strftime('%Y-%m-%d') if item.date else '',
                        'abstract': item.abstract,
                        'keywords': '; '.join(item.keywords),
                        'doi': item.doi,
                        'url': item.url,
                        'handle': item.handle,
                        'journal': item.journal,
                        'external_ids': '; '.join([f"{k}:{v}" for k, v in (item.external_ids or {}).items()]),
                        'affiliations': '; '.join(item.affiliations or []),
                        'full_text': item.full_text,
                        'image_url': item.image_url,
                        'orcid_id': item.orcid_id
                    }
                    writer.writerow(row)
                
            self.logger.info(f"Successfully exported {len(items)} items to {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {str(e)}")
            raise
