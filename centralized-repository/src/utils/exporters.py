import pandas as pd
from typing import List, Set
import os
from datetime import datetime

class ContentExporter:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = os.path.join(os.getcwd(), output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        self.seen_dois = set()
        self.seen_titles = set()

    def initialize_from_openalex(self, openalex_df: pd.DataFrame):
        """Initialize seen DOIs and titles from OpenAlex data"""
        self.seen_dois.update(set(openalex_df['DOI'].dropna().str.lower()))
        self.seen_titles.update(set(openalex_df['Title'].dropna().str.lower()))

    def is_duplicate(self, doi: str = None, title: str = None) -> bool:
        """Check if content is duplicate based on DOI or title"""
        if doi and doi.lower() in self.seen_dois:
            return True
        if title and title.lower() in self.seen_titles:
            return True
        return False

    def add_content(self, doi: str = None, title: str = None):
        """Add content identifiers to seen sets"""
        if doi:
            self.seen_dois.add(doi.lower())
        if title:
            self.seen_titles.add(title.lower())

    def export_to_csv(self, content_list: List, filename: str) -> str:
        """Export content to CSV, handling duplicates"""
        unique_content = []
        
        for content in content_list:
            if not self.is_duplicate(content.doi, content.title):
                unique_content.append({
                    'source': content.source,
                    'title': content.title,
                    'authors': '; '.join(content.authors),
                    'date': content.date.strftime('%Y-%m-%d') if content.date else '',
                    'abstract': content.abstract,
                    'doi': content.doi,
                    'url': content.url,
                    'content_type': content.content_type,
                    'keywords': '; '.join(content.keywords),
                    'journal': content.journal,
                    'document_type': content.document_type,
                    'handle': content.handle,
                    'orcid_id': content.orcid_id
                })
                self.add_content(content.doi, content.title)

        if unique_content:
            filepath = os.path.join(self.output_dir, filename)
            df = pd.DataFrame(unique_content)
            df.to_csv(filepath, index=False)
            return filepath
        return None

    def merge_all_sources(self, openalex_df: pd.DataFrame, additional_content: List) -> pd.DataFrame:
        """Merge OpenAlex data with additional content"""
        # Convert additional content to DataFrame
        additional_df = pd.DataFrame([{
            'source': content.source,
            'title': content.title,
            'authors': '; '.join(content.authors),
            'date': content.date.strftime('%Y-%m-%d') if content.date else '',
            'abstract': content.abstract,
            'doi': content.doi,
            'url': content.url,
            'content_type': content.content_type,
            'keywords': '; '.join(content.keywords),
            'journal': content.journal,
            'document_type': content.document_type,
            'handle': content.handle,
            'orcid_id': content.orcid_id
        } for content in additional_content])

        # Combine DataFrames
        combined_df = pd.concat([openalex_df, additional_df], ignore_index=True)
        
        # Remove duplicates based on DOI and title
        combined_df['title_lower'] = combined_df['Title'].str.lower()
        combined_df['doi_lower'] = combined_df['DOI'].str.lower()
        combined_df = combined_df.drop_duplicates(subset=['doi_lower', 'title_lower'], keep='first')
        combined_df = combined_df.drop(['title_lower', 'doi_lower'], axis=1)
        
        return combined_df