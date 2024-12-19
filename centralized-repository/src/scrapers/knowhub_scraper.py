from ..models.content import UnifiedContent
from .base_scraper import BaseScraper
from bs4 import BeautifulSoup
from typing import List, Dict
from time import sleep
from datetime import datetime
import re

class KnowhubScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://knowhub.aphrc.org"
        # Change to just browse all items instead of a specific handle
        self.browse_url = f"{self.base_url}/browse"
        self.seen_handles = set()


    def _extract_metadata(self, soup: BeautifulSoup) -> Dict:
        """Extract all metadata from detailed page"""
        metadata = {
            'authors': [],
            'date': '',
            'abstract': '',
            'subjects': [],
            'doi': '',
            'journal': '',
            'type': ''
        }

        # Try multiple possible metadata table selectors
        metadata_tables = soup.select('table.detailtable, div.ds-table-responsive table')
        for table in metadata_tables:
            try:
                rows = table.select('tr')
                for row in rows:
                    try:
                        # Try multiple possible selectors for label and value
                        label = row.select_one('td.label-cell, td.label, th.label-cell')
                        value = row.select_one('td.word-break, td:not(.label-cell)')
                        
                        if not label or not value:
                            continue
                        
                        label_text = label.text.strip().lower()
                        value_text = value.text.strip()
                        
                        if 'author' in label_text or 'creator' in label_text:
                            authors = [a.strip() for a in value_text.split(';')]
                            metadata['authors'].extend(a for a in authors if a)
                        elif any(date_term in label_text for date_term in ['issued', 'date', 'published']):
                            if not metadata['date']:  # Only set if not already set
                                metadata['date'] = value_text
                        elif 'abstract' in label_text or 'description' in label_text:
                            if not metadata['abstract']:  # Only set if not already set
                                metadata['abstract'] = value_text
                        elif 'subject' in label_text or 'keyword' in label_text:
                            subjects = [s.strip() for s in value_text.split(';')]
                            metadata['subjects'].extend(s for s in subjects if s)
                        elif 'doi' in label_text:
                            if not metadata['doi']:  # Only set if not already set
                                metadata['doi'] = value_text
                        elif 'journal' in label_text or 'published in' in label_text:
                            if not metadata['journal']:  # Only set if not already set
                                metadata['journal'] = value_text
                        elif 'type' in label_text:
                            if not metadata['type']:  # Only set if not already set
                                metadata['type'] = value_text
                                
                    except Exception as e:
                        self.logger.error(f"Error processing metadata row: {str(e)}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"Error processing metadata table: {str(e)}")
                continue

        # Process DOI if found
        if metadata['doi']:
            try:
                doi_match = re.search(r'10.\d{4,9}/[-._;()/:\w]+', metadata['doi'])
                if doi_match:
                    metadata['doi'] = doi_match.group(0)
            except Exception as e:
                self.logger.error(f"Error processing DOI: {str(e)}")

        # Ensure lists are unique while preserving order
        metadata['authors'] = list(dict.fromkeys(metadata['authors']))
        metadata['subjects'] = list(dict.fromkeys(metadata['subjects']))
        
        return metadata

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string into datetime object"""
        if not date_str:
            return None
            
        try:
            # Try common date formats
            formats = [
                '%Y-%m-%d',
                '%Y-%m',
                '%Y',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y/%m/%d'
            ]
            
            # Clean up date string
            date_str = date_str.strip()
            
            # Try each format
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            # If no format works, try to extract year
            year_match = re.search(r'\d{4}', date_str)
            if year_match:
                return datetime(int(year_match.group(0)), 1, 1)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_str}': {str(e)}")
            return None

    def get_collection_items(self, collection_id: str, limit: int = 100) -> List[UnifiedContent]:
        """Fetch items from a specific collection"""
        try:
            url = f"{self.base_url}/handle/{collection_id}"
            self.logger.info(f"Fetching collection: {url}")
            
            publications = []
            offset = 0
            page_size = 20
            
            while len(publications) < limit or limit == 0:
                params = {
                    'offset': offset,
                    'rpp': page_size
                }
                
                response = self._make_request(url, params)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                items = soup.select('div.artifact-description')
                if not items:
                    break
                
                for item in items:
                    try:
                        pub = self._parse_publication(item)
                        if pub and pub.handle not in self.seen_handles:
                            self.seen_handles.add(pub.handle)
                            publications.append(pub)
                            
                            if limit > 0 and len(publications) >= limit:
                                break
                    except Exception as e:
                        self.logger.error(f"Error parsing publication in collection: {str(e)}")
                        continue
                
                if limit > 0 and len(publications) >= limit:
                    break
                
                offset += page_size
                sleep(1)
            
            return publications[:limit] if limit > 0 else publications
            
        except Exception as e:
            self.logger.error(f"Error fetching collection {collection_id}: {str(e)}")
            return []

    def search_publications(self, query: str, limit: int = 100) -> List[UnifiedContent]:
        """Search for publications using the DSpace search functionality"""
        try:
            search_url = f"{self.base_url}/simple-search"
            publications = []
            page = 1
            
            while len(publications) < limit or limit == 0:
                params = {
                    'query': query,
                    'page': page,
                    'rpp': 20,
                    'sort_by': 2,
                    'order': 'desc'
                }
                
                response = self._make_request(search_url, params)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                items = soup.select('div.artifact-description')
                if not items:
                    break
                
                for item in items:
                    try:
                        pub = self._parse_publication(item)
                        if pub and pub.handle not in self.seen_handles:
                            self.seen_handles.add(pub.handle)
                            publications.append(pub)
                            
                            if limit > 0 and len(publications) >= limit:
                                break
                    except Exception as e:
                        self.logger.error(f"Error parsing search result: {str(e)}")
                        continue
                
                if limit > 0 and len(publications) >= limit:
                    break
                
                page += 1
                sleep(1)
            
            return publications[:limit] if limit > 0 else publications
            
        except Exception as e:
            self.logger.error(f"Error searching publications: {str(e)}")
            return []

    def fetch_publications(self, limit: int = 100) -> List[UnifiedContent]:
        """Fetch publications from Knowhub"""
        publications = []
        offset = 0
        page_size = 20
        
        while len(publications) < limit or limit == 0:
            try:
                self.logger.info(f"Fetching publications (offset: {offset})...")
                
                params = {
                    'offset': offset,
                    'rpp': page_size,
                    'sort_by': 'dc.date.issued_dt',
                    'order': 'desc'
                }
                
                response = self._make_request(self.browse_url, params)
                if not response or response.status_code != 200:
                    self.logger.error(f"Failed to fetch page, status: {response.status_code if response else 'No response'}")
                    break
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for items using multiple possible selectors
                items = soup.select('div.artifact-description, div.ds-artifact-item')
                if not items:
                    self.logger.warning(f"No items found on page with offset {offset}")
                    break
                
                self.logger.info(f"Found {len(items)} items on current page")
                items_processed = 0
                
                for item in items:
                    try:
                        pub = self._parse_publication(item)
                        if pub and pub.handle not in self.seen_handles:
                            self.seen_handles.add(pub.handle)
                            publications.append(pub)
                            self.logger.info(f"Found: {pub.title[:100]}...")
                            items_processed += 1
                            
                            if limit > 0 and len(publications) >= limit:
                                break
                    except Exception as e:
                        self.logger.error(f"Error parsing publication: {str(e)}")
                        continue
                
                self.logger.info(f"Processed {items_processed} items from current page")
                
                if limit > 0 and len(publications) >= limit:
                    break
                    
                if items_processed == 0:
                    self.logger.warning("No new items processed on this page, might be at the end")
                    break
                
                offset += page_size
                sleep(1)  # Be nice to the server
                
            except Exception as e:
                self.logger.error(f"Error fetching publications: {str(e)}")
                break
        
        return publications[:limit] if limit > 0 else publications

    def _parse_publication(self, item_soup: BeautifulSoup) -> UnifiedContent:
        """Parse a publication into UnifiedContent format"""
        try:
            # Get basic information
            title_elem = item_soup.select_one('h4 a')
            if not title_elem:
                return None
            
            url = title_elem.get('href', '')
            if not url.startswith('http'):
                url = self.base_url + url
            
            handle = url.split('handle/')[-1] if 'handle' in url else ''
            
            try:
                # Get detailed page
                response = self._make_request(url)
                detail_soup = BeautifulSoup(response.text, 'html.parser')
            except Exception as e:
                self.logger.error(f"Error fetching detail page {url}: {str(e)}")
                return None
            
            # Extract metadata
            metadata = self._extract_metadata(detail_soup)
            
            return UnifiedContent(
                title=title_elem.text.strip(),
                authors=metadata.get('authors', []),
                date=self._parse_date(metadata.get('date', '')),
                abstract=metadata.get('abstract', ''),
                url=url,
                source='knowhub',
                content_type='publication',
                keywords=metadata.get('subjects', []),
                doi=metadata.get('doi', ''),
                handle=handle,
                journal=metadata.get('journal', ''),
                document_type=metadata.get('type', ''),
                external_ids={},  # Empty dict for Knowhub publications
                affiliations=[],  # Empty list for Knowhub publications
                full_text='',    # Empty string for Knowhub publications
                image_url=''     # Empty string for Knowhub publications
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing publication: {str(e)}")
            return None