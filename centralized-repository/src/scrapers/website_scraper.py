from ..models.content import UnifiedContent
from .base_scraper import BaseScraper
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from time import sleep
from datetime import datetime
import re

class WebsiteScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://aphrc.org"
        self.urls = {
            'publications': f"{self.base_url}/publications/",
            'documents': f"{self.base_url}/documents_reports/",
            'ideas': f"{self.base_url}/ideas/"
        }

    def fetch_content(self, limit: int = 100) -> List[UnifiedContent]:
        """Fetch content from specified URLs"""
        all_items = []
        
        for section, url in self.urls.items():
            self.logger.info(f"Fetching {section} from {url}")
            try:
                response = self._make_request(url)
                if response.status_code != 200:
                    self.logger.error(f"Failed to access {section}: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                
                if self._has_load_more_button(soup):
                    items = self._fetch_with_load_more(url, section, limit)
                else:
                    items = self._fetch_with_pagination(url, section, limit)
                
                all_items.extend(items)
                self.logger.info(f"Found {len(items)} items in {section}")
                
                if limit and len(all_items) >= limit:
                    all_items = all_items[:limit]
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching {section}: {str(e)}")
                continue
                
        return all_items

    def _has_load_more_button(self, soup: BeautifulSoup) -> bool:
        """Check if page has a load more button or infinite scroll"""
        load_more_selectors = [
            '.load-more',
            '.elementor-button-link',
            'button[data-page]',
            '.elementor-pagination'
        ]
        return any(bool(soup.select(selector)) for selector in load_more_selectors)

    def _fetch_with_load_more(self, url: str, section: str, limit: int) -> List[UnifiedContent]:
        """Handle infinite scroll or load more pagination"""
        items = []
        page = 1
        
        while True:
            try:
                next_page_url = f"{url}page/{page}/"
                response = self._make_request(next_page_url)
                if response.status_code != 200:
                    break
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                new_items = self._extract_items(soup, section)
                
                if not new_items:
                    break
                    
                items.extend(new_items)
                self.logger.info(f"Loaded page {page}, total items: {len(items)}")
                
                if limit and len(items) >= limit:
                    items = items[:limit]
                    break
                    
                page += 1
                sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error loading more items: {str(e)}")
                break
                
        return items

    def _fetch_with_pagination(self, url: str, section: str, limit: int) -> List[UnifiedContent]:
        """Handle traditional numbered pagination"""
        items = []
        page = 1
        
        while True:
            try:
                page_url = f"{url}page/{page}/" if page > 1 else url
                response = self._make_request(page_url)
                
                if response.status_code != 200:
                    break
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                new_items = self._extract_items(soup, section)
                
                if not new_items:
                    break
                    
                items.extend(new_items)
                self.logger.info(f"Processed page {page}, total items: {len(items)}")
                
                if limit and len(items) >= limit:
                    items = items[:limit]
                    break
                    
                page += 1
                sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                break
                
        return items

    def _extract_items(self, soup: BeautifulSoup, section: str) -> List[UnifiedContent]:
        """Extract items from a page"""
        items = []
        
        selectors = {
            'publications': ['.elementor-post', '.publication-item', 'article'],
            'documents': ['.document-item', '.report-item', 'article'],
            'ideas': ['.post', '.idea-item', 'article']
        }
        
        for selector in selectors.get(section, []):
            elements = soup.select(selector)
            if elements:
                break
        
        for element in elements:
            try:
                item = self._parse_item(element, section)
                if item:
                    items.append(item)
            except Exception as e:
                self.logger.error(f"Error parsing item: {str(e)}")
                continue
                
        return items

    def _parse_item(self, element: BeautifulSoup, section: str) -> Optional[UnifiedContent]:
        """Parse a single item"""
        try:
            title_elem = element.select_one('h1, h2, h3, h4, a')
            if not title_elem:
                return None
                
            title = title_elem.text.strip()
            
            if title_elem.name == 'a':
                url = title_elem.get('href', '')
            else:
                link = element.find('a')
                url = link.get('href', '') if link else ''

            if not url.startswith('http'):
                url = self.base_url + url
                
            date_elem = element.select_one('.date, .elementor-post-date, time')
            date = None
            if date_elem:
                date_str = date_elem.get('datetime', '') or date_elem.text.strip()
                date = self._parse_date(date_str)
                
            excerpt_elem = element.select_one('.excerpt, .description, p')
            excerpt = excerpt_elem.text.strip() if excerpt_elem else ''

            return UnifiedContent(
                title=title,
                authors=[],
                date=date,
                abstract=excerpt,
                url=url,
                source='website',
                content_type=section,
                keywords=[],
                doi='',
                full_text=''
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing item: {str(e)}")
            return None

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string into datetime object"""
        if not date_str:
            return None
            
        try:
            formats = [
                '%Y-%m-%d',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y/%m/%d',
                '%d/%m/%Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
                    
            year_match = re.search(r'\d{4}', date_str)
            if year_match:
                return datetime(int(year_match.group(0)), 1, 1)
                
            return None
            
        except Exception:
            return None