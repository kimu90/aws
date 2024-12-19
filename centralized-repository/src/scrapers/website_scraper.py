# File: src/scrapers/website_scraper.py

from ..models.content import UnifiedContent
from .base_scraper import BaseScraper
from bs4 import BeautifulSoup
from typing import List, Dict
from time import sleep
from datetime import datetime
import re

class WebsiteScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://aphrc.org"
        self.content_types = {
            'publications': f"{self.base_url}/publications",
            'blogs': f"{self.base_url}/blog",
            'press': f"{self.base_url}/press-releases",
            'news': f"{self.base_url}/news",
            'stories': f"{self.base_url}/stories"
        }

    def fetch_content(self, content_types: List[str] = None, limit: int = 100) -> List[UnifiedContent]:
        """Fetch content from selected content types"""
        all_items = []
        types_to_fetch = content_types or list(self.content_types.keys())
        
        for content_type in types_to_fetch:
            if content_type not in self.content_types:
                self.logger.warning(f"Unknown content type: {content_type}")
                continue
                
            self.logger.info(f"Fetching {content_type}...")
            items = self._fetch_content_type(content_type, self.content_types[content_type], limit)
            all_items.extend(items)
            self.logger.info(f"Found {len(items)} {content_type} items")
        
        return all_items

    def _fetch_content_type(self, content_type: str, base_url: str, limit: int) -> List[UnifiedContent]:
        items = []
        page = 1
        seen_urls = set()
        
        while len(items) < limit or limit == 0:
            try:
                self.logger.info(f"Fetching page {page} of {content_type}...")
                
                # Handle pagination
                url = f"{base_url}/page/{page}/" if page > 1 else base_url
                response = self._make_request(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all content items on the page
                content_items = self._find_content_items(soup, content_type)
                if not content_items:
                    break
                
                for item in content_items:
                    try:
                        content = self._parse_content_item(item, content_type)
                        if content and content.url not in seen_urls:
                            seen_urls.add(content.url)
                            items.append(content)
                            self.logger.info(f"Found: {content.title[:100]}...")
                            
                            if limit > 0 and len(items) >= limit:
                                break
                    except Exception as e:
                        self.logger.error(f"Error parsing content item: {str(e)}")
                        continue
                
                if limit > 0 and len(items) >= limit:
                    break
                
                page += 1
                sleep(1)  # Be nice to the server
                
            except Exception as e:
                self.logger.error(f"Error on page {page}: {str(e)}")
                break
        
        return items[:limit] if limit > 0 else items

    def _find_content_items(self, soup: BeautifulSoup, content_type: str) -> List:
        """Find content items based on content type"""
        selectors = {
            'publications': ['article.publication', 'div.publication-item', '.research-publication'],
            'blogs': ['article.post', 'div.blog-post', '.blog-entry'],
            'press': ['article.press-release', 'div.press-item', '.media-release'],
            'news': ['article.news-item', 'div.news-content', '.news-entry'],
            'stories': ['article.story', 'div.story-item', '.story-entry']
        }
        
        items = []
        for selector in selectors.get(content_type, []):
            items.extend(soup.select(selector))
        
        if not items:
            # Fallback to generic selectors
            items = soup.select('article, .post, .entry-content')
            
        return items

    def _parse_content_item(self, item_soup: BeautifulSoup, content_type: str) -> UnifiedContent:
        """Parse a content item into UnifiedContent format"""
        try:
            # Find title and URL
            title_elem = item_soup.select_one('h1 a, h2 a, h3 a, h4 a, .entry-title a')
            if not title_elem:
                return None
            
            url = title_elem.get('href', '')
            if not url.startswith('http'):
                url = self.base_url + url
            
            try:
                # Get detailed page
                response = self._make_request(url)
                detail_soup = BeautifulSoup(response.text, 'html.parser')
            except Exception as e:
                self.logger.error(f"Error fetching detail page {url}: {str(e)}")
                detail_soup = item_soup
            
            # Extract DOI if available
            doi = self._extract_doi(detail_soup)
            
            # Extract date
            date_str = self._extract_date(detail_soup)
            pub_date = self._parse_date(date_str) if date_str else None
            
            return UnifiedContent(
                title=title_elem.text.strip(),
                authors=self._extract_authors(detail_soup),
                date=pub_date,
                abstract=self._extract_abstract(detail_soup),
                url=url,
                source='website',
                content_type=content_type,
                keywords=self._extract_tags(detail_soup),
                doi=doi,
                full_text=self._extract_full_text(detail_soup),
                image_url=self._extract_image_url(detail_soup)
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing content item: {str(e)}")
            return None

    def _extract_doi(self, soup: BeautifulSoup) -> str:
        """Extract DOI if available"""
        doi_elem = soup.select_one('.doi, .publication-doi, a[href*="doi.org"]')
        if doi_elem:
            doi_text = doi_elem.get('href', '') or doi_elem.text
            doi_match = re.search(r'10.\d{4,9}/[-._;()/:\w]+', doi_text)
            return doi_match.group(0) if doi_match else ''
        return ''

    def _extract_date(self, soup: BeautifulSoup) -> str:
        """Extract publication date"""
        date_elem = soup.select_one('.date, .entry-date, .published, time[datetime]')
        if date_elem:
            return date_elem.get('datetime', '') or date_elem.text.strip()
        return ''

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string into datetime object"""
        try:
            # Try common date formats
            formats = [
                '%Y-%m-%d',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y/%m/%d',
                '%d/%m/%Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None

    def _extract_authors(self, soup: BeautifulSoup) -> List[str]:
        """Extract author names"""
        authors = []
        
        # Try different author selectors
        author_elements = soup.select('.author, .entry-author, .post-author, .byline')
        
        for elem in author_elements:
            author_text = elem.text.strip()
            # Remove common prefixes
            for prefix in ['By', 'by', 'Author:', 'Authors:']:
                author_text = author_text.replace(prefix, '').strip()
            
            # Split multiple authors
            for author in author_text.split(','):
                author = author.strip()
                if author and author not in authors:
                    authors.append(author)
        
        return authors

    def _extract_abstract(self, soup: BeautifulSoup) -> str:
        """Extract abstract or excerpt"""
        abstract_elem = soup.select_one('.entry-summary, .excerpt, .abstract, .post-excerpt')
        return abstract_elem.text.strip() if abstract_elem else ''

    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract tags or keywords"""
        tags = []
        tag_elements = soup.select('.tags a, .entry-tags a, .post-tags a, .keywords a')
        
        for elem in tag_elements:
            tag = elem.text.strip()
            if tag and tag not in tags:
                tags.append(tag)
                
        return tags

    def _extract_full_text(self, soup: BeautifulSoup) -> str:
        """Extract full content text"""
        content_elem = soup.select_one('.entry-content, .post-content, .article-content')
        return content_elem.text.strip() if content_elem else ''

    def _extract_image_url(self, soup: BeautifulSoup) -> str:
        """Extract featured image URL"""
        image_elem = soup.select_one('.featured-image img, .post-thumbnail img')
        if image_elem and image_elem.get('src'):
            image_url = image_elem['src']
            return image_url if image_url.startswith('http') else self.base_url + image_url
        return ''