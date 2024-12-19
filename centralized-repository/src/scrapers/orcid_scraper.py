# File: src/scrapers/orcid_scraper.py

from ..models.content import UnifiedContent
from .base_scraper import BaseScraper
import requests
from datetime import datetime
import logging
from typing import List, Dict
import time

class OrcidScraper(BaseScraper):
    APHRC_KEYWORDS = [
        "APHRC",
        "African Population and Health Research Center",
        "African Population & Health Research Center",
        "African Population and Health Research Centre",
        "APHRC, Nairobi",
        "APHRC Kenya"
    ]

    def __init__(self, client_id: str, client_secret: str):
        super().__init__()
        self.base_url = "https://pub.orcid.org/v3.0"
        self.client_id = client_id
        self.client_secret = client_secret
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._get_access_token()}"
        }
        self.seen_dois = set()

    def _get_access_token(self) -> str:
        """Get access token from ORCID API"""
        token_url = "https://orcid.org/oauth/token"
        response = requests.post(
            token_url,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
                "scope": "/read-public"
            },
            headers={"Accept": "application/json"}
        )
        if response.status_code != 200:
            raise Exception(f"Failed to get access token: {response.text}")
        return response.json()["access_token"]

    def fetch_researcher_publications(self, orcid_id: str) -> List[UnifiedContent]:
        """Fetch publications for a researcher and convert to UnifiedContent format"""
        direct_pubs = self._get_researcher_publications(orcid_id)
        affiliated_pubs = self._get_affiliated_publications(orcid_id)
        
        # Combine and deduplicate based on DOI
        all_pubs = []
        seen_dois = set()
        
        for pub in direct_pubs + affiliated_pubs:
            if not pub.doi or pub.doi not in seen_dois:
                if pub.doi:
                    seen_dois.add(pub.doi)
                all_pubs.append(pub)
        
        return all_pubs

    def _get_researcher_publications(self, orcid_id: str) -> List[UnifiedContent]:
        """Fetch direct publications from ORCID profile"""
        url = f"{self.base_url}/{orcid_id}/works"
        return self._process_works(url, orcid_id, "orcid")

    def _get_affiliated_publications(self, orcid_id: str) -> List[UnifiedContent]:
        """Fetch publications through APHRC affiliation"""
        url = f"{self.base_url}/{orcid_id}/works"
        return self._process_works(url, orcid_id, "affiliation", check_affiliation=True)

    def _process_works(self, url: str, orcid_id: str, source: str, 
                      check_affiliation: bool = False) -> List[UnifiedContent]:
        try:
            response = self._make_request(url, headers=self.headers)
            data = response.json()
            
            if "group" not in data:
                return []
            
            publications = []
            for work in data["group"]:
                try:
                    work_summary = work["work-summary"][0]
                    put_code = work_summary["put-code"]
                    
                    # Get detailed work information
                    detailed_work = self._get_detailed_work(orcid_id, put_code)
                    
                    # Check affiliation if required
                    if check_affiliation:
                        affiliations = self._get_affiliations(detailed_work)
                        if not self._has_aphrc_affiliation(affiliations):
                            continue
                    
                    pub = self._convert_to_unified_content(
                        work_summary, 
                        detailed_work, 
                        orcid_id, 
                        source
                    )
                    
                    if pub:
                        publications.append(pub)
                        
                except Exception as e:
                    self.logger.error(f"Error processing work: {str(e)}")
                    continue
                    
            return publications
            
        except Exception as e:
            self.logger.error(f"Error fetching works: {str(e)}")
            return []

    def _get_detailed_work(self, orcid_id: str, put_code: str) -> Dict:
        """Fetch detailed information for a specific work"""
        url = f"{self.base_url}/{orcid_id}/work/{put_code}"
        
        try:
            response = self._make_request(url, headers=self.headers)
            return response.json()
        except requests.exceptions.RequestException as e:
            if e.response and e.response.status_code == 429:  # Rate limit
                time.sleep(3)
                return self._get_detailed_work(orcid_id, put_code)
            raise

    def _convert_to_unified_content(self, work_summary: Dict, 
                                  detailed_work: Dict, 
                                  orcid_id: str, 
                                  source: str) -> UnifiedContent:
        """Convert ORCID work to UnifiedContent format"""
        try:
            doi = self._get_identifier(work_summary, "doi")
            
            # Skip if we've seen this DOI before
            if doi and doi in self.seen_dois:
                return None
            
            if doi:
                self.seen_dois.add(doi)
            
            return UnifiedContent(
                title=work_summary.get("title", {}).get("title", {}).get("value", ""),
                authors=self._get_authors(detailed_work),
                date=self._parse_date(work_summary.get("publication-date")),
                abstract=self._get_abstract(detailed_work),
                url=self._get_identifier(work_summary, "url") or "",
                source=source,
                content_type="publication",
                keywords=self._get_keywords(detailed_work),
                doi=doi,
                journal=work_summary.get("journal-title", {}).get("value", ""),
                external_ids=self._get_external_ids(work_summary),
                affiliations=self._get_affiliations(detailed_work),
                orcid_id=orcid_id
            )
            
        except Exception as e:
            self.logger.error(f"Error converting work to unified content: {str(e)}")
            return None

    def _get_abstract(self, work: Dict) -> str:
        """Extract abstract from work"""
        try:
            return work.get("short-description", "")
        except:
            return ""

    def _get_keywords(self, work: Dict) -> List[str]:
        """Extract keywords from work"""
        try:
            keywords = work.get("keywords", {}).get("keyword", [])
            return [k.get("content", "") for k in keywords if k.get("content")]
        except:
            return []

    def _parse_date(self, date_dict: Dict) -> datetime:
        """Parse date from ORCID format"""
        if not date_dict:
            return None
        
        try:
            year = date_dict.get("year", {}).get("value", "")
            month = date_dict.get("month", {}).get("value", "1")
            day = date_dict.get("day", {}).get("value", "1")
            
            return datetime(int(year), int(month), int(day))
        except (ValueError, TypeError):
            return None

    def _get_authors(self, work: Dict) -> List[str]:
        """Extract author names"""
        authors = []
        contributors = work.get("contributors", {}).get("contributor", [])
        for contributor in contributors:
            credit_name = contributor.get("credit-name", {}).get("value", "")
            if credit_name:
                if "(" in credit_name:
                    credit_name = credit_name.split("(")[0].strip()
                authors.append(credit_name)
        return authors

    def _get_affiliations(self, work: Dict) -> List[str]:
        """Extract all affiliations"""
        affiliations = set()
        
        # Check contributors
        contributors = work.get("contributors", {}).get("contributor", [])
        for contributor in contributors:
            if "organization" in contributor:
                org_name = contributor["organization"].get("name")
                if org_name:
                    affiliations.add(org_name)
            
            credit_name = contributor.get("credit-name", {}).get("value", "")
            if credit_name and "(" in credit_name:
                affiliation = credit_name.split("(")[-1].strip(")")
                affiliations.add(affiliation)
        
        return list(affiliations)

    def _has_aphrc_affiliation(self, affiliations: List[str]) -> bool:
        """Check for APHRC affiliation"""
        return any(
            any(keyword.lower() in affiliation.lower() 
                for keyword in self.APHRC_KEYWORDS)
            for affiliation in affiliations
        )

    def _get_identifier(self, work: Dict, id_type: str) -> str:
        """Get specific identifier from work"""
        external_ids = work.get("external-ids", {}).get("external-id", [])
        for ext_id in external_ids:
            if ext_id.get("external-id-type") == id_type:
                return ext_id.get("external-id-value", "")
        return ""

    def _get_external_ids(self, work: Dict) -> Dict[str, str]:
        """Get all external identifiers"""
        external_ids = work.get("external-ids", {}).get("external-id", [])
        return {
            ext_id.get("external-id-type"): ext_id.get("external-id-value")
            for ext_id in external_ids
            if ext_id.get("external-id-type") and ext_id.get("external-id-value")
        }