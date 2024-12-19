# File: src/scrapers/orcid_scraper.py

from ..models.content import UnifiedContent
from .base_scraper import BaseScraper
import requests
from datetime import datetime
import logging
from typing import List, Dict
import time
from typing import List, Dict, Optional

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
        # Clean the ORCID ID - remove any URL prefix if present
        clean_orcid = orcid_id.replace('https://orcid.org/', '')
        url = f"{self.base_url}/{clean_orcid}/works"
        return self._process_works(url, clean_orcid, "orcid")

    def _get_affiliated_publications(self, orcid_id: str) -> List[UnifiedContent]:
        """Fetch publications through APHRC affiliation"""
        # Clean the ORCID ID - remove any URL prefix if present
        clean_orcid = orcid_id.replace('https://orcid.org/', '')
        url = f"{self.base_url}/{clean_orcid}/works"
        return self._process_works(url, clean_orcid, "affiliation", check_affiliation=True)

    def _process_works(self, url: str, orcid_id: str, source: str, 
                      check_affiliation: bool = False) -> List[UnifiedContent]:
        try:
            self.logger.info(f"Making request to: {url}")
            response = self._make_request(url, headers=self.headers)
            data = response.json()
            
            self.logger.info(f"Response structure: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            if "group" not in data:
                self.logger.warning(f"No 'group' in response for ORCID {orcid_id}")
                return []
            
            publications = []
            for work in data["group"]:
                try:
                    self.logger.info(f"Processing work structure: {list(work.keys()) if isinstance(work, dict) else 'Not a dict'}")
                    
                    if not isinstance(work, dict) or "work-summary" not in work:
                        self.logger.warning("Work missing required structure")
                        continue
                        
                    work_summaries = work.get("work-summary", [])
                    if not work_summaries:
                        self.logger.warning("Empty work summaries")
                        continue
                        
                    work_summary = work_summaries[0]
                    if not isinstance(work_summary, dict):
                        self.logger.warning(f"Work summary not a dict: {type(work_summary)}")
                        continue
                        
                    put_code = work_summary.get("put-code")
                    if not put_code:
                        self.logger.warning("No put-code found")
                        continue
                    
                    self.logger.info(f"Fetching detailed work for put-code: {put_code}")
                    detailed_work = self._get_detailed_work(orcid_id, put_code)
                    
                    if not detailed_work:
                        self.logger.warning(f"No detailed work data for put-code: {put_code}")
                        continue
                    
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
                    self.logger.error(f"Error processing work: {str(e)}", exc_info=True)
                    continue
                    
            return publications
            
        except Exception as e:
            self.logger.error(f"Error fetching works: {str(e)}", exc_info=True)
            return []

    def _safe_get_abstract(self, work: Dict) -> str:
        """Safely extract abstract from work"""
        if not work or not isinstance(work, dict):
            return ""
        return work.get("short-description", "")

    def _safe_get_keywords(self, work: Dict) -> List[str]:
        """Safely extract keywords from work"""
        if not work or not isinstance(work, dict):
            return []
            
        keywords_container = work.get("keywords", {})
        if not isinstance(keywords_container, dict):
            return []
            
        keywords_list = keywords_container.get("keyword", [])
        if not isinstance(keywords_list, list):
            return []
            
        return [k.get("content", "") for k in keywords_list 
                if isinstance(k, dict) and k.get("content")]

    def _safe_get_external_ids(self, work: Dict) -> Dict[str, str]:
        """Safely get all external identifiers"""
        if not work or not isinstance(work, dict):
            return {}
            
        external_ids = work.get("external-ids", {})
        if not isinstance(external_ids, dict):
            return {}
            
        ext_id_list = external_ids.get("external-id", [])
        if not isinstance(ext_id_list, list):
            return {}
            
        return {
            ext_id.get("external-id-type"): ext_id.get("external-id-value")
            for ext_id in ext_id_list
            if isinstance(ext_id, dict) 
            and ext_id.get("external-id-type") 
            and ext_id.get("external-id-value")
        }

    def _safe_get_affiliations(self, work: Dict) -> List[str]:
        """Safely extract all affiliations"""
        if not work or not isinstance(work, dict):
            return []
            
        affiliations = set()
        
        # Get contributors
        contributors = work.get("contributors", {})
        if not isinstance(contributors, dict):
            return []
            
        contributor_list = contributors.get("contributor", [])
        if not isinstance(contributor_list, list):
            return []
            
        for contributor in contributor_list:
            if not isinstance(contributor, dict):
                continue
                
            # Check organization name
            if "organization" in contributor:
                org = contributor["organization"]
                if isinstance(org, dict):
                    org_name = org.get("name")
                    if org_name:
                        affiliations.add(org_name)
            
            # Check credit name for parenthetical affiliations
            credit_name = contributor.get("credit-name", {})
            if isinstance(credit_name, dict):
                name = credit_name.get("value", "")
                if name and "(" in name:
                    affiliation = name.split("(")[-1].strip(")")
                    affiliations.add(affiliation)
        
        return list(affiliations)

    def _convert_to_unified_content(self, work_summary: Dict, 
                              detailed_work: Dict, 
                              orcid_id: str, 
                              source: str) -> UnifiedContent:
        """Convert ORCID work to UnifiedContent format"""
        try:
            # Log the input data structure
            self.logger.info(f"Converting work summary keys: {list(work_summary.keys()) if work_summary else None}")
            self.logger.info(f"Detailed work keys: {list(detailed_work.keys()) if detailed_work else None}")
            
            # Basic validation
            if not work_summary or not detailed_work:
                self.logger.warning("Missing work_summary or detailed_work")
                return None

            # Get title safely
            title = self._safe_get_nested_value(
                work_summary, 
                ["title", "title", "value"], 
                ""
            )
            
            if not title:
                self.logger.warning("No title found")
                return None
                
            # Get DOI safely
            doi = self._get_identifier(work_summary, "doi")
            
            return UnifiedContent(
                title=title,
                authors=self._safe_get_authors(detailed_work),
                date=self._safe_parse_date(work_summary.get("publication-date")),
                abstract=self._safe_get_abstract(detailed_work),
                url=self._get_identifier(work_summary, "url") or "",
                source=source,
                content_type="publication",
                keywords=self._safe_get_keywords(detailed_work),
                doi=doi,
                journal=self._safe_get_nested_value(work_summary, ["journal-title", "value"], ""),
                external_ids=self._safe_get_external_ids(work_summary),
                affiliations=self._safe_get_affiliations(detailed_work),
                orcid_id=orcid_id
            )
                
        except Exception as e:
            self.logger.error(f"Error converting work to unified content: {str(e)}", exc_info=True)
            return None

    def _safe_get_nested_value(self, data: Dict, keys: List[str], default: any) -> any:
        """Safely get nested dictionary value"""
        try:
            result = data
            for key in keys:
                if not isinstance(result, dict):
                    return default
                result = result.get(key, default)
            return result
        except Exception:
            return default

    def _safe_get_authors(self, work: Dict) -> List[str]:
        """Safe version of get_authors"""
        try:
            contributors = work.get("contributors", {})
            if not isinstance(contributors, dict):
                return []
            contributor_list = contributors.get("contributor", [])
            if not isinstance(contributor_list, list):
                return []
                
            authors = []
            for contributor in contributor_list:
                name = self._safe_get_nested_value(contributor, ["credit-name", "value"], "")
                if name:
                    if "(" in name:
                        name = name.split("(")[0].strip()
                    authors.append(name)
            return authors
        except Exception as e:
            self.logger.error(f"Error getting authors: {str(e)}")
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
    def _get_abstract(self, work: Dict) -> str:
        """Extract abstract with better error handling"""
        if not work or not isinstance(work, dict):
            return ""
        return work.get("short-description", "")

    def _get_keywords(self, work: Dict) -> List[str]:
        """Extract keywords with better error handling"""
        if not work or not isinstance(work, dict):
            return []
            
        keywords_container = work.get("keywords", {})
        if not isinstance(keywords_container, dict):
            return []
            
        keywords_list = keywords_container.get("keyword", [])
        if not isinstance(keywords_list, list):
            return []
            
        return [k.get("content", "") for k in keywords_list 
                if isinstance(k, dict) and k.get("content")]
    def _safe_parse_date(self, date_dict: Dict) -> Optional[datetime]:
        """Safely parse date from ORCID format"""
        if not date_dict or not isinstance(date_dict, dict):
            return None
        
        try:
            year = date_dict.get("year", {}).get("value", "")
            month = date_dict.get("month", {}).get("value", "1")
            day = date_dict.get("day", {}).get("value", "1")
            
            if not year:
                return None
                
            # Convert to integers with defaults
            year = int(year)
            month = int(month) if month else 1
            day = int(day) if day else 1
            
            # Validate date components
            if not (1 <= month <= 12 and 1 <= day <= 31):
                return None
                
            return datetime(year, month, day)
        except (ValueError, TypeError, AttributeError):
            return None
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
        """Extract author names with better error handling"""
        authors = []
        if not work or not isinstance(work, dict):
            return authors
            
        contributors = work.get("contributors", {})
        if not isinstance(contributors, dict):
            return authors
            
        contributor_list = contributors.get("contributor", [])
        if not isinstance(contributor_list, list):
            return authors
            
        for contributor in contributor_list:
            if not isinstance(contributor, dict):
                continue
                
            credit_name = contributor.get("credit-name", {})
            if not isinstance(credit_name, dict):
                continue
                
            name = credit_name.get("value", "")
            if name:
                if "(" in name:
                    name = name.split("(")[0].strip()
                authors.append(name)
                
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