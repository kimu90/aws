import csv
import requests 
import asyncio
from datetime import datetime
import logging
from typing import List, Dict, Optional, Tuple
import os
import random
import time

# Gemini imports
import google.generativeai as genai
from google.generativeai.types import GenerationConfig
from google.generativeai.types import SafetySettingDict
from google.generativeai.types.safety_types import HarmCategory, HarmBlockThreshold

import aiohttp
import pandas as pd

class OrcidScraper:
    def __init__(self, client_id=None, client_secret=None, gemini_api_key=None):
        """Initialize with credentials and AI configuration"""
        self.base_url = "https://pub.orcid.org/v3.0"
        
        # ORCID Credentials
        self.client_id = client_id or os.getenv('ORCID_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('ORCID_CLIENT_SECRET')
        
        if not self.client_id or not self.client_secret:
            raise ValueError("ORCID credentials not found")
        
        # Get access token and set headers
        access_token = self._get_access_token()
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        # Gemini API Configuration
        gemini_api_key = gemini_api_key or os.getenv('GEMINI_API_KEY')
        if not gemini_api_key:
            raise ValueError("Gemini API key not found")
        
        # Configure Gemini
        genai.configure(api_key=gemini_api_key)

        # Safety settings
        safety_settings = [
            SafetySettingDict(
                category=HarmCategory.HARM_CATEGORY_HARASSMENT, 
                threshold=HarmBlockThreshold.BLOCK_NONE
            ),
            SafetySettingDict(
                category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, 
                threshold=HarmBlockThreshold.BLOCK_NONE
            ),
            SafetySettingDict(
                category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, 
                threshold=HarmBlockThreshold.BLOCK_NONE
            ),
            SafetySettingDict(
                category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, 
                threshold=HarmBlockThreshold.BLOCK_NONE
            )
        ]

        # Generation configuration
        self.generation_config = GenerationConfig(
            temperature=0.7,
            top_p=0.9,
            max_output_tokens=150
        )

        # Initialize model with safety settings
        self.model = genai.GenerativeModel(
            'gemini-pro', 
            generation_config=self.generation_config,
            safety_settings=safety_settings
        )

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

    async def fetch_researcher_publications(self, orcid_id: str) -> List[Dict]:
        """Fetch publications for a researcher"""
        # Clean the ORCID ID
        clean_orcid = orcid_id.replace('https://orcid.org/', '')
        url = f"{self.base_url}/{clean_orcid}/works"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as response:
                    data = await response.json()
            
            publications = []
            for work in data.get("group", []):
                try:
                    # Ensure work has necessary structure
                    work_summaries = work.get("work-summary", [])
                    if not work_summaries:
                        continue
                    
                    work_summary = work_summaries[0]
                    put_code = work_summary.get("put-code")
                    if not put_code:
                        continue
                    
                    # Get detailed work
                    detailed_work = await self._get_detailed_work(clean_orcid, put_code)
                    if not detailed_work:
                        continue
                    
                    # Extract publication details
                    title = self._safe_get_nested_value(work_summary, ["title", "title", "value"], "Unknown Title")
                    doi = self._get_identifier(work_summary, "doi")
                    abstract = detailed_work.get("short-description", "No abstract available")
                    authors = self._get_authors(detailed_work)
                    publication_year = self._safe_parse_date(work_summary.get("publication-date"))
                    journal = self._safe_get_nested_value(work_summary, ["journal-title", "value"], "")
                    
                    # Construct publication dictionary
                    pub = {
                        "Title": title,
                        "DOI": doi,
                        "Abstract": abstract,
                        "Authors": "; ".join(authors),
                        "Publication_Year": publication_year.year if publication_year else "",
                        "Journal": journal
                    }
                    
                    publications.append(pub)
                    
                    # Return only the first publication
                    return publications
                        
                except Exception as e:
                    print(f"Error processing work: {e}")
                    continue
            
            return publications
        
        except Exception as e:
            print(f"Error fetching publications: {e}")
            return []

    async def _get_detailed_work(self, orcid_id: str, put_code: str) -> Optional[Dict]:
        """Fetch detailed work information"""
        url = f"{self.base_url}/{orcid_id}/work/{put_code}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as response:
                    return await response.json()
        except Exception as e:
            print(f"Error fetching detailed work: {e}")
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

    def _get_authors(self, work: Dict) -> List[str]:
        """Extract author names"""
        authors = []
        try:
            contributors = work.get("contributors", {}).get("contributor", [])
            for contributor in contributors:
                name = self._safe_get_nested_value(contributor, ["credit-name", "value"], "")
                if name:
                    if "(" in name:
                        name = name.split("(")[0].strip()
                    authors.append(name)
        except Exception as e:
            print(f"Error extracting authors: {e}")
        return authors

    def _get_identifier(self, work_summary: Dict, id_type: str) -> str:
        """Get specific identifier from work summary"""
        try:
            external_ids = work_summary.get("external-ids", {}).get("external-id", [])
            for ext_id in external_ids:
                if ext_id.get("external-id-type") == id_type:
                    return ext_id.get("external-id-value", "")
        except Exception as e:
            print(f"Error getting {id_type} identifier: {e}")
        return ""

    def _safe_parse_date(self, date_dict: Dict) -> Optional[datetime]:
        """Safely parse date from ORCID format"""
        try:
            if not date_dict or not isinstance(date_dict, dict):
                return None

            year = date_dict.get("year", {}).get("value", "")
            month = date_dict.get("month", {}).get("value", "1")
            day = date_dict.get("day", {}).get("value", "1")

            if not year:
                return None

            year = int(year)
            month = int(month) if month else 1
            day = int(day) if day else 1

            return datetime(year, month, day)
        except (ValueError, TypeError, AttributeError):
            return None