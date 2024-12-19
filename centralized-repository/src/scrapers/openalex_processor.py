import pandas as pd
import asyncio
import aiohttp
import logging
import os
import requests
from typing import Dict, List, Tuple, Optional
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class OpenAlexCSVProcessor:
    def __init__(self, base_url: str = 'https://api.openalex.org'):
        """Initialize the processor with OpenAlex API URL."""
        self.base_url = base_url
        
    async def process_csv(self, input_csv_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process input CSV and return enriched DataFrames."""
        # Read initial CSV
        df = pd.read_csv(input_csv_path)
        
        # Prepare output lists
        experts_enriched = []
        publications_data = []
        
        # Process each expert
        async with aiohttp.ClientSession() as session:
            for _, row in df.iterrows():
                try:
                    first_name = row['First_name']
                    last_name = row['Last_name']
                    
                    logger.info(f"Processing expert: {first_name} {last_name}")
                    
                    # Get OpenAlex data
                    expert_data = await self.get_expert_data(session, first_name, last_name)
                    if expert_data:
                        # Add enriched expert data
                        expert_row = {
                            'First_name': first_name,
                            'Last_name': last_name,
                            'Designation': row.get('Designation', ''),
                            'Theme': row.get('Theme', ''),
                            'Unit': row.get('Unit', ''),
                            'Contact_Details': row.get('Contact Details', ''),
                            'Knowledge_Expertise': row.get('Knowledge and Expertise', ''),
                            'ORCID': expert_data[0],
                            'OpenAlex_ID': expert_data[1],
                            'Domains': '',
                            'Fields': '',
                            'Subfields': ''
                        }
                        
                        # Get domains, fields, and subfields
                        if expert_data[1]:
                            domains, fields, subfields = await self.get_expert_domains(
                                session, first_name, last_name, expert_data[1]
                            )
                            expert_row['Domains'] = '|'.join(domains)
                            expert_row['Fields'] = '|'.join(fields)
                            expert_row['Subfields'] = '|'.join(subfields)
                        
                        experts_enriched.append(expert_row)
                        
                        # Process publications
                        if expert_data[1]:
                            pubs = await self.get_expert_works(
                                session, 
                                expert_data[1]
                            )
                            for pub in pubs:
                                pub_data = self.process_publication(pub, first_name, last_name)
                                if pub_data:
                                    publications_data.append(pub_data)
                    
                except Exception as e:
                    logger.error(f"Error processing {first_name} {last_name}: {e}")
                    continue
        
        # Create DataFrames
        experts_df = pd.DataFrame(experts_enriched)
        publications_df = pd.DataFrame(publications_data)
        
        return experts_df, publications_df
    
    async def get_expert_domains(self, session: aiohttp.ClientSession,
                               first_name: str, last_name: str, 
                               openalex_id: str) -> Tuple[List[str], List[str], List[str]]:
        """Get expert domains from their works."""
        works = await self.get_expert_works(session, openalex_id)
        
        domains = set()
        fields = set()
        subfields = set()
        
        for work in works:
            try:
                for topic in work.get('topics', []):
                    domain = topic.get('domain', {}).get('display_name')
                    field = topic.get('field', {}).get('display_name')
                    topic_subfields = [sf.get('display_name') 
                                     for sf in topic.get('subfields', [])]
                    
                    if domain:
                        domains.add(domain)
                    if field:
                        fields.add(field)
                    subfields.update(sf for sf in topic_subfields if sf)
            except Exception as e:
                logger.error(f"Error processing work topic: {e}")
                continue
                
        return list(domains), list(fields), list(subfields)
    
    async def get_expert_works(self, session: aiohttp.ClientSession, 
                             openalex_id: str, per_page: int = 25) -> List[Dict]:
        """Fetch expert works from OpenAlex."""
        try:
            works_url = f"{self.base_url}/works"
            params = {
                'filter': f"authorships.author.id:{openalex_id}",
                'per-page': per_page
            }
            
            async with session.get(works_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('results', [])
                else:
                    logger.error(f"Error fetching works: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching works: {e}")
            return []
    
    async def get_expert_data(self, session: aiohttp.ClientSession, 
                            first_name: str, last_name: str) -> Tuple[str, str]:
        """Get expert's ORCID and OpenAlex ID."""
        search_url = f"{self.base_url}/authors"
        params = {
            "search": f"{first_name} {last_name}",
            "filter": "display_name.search:" + f'"{first_name} {last_name}"'
        }
        
        try:
            for attempt in range(3):  # Add retry logic
                try:
                    async with session.get(search_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = data.get('results', [])
                            if results:
                                author = results[0]
                                orcid = author.get('orcid', '')
                                openalex_id = author.get('id', '')
                                return orcid, openalex_id
                        
                        elif response.status == 429:  # Rate limit
                            wait_time = (attempt + 1) * 5
                            logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                            
                except Exception as e:
                    logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                    if attempt < 2:  # Only sleep if we're going to retry
                        await asyncio.sleep(5)
                    continue
                
        except Exception as e:
            logger.error(f"Error fetching data for {first_name} {last_name}: {e}")
        return '', ''

    def process_publication(self, work: Dict, author_first_name: str, 
                          author_last_name: str) -> Optional[Dict]:
        """Process a single publication work."""
        try:
            # Extract basic publication data
            doi = work.get('doi', '')
            if not doi:
                return None
                
            title = work.get('title', '')
            if not title:
                return None
                
            # Process abstract
            abstract = work.get('abstract_inverted_index', '')
            if abstract:
                abstract = ' '.join([word for word, positions in abstract.items()])
            
            # Get author names
            authors = []
            for authorship in work.get('authorships', []):
                author = authorship.get('author', {})
                if author:
                    authors.append(author.get('display_name', ''))
            
            # Get concepts/tags
            concepts = []
            for concept in work.get('concepts', []):
                concept_name = concept.get('display_name')
                if concept_name:
                    concepts.append(concept_name)
            
            return {
                'DOI': doi,
                'Title': title,
                'Abstract': abstract,
                'Authors': '|'.join(authors),
                'Concepts': '|'.join(concepts),
                'Publication_Year': work.get('publication_year', ''),
                'Journal': work.get('primary_location', {}).get('source', {}).get('display_name', ''),
                'Citations_Count': work.get('cited_by_count', 0),
                'Expert_First_Name': author_first_name,
                'Expert_Last_Name': author_last_name
            }
            
        except Exception as e:
            logger.error(f"Error processing publication: {e}")
            return None