# File: main.py

from src.scrapers.website_scraper import WebsiteScraper
from src.scrapers.knowhub_scraper import KnowhubScraper
from src.scrapers.orcid_scraper import OrcidScraper
from src.utils.exporters import ContentExporter
from src.scrapers.openalex_processor import OpenAlexCSVProcessor
import logging
from dotenv import load_dotenv
import os
import asyncio
from datetime import datetime
import pandas as pd
from typing import Dict, List
import glob

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'scraping_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def find_experts_csv(base_path: str = None) -> str:
    """
    Intelligently find the experts CSV file
    
    Search strategy:
    1. Current directory
    2. src/scrapers directory
    3. Recursive search from base path
    """
    # Possible search paths
    if base_path is None:
        base_path = os.getcwd()
    
    search_paths = [
        os.path.join(os.getcwd(), 'experts.csv'),
        os.path.join(os.getcwd(), 'src', 'scrapers', 'experts.csv'),
        os.path.join(base_path, 'experts.csv'),
        os.path.join(base_path, 'src', 'scrapers', 'experts.csv')
    ]
    
    # Recursive search function
    def recursive_csv_search(directory):
        for root, _, files in os.walk(directory):
            for file in files:
                if file.lower().endswith('.csv') and 'experts' in file.lower():
                    return os.path.join(root, file)
        return None
    
    # Check predefined paths
    for path in search_paths:
        if os.path.exists(path):
            return path
    
    # Recursive search from base path
    recursive_result = recursive_csv_search(base_path)
    if recursive_result:
        return recursive_result
    
    raise FileNotFoundError("Could not find experts CSV file. Please specify the exact path.")

async def process_experts(input_csv: str, logger) -> Dict[str, pd.DataFrame]:
    """Process experts through OpenAlex and return enriched data"""
    try:
        logger.info("Starting OpenAlex processing...")
        processor = OpenAlexCSVProcessor()
        
        # Process the CSV and get the output DataFrames
        experts_df, publications_df = await processor.process_csv(input_csv)
        
        logger.info(f"OpenAlex processing complete. Found {len(experts_df)} experts and {len(publications_df)} publications.")
        
        return {
            'experts': experts_df,
            'publications': publications_df
        }
    except Exception as e:
        logger.error(f"Error in OpenAlex processing: {e}")
        raise

async def process_additional_sources(output_dir: str, exporter: ContentExporter, 
                                  limit: int, logger) -> Dict[str, List]:
    """Process additional sources with incremental saving"""
    output_sources = {}
    
    try:
        # Read OpenAlex CSV for ORCIDs
        openalex_path = os.path.join(output_dir, "openalex.csv")
        if not os.path.exists(openalex_path):
            logger.error(f"OpenAlex CSV not found at {openalex_path}")
            return output_sources
            
        openalex_df = pd.read_csv(openalex_path)
        
        # Initialize exporter with OpenAlex data
        exporter.initialize_from_openalex(openalex_df)
        
        # 1. Process ORCID 
        orcid_results = []
        logger.info("Fetching ORCID publications...")
        orcid_client_id = os.getenv("ORCID_CLIENT_ID")
        orcid_client_secret = os.getenv("ORCID_CLIENT_SECRET")
        
        if orcid_client_id and orcid_client_secret:
            try:
                orcid_scraper = OrcidScraper(orcid_client_id, orcid_client_secret)
                
                # Get unique ORCIDs from OpenAlex CSV
                orcids = openalex_df['Expert_ORCID'].dropna().unique()
                logger.info(f"Found {len(orcids)} unique ORCIDs in OpenAlex data")
                
                for orcid_id in orcids:
                    try:
                        pubs = orcid_scraper.fetch_researcher_publications(orcid_id)
                        unique_pubs = [pub for pub in pubs 
                                     if not exporter.is_duplicate(pub.doi, pub.title)]
                        
                        # Add source to each publication
                        for pub in unique_pubs:
                            pub.source = 'ORCID'
                            pub.orcid_id = orcid_id  # Ensure ORCID is preserved
                            exporter.add_content(pub.doi, pub.title)
                        
                        orcid_results.extend(unique_pubs)
                        logger.info(f"Found {len(unique_pubs)} unique ORCID publications for {orcid_id}")
                    except Exception as e:
                        logger.error(f"Error fetching ORCID ID {orcid_id}: {str(e)}")
                        continue
                
                output_sources['orcid'] = orcid_results
            
            except Exception as e:
                logger.error(f"Error initializing ORCID scraper: {e}")
        
        # 2. Process Knowhub
        try:
            logger.info("Fetching Knowhub publications...")
            knowhub_scraper = KnowhubScraper()
            knowhub_content = knowhub_scraper.fetch_publications(limit)
            
            # Filter unique Knowhub content
            unique_knowhub = [content for content in knowhub_content 
                             if not exporter.is_duplicate(content.doi, content.title)]
            
            # Add source to each publication
            for content in unique_knowhub:
                content.source = 'Knowhub'
                exporter.add_content(content.doi, content.title)
            
            output_sources['knowhub'] = unique_knowhub
            logger.info(f"Found {len(unique_knowhub)} unique Knowhub publications")
        
        except Exception as e:
            logger.error(f"Error fetching Knowhub publications: {e}")
        
        # 3. Process Website
        try:
            logger.info("Fetching website content...")
            website_scraper = WebsiteScraper()
            website_content = website_scraper.fetch_content(limit=limit)
            
            # Filter unique website content
            unique_website = [content for content in website_content 
                             if not exporter.is_duplicate(content.doi, content.title)]
            
            # Add source to each publication
            for content in unique_website:
                content.source = 'Website'
                exporter.add_content(content.doi, content.title)
            
            output_sources['website'] = unique_website
            logger.info(f"Found {len(unique_website)} unique website items")
        
        except Exception as e:
            logger.error(f"Error fetching website content: {e}")
        
        return output_sources
        
    except Exception as e:
        logger.error(f"Error processing additional sources: {e}")
        return output_sources  # Return whatever content was collected so far

async def main():
    # Load environment variables and setup logging
    load_dotenv()
    logger = setup_logging()
    
    try:
        print("\nAPHRC Content Aggregator")
        print("=====================")
        
        # Get number of items to fetch per source
        limit = input("\nHow many items to fetch per source? (default 100, 0 for all): ")
        limit = int(limit) if limit.isdigit() else 100
        
        # Create exporter
        exporter = ContentExporter()
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Only process Website
        output_sources = {}
        
        # Process Website
        try:
            logger.info("Fetching website content...")
            website_scraper = WebsiteScraper()
            website_content = website_scraper.fetch_content(limit=limit)
            
            # Store Website content
            output_sources['website'] = website_content
            logger.info(f"Found {len(website_content)} website items")
            
            # Save Website content
            if website_content:
                website_df = pd.DataFrame([{
                    'title': item.title,
                    'authors': '; '.join(item.authors),
                    'date': item.date.strftime('%Y-%m-%d') if item.date else '',
                    'abstract': item.abstract,
                    'doi': item.doi,
                    'url': item.url,
                    'content_type': item.content_type,
                    'keywords': '; '.join(item.keywords),
                    'full_text': item.full_text,
                    'image_url': item.image_url
                } for item in website_content])
                
                website_path = os.path.join(output_dir, "website.csv")
                website_df.to_csv(website_path, index=False)
                print(f"\nSaved Website content to: {website_path}")
        
        except Exception as e:
            logger.error(f"Error fetching website content: {e}")
        
        print(f"\nResults summary:")
        print(f"- Website items: {len(output_sources.get('website', []))}")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"\nAn error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())