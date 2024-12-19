# File: main.py

from src.scrapers.website_scraper import WebsiteScraper
from src.scrapers.knowhub_scraper import KnowhubScraper
from src.scrapers.orcid_scraper import OrcidScraper
from src.utils.exporters import ContentExporter
import logging
from dotenv import load_dotenv
import os
from datetime import datetime

# APHRC researcher ORCID IDs
RESEARCHER_ORCID_IDS = [
    "0000-0002-6004-3972", "0000-0001-6205-3296", "0000-0002-0735-9839",
    "0000-0001-7155-3786", "0000-0003-1866-3905", "0000-0001-7742-9954",
    "0000-0002-7200-6116", "0000-0002-6878-0627", "0000-0002-3682-4744",
    "0000-0002-0508-1773", "0000-0003-4206-9746", "0000-0001-8440-064X",
    # ... rest of the ORCID IDs ...
]

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

def main():
    # Load environment variables and setup logging
    load_dotenv()
    logger = setup_logging()
    
    try:
        print("\nAPHRC Content Scraper")
        print("===================")
        
        print("\nWhat would you like to fetch?")
        print("1. Website content")
        print("2. Knowhub publications")
        print("3. ORCID publications")
        print("4. All sources")
        
        choice = input("\nEnter your choice (1-4): ")
        
        # Get number of items to fetch
        limit = input("\nHow many items to fetch per source? (default 100, 0 for all): ")
        limit = int(limit) if limit.isdigit() else 100
        
        all_content = []
        
        # Fetch website content
        if choice in ['1', '4']:
            logger.info("Fetching website content...")
            website_scraper = WebsiteScraper()
            website_content = website_scraper.fetch_content(limit=limit)
            all_content.extend(website_content)
            logger.info(f"Found {len(website_content)} website items")
        
        # Fetch Knowhub publications
        if choice in ['2', '4']:
            logger.info("Fetching Knowhub publications...")
            knowhub_scraper = KnowhubScraper()
            knowhub_content = knowhub_scraper.fetch_publications(limit=limit)
            all_content.extend(knowhub_content)
            logger.info(f"Found {len(knowhub_content)} Knowhub publications")
        
        # Fetch ORCID publications
        if choice in ['3', '4']:
            logger.info("Fetching ORCID publications...")
            orcid_client_id = os.getenv("ORCID_CLIENT_ID")
            orcid_client_secret = os.getenv("ORCID_CLIENT_SECRET")
            
            if not orcid_client_id or not orcid_client_secret:
                logger.error("ORCID credentials not found in environment variables")
            else:
                orcid_scraper = OrcidScraper(orcid_client_id, orcid_client_secret)
                orcid_content = []
                
                for orcid_id in RESEARCHER_ORCID_IDS:
                    try:
                        pubs = orcid_scraper.fetch_researcher_publications(orcid_id)
                        orcid_content.extend(pubs)
                        logger.info(f"Found {len(pubs)} publications for ORCID ID: {orcid_id}")
                    except Exception as e:
                        logger.error(f"Error fetching ORCID ID {orcid_id}: {str(e)}")
                
                all_content.extend(orcid_content)
                logger.info(f"Found {len(orcid_content)} total ORCID publications")
        
        if all_content:
            # Export to CSV
            exporter = ContentExporter()
            csv_file = exporter.export_to_csv(
                all_content,
                f"aphrc_content_{datetime.now().strftime('%Y%m%d')}.csv"
            )
            
            print(f"\nTotal items fetched: {len(all_content)}")
            print(f"Results exported to: {csv_file}")
            
        else:
            print("\nNo content found.")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"\nAn error occurred: {str(e)}")

if __name__ == "__main__":
    main()