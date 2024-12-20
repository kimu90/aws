from typing import List, Dict, Optional
import pandas as pd 
import os 
from datetime import datetime
import google.generativeai as genai


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
     
    def export_to_csv(self, content_list: List[Dict], filename: str) -> Optional[str]:
        """Export content to CSV, handling duplicates"""
        unique_content = []
        
        for content in content_list:
            if not self.is_duplicate(content.get('DOI'), content.get('Title')):
                unique_content.append({
                    'title': content.get('Title', ''),
                    'authors': content.get('Authors', ''),
                    'date': content.get('Publication_Year', ''),
                    'abstract': content.get('Abstract', ''),
                    'doi': content.get('DOI', ''),
                    'journal': content.get('Journal', ''),
                    'orcid_id': content.get('orcid_id', '')
                })
                self.add_content(content.get('DOI'), content.get('Title'))
         
        if unique_content:
            filepath = os.path.join(self.output_dir, filename)
            df = pd.DataFrame(unique_content)
            df.to_csv(filepath, index=False)
            return filepath
        return None
     
    def merge_all_sources(self, *source_dataframes: pd.DataFrame) -> pd.DataFrame:
        """Merge multiple source DataFrames with a source column"""
        # Add source column to each DataFrame
        source_dfs_with_source = []
        source_names = ['openalex', 'orcid', 'knowhub', 'website']
        
        for i, df in enumerate(source_dataframes):
            # Create a copy to avoid modifying the original
            df_copy = df.copy()
            
            # Add source column if it doesn't exist
            if i < len(source_names):
                df_copy['source'] = source_names[i]
            else:
                df_copy['source'] = f'source_{i}'
            
            source_dfs_with_source.append(df_copy)
        
        # Combine DataFrames
        combined_df = pd.concat(source_dfs_with_source, ignore_index=True)
        
        # Remove duplicates based on DOI and title
        combined_df['Title_lower'] = combined_df['Title'].str.lower()
        combined_df['DOI_lower'] = combined_df['DOI'].str.lower()
        combined_df = combined_df.drop_duplicates(subset=['DOI_lower', 'Title_lower'], keep='first')
        combined_df = combined_df.drop(['Title_lower', 'DOI_lower'], axis=1)

        return combined_df

    def generate_summary(self, title: str, abstract: str) -> str:
        """Generate a summary using the Gemini API"""
        prompt = f"Title: {title}\nAbstract: {abstract}\n\nGenerate a brief summary of the research article:"

        if not abstract:
            prompt = f"Title: {title}\n\nGenerate a brief summary of the research article based on the title:"

        model = genai.Model(model="text-bison-001"

        response = model.predict(
            prompt=prompt,
            temperature=0.7,
            max_output_tokens=50,
            top_p=0.8
        )

        return response.result