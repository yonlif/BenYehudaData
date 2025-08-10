import requests
import json
from typing import Dict, Optional, List, Iterator
import os
from dotenv import load_dotenv
from pathlib import Path
import time
from tqdm import tqdm
import logging

class BenYehudaAPI:
    """API client for the Ben Yehuda Project."""
    
    BASE_URL = "https://benyehuda.org/api/v1"
    BATCH_SIZE = 20  # Number of texts to fetch in each batch
    
    def __init__(self, api_key: Optional[str] = None, output_dir: str = "benyehuda_data"):
        """Initialize the API client.
        
        Args:
            api_key: API key for Ben Yehuda Project. If not provided, will look for BENYEHUDA_API_KEY env variable.
            output_dir: Directory to save the scraped works
        """
        load_dotenv()  # Load environment variables from .env file
        self.api_key = api_key or os.getenv('BENYEHUDA_API_KEY')
        if not self.api_key:
            raise ValueError("API key is required. Either pass it to the constructor or set BENYEHUDA_API_KEY environment variable.")
        
        # Setup output directory
        self.output_dir = Path(output_dir)
        self.works_dir = self.output_dir / "works"
        self.works_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_texts_batch(self, text_ids: List[int], view: str = 'enriched', file_format: str = 'txt') -> List[Dict]:
        """Get multiple texts in a single batch request.
        
        Args:
            text_ids: List of text IDs to retrieve
            view: One of 'metadata', 'basic', or 'enriched'
            file_format: One of 'html', 'txt', 'pdf', 'epub', 'mobi', 'docx', 'odt'
            
        Returns:
            List of text data dictionaries
        """
        payload = {
            'key': self.api_key,
            'ids': text_ids,
            'view': view,
            'file_format': file_format,
            'snippet': True
        }
        
        response = requests.post(f"{self.BASE_URL}/texts/batch", json=payload)
        response.raise_for_status()
        return response.json()

    def search_texts(self, search_after: Optional[List[str]] = None, **kwargs) -> Dict:
        """Search for texts using various criteria.
        
        Args:
            search_after: Token for pagination
            **kwargs: Additional search parameters
            
        Returns:
            Dict containing search results
        """
        payload = {
            'key': self.api_key,
            'view': kwargs.get('view', 'enriched'),
            'file_format': kwargs.get('file_format', 'txt'),
            'snippet': kwargs.get('include_snippet', True),
            'sort_by': kwargs.get('sort_by', 'alphabetical'),
            'sort_dir': kwargs.get('sort_dir', 'asc')
        }
        
        if search_after:
            payload['search_after'] = search_after
            
        response = requests.post(f"{self.BASE_URL}/search", json=payload)
        response.raise_for_status()
        return response.json()

    def get_total_works_count(self) -> int:
        """Get the total number of works available in the project.
        
        Returns:
            int: Total number of works
        """
        results = self.search_texts()
        return results.get('total_count', 0)

    def get_all_works(self) -> Iterator[Dict]:
        """Get all works from the project using pagination.
        
        Yields:
            Dict: Work data
        """
        search_after = None
        
        while True:
            try:
                results = self.search_texts(search_after=search_after)

                if not results.get('data'):
                    break
                
                for work in results['data']:
                    yield work
                
                search_after = results.get('next_page_search_after')
                if not search_after:
                    break
                    
                # Be nice to the API
                #time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error fetching page: {str(e)}")
                break

    def save_work(self, work: Dict) -> None:
        """Save a work to file.
        
        Args:
            work: Work data to save
        """
        work_id = work.get('id')
        if not work_id:
            self.logger.warning("Work has no ID, skipping")
            return
            
        work_file = self.works_dir / f"work_{work_id}.json"
        try:
            with open(work_file, 'w', encoding='utf-8') as f:
                json.dump(work, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving work {work_id}: {str(e)}")

    def process_works_batch(self, works: List[Dict], scraped_ids: set) -> List[int]:
        """Process a batch of works, saving only new ones.
        
        Args:
            works: List of work metadata from search
            scraped_ids: Set of already scraped work IDs
            
        Returns:
            List of IDs that need to be fetched
        """
        ids_to_fetch = []
        for work in works:
            work_id = work.get('id')
            if work_id and work_id not in scraped_ids:
                ids_to_fetch.append(work_id)
        return ids_to_fetch

    def scrape_all_works(self) -> None:
        """Scrape all works from the Ben Yehuda Project."""
        self.logger.info("Starting to scrape all works...")
        
        # Get total count for progress tracking
        total_count = self.get_total_works_count()
        self.logger.info(f"Found {total_count} works to scrape")
        
        # Create progress file to track progress
        progress_file = self.output_dir / "progress.json"
        scraped_ids = list()
        
        # Load progress if exists
        if progress_file.exists():
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    scraped_ids = list(json.load(f))
                self.logger.info(f"Loaded {len(scraped_ids)} previously scraped works")
            except Exception as e:
                self.logger.error(f"Error loading progress file: {str(e)}")
        
        try:
            # Buffer for collecting works to batch process
            works_buffer = []
            
            for work in tqdm(self.get_all_works(), desc="Scraping works", total=total_count):
                works_buffer.append(work)
                
                # Process in batches
                if len(works_buffer) >= self.BATCH_SIZE:
                    # Get IDs that haven't been scraped yet
                    ids_to_fetch = self.process_works_batch(works_buffer, scraped_ids)

                    if ids_to_fetch:
                        try:
                            # Fetch full details for the batch
                            full_works = self.get_texts_batch(ids_to_fetch)
                            
                            # Save each work
                            for full_work in full_works:
                                self.save_work(full_work)
                                work_id = full_work.get('id')
                                if work_id:
                                    scraped_ids.append(work_id)
                            
                            # Update progress file
                            if len(scraped_ids) % (self.BATCH_SIZE * 2) == 0:
                                with open(progress_file, 'w', encoding='utf-8') as f:
                                    json.dump(list(scraped_ids), f)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing batch: {str(e)}")
                    
                    # Clear the buffer
                    works_buffer = []
                    
                    # Be nice to the API
                    time.sleep(0.5)
            
            # Process any remaining works in the buffer
            if works_buffer:
                ids_to_fetch = self.process_works_batch(works_buffer, scraped_ids)
                if ids_to_fetch:
                    try:
                        full_works = self.get_texts_batch(ids_to_fetch)
                        for full_work in full_works:
                            self.save_work(full_work)
                            work_id = full_work.get('id')
                            if work_id:
                                scraped_ids.append(work_id)
                    except Exception as e:
                        self.logger.error(f"Error processing final batch: {str(e)}")
                
        except KeyboardInterrupt:
            self.logger.info("Scraping interrupted by user")
        finally:
            # Save final progress
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(list(scraped_ids), f)
            
            self.logger.info(f"Finished scraping. Total works scraped: {len(scraped_ids)}")

def main():
    """Example usage of the API client."""
    try:
        # Initialize the client
        client = BenYehudaAPI()
        
        # Get total count and start scraping
        total_count = client.get_total_works_count()
        print(f"\nStarting to scrape {total_count} works...")
        
        # Scrape all works
        client.scrape_all_works()
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 
