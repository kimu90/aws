import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Dict
import urllib3
from time import sleep

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class BaseScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.timeout = 15
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _make_request(self, url: str, params: Dict = None, headers: Dict = None) -> requests.Response:
        try:
            combined_headers = self.session.headers.copy()
            if headers:
                combined_headers.update(headers)
                
            response = self.session.get(url, params=params, headers=combined_headers, timeout=self.timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            self.logger.error(f"Error making request to {url}: {str(e)}")
            raise