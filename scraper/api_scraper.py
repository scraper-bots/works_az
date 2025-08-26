import requests
import json
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIJobScraper:
    def __init__(self):
        self.base_api_url = "https://api.glorri.az/job-service-v2/jobs"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'az',
            'origin': 'https://jobs.glorri.az',
            'referer': 'https://jobs.glorri.az/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def get_all_companies(self) -> List[str]:
        """Get list of all company slugs by discovering from general job searches"""
        companies = set()
        try:
            logger.info("Discovering companies from job listings...")
            
            # Try different search approaches to discover companies
            search_queries = [
                "",  # Empty search to get general jobs
                "vakansiya",  # General job term
                "iş",  # Work in Azerbaijani
                "mütəxəssis",  # Specialist
                "menecer",  # Manager
            ]
            
            for query in search_queries:
                try:
                    # Try general jobs endpoint first
                    jobs_data = self.get_general_jobs(skip=0, limit=20)
                    if jobs_data and jobs_data.get('entities'):
                        new_companies = self.discover_companies_from_jobs(jobs_data['entities'])
                        companies.update(new_companies)
                        logger.info(f"Found {len(new_companies)} companies from general search")
                    
                    # Try search endpoint
                    search_data = self.search_jobs(query=query, skip=0, limit=20)
                    if search_data and search_data.get('entities'):
                        new_companies = self.discover_companies_from_jobs(search_data['entities'])
                        companies.update(new_companies)
                        logger.info(f"Found {len(new_companies)} companies from search '{query}'")
                    
                except Exception as e:
                    logger.warning(f"Error in search '{query}': {e}")
                    continue
            
            # Try to get companies from different pagination offsets
            for skip in [0, 20, 40, 60, 80]:
                try:
                    jobs_data = self.get_general_jobs(skip=skip, limit=20)
                    if jobs_data and jobs_data.get('entities'):
                        new_companies = self.discover_companies_from_jobs(jobs_data['entities'])
                        companies.update(new_companies)
                except Exception as e:
                    logger.warning(f"Error getting jobs at offset {skip}: {e}")
                    continue
            
            companies_list = list(companies)
            
            # If no companies discovered from API, fallback to known companies list
            if not companies_list:
                logger.info("No companies discovered from API endpoints. Using fallback known companies list...")
                companies_list = self.get_known_companies()
                
            logger.info(f"Total unique companies discovered: {len(companies_list)}")
            return companies_list
            
        except Exception as e:
            logger.error(f"Error discovering companies: {e}")
            # Fallback to known companies
            logger.info("Using fallback known companies list due to discovery errors...")
            return self.get_known_companies()
    
    def get_known_companies(self) -> List[str]:
        """Return a list of known company slugs as fallback"""
        known_companies = [
            'abc-telecom',
            'kapital-bank', 
            'pasha-bank',
            'access-bank',
            'baku-electronics',
            'Gilan-holdinq',
            'rabitabank',
            'express-bank',
            'azersu',
            'azercell',
            'bakcell',
            'nar',
            'socar',
            'azerishiq',
            'aztelecom',
            'pmu',
            'azpost',
            'stp',
            'azermash',
            'azerenerji',
            'azerturk-bank',
            'yelo-bank',
            'xalq-bank',
            'gunay-bank',
            'amrahbank',
            'turanbank',
            'muganbank',
            'ziraat-bank-azerbaijan',
            'abb-bank',
            'pasha-life',
            'pasha-insurance',
            'ateshgah-insurance'
        ]
        
        logger.info(f"Using {len(known_companies)} known companies")
        return known_companies
    
    def get_company_jobs(self, company_slug: str, skip: int = 0, limit: int = 20) -> Optional[Dict]:
        """Get jobs for a specific company"""
        try:
            url = f"{self.base_api_url}/company/{company_slug}/public"
            params = {'skip': skip, 'limit': limit}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Fetched {len(data.get('entities', []))} jobs for {company_slug}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching jobs for company {company_slug}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response for company {company_slug}: {e}")
            return None
    
    def get_all_company_jobs(self, company_slug: str) -> List[Dict]:
        """Get all jobs for a company with pagination"""
        all_jobs = []
        skip = 0
        limit = 20
        
        while True:
            data = self.get_company_jobs(company_slug, skip, limit)
            if not data or not data.get('entities'):
                break
                
            jobs = data['entities']
            all_jobs.extend(jobs)
            
            # Check if we've fetched all jobs
            if len(jobs) < limit or len(all_jobs) >= data.get('totalCount', 0):
                break
                
            skip += limit
            time.sleep(1)  # Be respectful to the API
            
        logger.info(f"Total jobs fetched for {company_slug}: {len(all_jobs)}")
        return all_jobs
    
    def get_general_jobs(self, skip: int = 0, limit: int = 20) -> Optional[Dict]:
        """Get general job listings (not company-specific)"""
        try:
            # This might be a different endpoint - adjust based on actual API
            url = f"{self.base_api_url}/public"
            params = {'skip': skip, 'limit': limit}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Fetched {len(data.get('entities', []))} general jobs")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"General jobs endpoint might not exist: {e}")
            return None
    
    def search_jobs(self, query: str = "", location: str = "", category: str = "", 
                   skip: int = 0, limit: int = 20) -> Optional[Dict]:
        """Search for jobs with filters"""
        try:
            url = f"{self.base_api_url}/search"
            params = {
                'skip': skip,
                'limit': limit
            }
            
            if query:
                params['q'] = query
            if location:
                params['location'] = location
            if category:
                params['category'] = category
                
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Found {len(data.get('entities', []))} jobs for search query")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Search endpoint might not exist: {e}")
            return None
    
    def parse_job_data(self, job: Dict) -> Dict:
        """Parse job data from API response"""
        try:
            # Parse posted date
            posted_date = None
            if job.get('postedDate'):
                try:
                    posted_date = datetime.fromisoformat(job['postedDate'].replace('Z', '+00:00'))
                except ValueError:
                    logger.warning(f"Could not parse date: {job.get('postedDate')}")
            
            # Build job URL
            company_slug = job.get('company', {}).get('slug', '')
            job_slug = job.get('slug', '')
            job_url = f"https://jobs.glorri.az/vacancies/{company_slug}/{job_slug}" if company_slug and job_slug else None
            
            parsed_job = {
                'title': job.get('title', ''),
                'slug': job.get('slug', ''),
                'company_name': job.get('company', {}).get('name', ''),
                'company_slug': company_slug,
                'company_logo': job.get('company', {}).get('logo', ''),
                'location': job.get('location', ''),
                'posted_date': posted_date,
                'view_count': job.get('viewCount', 0),
                'job_url': job_url,
                'is_active': True
            }
            
            return parsed_job
            
        except Exception as e:
            logger.error(f"Error parsing job data: {e}")
            return {}
    
    def discover_companies_from_jobs(self, jobs: List[Dict]) -> List[str]:
        """Extract unique company slugs from job listings"""
        companies = set()
        for job in jobs:
            company = job.get('company', {})
            if company.get('slug'):
                companies.add(company['slug'])
        
        return list(companies)
    
    def scrape_all_jobs_iteratively(self, max_companies: int = None) -> List[Dict]:
        """Scrape jobs by discovering companies iteratively"""
        all_jobs = []
        discovered_companies = set()
        processed_companies = set()
        
        # Start with some known companies or try to get them from a general endpoint
        initial_companies = self.get_all_companies()
        
        # Try to get general jobs to discover companies
        general_jobs_data = self.get_general_jobs()
        if general_jobs_data and general_jobs_data.get('entities'):
            general_jobs = general_jobs_data['entities']
            all_jobs.extend([self.parse_job_data(job) for job in general_jobs])
            discovered_companies.update(self.discover_companies_from_jobs(general_jobs))
        
        # Add initial companies to discovered set
        discovered_companies.update(initial_companies)
        
        logger.info(f"Starting with {len(discovered_companies)} companies to process")
        
        # Process each discovered company
        companies_processed = 0
        for company_slug in list(discovered_companies):
            if max_companies and companies_processed >= max_companies:
                break
                
            if company_slug in processed_companies:
                continue
                
            logger.info(f"Processing company: {company_slug} ({companies_processed + 1})")
            
            company_jobs = self.get_all_company_jobs(company_slug)
            if company_jobs:
                parsed_jobs = [self.parse_job_data(job) for job in company_jobs]
                all_jobs.extend(parsed_jobs)
                
                # Discover more companies from these jobs
                new_companies = self.discover_companies_from_jobs(company_jobs)
                discovered_companies.update(new_companies)
                
            processed_companies.add(company_slug)
            companies_processed += 1
            
            # Be respectful to the API
            time.sleep(2)
        
        logger.info(f"Scraping complete. Total jobs: {len(all_jobs)}, Companies processed: {len(processed_companies)}")
        return all_jobs
    
    def close(self):
        """Close the session"""
        self.session.close()