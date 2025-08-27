#!/usr/bin/env python3
"""
API scraper for jobs and companies
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncAPIJobScraper:
    def __init__(self):
        self.base_api_url = "https://api.glorri.az/job-service-v2/jobs"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'az',
            'origin': 'https://jobs.glorri.az',
            'referer': 'https://jobs.glorri.az/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
    
    async def discover_all_companies(self) -> List[str]:
        """Discover all companies using the companies API endpoint"""
        
        companies_api_url = "https://api.glorri.az/user-service-v2/companies/public"
        companies = []
        
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        ) as session:
            
            offset = 0
            limit = 18
            
            while True:
                try:
                    params = {'limit': limit, 'offset': offset}
                    
                    async with session.get(companies_api_url, params=params) as response:
                        if response.status != 200:
                            logger.error(f"Companies API returned {response.status}")
                            break
                        
                        data = await response.json()
                        
                        # Extract companies from response
                        if 'entities' in data and data['entities']:
                            batch_companies = []
                            for company in data['entities']:
                                if company.get('slug'):
                                    companies.append(company['slug'])
                                    batch_companies.append({
                                        'slug': company['slug'],
                                        'name': company.get('name', ''),
                                        'job_count': company.get('jobCount', 0)
                                    })
                            
                            logger.info(f"Found {len(batch_companies)} companies in batch (offset: {offset})")
                            
                            # Check if we have more companies
                            if len(data['entities']) < limit:
                                break
                            
                            offset += limit
                            await asyncio.sleep(0.5)  # Rate limiting
                        else:
                            break
                            
                except Exception as e:
                    logger.error(f"Error fetching companies: {e}")
                    break
        
        total_companies = len(companies)
        logger.info(f"🎉 Discovered {total_companies} total companies from API")
        
        # Get job counts summary
        if total_companies > 0:
            logger.info(f"📊 Company discovery complete!")
            logger.info(f"   Total companies: {total_companies}")
        
        return companies
    
    async def get_company_jobs(self, session: aiohttp.ClientSession, company_slug: str) -> List[Dict]:
        """Get all jobs for a company with async pagination"""
        all_jobs = []
        skip = 0
        limit = 20
        
        while True:
            try:
                url = f"{self.base_api_url}/company/{company_slug}/public"
                params = {'skip': skip, 'limit': limit}
                
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        break
                    
                    data = await response.json()
                    jobs = data.get('entities', [])
                    
                    if not jobs:
                        break
                    
                    all_jobs.extend(jobs)
                    
                    # Check if we've fetched all jobs
                    if len(jobs) < limit or len(all_jobs) >= data.get('totalCount', 0):
                        break
                    
                    skip += limit
                    await asyncio.sleep(0.5)  # Rate limiting
                    
            except Exception as e:
                logger.error(f"Error fetching jobs for {company_slug}: {e}")
                break
        
        logger.info(f"Fetched {len(all_jobs)} jobs for {company_slug}")
        return all_jobs
    
    async def scrape_all_companies(self, companies: List[str]) -> List[Dict]:
        """Scrape jobs from all companies concurrently"""
        
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=3)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector, 
            timeout=timeout
        ) as session:
            
            semaphore = asyncio.Semaphore(5)  # Limit concurrent company requests
            
            async def scrape_company(company_slug: str) -> List[Dict]:
                async with semaphore:
                    jobs = await self.get_company_jobs(session, company_slug)
                    # Parse job data
                    return [self.parse_job_data(job, company_slug) for job in jobs if job]
            
            # Scrape all companies
            tasks = [scrape_company(company) for company in companies]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Flatten results
            all_jobs = []
            for result in results:
                if isinstance(result, list):
                    all_jobs.extend(result)
                elif isinstance(result, Exception):
                    logger.error(f"Error scraping company: {result}")
            
            logger.info(f"Total jobs scraped: {len(all_jobs)}")
            return all_jobs
    
    def parse_job_data(self, job: Dict, company_slug: str) -> Dict:
        """Parse job data from API response"""
        try:
            parsed_job = {
                'title': job.get('title', ''),
                'slug': job.get('slug', ''),
                'company_name': job.get('company', {}).get('name', ''),
                'company_slug': company_slug,
                'company_logo': job.get('company', {}).get('logo', ''),
                'location': job.get('location', ''),
                'job_type': job.get('workSchedule', ''),
                'experience_level': job.get('experience', ''),
                'posted_date': self._parse_date(job.get('createdAt')),
                'deadline': self._parse_date(job.get('deadlineAt')),
                'view_count': job.get('viewCount', 0),
                'category': job.get('category', {}).get('name', '') if job.get('category') else '',
                'job_url': f"https://jobs.glorri.az/vacancies/{company_slug}/{job.get('slug', '')}",
                'is_active': True
            }
            return parsed_job
        except Exception as e:
            logger.error(f"Error parsing job data: {e}")
            return {}
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to datetime"""
        if not date_str:
            return None
        try:
            from datetime import datetime
            # Handle ISO format dates
            if 'T' in date_str:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            return date_str
        except:
            return None

# Test discovery function
async def discover_and_test():
    """Discover companies and test the scraping"""
    scraper = AsyncAPIJobScraper()
    
    print("Discovering companies...")
    companies = await scraper.discover_all_companies()
    
    if companies:
        print(f"\nFound companies: {companies}")
        print(f"\nTesting scraping first 3 companies...")
        
        test_jobs = await scraper.scrape_all_companies(companies[:3])
        print(f"Successfully scraped {len(test_jobs)} jobs from test companies")
        
        if test_jobs:
            print("\nSample job:")
            sample = test_jobs[0]
            for key, value in sample.items():
                print(f"  {key}: {value}")
    
    return companies

if __name__ == "__main__":
    asyncio.run(discover_and_test())