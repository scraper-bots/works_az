#!/usr/bin/env python3
"""
Discover ALL companies by scraping the general jobs endpoint
"""

import asyncio
import aiohttp
import logging
from typing import Set, List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompanyDiscovery:
    def __init__(self):
        self.base_api_url = "https://api.glorri.az/job-service-v2/jobs"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'az',
            'content-type': 'application/json',
            'dnt': '1',
            'origin': 'https://jobs.glorri.az',
            'referer': 'https://jobs.glorri.az/',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
        }
    
    async def discover_all_companies(self) -> List[str]:
        """Discover all companies by scraping the general jobs API"""
        
        companies = set()
        
        # Try different potential endpoints for getting all jobs
        endpoints_to_try = [
            "/public",  # General public jobs
            "",         # Base endpoint
            "/search",  # Search endpoint
            "/all",     # All jobs
            "/list"     # List endpoint
        ]
        
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        ) as session:
            
            # Try each endpoint
            for endpoint in endpoints_to_try:
                try:
                    url = f"{self.base_api_url}{endpoint}"
                    logger.info(f"Trying endpoint: {url}")
                    
                    # Try with different parameters
                    param_combinations = [
                        {'skip': 0, 'limit': 20},
                        {'skip': 0, 'limit': 100},
                        {'page': 0, 'size': 20},
                        {'offset': 0, 'limit': 20},
                        {}  # No parameters
                    ]
                    
                    for params in param_combinations:
                        try:
                            async with session.get(url, params=params) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    logger.info(f"✅ Success with {url} and params {params}")
                                    
                                    # Extract companies from the response
                                    if 'entities' in data:
                                        for job in data['entities']:
                                            if 'company' in job and 'slug' in job['company']:
                                                companies.add(job['company']['slug'])
                                        
                                        logger.info(f"Found {len(companies)} unique companies so far")
                                        
                                        # If this endpoint works, get more pages
                                        await self._scrape_all_pages(session, url, params, companies)
                                        break
                                        
                                else:
                                    logger.debug(f"❌ {response.status} for {url} with {params}")
                        
                        except Exception as e:
                            logger.debug(f"❌ Error with {url} and {params}: {e}")
                            continue
                    
                    if companies:  # If we found companies, no need to try other endpoints
                        break
                        
                except Exception as e:
                    logger.error(f"Error trying endpoint {endpoint}: {e}")
                    continue
            
            # If no general endpoint worked, try search with common terms
            if not companies:
                logger.info("No general endpoint worked. Trying search terms...")
                companies = await self._search_discovery(session)
        
        companies_list = sorted(list(companies))
        logger.info(f"🎉 DISCOVERY COMPLETE: Found {len(companies_list)} companies!")
        
        # Get job counts for each company
        company_counts = await self._get_company_job_counts(companies_list)
        
        # Sort by job count
        sorted_companies = sorted(company_counts, key=lambda x: x[1], reverse=True)
        total_jobs = sum(count for _, count in sorted_companies)
        
        logger.info(f"📊 FINAL RESULTS:")
        logger.info(f"   Companies: {len(sorted_companies)}")
        logger.info(f"   Total Jobs: {total_jobs}")
        
        logger.info(f"🏆 Top 20 companies by job count:")
        for company, count in sorted_companies[:20]:
            logger.info(f"   {company}: {count} jobs")
        
        return [company for company, _ in sorted_companies]
    
    async def _scrape_all_pages(self, session: aiohttp.ClientSession, url: str, base_params: dict, companies: set):
        """Scrape all pages from a working endpoint"""
        
        skip = base_params.get('skip', 0)
        limit = base_params.get('limit', 20)
        page = 0
        
        while True:
            try:
                # Update parameters for pagination
                if 'skip' in base_params:
                    params = {**base_params, 'skip': skip}
                elif 'page' in base_params:
                    params = {**base_params, 'page': page}
                else:
                    params = {**base_params, 'skip': skip, 'limit': limit}
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if 'entities' in data and data['entities']:
                            new_companies = 0
                            for job in data['entities']:
                                if 'company' in job and 'slug' in job['company']:
                                    if job['company']['slug'] not in companies:
                                        new_companies += 1
                                    companies.add(job['company']['slug'])
                            
                            logger.info(f"Page {page + 1}: Found {new_companies} new companies (total: {len(companies)})")
                            
                            # Check if we have more pages
                            if len(data['entities']) < limit:
                                break
                            
                            skip += limit
                            page += 1
                            
                            # Rate limiting
                            await asyncio.sleep(0.5)
                        else:
                            break
                    else:
                        break
                        
            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
                break
    
    async def _search_discovery(self, session: aiohttp.ClientSession) -> Set[str]:
        """Try to discover companies through search terms"""
        
        companies = set()
        
        # Common search terms in Azerbaijani
        search_terms = [
            '', 'iş', 'vakansiya', 'mütəxəssis', 'menecer', 'operator', 
            'bank', 'şirkət', 'holding', 'mağaza', 'satış', 'kredit',
            'teknologiya', 'insan', 'maliyyə', 'biznes', 'xidmət'
        ]
        
        search_url = f"{self.base_api_url}/search"
        
        for term in search_terms:
            try:
                params = {'q': term, 'skip': 0, 'limit': 20} if term else {'skip': 0, 'limit': 20}
                
                async with session.get(search_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if 'entities' in data:
                            for job in data['entities']:
                                if 'company' in job and 'slug' in job['company']:
                                    companies.add(job['company']['slug'])
                            
                            logger.info(f"Search '{term}': Found {len(companies)} total companies")
                
                await asyncio.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.debug(f"Search failed for '{term}': {e}")
                continue
        
        return companies
    
    async def _get_company_job_counts(self, companies: List[str]) -> List[tuple]:
        """Get job counts for all companies"""
        
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        ) as session:
            
            semaphore = asyncio.Semaphore(10)
            
            async def get_company_count(company_slug: str) -> tuple:
                async with semaphore:
                    try:
                        url = f"{self.base_api_url}/company/{company_slug}/public"
                        async with session.get(url, params={'skip': 0, 'limit': 1}) as response:
                            if response.status == 200:
                                data = await response.json()
                                return company_slug, data.get('totalCount', 0)
                    except:
                        pass
                    return company_slug, 0
            
            # Process in batches
            batch_size = 20
            results = []
            
            for i in range(0, len(companies), batch_size):
                batch = companies[i:i+batch_size]
                tasks = [get_company_count(company) for company in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, tuple):
                        results.append(result)
                
                logger.info(f"Processed {min(i+batch_size, len(companies))}/{len(companies)} companies")
                await asyncio.sleep(1)  # Rate limiting between batches
        
        return results

async def main():
    discovery = CompanyDiscovery()
    companies = await discovery.discover_all_companies()
    
    print(f"\n🎯 FINAL COMPANY LIST ({len(companies)} companies):")
    print("=" * 60)
    for company in companies:
        print(f"  '{company}',")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())