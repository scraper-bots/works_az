#!/usr/bin/env python3
"""
Fast async main scraper for Glorri.az job listings
Uses asyncio and aiohttp for high-performance scraping
"""

import asyncio
import aiohttp
import logging
import sys
from datetime import datetime
from typing import List, Dict

from database import DatabaseManager
from api_scraper_async import AsyncAPIJobScraper
from page_scraper import JobPageScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class AsyncGlorriJobScraper:
    def __init__(self):
        self.db = DatabaseManager()
        self.api_scraper = AsyncAPIJobScraper()
        self.page_scraper = JobPageScraper()
        logger.info("Async Glorri Job Scraper initialized")
    
    async def scrape_all_jobs(self) -> Dict[str, int]:
        """
        Complete async scraping pipeline:
        1. Discover all companies
        2. Scrape all jobs from API
        3. Enhance with detailed page data
        4. Store in database
        5. Clean up old data
        """
        stats = {
            'companies_found': 0,
            'jobs_found': 0,
            'jobs_stored': 0,
            'old_jobs_removed': 0,
            'errors': 0
        }
        
        logger.info("="*60)
        logger.info("STARTING ASYNC JOB SCRAPING PIPELINE")
        logger.info("="*60)
        start_time = datetime.now()
        
        try:
            # Step 1: Mark scraping start
            scrape_timestamp = self.db.mark_scraping_start()
            
            # Step 2: Discover all companies
            logger.info("Discovering all companies...")
            companies = await self.api_scraper.discover_all_companies()
            stats['companies_found'] = len(companies)
            
            if not companies:
                logger.error("No companies found!")
                return stats
            
            logger.info(f"Found {len(companies)} companies: {companies}")
            
            # Step 3: Scrape all jobs from API
            logger.info("Scraping jobs from all companies...")
            api_jobs = await self.api_scraper.scrape_all_companies(companies)
            stats['jobs_found'] = len(api_jobs)
            
            if not api_jobs:
                logger.warning("No jobs found from API!")
                return stats
            
            logger.info(f"Found {len(api_jobs)} jobs from API")
            
            # Step 4: Enhance jobs with detailed page data
            logger.info("Enhancing jobs with detailed page data...")
            enhanced_jobs = await self._enhance_jobs_with_details(api_jobs)
            
            # Step 5: Store everything
            logger.info("Storing data in database...")
            self._store_companies(enhanced_jobs)
            stats['jobs_stored'] = self._store_jobs(enhanced_jobs)
            
            # Step 6: Clean up old data
            logger.info("Removing old jobs...")
            cleanup_stats = self.db.cleanup_old_data(scrape_timestamp)
            stats['old_jobs_removed'] = cleanup_stats['jobs_removed']
            
            # Final results
            duration = datetime.now() - start_time
            logger.info("="*60)
            logger.info("ASYNC SCRAPING COMPLETED SUCCESSFULLY!")
            logger.info(f"Duration: {duration}")
            logger.info(f"Companies found: {stats['companies_found']}")
            logger.info(f"Jobs found: {stats['jobs_found']}")
            logger.info(f"Jobs stored: {stats['jobs_stored']}")
            logger.info(f"Old jobs removed: {stats['old_jobs_removed']}")
            logger.info(f"Errors: {stats['errors']}")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            stats['errors'] += 1
            
        return stats
    
    async def _enhance_jobs_with_details(self, jobs: List[Dict]) -> List[Dict]:
        """Enhance jobs with detailed page data using async processing"""
        
        # Filter jobs that have URLs
        jobs_with_urls = [job for job in jobs if job.get('job_url')]
        logger.info(f"Enhancing {len(jobs_with_urls)} jobs with page details")
        
        if not jobs_with_urls:
            return jobs
        
        enhanced_jobs = []
        semaphore = asyncio.Semaphore(3)  # Limit concurrent page scraping
        
        async def enhance_single_job(job):
            async with semaphore:
                try:
                    job_url = job.get('job_url')
                    if not job_url:
                        return job
                    
                    # Run page scraper in thread pool (it's sync)
                    loop = asyncio.get_event_loop()
                    detailed_data = await loop.run_in_executor(
                        None, self.page_scraper.scrape_job_page, job_url
                    )
                    
                    if detailed_data and len(detailed_data) > 2:
                        # Merge API data with detailed data
                        enhanced_job = {**job, **detailed_data}
                        logger.info(f"Enhanced: {job.get('title', 'Unknown')}")
                        return enhanced_job
                    else:
                        logger.warning(f"Could not enhance: {job.get('title', 'Unknown')}")
                        return job
                    
                except Exception as e:
                    logger.error(f"Error enhancing job {job.get('title')}: {e}")
                    return job
        
        # Process jobs concurrently but with rate limiting
        batch_size = 10
        for i in range(0, len(jobs_with_urls), batch_size):
            batch = jobs_with_urls[i:i+batch_size]
            
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(jobs_with_urls)-1)//batch_size + 1}")
            
            # Process batch
            tasks = [enhance_single_job(job) for job in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Add results
            for result in batch_results:
                if isinstance(result, dict):
                    enhanced_jobs.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Batch error: {result}")
            
            # Rate limiting between batches
            await asyncio.sleep(2)
        
        # Add jobs without URLs
        jobs_without_urls = [job for job in jobs if not job.get('job_url')]
        enhanced_jobs.extend(jobs_without_urls)
        
        logger.info(f"Enhanced {len(enhanced_jobs)} total jobs")
        return enhanced_jobs
    
    def _store_companies(self, jobs: List[Dict]):
        """Store unique companies from jobs"""
        companies = {}
        for job in jobs:
            slug = job.get('company_slug')
            if slug and slug not in companies:
                companies[slug] = {
                    'name': job.get('company_name', ''),
                    'slug': slug,
                    'logo': job.get('company_logo', '')
                }
        
        for company_data in companies.values():
            try:
                self.db.insert_company(company_data)
            except Exception as e:
                logger.error(f"Error storing company {company_data.get('name')}: {e}")
    
    def _store_jobs(self, jobs: List[Dict]) -> int:
        """Store jobs in database"""
        stored = 0
        for job in jobs:
            try:
                # Get company_id
                if job.get('company_slug'):
                    self.db.cursor.execute(
                        'SELECT id FROM "apply-bot".companies WHERE slug = %s',
                        (job['company_slug'],)
                    )
                    result = self.db.cursor.fetchone()
                    if result:
                        job['company_id'] = result['id']
                
                # Truncate long fields
                if job.get('job_type') and len(job['job_type']) > 100:
                    job['job_type'] = job['job_type'][:100]
                if job.get('experience_level') and len(job['experience_level']) > 100:
                    job['experience_level'] = job['experience_level'][:100]
                
                if self.db.insert_job(job):
                    stored += 1
                    
            except Exception as e:
                logger.error(f"Error storing job {job.get('title')}: {e}")
        
        return stored
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        self.page_scraper.close()

async def main():
    """Main async entry point"""
    scraper = None
    try:
        scraper = AsyncGlorriJobScraper()
        stats = await scraper.scrape_all_jobs()
        
        print("\n" + "="*50)
        print("FINAL RESULTS")
        print("="*50)
        print(f"Companies Found: {stats.get('companies_found', 0)}")
        print(f"Jobs Found: {stats.get('jobs_found', 0)}")
        print(f"Jobs Stored: {stats.get('jobs_stored', 0)}")
        print(f"Old Jobs Removed: {stats.get('old_jobs_removed', 0)}")
        print(f"Errors: {stats.get('errors', 0)}")
        print("="*50)
        
        if stats.get('jobs_stored', 0) > 0:
            print("✅ Async scraping completed successfully!")
        else:
            print("❌ No jobs were stored. Check logs for errors.")
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    asyncio.run(main())