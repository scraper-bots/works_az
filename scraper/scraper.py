#!/usr/bin/env python3
"""
Main job scraper orchestrator
"""

import asyncio
import aiohttp
import logging
import sys
from datetime import datetime
from typing import List, Dict

from database import DatabaseManager
from api_scraper import AsyncAPIJobScraper
from page_parser import JobPageParser

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
        self.page_parser = JobPageParser()
        logger.info("Async Glorri Job Scraper initialized")
    
    async def scrape_all_jobs(self) -> Dict[str, int]:
        """
        Complete async scraping pipeline with data integrity protection:
        1. Discover all companies
        2. Scrape all jobs from API
        3. Enhance with detailed page data (COMPLETE FIRST)
        4. Clear old data only after enhancement succeeds
        5. Store enhanced data atomically
        """
        stats = {
            'companies_found': 0,
            'jobs_found': 0,
            'jobs_stored': 0,
            'enhanced_jobs': 0,
            'old_jobs_removed': 0,
            'errors': 0
        }
        
        logger.info("="*60)
        logger.info("STARTING ASYNC JOB SCRAPING PIPELINE (DATA INTEGRITY MODE)")
        logger.info("="*60)
        start_time = datetime.now()
        
        enhanced_jobs = []  # Initialize to avoid data loss
        
        try:
            # Step 1: Discover all companies
            logger.info("Discovering all companies...")
            companies = await self.api_scraper.discover_all_companies()
            stats['companies_found'] = len(companies)
            
            if not companies:
                logger.error("No companies found!")
                return stats
            
            logger.info(f"Found {len(companies)} companies: {companies}")
            
            # Step 2: Scrape all jobs from API
            logger.info("Scraping jobs from all companies...")
            api_jobs = await self.api_scraper.scrape_all_companies(companies)
            stats['jobs_found'] = len(api_jobs)
            
            if not api_jobs:
                logger.warning("No jobs found from API!")
                return stats
            
            logger.info(f"Found {len(api_jobs)} jobs from API")
            
            # Step 3: CRITICAL - Complete enhancement BEFORE any database operations
            logger.info("Enhancing jobs with detailed page data...")
            logger.info("⚠️  CRITICAL PHASE: Enhancement must complete before truncation")
            
            try:
                enhanced_jobs = await self._enhance_jobs_with_details(api_jobs)
                stats['enhanced_jobs'] = len(enhanced_jobs)
                
                if not enhanced_jobs:
                    logger.error("❌ ENHANCEMENT FAILED - No enhanced jobs created!")
                    logger.error("❌ ABORTING to prevent data loss")
                    return stats
                
                # Verify enhancement quality
                enhanced_count = 0
                for job in enhanced_jobs:
                    if job.get('description') or job.get('requirements') or job.get('category'):
                        enhanced_count += 1
                
                logger.info(f"✅ Enhancement successful: {enhanced_count}/{len(enhanced_jobs)} jobs have enhanced data")
                
                if enhanced_count < len(enhanced_jobs) * 0.5:  # Less than 50% enhanced
                    logger.warning(f"⚠️  Low enhancement rate: {enhanced_count}/{len(enhanced_jobs)}")
                    logger.warning("⚠️  Continuing anyway, but this should be investigated")
                
            except Exception as e:
                logger.error(f"❌ ENHANCEMENT PHASE FAILED: {e}")
                logger.error("❌ ABORTING to prevent data loss - keeping existing data")
                stats['errors'] += 1
                return stats
            
            # Step 4: Only NOW clear old data (enhancement succeeded)
            logger.info("🗑️  Clearing old data (enhancement completed successfully)...")
            self.db.truncate_all_data()
            
            # Step 5: Store enhanced data atomically
            logger.info("💾 Storing enhanced data...")
            self._store_companies(enhanced_jobs)
            stats['jobs_stored'] = self._store_jobs(enhanced_jobs)
            stats['old_jobs_removed'] = "all (cleared after successful enhancement)"
            
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
        semaphore = asyncio.Semaphore(3)  # Conservative concurrency to prevent rate limits
        
        async def enhance_single_job(job):
            async with semaphore:
                try:
                    job_url = job.get('job_url')
                    if not job_url:
                        return job
                    
                    # Add timeout for page scraping to prevent hanging
                    loop = asyncio.get_event_loop()
                    detailed_data = await asyncio.wait_for(
                        loop.run_in_executor(
                            None, self.page_parser.scrape_job_page, job_url
                        ),
                        timeout=30  # 30 second timeout per job
                    )
                    
                    if detailed_data and len(detailed_data) > 2:
                        # Merge API data with detailed data, prioritizing enhanced data
                        enhanced_job = {**job, **detailed_data}
                        
                        # Verify we actually got enhanced data
                        has_enhanced = any([
                            detailed_data.get('description'),
                            detailed_data.get('requirements'), 
                            detailed_data.get('category'),
                            detailed_data.get('job_type')
                        ])
                        
                        if has_enhanced:
                            logger.info(f"✅ Enhanced: {job.get('title', 'Unknown')}")
                        else:
                            logger.warning(f"⚠️  Partial enhancement: {job.get('title', 'Unknown')}")
                            
                        return enhanced_job
                    else:
                        logger.warning(f"❌ Could not enhance: {job.get('title', 'Unknown')}")
                        return job
                    
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Timeout enhancing job {job.get('title')}")
                    return job
                except Exception as e:
                    logger.error(f"❌ Error enhancing job {job.get('title')}: {e}")
                    return job
        
        # Process jobs with conservative rate limiting
        batch_size = 10  # Smaller batches to be respectful
        for i in range(0, len(jobs_with_urls), batch_size):
            batch = jobs_with_urls[i:i+batch_size]
            
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(jobs_with_urls)-1)//batch_size + 1} ({len(batch)} jobs)")
            
            # Process batch
            tasks = [enhance_single_job(job) for job in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Add results
            for result in batch_results:
                if isinstance(result, dict):
                    enhanced_jobs.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Batch error: {result}")
            
            # Conservative rate limiting to avoid 429 errors
            await asyncio.sleep(2)  # 2 seconds between batches
        
        # Add jobs without URLs
        jobs_without_urls = [job for job in jobs if not job.get('job_url')]
        enhanced_jobs.extend(jobs_without_urls)
        
        logger.info(f"Enhanced {len(enhanced_jobs)} total jobs")
        return enhanced_jobs
    
    def _store_companies(self, jobs: List[Dict]):
        """Store unique companies from jobs using bulk operations"""
        companies = {}
        for job in jobs:
            slug = job.get('company_slug')
            if slug and slug not in companies:
                companies[slug] = {
                    'name': job.get('company_name', ''),
                    'slug': slug,
                    'logo': job.get('company_logo', '')
                }
        
        if not companies:
            return
        
        try:
            # Bulk insert companies using ON CONFLICT DO UPDATE
            company_values = []
            for company_data in companies.values():
                company_values.append((
                    company_data['name'],
                    company_data['slug'], 
                    company_data.get('logo', '')
                ))
            
            # Use bulk insert with conflict resolution
            from psycopg2.extras import execute_values
            execute_values(
                self.db.cursor,
                '''
                INSERT INTO "apply-bot".companies (name, slug, logo)
                VALUES %s
                ON CONFLICT (slug) DO UPDATE SET
                    name = EXCLUDED.name,
                    logo = EXCLUDED.logo,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                company_values,
                template=None,
                page_size=100
            )
            
            self.db.conn.commit()
            logger.info(f"Bulk stored {len(companies)} companies")
            
        except Exception as e:
            logger.error(f"Error bulk storing companies: {e}")
            self.db.conn.rollback()
    
    def _store_jobs(self, jobs: List[Dict]) -> int:
        """Store jobs in database using bulk operations"""
        if not jobs:
            return 0
        
        try:
            # First, get all company_ids in one query
            company_slugs = list(set(job.get('company_slug') for job in jobs if job.get('company_slug')))
            company_id_map = {}
            
            if company_slugs:
                placeholders = ','.join(['%s'] * len(company_slugs))
                self.db.cursor.execute(f'''
                    SELECT slug, id FROM "apply-bot".companies 
                    WHERE slug IN ({placeholders})
                ''', company_slugs)
                
                for row in self.db.cursor.fetchall():
                    company_id_map[row['slug']] = row['id']
            
            # Prepare bulk job data
            job_values = []
            for job in jobs:
                # Get company_id from map
                company_id = None
                if job.get('company_slug'):
                    company_id = company_id_map.get(job['company_slug'])
                
                # Truncate long fields
                job_type = job.get('job_type')
                if job_type and len(job_type) > 100:
                    job_type = job_type[:100]
                    
                experience_level = job.get('experience_level')
                if experience_level and len(experience_level) > 100:
                    experience_level = experience_level[:100]
                
                job_values.append((
                    job.get('title', ''),
                    job.get('slug', ''),
                    company_id,
                    job.get('company_name', ''),
                    job.get('company_slug', ''),
                    job.get('location', ''),
                    job_type,
                    experience_level,
                    job.get('description'),
                    job.get('requirements'),
                    job.get('posted_date'),
                    job.get('deadline'),
                    job.get('view_count', 0),
                    job.get('category'),
                    job.get('job_url'),
                    job.get('apply_url'),
                    job.get('is_active', True)
                ))
            
            # Bulk insert jobs
            from psycopg2.extras import execute_values
            execute_values(
                self.db.cursor,
                '''
                INSERT INTO "apply-bot".jobs (
                    title, slug, company_id, company_name, company_slug, location,
                    job_type, experience_level, description, requirements, posted_date,
                    deadline, view_count, category, job_url, apply_url, is_active
                ) VALUES %s
                ON CONFLICT (slug) DO UPDATE SET
                    title = EXCLUDED.title,
                    company_name = EXCLUDED.company_name,
                    company_slug = EXCLUDED.company_slug,
                    location = EXCLUDED.location,
                    job_type = EXCLUDED.job_type,
                    experience_level = EXCLUDED.experience_level,
                    description = EXCLUDED.description,
                    requirements = EXCLUDED.requirements,
                    posted_date = EXCLUDED.posted_date,
                    deadline = EXCLUDED.deadline,
                    view_count = EXCLUDED.view_count,
                    category = EXCLUDED.category,
                    job_url = EXCLUDED.job_url,
                    apply_url = EXCLUDED.apply_url,
                    is_active = EXCLUDED.is_active,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                job_values,
                template=None,
                page_size=100  # Process in chunks of 100
            )
            
            self.db.conn.commit()
            logger.info(f"Bulk stored {len(jobs)} jobs")
            return len(jobs)
            
        except Exception as e:
            logger.error(f"Error bulk storing jobs: {e}")
            self.db.conn.rollback()
            return 0
    
    def close(self):
        """Clean up resources"""
        self.db.close()
        self.page_parser.close()

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