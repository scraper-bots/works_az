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
                
                # Verify enhancement quality - fix detection logic
                enhanced_count = 0
                for job in enhanced_jobs:
                    # Count jobs with ANY enhanced fields (not empty strings)
                    has_enhanced_data = any([
                        job.get('description') and str(job.get('description')).strip(),
                        job.get('requirements') and str(job.get('requirements')).strip(), 
                        job.get('category') and str(job.get('category')).strip(),
                        job.get('job_type') and str(job.get('job_type')).strip(),
                        job.get('apply_url') and str(job.get('apply_url')).strip()
                    ])
                    if has_enhanced_data:
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
        semaphore = asyncio.Semaphore(1)  # Very conservative - one at a time to prevent rate limits
        
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
                        
                        # Verify we got enhanced data 
                        enhanced_fields = []
                        if detailed_data.get('description') and str(detailed_data.get('description')).strip():
                            enhanced_fields.append('description')
                        if detailed_data.get('requirements') and str(detailed_data.get('requirements')).strip():
                            enhanced_fields.append('requirements')
                        if detailed_data.get('category') and str(detailed_data.get('category')).strip():
                            enhanced_fields.append('category')
                        if detailed_data.get('job_type') and str(detailed_data.get('job_type')).strip():
                            enhanced_fields.append('job_type')
                        if detailed_data.get('apply_url') and str(detailed_data.get('apply_url')).strip():
                            enhanced_fields.append('apply_url')
                        
                        job_title = enhanced_job.get('title', 'Unknown Job')
                        
                        if len(enhanced_fields) >= 3:  # Good enhancement
                            logger.info(f"✅ Enhanced: {job_title} ({len(enhanced_fields)} fields)")
                        elif len(enhanced_fields) >= 1:  # Partial enhancement 
                            logger.info(f"⚠️  Partial: {job_title} ({', '.join(enhanced_fields)})")
                        else:  # No enhancement
                            logger.warning(f"❌ No enhancement: {job_title}")
                            
                        return enhanced_job
                    else:
                        logger.warning(f"❌ Could not extract: {job.get('title', 'Unknown')}")
                        return job
                    
                except asyncio.TimeoutError:
                    logger.error(f"⏰ Timeout enhancing job {job.get('title')}")
                    return job
                except Exception as e:
                    logger.error(f"❌ Error enhancing job {job.get('title')}: {e}")
                    return job
        
        # Process jobs one by one to avoid rate limits completely
        logger.info(f"Processing {len(jobs_with_urls)} jobs sequentially (no batching)")
        
        for i, job in enumerate(jobs_with_urls):
            logger.info(f"Processing job {i+1}/{len(jobs_with_urls)}: {job.get('title', 'Unknown')}")
            
            try:
                result = await enhance_single_job(job)
                if isinstance(result, dict):
                    enhanced_jobs.append(result)
                else:
                    logger.error(f"Invalid result type: {type(result)}")
            except Exception as e:
                logger.error(f"Error processing job {job.get('title', 'Unknown')}: {e}")
                # Still add the original job data
                enhanced_jobs.append(job)
            
            # Very long delay between each request to avoid blocking
            if i < len(jobs_with_urls) - 1:  # Don't wait after the last job
                await asyncio.sleep(5)  # 5 seconds between each job
        
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
            
            # Prepare bulk job data with data cleaning
            job_values = []
            for job in jobs:
                # Get company_id from map
                company_id = None
                if job.get('company_slug'):
                    company_id = company_id_map.get(job['company_slug'])
                
                # Clean and truncate text fields
                def clean_text(text, max_length=None):
                    if not text:
                        return None
                    # Remove null characters that cause PostgreSQL errors
                    cleaned = str(text).replace('\x00', '').strip()
                    if max_length and len(cleaned) > max_length:
                        cleaned = cleaned[:max_length]
                    return cleaned if cleaned else None
                
                job_values.append((
                    clean_text(job.get('title'), 500),
                    clean_text(job.get('slug'), 500),
                    company_id,
                    clean_text(job.get('company_name'), 255),
                    clean_text(job.get('company_slug'), 255),
                    clean_text(job.get('location'), 255),
                    clean_text(job.get('job_type'), 100),
                    clean_text(job.get('experience_level'), 100),
                    clean_text(job.get('description')),
                    clean_text(job.get('requirements')),
                    job.get('posted_date'),
                    job.get('deadline'),
                    job.get('view_count', 0),
                    clean_text(job.get('category'), 255),
                    clean_text(job.get('job_url'), 1000),
                    clean_text(job.get('apply_url'), 1000),
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