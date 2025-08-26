#!/usr/bin/env python3
"""
Main scraper for Glorri.az job listings
Combines API scraping with detailed page scraping and stores everything in PostgreSQL
"""

import logging
import argparse
import sys
import time
from datetime import datetime
from typing import List, Dict
import concurrent.futures
from threading import Lock

from database import DatabaseManager
from api_scraper import APIJobScraper
from page_scraper import JobPageScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class GlorriJobScraper:
    def __init__(self, max_workers: int = 5):
        self.db = DatabaseManager()
        self.api_scraper = APIJobScraper()
        self.page_scraper = JobPageScraper()
        self.max_workers = max_workers
        self.db_lock = Lock()
        
        logger.info("Glorri Job Scraper initialized")
    
    def scrape_jobs_full_pipeline(self, max_companies: int = None, 
                                 max_jobs_per_company: int = None,
                                 scrape_details: bool = True,
                                 truncate_first: bool = False) -> Dict[str, int]:
        """
        Complete scraping pipeline with truncate-and-load architecture:
        1. Mark scraping cycle start
        2. Optionally truncate old data
        3. Discover all companies automatically
        4. Get job listings from API for all companies
        5. Scrape detailed info from individual pages
        6. Store everything in database
        7. Clean up old/stale data
        """
        stats = {
            'jobs_found': 0,
            'jobs_detailed': 0,
            'jobs_stored': 0,
            'companies_discovered': 0,
            'companies_processed': 0,
            'jobs_removed': 0,
            'companies_removed': 0,
            'errors': 0
        }
        
        logger.info("Starting FULL SCRAPING PIPELINE (Truncate & Load Architecture)")
        start_time = datetime.now()
        
        try:
            # Step 0: Mark scraping cycle start and optionally truncate
            logger.info("Phase 0: Initializing scraping cycle...")
            scrape_timestamp = self.db.mark_scraping_start()
            
            if truncate_first:
                logger.info("Truncating all existing data...")
                self.db.truncate_all_data()
            
            # Step 1: Discover ALL companies automatically
            logger.info("Phase 1: Auto-discovering all companies...")
            discovered_companies = self.api_scraper.get_all_companies()
            stats['companies_discovered'] = len(discovered_companies)
            
            if not discovered_companies:
                logger.warning("No companies discovered! Falling back to iterative discovery...")
                # Fallback to old method
                api_jobs = self.api_scraper.scrape_all_jobs_iteratively(max_companies)
            else:
                logger.info(f"Discovered {len(discovered_companies)} companies")
                if max_companies:
                    discovered_companies = discovered_companies[:max_companies]
                    logger.info(f"Limited to {len(discovered_companies)} companies")
                
                # Step 2: Scrape ALL companies
                logger.info("Phase 2: Scraping all discovered companies...")
                api_jobs = []
                
                for i, company_slug in enumerate(discovered_companies, 1):
                    logger.info(f"Scraping company {i}/{len(discovered_companies)}: {company_slug}")
                    try:
                        company_jobs = self.api_scraper.get_all_company_jobs(company_slug)
                        if company_jobs:
                            parsed_jobs = []
                            for job in company_jobs:
                                if isinstance(job, dict):  # Make sure job is a dictionary
                                    parsed_job = self.api_scraper.parse_job_data(job)
                                    if parsed_job:  # Only add non-empty parsed jobs
                                        parsed_jobs.append(parsed_job)
                                else:
                                    logger.warning(f"Invalid job data format for {company_slug}: {type(job)}")
                            
                            # Apply per-company limit if specified
                            if max_jobs_per_company and len(parsed_jobs) > max_jobs_per_company:
                                parsed_jobs = parsed_jobs[:max_jobs_per_company]
                                logger.info(f"Limited {company_slug} to {max_jobs_per_company} jobs")
                            
                            api_jobs.extend(parsed_jobs)
                        
                        # Rate limiting
                        if i % 10 == 0:
                            time.sleep(2)
                        else:
                            time.sleep(1)
                            
                    except Exception as e:
                        logger.error(f"Error scraping company {company_slug}: {e}")
                        stats['errors'] += 1
                        continue
            
            stats['jobs_found'] = len(api_jobs)
            
            if not api_jobs:
                logger.warning("No jobs found from company scraping")
                return stats
            
            logger.info(f"Found {len(api_jobs)} total jobs from all companies")
            
            # Step 3: Store companies first
            logger.info("Phase 3: Storing company information...")
            companies_stored = self._store_companies_from_jobs(api_jobs)
            stats['companies_processed'] = companies_stored
            
            # Step 4: Scrape detailed information if requested
            detailed_jobs = api_jobs.copy()
            
            if scrape_details:
                logger.info("Phase 4: Scraping detailed job information...")
                
                # Filter jobs that have URLs
                jobs_with_urls = [job for job in api_jobs if job.get('job_url')]
                logger.info(f"Found {len(jobs_with_urls)} jobs with URLs for detailed scraping")
                
                if jobs_with_urls:
                    detailed_jobs = self._scrape_job_details_parallel(jobs_with_urls)
                    stats['jobs_detailed'] = len(detailed_jobs)
            
            # Step 5: Store all jobs in database
            logger.info("Phase 5: Storing jobs in database...")
            jobs_stored = self._store_jobs_in_database(detailed_jobs)
            stats['jobs_stored'] = jobs_stored
            
            # Step 6: Clean up old/stale data
            logger.info("Phase 6: Cleaning up old data...")
            cleanup_stats = self.db.cleanup_old_data(scrape_timestamp)
            stats['jobs_removed'] = cleanup_stats['jobs_removed']
            stats['companies_removed'] = cleanup_stats['companies_removed']
            
            # Final statistics
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("="*60)
            logger.info("FULL SCRAPING PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            logger.info(f"Duration: {duration}")
            logger.info(f"Companies discovered: {stats['companies_discovered']}")
            logger.info(f"Companies processed: {stats['companies_processed']}")
            logger.info(f"Jobs found via API: {stats['jobs_found']}")
            logger.info(f"Jobs with detailed info: {stats['jobs_detailed']}")
            logger.info(f"Jobs stored in database: {stats['jobs_stored']}")
            logger.info(f"Old jobs removed: {stats['jobs_removed']}")
            logger.info(f"Old companies removed: {stats['companies_removed']}")
            logger.info(f"Errors encountered: {stats['errors']}")
            
            # Final database statistics
            total_jobs = self.db.get_jobs_count()
            total_companies = self.db.get_companies_count()
            logger.info(f"Final jobs in database: {total_jobs}")
            logger.info(f"Final companies in database: {total_companies}")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Fatal error in scraping pipeline: {e}")
            stats['errors'] += 1
            
        return stats
    
    def _store_companies_from_jobs(self, jobs: List[Dict]) -> int:
        """Extract and store unique companies from job listings"""
        companies = {}
        
        for job in jobs:
            company_slug = job.get('company_slug')
            if company_slug and company_slug not in companies:
                companies[company_slug] = {
                    'name': job.get('company_name', ''),
                    'slug': company_slug,
                    'logo': job.get('company_logo', '')
                }
        
        stored_count = 0
        for company_data in companies.values():
            try:
                company_id = self.db.insert_company(company_data)
                if company_id:
                    stored_count += 1
            except Exception as e:
                logger.error(f"Error storing company {company_data.get('name')}: {e}")
        
        logger.info(f"Stored {stored_count} companies")
        return stored_count
    
    def _scrape_job_details_parallel(self, jobs: List[Dict]) -> List[Dict]:
        """Scrape job details using parallel processing"""
        detailed_jobs = []
        
        def scrape_single_job(job):
            try:
                job_url = job.get('job_url')
                if not job_url:
                    return job
                
                detailed_data = self.page_scraper.scrape_job_page(job_url)
                if detailed_data:
                    # Merge API data with scraped details
                    merged_job = {**job, **detailed_data}
                    return merged_job
                else:
                    logger.warning(f"Could not scrape details for {job_url}")
                    return job
                    
            except Exception as e:
                logger.error(f"Error scraping job details for {job.get('job_url')}: {e}")
                return job
        
        logger.info(f"Starting parallel scraping with {self.max_workers} workers")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all jobs for processing
            future_to_job = {
                executor.submit(scrape_single_job, job): job 
                for job in jobs
            }
            
            # Collect results
            for i, future in enumerate(concurrent.futures.as_completed(future_to_job), 1):
                try:
                    result = future.result()
                    detailed_jobs.append(result)
                    
                    if i % 10 == 0:
                        logger.info(f"Scraped {i}/{len(jobs)} job details")
                        
                except Exception as e:
                    original_job = future_to_job[future]
                    logger.error(f"Error processing job {original_job.get('title')}: {e}")
                    detailed_jobs.append(original_job)  # Include original data
        
        logger.info(f"Completed detailed scraping for {len(detailed_jobs)} jobs")
        return detailed_jobs
    
    def _store_jobs_in_database(self, jobs: List[Dict]) -> int:
        """Store job listings in database"""
        stored_count = 0
        
        for job in jobs:
            try:
                # Ensure company_id is set by looking up the company
                if job.get('company_slug') and not job.get('company_id'):
                    self.db.cursor.execute(
                        'SELECT id FROM "apply-bot".companies WHERE slug = %s',
                        (job['company_slug'],)
                    )
                    result = self.db.cursor.fetchone()
                    if result:
                        job['company_id'] = result['id']
                
                success = self.db.insert_job(job)
                if success:
                    stored_count += 1
            except Exception as e:
                logger.error(f"Error storing job {job.get('title')}: {e}")
        
        logger.info(f"Stored {stored_count} jobs in database")
        return stored_count
    
    def scrape_specific_company(self, company_slug: str, scrape_details: bool = True) -> Dict[str, int]:
        """Scrape jobs for a specific company"""
        stats = {'jobs_found': 0, 'jobs_stored': 0, 'errors': 0}
        
        try:
            logger.info(f"Scraping jobs for company: {company_slug}")
            
            # Get jobs from API
            api_jobs = self.api_scraper.get_all_company_jobs(company_slug)
            if not api_jobs:
                logger.warning(f"No jobs found for company: {company_slug}")
                return stats
            
            # Parse job data
            parsed_jobs = [self.api_scraper.parse_job_data(job) for job in api_jobs]
            stats['jobs_found'] = len(parsed_jobs)
            
            # Store company info
            if parsed_jobs:
                company_data = {
                    'name': parsed_jobs[0].get('company_name', ''),
                    'slug': company_slug,
                    'logo': parsed_jobs[0].get('company_logo', '')
                }
                self.db.insert_company(company_data)
            
            # Scrape details if requested
            if scrape_details:
                detailed_jobs = self._scrape_job_details_parallel(parsed_jobs)
            else:
                detailed_jobs = parsed_jobs
            
            # Store in database
            stored_count = self._store_jobs_in_database(detailed_jobs)
            stats['jobs_stored'] = stored_count
            
            logger.info(f"Company scraping complete: {stored_count} jobs stored")
            
        except Exception as e:
            logger.error(f"Error scraping company {company_slug}: {e}")
            stats['errors'] += 1
        
        return stats
    
    def close(self):
        """Clean up resources"""
        try:
            self.db.close()
            self.api_scraper.close()
            self.page_scraper.close()
            logger.info("Scraper resources cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up resources: {e}")

def main():
    parser = argparse.ArgumentParser(description='Glorri.az Job Scraper')
    parser.add_argument('--max-companies', type=int, help='Maximum number of companies to scrape')
    parser.add_argument('--max-jobs-per-company', type=int, help='Maximum jobs to scrape per company')
    parser.add_argument('--company', type=str, help='Scrape specific company by slug')
    parser.add_argument('--no-details', action='store_true', help='Skip detailed page scraping')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers')
    
    args = parser.parse_args()
    
    scraper = None
    try:
        scraper = GlorriJobScraper(max_workers=args.workers)
        
        if args.company:
            # Scrape specific company
            stats = scraper.scrape_specific_company(
                args.company, 
                scrape_details=not args.no_details
            )
        else:
            # Full pipeline scraping
            stats = scraper.scrape_jobs_full_pipeline(
                max_companies=args.max_companies,
                max_jobs_per_company=args.max_jobs_per_company,
                scrape_details=not args.no_details
            )
        
        # Print final stats
        print("\n" + "="*50)
        print("SCRAPING SUMMARY")
        print("="*50)
        for key, value in stats.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        print("="*50)
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()