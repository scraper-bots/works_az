#!/usr/bin/env python3
"""
Glorri.az job scraper - main entry point
"""

from scraper import AsyncGlorriJobScraper
import asyncio
import sys

async def main():
    """Run job scraping pipeline"""
    print("🚀 Glorri Job Scraper")
    print("Scraping all companies and jobs with complete details")
    print("Old data will be cleared after scraping completes")
    print("=" * 60)
    
    scraper = None
    try:
        scraper = AsyncGlorriJobScraper()
        stats = await scraper.scrape_all_jobs()
        
        print("\n" + "=" * 40)
        print("🏆 RESULTS")
        print("=" * 40)
        print(f"Companies: {stats.get('companies_found', 0)}")
        print(f"Jobs Found: {stats.get('jobs_found', 0)}")
        print(f"Jobs Stored: {stats.get('jobs_stored', 0)}")
        print(f"Old Data: {stats.get('old_jobs_removed', 'cleared')}")
        print(f"Errors: {stats.get('errors', 0)}")
        print("=" * 40)
        
        if stats.get('jobs_stored', 0) > 0:
            print("✅ Scraping completed!")
        else:
            print("⚠️  No jobs stored - check logs")
            
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
    except Exception as e:
        print(f"💥 Error: {e}")
        sys.exit(1)
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    asyncio.run(main())