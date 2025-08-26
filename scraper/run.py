#!/usr/bin/env python3
"""
Fast async job scraper for Glorri.az
Clean, optimized version with asyncio and aiohttp
"""

from main_scraper_async import AsyncGlorriJobScraper
import asyncio
import sys

async def main():
    """Run the complete async job scraping pipeline"""
    print("🚀 Starting Fast Async Glorri Job Scraper")
    print("This will scrape ALL companies and jobs with complete details")
    print("Old jobs will be automatically removed after scraping")
    print("=" * 60)
    
    scraper = None
    try:
        scraper = AsyncGlorriJobScraper()
        stats = await scraper.scrape_all_jobs()
        
        print("\n" + "=" * 50)
        print("🏆 SCRAPING RESULTS")
        print("=" * 50)
        print(f"✅ Companies Found: {stats.get('companies_found', 0)}")
        print(f"✅ Jobs Found: {stats.get('jobs_found', 0)}")
        print(f"✅ Jobs Stored: {stats.get('jobs_stored', 0)}")
        print(f"🗑️  Old Jobs Removed: {stats.get('old_jobs_removed', 0)}")
        print(f"❌ Errors: {stats.get('errors', 0)}")
        print("=" * 50)
        
        if stats.get('jobs_stored', 0) > 0:
            print("🎉 Async scraping completed successfully!")
        else:
            print("⚠️  No jobs were stored. Check logs for errors.")
            
    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        sys.exit(1)
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    asyncio.run(main())