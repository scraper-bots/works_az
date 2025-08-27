#!/usr/bin/env python3
"""
Test run with limited companies to verify fixes
"""

from main_scraper_async import AsyncGlorriJobScraper
import asyncio
import sys

async def main():
    """Run test with limited companies"""
    print("🧪 Starting Test Run - Limited Companies")
    print("Testing database connection fixes")
    print("=" * 50)
    
    scraper = None
    try:
        scraper = AsyncGlorriJobScraper()
        
        # Override company discovery for testing
        original_discover = scraper.api_scraper.discover_all_companies
        async def test_discover():
            companies = await original_discover()
            # Limit to first 3 companies for testing
            test_companies = companies[:3]
            print(f"Test mode: Using first {len(test_companies)} companies: {test_companies}")
            return test_companies
        
        scraper.api_scraper.discover_all_companies = test_discover
        
        # Run scraping
        stats = await scraper.scrape_all_jobs()
        
        print("\n" + "=" * 40)
        print("🧪 TEST RESULTS")
        print("=" * 40)
        print(f"✅ Companies Found: {stats.get('companies_found', 0)}")
        print(f"✅ Jobs Found: {stats.get('jobs_found', 0)}")
        print(f"✅ Jobs Stored: {stats.get('jobs_stored', 0)}")
        print(f"🗑️  Old Jobs Removed: {stats.get('old_jobs_removed', 0)}")
        print(f"❌ Errors: {stats.get('errors', 0)}")
        print("=" * 40)
        
        if stats.get('jobs_stored', 0) > 0:
            print("✅ Database connection fixes successful!")
        else:
            print("❌ Issues remain - check logs")
            
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"💥 Test failed: {e}")
        sys.exit(1)
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    asyncio.run(main())