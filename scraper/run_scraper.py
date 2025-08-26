#!/usr/bin/env python3
"""
Quick runner script for the Glorri job scraper
"""

from main_scraper import GlorriJobScraper
from database import DatabaseManager

def run_basic_scrape():
    """Run a basic scrape to test everything works"""
    scraper = GlorriJobScraper(max_workers=3)
    
    print("🚀 Starting basic scrape...")
    print("This will scrape abc-telecom with 5 jobs and detailed info")
    
    stats = scraper.scrape_specific_company(
        company_slug='abc-telecom',
        scrape_details=True
    )
    
    scraper.close()
    
    print("\n✅ Scraping completed!")
    print(f"Jobs found: {stats['jobs_found']}")
    print(f"Jobs stored: {stats['jobs_stored']}")
    
    # Show current database stats
    with DatabaseManager() as db:
        total_jobs = db.get_jobs_count()
        total_companies = db.get_companies_count()
        print(f"\nDatabase now contains:")
        print(f"📊 Total jobs: {total_jobs}")
        print(f"🏢 Total companies: {total_companies}")

def run_full_scrape():
    """Run a full scrape (WARNING: This will take a long time!)"""
    scraper = GlorriJobScraper(max_workers=5)
    
    print("🚀 Starting FULL scrape...")
    print("WARNING: This may take several hours!")
    response = input("Are you sure? (yes/no): ")
    
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    stats = scraper.scrape_jobs_full_pipeline(
        max_companies=50,  # Limit to 50 companies
        scrape_details=True
    )
    
    scraper.close()
    
    print("\n✅ Full scraping completed!")
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title()}: {value}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'full':
        run_full_scrape()
    else:
        run_basic_scrape()