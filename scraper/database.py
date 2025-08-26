import psycopg2
import psycopg2.extras
from psycopg2 import sql
import os
from dotenv import load_dotenv
import logging
from typing import Dict, List, Optional

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection_string = os.getenv('DATABASE_URL')
        if not self.connection_string:
            raise ValueError("DATABASE_URL not found in environment variables")
        
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Set search path to apply-bot schema
            self.cursor.execute("SET search_path TO 'apply-bot'")
            self.conn.commit()
            
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            # Create schema if it doesn't exist
            self.cursor.execute("CREATE SCHEMA IF NOT EXISTS \"apply-bot\"")
            
            # Companies table
            companies_table = """
            CREATE TABLE IF NOT EXISTS "apply-bot".companies (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                slug VARCHAR(255) UNIQUE NOT NULL,
                logo VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Jobs table
            jobs_table = """
            CREATE TABLE IF NOT EXISTS "apply-bot".jobs (
                id SERIAL PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                slug VARCHAR(500) UNIQUE NOT NULL,
                company_id INTEGER REFERENCES "apply-bot".companies(id),
                company_name VARCHAR(255),
                company_slug VARCHAR(255),
                location VARCHAR(255),
                job_type VARCHAR(100),
                experience_level VARCHAR(100),
                description TEXT,
                requirements TEXT,
                posted_date TIMESTAMP,
                deadline TIMESTAMP,
                view_count INTEGER DEFAULT 0,
                category VARCHAR(255),
                job_url VARCHAR(1000),
                apply_url VARCHAR(1000),
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create indexes
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_jobs_company_slug ON "apply-bot".jobs(company_slug);',
                'CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON "apply-bot".jobs(posted_date);',
                'CREATE INDEX IF NOT EXISTS idx_jobs_location ON "apply-bot".jobs(location);',
                'CREATE INDEX IF NOT EXISTS idx_jobs_is_active ON "apply-bot".jobs(is_active);',
                'CREATE INDEX IF NOT EXISTS idx_companies_slug ON "apply-bot".companies(slug);'
            ]
            
            self.cursor.execute(companies_table)
            self.cursor.execute(jobs_table)
            
            for index in indexes:
                self.cursor.execute(index)
            
            self.conn.commit()
            logger.info("Tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.conn.rollback()
            raise
    
    def insert_company(self, company_data: Dict) -> int:
        """Insert or update company and return company ID"""
        try:
            # Check if company exists
            self.cursor.execute(
                'SELECT id FROM "apply-bot".companies WHERE slug = %s',
                (company_data['slug'],)
            )
            result = self.cursor.fetchone()
            
            if result:
                # Update existing company
                self.cursor.execute('''
                    UPDATE "apply-bot".companies 
                    SET name = %s, logo = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE slug = %s
                    RETURNING id
                ''', (company_data['name'], company_data.get('logo'), company_data['slug']))
                company_id = self.cursor.fetchone()['id']
            else:
                # Insert new company
                self.cursor.execute('''
                    INSERT INTO "apply-bot".companies (name, slug, logo)
                    VALUES (%s, %s, %s)
                    RETURNING id
                ''', (company_data['name'], company_data['slug'], company_data.get('logo')))
                company_id = self.cursor.fetchone()['id']
            
            self.conn.commit()
            return company_id
            
        except Exception as e:
            logger.error(f"Error inserting company: {e}")
            self.conn.rollback()
            raise
    
    def insert_job(self, job_data: Dict) -> bool:
        """Insert or update job listing"""
        try:
            # Check if job exists
            self.cursor.execute(
                'SELECT id FROM "apply-bot".jobs WHERE slug = %s',
                (job_data['slug'],)
            )
            result = self.cursor.fetchone()
            
            if result:
                # Update existing job
                self.cursor.execute('''
                    UPDATE "apply-bot".jobs 
                    SET title = %s, company_name = %s, company_slug = %s, location = %s,
                        job_type = %s, experience_level = %s, description = %s, requirements = %s,
                        posted_date = %s, deadline = %s, view_count = %s, category = %s,
                        job_url = %s, apply_url = %s, updated_at = CURRENT_TIMESTAMP,
                        is_active = %s
                    WHERE slug = %s
                ''', (
                    job_data['title'], job_data.get('company_name'), job_data.get('company_slug'),
                    job_data.get('location'), job_data.get('job_type'), job_data.get('experience_level'),
                    job_data.get('description'), job_data.get('requirements'), job_data.get('posted_date'),
                    job_data.get('deadline'), job_data.get('view_count'), job_data.get('category'),
                    job_data.get('job_url'), job_data.get('apply_url'), job_data.get('is_active', True),
                    job_data['slug']
                ))
                logger.info(f"Updated job: {job_data['title']}")
            else:
                # Insert new job
                self.cursor.execute('''
                    INSERT INTO "apply-bot".jobs (
                        title, slug, company_id, company_name, company_slug, location,
                        job_type, experience_level, description, requirements, posted_date,
                        deadline, view_count, category, job_url, apply_url, is_active
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                ''', (
                    job_data['title'], job_data['slug'], job_data.get('company_id'),
                    job_data.get('company_name'), job_data.get('company_slug'),
                    job_data.get('location'), job_data.get('job_type'), job_data.get('experience_level'),
                    job_data.get('description'), job_data.get('requirements'), job_data.get('posted_date'),
                    job_data.get('deadline'), job_data.get('view_count'), job_data.get('category'),
                    job_data.get('job_url'), job_data.get('apply_url'), job_data.get('is_active', True)
                ))
                logger.info(f"Inserted new job: {job_data['title']}")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error inserting job: {e}")
            self.conn.rollback()
            return False
    
    def get_jobs_count(self) -> int:
        """Get total number of active jobs"""
        try:
            self.cursor.execute('SELECT COUNT(*) FROM "apply-bot".jobs WHERE is_active = TRUE')
            return self.cursor.fetchone()['count']
        except Exception as e:
            logger.error(f"Error getting jobs count: {e}")
            return 0
    
    def get_companies_count(self) -> int:
        """Get total number of companies"""
        try:
            self.cursor.execute('SELECT COUNT(*) FROM "apply-bot".companies')
            return self.cursor.fetchone()['count']
        except Exception as e:
            logger.error(f"Error getting companies count: {e}")
            return 0
    
    def mark_scraping_start(self) -> str:
        """Mark the start of a scraping cycle and return timestamp"""
        try:
            from datetime import datetime
            scrape_timestamp = datetime.now()
            logger.info(f"Marking scraping cycle start: {scrape_timestamp}")
            return scrape_timestamp.isoformat()
        except Exception as e:
            logger.error(f"Error marking scraping start: {e}")
            return None
    
    def cleanup_old_data(self, scrape_timestamp: str = None) -> Dict[str, int]:
        """Clean up old job data based on truncate-and-load architecture"""
        cleanup_stats = {'jobs_removed': 0, 'companies_removed': 0}
        
        try:
            if scrape_timestamp:
                # Debug: Check what timestamps we're working with
                self.cursor.execute('SELECT COUNT(*), MIN(updated_at), MAX(updated_at) FROM "apply-bot".jobs')
                result = self.cursor.fetchone()
                if result:
                    logger.info(f"Before cleanup: {result['count']} jobs, timestamps from {result['min']} to {result['max']}")
                    logger.info(f"Scrape timestamp: {scrape_timestamp}")
                
                # Remove jobs that are older than 1 hour before scrape start time
                # This ensures we keep all jobs updated during this scraping session
                self.cursor.execute('''
                    DELETE FROM "apply-bot".jobs 
                    WHERE updated_at < (%s::timestamp - INTERVAL '1 hour') OR updated_at IS NULL
                ''', (scrape_timestamp,))
                cleanup_stats['jobs_removed'] = self.cursor.rowcount
                
                # Remove companies that no longer have any jobs
                self.cursor.execute('''
                    DELETE FROM "apply-bot".companies 
                    WHERE id NOT IN (
                        SELECT DISTINCT company_id 
                        FROM "apply-bot".jobs 
                        WHERE company_id IS NOT NULL
                    )
                ''')
                cleanup_stats['companies_removed'] = self.cursor.rowcount
            else:
                # Alternative: Remove jobs older than 7 days
                self.cursor.execute('''
                    DELETE FROM "apply-bot".jobs 
                    WHERE scraped_at < NOW() - INTERVAL '7 days'
                ''')
                cleanup_stats['jobs_removed'] = self.cursor.rowcount
                
                # Remove orphaned companies
                self.cursor.execute('''
                    DELETE FROM "apply-bot".companies 
                    WHERE id NOT IN (
                        SELECT DISTINCT company_id 
                        FROM "apply-bot".jobs 
                        WHERE company_id IS NOT NULL
                    )
                ''')
                cleanup_stats['companies_removed'] = self.cursor.rowcount
            
            self.conn.commit()
            logger.info(f"Cleanup completed: {cleanup_stats}")
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            self.conn.rollback()
            return cleanup_stats
    
    def truncate_all_data(self):
        """Truncate all job and company data for fresh start"""
        try:
            self.cursor.execute('TRUNCATE TABLE "apply-bot".jobs CASCADE')
            self.cursor.execute('TRUNCATE TABLE "apply-bot".companies CASCADE')
            self.conn.commit()
            logger.info("All data truncated successfully")
        except Exception as e:
            logger.error(f"Error truncating data: {e}")
            self.conn.rollback()
            raise
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()