#!/usr/bin/env python3
"""
Quick status checker for the database
"""

from database import DatabaseManager

def check_status():
    with DatabaseManager() as db:
        # Get current counts
        db.cursor.execute('SELECT COUNT(*) as count FROM "apply-bot".jobs WHERE is_active = TRUE')
        jobs_count = db.cursor.fetchone()['count']
        
        db.cursor.execute('SELECT COUNT(*) as count FROM "apply-bot".companies')
        companies_count = db.cursor.fetchone()['count']
        
        # Get field completion rates
        fields = ['description', 'requirements', 'category', 'apply_url', 'job_type', 'deadline']
        completion = {}
        
        for field in fields:
            if field in ['deadline']:
                db.cursor.execute(f'SELECT COUNT(*) as complete FROM "apply-bot".jobs WHERE is_active = TRUE AND {field} IS NOT NULL')
            else:
                db.cursor.execute(f'SELECT COUNT(*) as complete FROM "apply-bot".jobs WHERE is_active = TRUE AND {field} IS NOT NULL AND {field} != \'\'')
            completion[field] = db.cursor.fetchone()['complete']
        
        # Get latest update time
        db.cursor.execute('SELECT MAX(updated_at) as latest FROM "apply-bot".jobs')
        latest = db.cursor.fetchone()['latest']
        
        print("="*50)
        print("DATABASE STATUS")
        print("="*50)
        print(f"Active Jobs: {jobs_count}")
        print(f"Companies: {companies_count}")
        print(f"Last Update: {latest}")
        print()
        print("Field Completion:")
        for field, count in completion.items():
            pct = (count / jobs_count * 100) if jobs_count > 0 else 0
            print(f"  {field}: {count}/{jobs_count} ({pct:.1f}%)")
        print("="*50)

if __name__ == "__main__":
    check_status()