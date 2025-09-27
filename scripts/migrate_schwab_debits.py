#!/usr/bin/env python3
"""
Database migration script for Schwab debit transaction categorization enhancement.
This script updates existing DEBIT transactions to more specific categories:
- AMEX credit card payments → AMEX_DEBIT
- Capital One credit card payments → C1_DEBIT  
- TD Ameritrade account transfers → TRANSFER_DEBIT
"""

import sys
import sqlite3
import os
from datetime import datetime

def run_migration(db_path="data/database/portfolio.db", dry_run=False):
    """
    Run the database migration to categorize Schwab debit transactions.
    
    Args:
        db_path (str): Path to the SQLite database file
        dry_run (bool): If True, shows what would be changed without making changes
    
    Returns:
        dict: Migration results summary
    """
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return None
    
    print(f"{'🔍 DRY RUN' if dry_run else '🚀 RUNNING'} Schwab Debit Migration")
    print(f"Database: {db_path}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Check current state
        cursor.execute("""
            SELECT transaction_type, COUNT(*) 
            FROM transactions 
            WHERE broker = 'SCHWAB' AND transaction_type LIKE '%DEBIT%'
            GROUP BY transaction_type
            ORDER BY transaction_type
        """)
        current_state = cursor.fetchall()
        
        print("CURRENT STATE:")
        total_debits = 0
        for trans_type, count in current_state:
            print(f"  {trans_type}: {count} records")
            total_debits += count
        print(f"  Total SCHWAB debits: {total_debits}")
        
        if dry_run:
            print("\nDRY RUN - Analyzing what would be changed:")
            
            # Show AMEX records that would be updated
            cursor.execute("""
                SELECT description, amount, transaction_date
                FROM transactions 
                WHERE broker = 'SCHWAB' 
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%AMEX%' OR description LIKE '%amex%')
                LIMIT 5
            """)
            amex_samples = cursor.fetchall()
            amex_count = len(amex_samples)
            
            cursor.execute("""
                SELECT COUNT(*) FROM transactions 
                WHERE broker = 'SCHWAB' 
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%AMEX%' OR description LIKE '%amex%')
            """)
            amex_total = cursor.fetchone()[0]
            
            print(f"\n🔍 AMEX_DEBIT candidates: {amex_total} records")
            for i, (desc, amount, date) in enumerate(amex_samples[:3]):
                print(f"    {i+1}. {date} | ${amount} | {desc[:50]}...")
            if amex_total > 3:
                print(f"    ... and {amex_total - 3} more")
            
            # Show Capital One records
            cursor.execute("""
                SELECT COUNT(*) FROM transactions 
                WHERE broker = 'SCHWAB' 
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%CAPITAL ONE%' OR description LIKE '%capital one%')
            """)
            c1_total = cursor.fetchone()[0]
            print(f"\n🔍 C1_DEBIT candidates: {c1_total} records")
            
            # Show TD Ameritrade records
            cursor.execute("""
                SELECT COUNT(*) FROM transactions 
                WHERE broker = 'SCHWAB' 
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%TD AMERITRADE%' OR description LIKE '%td ameritrade%')
            """)
            td_total = cursor.fetchone()[0]
            print(f"\n🔍 TRANSFER_DEBIT candidates: {td_total} records")
            
            print(f"\nTotal records to be migrated: {amex_total + c1_total + td_total}")
            print("\n⚠️  This is a DRY RUN - no changes made")
            return {
                'dry_run': True,
                'amex_candidates': amex_total,
                'c1_candidates': c1_total,
                'td_candidates': td_total
            }
        
        else:
            print("\n🚀 EXECUTING MIGRATION:")
            
            # Perform actual migration
            cursor.execute("""
                UPDATE transactions 
                SET transaction_type = 'AMEX_DEBIT'
                WHERE broker = 'SCHWAB' 
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%AMEX%' OR description LIKE '%amex%')
            """)
            amex_updated = cursor.rowcount
            
            cursor.execute("""
                UPDATE transactions
                SET transaction_type = 'C1_DEBIT'
                WHERE broker = 'SCHWAB'
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%CAPITAL ONE%' OR description LIKE '%capital one%')
            """)
            c1_updated = cursor.rowcount
            
            cursor.execute("""
                UPDATE transactions
                SET transaction_type = 'TRANSFER_DEBIT'  
                WHERE broker = 'SCHWAB'
                  AND transaction_type = 'DEBIT'
                  AND (description LIKE '%TD AMERITRADE%' OR description LIKE '%td ameritrade%')
            """)
            td_updated = cursor.rowcount
            
            conn.commit()
            
            print(f"  ✅ AMEX_DEBIT: {amex_updated} records updated")
            print(f"  ✅ C1_DEBIT: {c1_updated} records updated") 
            print(f"  ✅ TRANSFER_DEBIT: {td_updated} records updated")
            print(f"  📊 Total migrated: {amex_updated + c1_updated + td_updated} records")
            
            # Show new state
            cursor.execute("""
                SELECT transaction_type, COUNT(*) 
                FROM transactions 
                WHERE broker = 'SCHWAB' AND transaction_type LIKE '%DEBIT%'
                GROUP BY transaction_type
                ORDER BY transaction_type
            """)
            new_state = cursor.fetchall()
            
            print(f"\nNEW STATE:")
            for trans_type, count in new_state:
                print(f"  {trans_type}: {count} records")
            
            return {
                'dry_run': False,
                'amex_updated': amex_updated,
                'c1_updated': c1_updated,
                'td_updated': td_updated,
                'total_migrated': amex_updated + c1_updated + td_updated
            }

def main():
    """Main function to run migration with command line options"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate Schwab debit transactions to specific categories')
    parser.add_argument('--db-path', default='data/database/portfolio.db', 
                       help='Path to database file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes')
    parser.add_argument('--force', action='store_true',
                       help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    if not args.dry_run and not args.force:
        print("⚠️  This will modify your database. Run with --dry-run first to preview changes.")
        response = input("Continue? [y/N]: ")
        if response.lower() != 'y':
            print("Migration cancelled.")
            return
    
    result = run_migration(args.db_path, args.dry_run)
    
    if result and not result.get('dry_run'):
        print(f"\n✅ Migration completed successfully!")
        print(f"📝 Summary: {result['total_migrated']} records categorized")

if __name__ == "__main__":
    main()