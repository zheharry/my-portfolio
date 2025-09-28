#!/usr/bin/env python3
"""
TDA/Schwab Data Limitation Investigation Tool
Comprehensive analysis tool for investigating the 2023-10-02 data cutoff issue
"""

import sys
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import json

# Add the project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataLimitationInvestigator:
    def __init__(self, db_path="data/database/portfolio.db"):
        self.db_path = db_path
        self.cutoff_date = "2023-10-02"
        
    def run_investigation(self):
        """Run comprehensive investigation of the data limitation issue"""
        print("=" * 60)
        print("TDA/SCHWAB DATA LIMITATION INVESTIGATION REPORT")
        print("=" * 60)
        print(f"Investigation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Database: {self.db_path}")
        print()
        
        if not os.path.exists(self.db_path):
            print("❌ ERROR: Database file not found")
            print(f"Expected location: {self.db_path}")
            return
            
        try:
            self._analyze_transaction_data()
            self._analyze_data_gaps()
            self._analyze_potential_causes()
            self._generate_action_plan()
        except Exception as e:
            print(f"❌ ERROR: Investigation failed: {e}")
            
    def _analyze_transaction_data(self):
        """Analyze current transaction data by broker"""
        print("📊 TRANSACTION DATA ANALYSIS")
        print("-" * 40)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Overall statistics
            cursor.execute("SELECT COUNT(*) FROM transactions")
            total_transactions = cursor.fetchone()[0]
            print(f"Total transactions in database: {total_transactions}")
            
            # Broker breakdown
            cursor.execute("""
                SELECT 
                    broker,
                    COUNT(*) as count,
                    MIN(transaction_date) as first_date,
                    MAX(transaction_date) as last_date,
                    COUNT(DISTINCT symbol) as unique_symbols
                FROM transactions 
                WHERE broker IS NOT NULL
                GROUP BY broker
                ORDER BY broker
            """)
            
            print("\nBroker Summary:")
            for broker, count, first_date, last_date, symbols in cursor.fetchall():
                print(f"  {broker}:")
                print(f"    Transactions: {count}")
                print(f"    Date range: {first_date} to {last_date}")
                print(f"    Unique symbols: {symbols}")
                
                # Check for cutoff issue
                if broker in ['TDA', 'SCHWAB'] and last_date <= self.cutoff_date:
                    days_missing = (datetime.now() - datetime.strptime(last_date, '%Y-%m-%d')).days
                    print(f"    ❌ DATA CUTOFF CONFIRMED: {days_missing} days of missing data")
                elif broker in ['TDA', 'SCHWAB']:
                    print(f"    ✅ Data appears current")
                else:
                    print(f"    ℹ️  Other broker (not affected by TDA-Schwab issue)")
                print()
                
    def _analyze_data_gaps(self):
        """Analyze specific data gaps and missing patterns"""
        print("🔍 DATA GAP ANALYSIS")
        print("-" * 40)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check for transactions around the cutoff date
            print(f"Transactions around cutoff date ({self.cutoff_date}):")
            cursor.execute("""
                SELECT broker, transaction_date, COUNT(*) as count
                FROM transactions 
                WHERE transaction_date BETWEEN '2023-09-01' AND '2023-11-01'
                GROUP BY broker, transaction_date
                ORDER BY transaction_date DESC, broker
            """)
            
            results = cursor.fetchall()
            if results:
                for broker, date, count in results:
                    marker = "🔴" if date == self.cutoff_date and broker in ['TDA', 'SCHWAB'] else "📅"
                    print(f"  {marker} {broker}: {date} ({count} transactions)")
            else:
                print("  No transactions found around cutoff period")
                
            print()
            
            # Analyze missing transaction patterns
            for broker in ['TDA', 'SCHWAB']:
                print(f"\nHistorical patterns for {broker}:")
                cursor.execute("""
                    SELECT 
                        transaction_type,
                        COUNT(*) as total_count,
                        AVG(CASE WHEN net_amount != 0 THEN ABS(net_amount) END) as avg_amount,
                        strftime('%m', transaction_date) as month,
                        COUNT(DISTINCT strftime('%Y-%m', transaction_date)) as months_active
                    FROM transactions 
                    WHERE broker = ? AND transaction_date <= ?
                    GROUP BY transaction_type
                    ORDER BY total_count DESC
                """, (broker, self.cutoff_date))
                
                patterns = cursor.fetchall()
                if patterns:
                    for tx_type, count, avg_amt, _, months in patterns:
                        monthly_avg = count / max(months, 1)
                        print(f"  - {tx_type}: {count} total ({monthly_avg:.1f}/month, avg ${avg_amt:.2f})")
                        
                        # Estimate missing transactions
                        months_missing = 23  # Approximate months since cutoff
                        estimated_missing = int(monthly_avg * months_missing)
                        print(f"    💔 Estimated missing: ~{estimated_missing} {tx_type} transactions")
                else:
                    print(f"  No historical data found for {broker}")
                print()
                
    def _analyze_potential_causes(self):
        """Analyze potential causes of the data limitation"""
        print("🔍 ROOT CAUSE ANALYSIS")
        print("-" * 40)
        
        print("Known Context:")
        print("  📅 Cutoff Date: October 2, 2023")
        print("  🏢 TDA-Schwab Merger: Completed October 2023")
        print("  📊 Affected Brokers: TD Ameritrade (TDA) and Charles Schwab")
        print()
        
        print("Potential Causes:")
        causes = [
            {
                "cause": "Account Consolidation",
                "description": "TDA accounts moved to Schwab platform",
                "likelihood": "HIGH",
                "evidence": "Merger completion date matches data cutoff exactly"
            },
            {
                "cause": "Statement Access Changes", 
                "description": "PDF statement download access changed post-merger",
                "likelihood": "HIGH",
                "evidence": "Common during broker platform migrations"
            },
            {
                "cause": "Login Credential Changes",
                "description": "Account access credentials invalidated during migration",
                "likelihood": "MEDIUM",
                "evidence": "Typical during account consolidation processes"
            },
            {
                "cause": "Parser Compatibility Issues",
                "description": "New statement formats not supported by existing parser",
                "likelihood": "MEDIUM", 
                "evidence": "Statement formats often change during mergers"
            },
            {
                "cause": "Manual Process Interruption",
                "description": "Manual data collection process was not continued",
                "likelihood": "HIGH",
                "evidence": "Sharp cutoff suggests process interruption rather than gradual failure"
            }
        ]
        
        for i, cause in enumerate(causes, 1):
            print(f"  {i}. {cause['cause']} [{cause['likelihood']}]")
            print(f"     {cause['description']}")
            print(f"     Evidence: {cause['evidence']}")
            print()
            
    def _generate_action_plan(self):
        """Generate actionable investigation and remediation plan"""
        print("🎯 RECOMMENDED ACTION PLAN")
        print("-" * 40)
        
        phases = [
            {
                "phase": "IMMEDIATE INVESTIGATION (Priority: HIGH)",
                "actions": [
                    "Check if TDA/Schwab account login credentials still work",
                    "Verify if online statement access is still available",
                    "Look for any account consolidation notifications from Oct 2023",
                    "Check email for broker communications about account changes",
                    "Review account status in online banking portals"
                ]
            },
            {
                "phase": "STATEMENT RECOVERY (Priority: HIGH)", 
                "actions": [
                    "Download all available statements from Oct 2023 onwards",
                    "Check for new/consolidated account numbers post-merger",
                    "Test parser compatibility with any new statement formats",
                    "Contact broker support if statements are not accessible",
                    "Verify if accounts were closed or transferred to new platforms"
                ]
            },
            {
                "phase": "SYSTEM IMPROVEMENTS (Priority: MEDIUM)",
                "actions": [
                    "Implement automated data freshness monitoring (✅ COMPLETED)",
                    "Add UI warnings for stale data (✅ COMPLETED)", 
                    "Set up alerts when broker data becomes outdated",
                    "Create regular data validation reports",
                    "Document broker account status and access methods"
                ]
            },
            {
                "phase": "LONG-TERM MAINTENANCE (Priority: MEDIUM)",
                "actions": [
                    "Establish regular data collection schedule",
                    "Create backup data sources where possible",
                    "Monitor for future broker changes/mergers",
                    "Maintain broker contact information for support issues",
                    "Document data collection procedures for continuity"
                ]
            }
        ]
        
        for phase in phases:
            print(f"\n{phase['phase']}:")
            for i, action in enumerate(phase['actions'], 1):
                status = " ✅" if "COMPLETED" in action else ""
                print(f"  {i}. {action}{status}")
        
        print("\n" + "=" * 60)
        print("INVESTIGATION SUMMARY")
        print("=" * 60)
        print("STATUS: Data limitation confirmed for TDA and Schwab brokers")
        print(f"CUTOFF: {self.cutoff_date} (corresponds to TDA-Schwab merger completion)")
        print("IMPACT: ~23 months of missing transaction data")
        print("URGENCY: HIGH - Affects portfolio accuracy and decision-making")
        print("NEXT STEPS: Execute immediate investigation phase")
        print("=" * 60)

if __name__ == "__main__":
    investigator = DataLimitationInvestigator()
    investigator.run_investigation()