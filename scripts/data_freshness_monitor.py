#!/usr/bin/env python3
"""
Data Freshness Monitor
Monitors portfolio data for staleness and gaps, particularly the TDA/Schwab 2023-10-02 limitation
"""

import sys
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# Add the project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class DataFreshnessMonitor:
    def __init__(self, db_path="data/database/portfolio.db"):
        self.db_path = db_path
        self.stale_threshold_days = 30  # Consider data stale after 30 days
        self.known_cutoff_date = "2023-10-02"  # Known TDA/Schwab cutoff
        
    def get_broker_freshness_status(self) -> Dict:
        """Get freshness status for each broker"""
        if not os.path.exists(self.db_path):
            return {"error": "Database not found"}
            
        status = {}
        current_date = datetime.now().date()
        cutoff_date = datetime.strptime(self.known_cutoff_date, "%Y-%m-%d").date()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if transactions table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
            if not cursor.fetchone():
                return {"error": "No transactions table found"}
            
            # Get broker data status
            cursor.execute("""
                SELECT 
                    broker,
                    COUNT(*) as transaction_count,
                    MIN(transaction_date) as earliest_date,
                    MAX(transaction_date) as latest_date
                FROM transactions 
                WHERE broker IS NOT NULL 
                GROUP BY broker
            """)
            
            for broker, count, min_date, max_date in cursor.fetchall():
                try:
                    latest = datetime.strptime(max_date, "%Y-%m-%d").date()
                    days_old = (current_date - latest).days
                    
                    broker_status = {
                        "broker": broker,
                        "transaction_count": count,
                        "earliest_date": min_date,
                        "latest_date": max_date,
                        "days_since_last_transaction": days_old,
                        "is_stale": days_old > self.stale_threshold_days,
                        "data_coverage_percent": self._calculate_coverage_percent(latest, current_date)
                    }
                    
                    # Special handling for TDA/Schwab known limitation
                    if broker in ["TDA", "SCHWAB"]:
                        if latest <= cutoff_date:
                            broker_status["known_limitation"] = True
                            broker_status["limitation_details"] = {
                                "type": "TDA-Schwab merger cutoff",
                                "cutoff_date": self.known_cutoff_date,
                                "missing_months": self._calculate_missing_months(latest, current_date),
                                "severity": "HIGH"
                            }
                        else:
                            broker_status["known_limitation"] = False
                    
                    status[broker] = broker_status
                    
                except ValueError as e:
                    status[broker] = {"error": f"Invalid date format: {e}"}
                    
        return status
    
    def _calculate_coverage_percent(self, latest_date, current_date):
        """Calculate data coverage percentage based on expected timeline"""
        # Assume data should be current (within last 30 days for good coverage)
        days_gap = (current_date - latest_date).days
        if days_gap <= 30:
            return 100
        elif days_gap <= 180:  # 6 months
            return max(70, 100 - (days_gap - 30) * 0.5)
        else:
            return max(20, 100 - days_gap * 0.2)
    
    def _calculate_missing_months(self, last_date, current_date):
        """Calculate missing months of data"""
        return ((current_date.year - last_date.year) * 12 + 
                (current_date.month - last_date.month))
    
    def generate_freshness_report(self) -> Dict:
        """Generate comprehensive freshness report"""
        status = self.get_broker_freshness_status()
        
        if "error" in status:
            return status
            
        report = {
            "report_date": datetime.now().isoformat(),
            "overall_status": "HEALTHY",
            "brokers": status,
            "alerts": [],
            "recommendations": []
        }
        
        # Analyze overall health
        stale_brokers = [b for b, data in status.items() if data.get("is_stale", False)]
        limited_brokers = [b for b, data in status.items() if data.get("known_limitation", False)]
        
        if stale_brokers or limited_brokers:
            report["overall_status"] = "DEGRADED"
            
        # Generate alerts
        for broker, data in status.items():
            if data.get("known_limitation"):
                limitation = data["limitation_details"]
                report["alerts"].append({
                    "severity": limitation["severity"],
                    "broker": broker,
                    "type": "DATA_LIMITATION",
                    "message": f"{broker} data limited to {limitation['cutoff_date']} ({limitation['missing_months']} months missing)",
                    "action_required": True
                })
                
            elif data.get("is_stale"):
                report["alerts"].append({
                    "severity": "MEDIUM",
                    "broker": broker,
                    "type": "STALE_DATA",
                    "message": f"{broker} data is {data['days_since_last_transaction']} days old",
                    "action_required": True
                })
        
        # Generate recommendations
        if limited_brokers:
            report["recommendations"].extend([
                "Investigate TDA/Schwab statement availability after 2023-10-02",
                "Check for account consolidation due to TDA-Schwab merger",
                "Verify access credentials for affected broker accounts",
                "Consider contacting brokers for historical statement access"
            ])
            
        return report

if __name__ == "__main__":
    monitor = DataFreshnessMonitor()
    report = monitor.generate_freshness_report()
    
    print("=== Data Freshness Report ===")
    print(f"Report Date: {report.get('report_date', 'Unknown')}")
    print(f"Overall Status: {report.get('overall_status', 'Unknown')}")
    
    if report.get("alerts"):
        print(f"\n🚨 ALERTS ({len(report['alerts'])}):")
        for alert in report["alerts"]:
            print(f"  [{alert['severity']}] {alert['message']}")
            
    if report.get("recommendations"):
        print(f"\n💡 RECOMMENDATIONS:")
        for rec in report["recommendations"]:
            print(f"  • {rec}")