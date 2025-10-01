from flask import Flask, render_template, request, jsonify
import sqlite3
import json
import pandas as pd
import os
from datetime import datetime, timedelta
from pathlib import Path
from scripts.multi_broker_parser import MultiBrokerPortfolioParser

app = Flask(__name__)

class PortfolioAPI:
    def __init__(self, db_path="data/database/portfolio.db"):
        self.db_path = db_path
        # Add caching for exchange rates and stock prices
        self._forex_cache = {}
        self._stock_price_cache = {}
        self._cache_timestamp = None
        self._cache_duration = 86400  # 24 hours cache duration (was 30 minutes)
        self._request_count = 0  # Track API requests for rate limiting
        self._last_request_time = None
        self._yahoo_finance_available = True  # Circuit breaker for Yahoo Finance
        self._last_yahoo_check = None
        self.ensure_database_exists()
    
    def ensure_database_exists(self):
        """Create database and tables if they don't exist"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create accounts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    institution TEXT,
                    broker TEXT,
                    account_type TEXT,
                    account_holder TEXT,
                    created_date TEXT,
                    currency TEXT DEFAULT 'USD'
                )
            """)
            
            # Create enhanced transactions table with fees
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT,
                    transaction_date TEXT,
                    symbol TEXT,
                    chinese_name TEXT,
                    transaction_type TEXT,
                    quantity INTEGER,
                    price REAL,
                    amount REAL,
                    fee REAL DEFAULT 0,
                    tax REAL DEFAULT 0,
                    net_amount REAL,
                    broker TEXT,
                    order_id TEXT,
                    description TEXT,
                    currency TEXT DEFAULT 'USD',
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                )
            """)
            
            # Create positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT,
                    symbol TEXT,
                    chinese_name TEXT,
                    quantity INTEGER,
                    average_cost REAL,
                    market_value REAL,
                    cost_basis REAL,
                    unrealized_gain_loss REAL,
                    statement_date TEXT,
                    broker TEXT,
                    currency TEXT DEFAULT 'USD',
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                )
            """)
            
            conn.commit()
            
            # Add currency columns to existing tables if they don't exist
            self._migrate_currency_columns(cursor)
    
    def _migrate_currency_columns(self, cursor):
        """Add currency and chinese_name columns to existing tables and populate based on broker"""
        try:
            # Check and add currency column to accounts table
            cursor.execute("PRAGMA table_info(accounts)")
            accounts_columns = [row[1] for row in cursor.fetchall()]
            
            if 'currency' not in accounts_columns:
                cursor.execute("ALTER TABLE accounts ADD COLUMN currency TEXT DEFAULT 'USD'")
                print("Added currency column to accounts table")
            
            # Check and add currency column to transactions table
            cursor.execute("PRAGMA table_info(transactions)")
            transactions_columns = [row[1] for row in cursor.fetchall()]
            
            if 'currency' not in transactions_columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN currency TEXT DEFAULT 'USD'")
                print("Added currency column to transactions table")
                
            # Check and add chinese_name column to transactions table
            if 'chinese_name' not in transactions_columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN chinese_name TEXT")
                print("Added chinese_name column to transactions table")
            
            # Check and add currency column to positions table
            cursor.execute("PRAGMA table_info(positions)")
            positions_columns = [row[1] for row in cursor.fetchall()]
            
            if 'currency' not in positions_columns:
                cursor.execute("ALTER TABLE positions ADD COLUMN currency TEXT DEFAULT 'USD'")
                print("Added currency column to positions table")
                
            # Check and add chinese_name column to positions table
            if 'chinese_name' not in positions_columns:
                cursor.execute("ALTER TABLE positions ADD COLUMN chinese_name TEXT")
                print("Added chinese_name column to positions table")
            
            # Update existing records with appropriate currency based on broker
            # CATHAY/國泰證券 → TWD, SCHWAB/TDA → USD
            cursor.execute("""
                UPDATE accounts 
                SET currency = CASE 
                    WHEN broker LIKE '%CATHAY%' OR broker LIKE '%國泰證券%' THEN 'TWD'
                    WHEN broker LIKE '%SCHWAB%' OR broker LIKE '%TDA%' THEN 'USD'
                    ELSE 'USD'
                END
                WHERE currency = 'USD' OR currency IS NULL
            """)
            
            cursor.execute("""
                UPDATE transactions 
                SET currency = CASE 
                    WHEN broker LIKE '%CATHAY%' OR broker LIKE '%國泰證券%' THEN 'TWD'
                    WHEN broker LIKE '%SCHWAB%' OR broker LIKE '%TDA%' THEN 'USD'
                    ELSE 'USD'
                END
                WHERE currency = 'USD' OR currency IS NULL
            """)
            
            cursor.execute("""
                UPDATE positions 
                SET currency = CASE 
                    WHEN broker LIKE '%CATHAY%' OR broker LIKE '%國泰證券%' THEN 'TWD'
                    WHEN broker LIKE '%SCHWAB%' OR broker LIKE '%TDA%' THEN 'USD'
                    ELSE 'USD'
                END
                WHERE currency = 'USD' OR currency IS NULL
            """)
            
            print("Updated existing records with appropriate currency values")
            
        except Exception as e:
            print(f"Migration warning: {e}")
            # Don't fail if migration has issues
    
    def load_csv_data(self, csv_path):
        """Load and process the new CSV data (legacy 國泰證券 method)"""
        try:
            # Use the multi-broker parser for consistency
            parser = MultiBrokerPortfolioParser(db_path=self.db_path)
            success = parser.process_file(Path(csv_path))
            
            if success:
                print(f"Successfully processed CSV file: {csv_path}")
                return True
            else:
                print(f"Failed to process CSV file: {csv_path}")
                return False
                
        except Exception as e:
            print(f"Error loading CSV: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def process_all_broker_statements(self):
        """Process all broker statements (CSV and PDF)"""
        try:
            parser = MultiBrokerPortfolioParser(db_path=self.db_path)
            parser.process_all_statements()
            return True
        except Exception as e:
            print(f"Error processing broker statements: {e}")
            return False
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_database_info(self):
        """Get database timestamp and basic stats"""
        import os
        from datetime import datetime
        
        if not os.path.exists(self.db_path):
            return None
            
        # Get file modification time
        db_mtime = os.path.getmtime(self.db_path)
        db_timestamp = datetime.fromtimestamp(db_mtime)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get total record counts
            cursor.execute("SELECT COUNT(*) FROM transactions")
            transaction_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM accounts") 
            account_count = cursor.fetchone()[0]
            
        return {
            'timestamp': db_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'transaction_count': transaction_count,
            'account_count': account_count
        }
    
    def get_accounts(self):
        """Get all accounts with broker info"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT account_id, institution, broker, account_type, account_holder
                FROM accounts
                ORDER BY broker, institution, account_id
            """)
            return [dict(zip([col[0] for col in cursor.description], row)) 
                   for row in cursor.fetchall()]
    
    def get_brokers(self):
        """Get all unique brokers with account details for multi-account brokers"""
        # Mapping from short names to full names
        broker_mapping = {
            'CATHAY': '國泰證券',
            'SCHWAB': 'Charles Schwab', 
            'TDA': 'TD Ameritrade'
        }
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get broker info with account counts to identify multi-account brokers
            cursor.execute("""
                SELECT 
                    t.broker, 
                    a.account_id, 
                    a.institution,
                    COUNT(*) as transaction_count
                FROM transactions t
                JOIN accounts a ON t.account_id = a.account_id
                WHERE t.broker IS NOT NULL 
                GROUP BY t.broker, a.account_id, a.institution
                ORDER BY t.broker, a.account_id
            """)
            
            broker_accounts = cursor.fetchall()
            
            # Group by broker to check for multiple accounts
            broker_groups = {}
            for broker, account_id, institution, trans_count in broker_accounts:
                if broker not in broker_groups:
                    broker_groups[broker] = []
                broker_groups[broker].append({
                    'account_id': account_id,
                    'institution': institution,
                    'transaction_count': trans_count
                })
            
            # Generate broker entries
            broker_entries = []
            for broker, accounts in broker_groups.items():
                full_name = broker_mapping.get(broker, broker)
                
                if len(accounts) > 1:
                    # Multi-account broker: show separate entries for each account
                    for account_info in accounts:
                        account_display = f"{full_name} ({account_info['account_id'][-4:]})"  # Show last 4 digits
                        broker_entries.append({
                            'key': f"{broker}|{account_info['account_id']}",  # Use composite key for filtering
                            'display': account_display,
                            'sort_key': f"{full_name}_{account_info['account_id']}"
                        })
                else:
                    # Single account broker: show as normal
                    broker_entries.append({
                        'key': broker,  # Use the short broker code for single accounts
                        'display': full_name,
                        'sort_key': full_name
                    })
            
            # Sort by display name and return the display names
            broker_entries.sort(key=lambda x: x['sort_key'])
            
            # Return both keys and display names for the frontend
            return {
                'brokers': [entry['display'] for entry in broker_entries],
                'broker_keys': {entry['display']: entry['key'] for entry in broker_entries}
            }
    
    def get_symbols(self, broker_filters=None):
        """Get all unique symbols, optionally filtered by broker"""
        # Broker mapping for filtering (same as in get_transactions)
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        query = """
            SELECT DISTINCT t.symbol 
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            WHERE t.symbol IS NOT NULL
        """
        params = []
        
        if broker_filters:
            if isinstance(broker_filters, list):
                if len(broker_filters) > 0:
                    # Use the same filtering logic as get_transactions to handle composite keys
                    broker_conditions, broker_params = self._parse_broker_filter(broker_filters, use_account_join=True)
                    if broker_conditions:
                        query += f" AND ({' OR '.join(broker_conditions)})"
                        params.extend(broker_params)
            else:
                # Single broker (backward compatibility)
                if '|' in broker_filters:
                    # Composite key: specific account
                    broker_short, account_id = broker_filters.split('|', 1)
                    query += " AND (t.broker = ? AND t.account_id = ?)"
                    params.extend([broker_short, account_id])
                else:
                    # Regular broker - check both original name and mapped name
                    short_name = broker_mapping.get(broker_filters, broker_filters)
                    query += " AND (t.broker = ? OR t.broker = ? OR a.broker = ? OR a.institution = ?)"
                    params.extend([broker_filters, short_name, short_name, broker_filters])
        
        query += " ORDER BY t.symbol"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [row[0] for row in cursor.fetchall()]
    
    def get_currencies(self):
        """Get all unique currencies"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT currency FROM transactions WHERE currency IS NOT NULL ORDER BY currency")
            return [row[0] for row in cursor.fetchall()]
    
    def _is_cache_valid(self):
        """Check if cache is still valid"""
        if self._cache_timestamp is None:
            return False
        
        import time
        return (time.time() - self._cache_timestamp) < self._cache_duration
    
    def _throttle_requests(self, is_cached_request=False):
        """Implement request throttling to avoid rate limits"""
        # Skip throttling for cached responses
        if is_cached_request:
            return
            
        import time
        
        current_time = time.time()
        
        # Reset counter every hour
        if self._last_request_time is None or (current_time - self._last_request_time) > 3600:
            self._request_count = 0
            self._last_request_time = current_time
        
        # If we've made too many requests, add a longer delay
        if self._request_count > 50:  # Conservative limit
            delay = min(2.0 + (self._request_count - 50) * 0.1, 5.0)  # Cap at 5 seconds
            print(f"Rate limiting: sleeping for {delay:.1f}s (request #{self._request_count})")
            time.sleep(delay)
        
        self._request_count += 1
        self._last_request_time = current_time
    
    def _check_yahoo_finance_availability(self):
        """Check if Yahoo Finance is available and implement circuit breaker"""
        import time
        
        current_time = time.time()
        
        # Check every 10 minutes
        if self._last_yahoo_check is None or (current_time - self._last_yahoo_check) > 600:
            # Try a simple request to check availability
            try:
                import yfinance as yf
                test_ticker = yf.Ticker("AAPL")
                test_hist = test_ticker.history(period="1d")
                
                if not test_hist.empty:
                    self._yahoo_finance_available = True
                    print("Yahoo Finance availability check: OK")
                else:
                    self._yahoo_finance_available = False
                    print("Yahoo Finance availability check: No data returned")
                    
            except Exception as e:
                error_msg = str(e).lower()
                if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                    self._yahoo_finance_available = False
                    print("Yahoo Finance availability check: Rate limited")
                else:
                    self._yahoo_finance_available = False
                    print(f"Yahoo Finance availability check: Error - {e}")
            
            self._last_yahoo_check = current_time
        
        return self._yahoo_finance_available
    
    def get_forex_rate(self, from_currency, to_currency):
        """Get forex rates from Yahoo Finance with caching and rate limiting protection"""
        if from_currency == to_currency:
            return 1.0
        
        # Create cache key
        cache_key = f"{from_currency}_{to_currency}"
        
        # Check cache first
        if self._is_cache_valid() and cache_key in self._forex_cache:
            return self._forex_cache[cache_key]
        
        # Clear cache if invalid and set new timestamp
        if not self._is_cache_valid():
            self._forex_cache.clear()
            self._stock_price_cache.clear()
            # Set new cache timestamp immediately to avoid multiple clears
            import time
            self._cache_timestamp = time.time()
        
        # Yahoo Finance forex symbols
        forex_mapping = {
            ('USD', 'TWD'): 'USDTWD=X',
            ('TWD', 'USD'): 'TWDUSD=X'
        }
        
        forex_symbol = forex_mapping.get((from_currency, to_currency))
        rate = None
        
        if forex_symbol and self._check_yahoo_finance_availability():
            try:
                import yfinance as yf
                import time
                import random
                
                # Apply request throttling only for actual API calls (not cached)
                self._throttle_requests(is_cached_request=False)
                
                ticker = yf.Ticker(forex_symbol)
                
                # Try different methods with better error handling
                try:
                    # Method 1: Recent history (less likely to be rate limited)
                    hist = ticker.history(period="2d", interval="1d")
                    if not hist.empty:
                        rate = hist['Close'].iloc[-1]
                except Exception as hist_error:
                    print(f"History method failed for {forex_symbol}: {hist_error}")
                    
                    # Method 2: Try ticker info as fallback
                    try:
                        time.sleep(random.uniform(0.3, 0.7))
                        info = ticker.info
                        if info and 'regularMarketPrice' in info and info['regularMarketPrice']:
                            rate = info['regularMarketPrice']
                        elif info and 'previousClose' in info and info['previousClose']:
                            rate = info['previousClose']
                    except Exception as info_error:
                        print(f"Info method failed for {forex_symbol}: {info_error}")
                        
            except Exception as e:
                error_msg = str(e).lower()
                if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                    print(f"Rate limited fetching forex rate {from_currency}/{to_currency}, using fallback")
                    self._yahoo_finance_available = False  # Temporarily disable
                else:
                    print(f"Error fetching forex rate {from_currency}/{to_currency}: {e}")
        elif forex_symbol:
            print(f"Yahoo Finance unavailable, using fallback rate for {from_currency}/{to_currency}")
        
        # Use fallback rates if Yahoo Finance fails
        if rate is None:
            fallback_rates = {
                ('USD', 'TWD'): 31.5,
                ('TWD', 'USD'): 1/31.5
            }
            rate = fallback_rates.get((from_currency, to_currency), 1.0)
            print(f"Using fallback rate for {from_currency}/{to_currency}: {rate}")
        
        # Cache the result
        self._forex_cache[cache_key] = rate
        
        return rate

    def convert_to_twd(self, amount, from_currency):
        """Convert amount to TWD using real-time or fallback exchange rate"""
        if amount is None:
            return 0
        
        if from_currency == 'TWD':
            return amount
        
        # Get real-time exchange rate
        rate = self.get_forex_rate(from_currency, 'TWD')
        return amount * rate
    
    def _apply_broker_filter(self, query, params, filters, use_account_join=False):
        """Apply broker filter with proper handling of name mapping"""
        if not filters or not filters.get('broker'):
            return query, params
            
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        broker_filter = filters['broker']
        if isinstance(broker_filter, list) and len(broker_filter) > 0:
            broker_conditions, broker_params = self._parse_broker_filter(broker_filter, use_account_join=use_account_join)
            if broker_conditions:
                query += f" AND ({' OR '.join(broker_conditions)})"
                params.extend(broker_params)
        elif isinstance(broker_filter, str):
            # Single broker (backward compatibility)
            if '|' in broker_filter:
                # Composite key: specific account
                broker_short, account_id = broker_filter.split('|', 1)
                query += " AND (t.broker = ? AND t.account_id = ?)"
                params.extend([broker_short, account_id])
            else:
                # Regular broker - check both original name and mapped name
                short_name = broker_mapping.get(broker_filter, broker_filter)
                if use_account_join:
                    query += " AND (t.broker = ? OR t.broker = ? OR a.broker = ? OR a.institution = ?)"
                    params.extend([broker_filter, short_name, short_name, broker_filter])
                else:
                    query += " AND (t.broker = ? OR t.broker = ?)"
                    params.extend([broker_filter, short_name])
                    
        return query, params

    def _parse_broker_filter(self, broker_filter_list, use_account_join=False):
        """Parse broker filter list that may contain composite keys (BROKER|ACCOUNT_ID)"""
        broker_conditions = []
        params = []
        
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        for broker_entry in broker_filter_list:
            if '|' in broker_entry:
                # Composite key: specific account
                broker_short, account_id = broker_entry.split('|', 1)
                broker_conditions.append("(t.broker = ? AND t.account_id = ?)")
                params.extend([broker_short, account_id])
            else:
                # Regular broker: either full name or short name
                short_name = broker_mapping.get(broker_entry, broker_entry)
                if use_account_join:
                    # When joining with accounts table, check both original name and mapped name
                    # This handles cases where transactions table has Chinese names but accounts table has short codes
                    broker_conditions.append("(t.broker = ? OR t.broker = ? OR a.broker = ? OR a.institution = ?)")
                    params.extend([broker_entry, short_name, short_name, broker_entry])
                else:
                    # When not joining, check both original and mapped name in transaction table
                    broker_conditions.append("(t.broker = ? OR t.broker = ?)")
                    params.extend([broker_entry, short_name])
        
        return broker_conditions, params

    def get_transactions(self, filters=None):
        """Get filtered transactions with enhanced filtering and multi-select support"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        query = """
            SELECT t.*, a.institution, a.broker as account_broker
            FROM transactions t
            JOIN accounts a ON t.account_id = a.account_id
            WHERE 1=1
        """
        params = []
        
        if filters:
            if filters.get('account_id'):
                query += " AND t.account_id = ?"
                params.append(filters['account_id'])
            
            # Handle multi-select broker filter with account separation support
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list) and len(broker_filter) > 0:
                    broker_conditions, broker_params = self._parse_broker_filter(broker_filter, use_account_join=True)
                    if broker_conditions:
                        query += f" AND ({' OR '.join(broker_conditions)})"
                        params.extend(broker_params)
                elif isinstance(broker_filter, str):
                    # Single broker (backward compatibility)
                    if '|' in broker_filter:
                        # Composite key: specific account
                        broker_short, account_id = broker_filter.split('|', 1)
                        query += " AND (t.broker = ? AND t.account_id = ?)"
                        params.extend([broker_short, account_id])
                    else:
                        # Regular broker - check both original name and mapped name
                        short_name = broker_mapping.get(broker_filter, broker_filter)
                        query += " AND (t.broker = ? OR t.broker = ? OR a.broker = ? OR a.institution = ?)"
                        params.extend([broker_filter, short_name, short_name, broker_filter])
            
            # Handle multi-select symbol filter
            if filters.get('symbol'):
                symbol_filter = filters['symbol']
                if isinstance(symbol_filter, list):
                    if len(symbol_filter) > 0:
                        placeholders = ','.join(['?' for _ in symbol_filter])
                        query += f" AND t.symbol IN ({placeholders})"
                        params.extend(symbol_filter)
                else:
                    # Single symbol (backward compatibility)
                    query += " AND t.symbol LIKE ?"
                    params.append(f"%{symbol_filter}%")
            
            # Handle multi-select transaction type filter
            if filters.get('transaction_type'):
                transaction_type_filter = filters['transaction_type']
                if isinstance(transaction_type_filter, list):
                    if len(transaction_type_filter) > 0:
                        conditions = []
                        for trans_type in transaction_type_filter:
                            if trans_type == 'DEPOSIT':
                                conditions.append("(t.net_amount > 0 AND t.symbol IS NULL)")
                            elif trans_type == 'WITHDRAWAL':
                                conditions.append("(t.net_amount < 0 AND t.symbol IS NULL)")
                            else:
                                conditions.append("t.transaction_type = ?")
                                params.append(trans_type)
                        
                        if conditions:
                            query += f" AND ({' OR '.join(conditions)})"
                else:
                    # Single transaction type (backward compatibility)
                    if transaction_type_filter == 'DEPOSIT':
                        query += " AND t.net_amount > 0 AND t.symbol IS NULL"
                    elif transaction_type_filter == 'WITHDRAWAL':
                        query += " AND t.net_amount < 0 AND t.symbol IS NULL"
                    else:
                        query += " AND t.transaction_type = ?"
                        params.append(transaction_type_filter)
            
            if filters.get('start_date'):
                query += " AND t.transaction_date >= ?"
                params.append(filters['start_date'])
            
            if filters.get('end_date'):
                query += " AND t.transaction_date <= ?"
                params.append(filters['end_date'])
            
            if filters.get('year'):
                query += " AND strftime('%Y', t.transaction_date) = ?"
                params.append(str(filters['year']))
        
        query += " ORDER BY t.transaction_date DESC, t.id DESC"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(zip([col[0] for col in cursor.description], row)) 
                   for row in cursor.fetchall()]
    
    def get_portfolio_summary(self, filters=None):
        """Get enhanced portfolio summary with fees and multi-select support, converted to TWD"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        # Get all transactions with filtering, but include currency for conversion
        query = """
            SELECT 
                t.net_amount,
                t.fee,
                t.tax,
                t.transaction_type,
                t.symbol,
                t.currency
            FROM transactions t
            WHERE 1=1
        """
        
        params = []
        if filters:
            # Handle multi-select broker filter with account separation support
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list) and len(broker_filter) > 0:
                    broker_conditions, broker_params = self._parse_broker_filter(broker_filter, use_account_join=False)
                    if broker_conditions:
                        query += f" AND ({' OR '.join(broker_conditions)})"
                        params.extend(broker_params)
                elif isinstance(broker_filter, str):
                    # Single broker (backward compatibility)
                    if '|' in broker_filter:
                        # Composite key: specific account
                        broker_short, account_id = broker_filter.split('|', 1)
                        query += " AND (t.broker = ? AND t.account_id = ?)"
                        params.extend([broker_short, account_id])
                    else:
                        # Regular broker - check both original name and mapped name
                        short_name = broker_mapping.get(broker_filter, broker_filter)
                        query += " AND (t.broker = ? OR t.broker = ?)"
                        params.extend([broker_filter, short_name])
            
            # Handle multi-select symbol filter
            if filters.get('symbol'):
                symbol_filter = filters['symbol']
                if isinstance(symbol_filter, list):
                    if len(symbol_filter) > 0:
                        placeholders = ','.join(['?' for _ in symbol_filter])
                        query += f" AND t.symbol IN ({placeholders})"
                        params.extend(symbol_filter)
                else:
                    # Single symbol (backward compatibility)
                    query += " AND t.symbol = ?"
                    params.append(symbol_filter)
            
            # Handle multi-select transaction type filter
            if filters.get('transaction_type'):
                transaction_type_filter = filters['transaction_type']
                if isinstance(transaction_type_filter, list):
                    if len(transaction_type_filter) > 0:
                        conditions = []
                        for trans_type in transaction_type_filter:
                            if trans_type == 'DEPOSIT':
                                conditions.append("(t.net_amount > 0 AND t.symbol IS NULL)")
                            elif trans_type == 'WITHDRAWAL':
                                conditions.append("(t.net_amount < 0 AND t.symbol IS NULL)")
                            else:
                                conditions.append("t.transaction_type = ?")
                                params.append(trans_type)
                        
                        if conditions:
                            query += f" AND ({' OR '.join(conditions)})"
                else:
                    # Single transaction type (backward compatibility)
                    if transaction_type_filter == 'DEPOSIT':
                        query += " AND t.net_amount > 0 AND t.symbol IS NULL"
                    elif transaction_type_filter == 'WITHDRAWAL':
                        query += " AND t.net_amount < 0 AND t.symbol IS NULL"
                    else:
                        query += " AND t.transaction_type = ?"
                        params.append(transaction_type_filter)
            
            if filters.get('year'):
                query += " AND strftime('%Y', t.transaction_date) = ?"
                params.append(str(filters['year']))
            
            if filters.get('start_date'):
                query += " AND t.transaction_date >= ?"
                params.append(filters['start_date'])
            
            if filters.get('end_date'):
                query += " AND t.transaction_date <= ?"
                params.append(filters['end_date'])
        
        # Execute query and convert amounts to TWD
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            # Initialize totals in TWD
            total_sales_twd = 0
            total_purchases_twd = 0
            total_dividends_twd = 0
            total_fees_twd = 0
            total_taxes_twd = 0
            total_deposits_twd = 0
            total_withdrawals_twd = 0
            total_transactions = 0
            
            # Process each transaction and convert to TWD
            for row in cursor.fetchall():
                net_amount, fee, tax, transaction_type, symbol, currency = row
                
                # Convert amounts to TWD
                net_amount_twd = self.convert_to_twd(net_amount or 0, currency or 'TWD')
                fee_twd = self.convert_to_twd(fee or 0, currency or 'TWD')
                tax_twd = self.convert_to_twd(tax or 0, currency or 'TWD')
                
                total_transactions += 1
                total_fees_twd += fee_twd
                total_taxes_twd += tax_twd
                
                # Categorize by transaction type
                if transaction_type == 'SELL':
                    total_sales_twd += net_amount_twd
                elif transaction_type == 'BUY':
                    total_purchases_twd += abs(net_amount_twd)
                elif transaction_type == 'DIVIDEND':
                    total_dividends_twd += net_amount_twd
                elif net_amount_twd > 0 and symbol is None:  # Deposit
                    total_deposits_twd += net_amount_twd
                elif net_amount_twd < 0 and symbol is None:  # Withdrawal
                    total_withdrawals_twd += abs(net_amount_twd)
            
            # Calculate CORRECTED realized P&L (only from sold quantities) 
            realized_gain_loss_twd = self._calculate_true_realized_pnl(filters)
            net_after_fees_twd = realized_gain_loss_twd - total_fees_twd - total_taxes_twd
            
            # Get detailed breakdown of realized P&L by symbol
            realized_pnl_breakdown = self._get_realized_pnl_breakdown(filters)
            
            # Calculate alternative P&L views
            current_holdings_realized_pnl = sum([item['realized_pnl_twd'] for item in realized_pnl_breakdown if item['remaining_shares'] > 0])
            closed_positions_realized_pnl = sum([item['realized_pnl_twd'] for item in realized_pnl_breakdown if item['remaining_shares'] == 0])
            
            # Calculate unrealized P&L for current holdings
            unrealized_pnl_data = self.calculate_unrealized_pnl(filters)
            unrealized_pnl_twd = unrealized_pnl_data.get('unrealized_pnl', 0)
            
            # Calculate True Cash Earnings
            # True Cash Earnings = Net Profit + Unrealized P&L + Dividends - Net Cash Invested  
            net_cash_invested = total_deposits_twd - total_withdrawals_twd
            true_cash_earnings = net_after_fees_twd + unrealized_pnl_twd + total_dividends_twd - net_cash_invested
            
            return {
                'total_sales': total_sales_twd,
                'total_purchases': total_purchases_twd,
                'total_dividends': total_dividends_twd,
                'total_fees': total_fees_twd,
                'total_taxes': total_taxes_twd,
                'total_deposits': total_deposits_twd,
                'total_withdrawals': total_withdrawals_twd,
                'total_transactions': total_transactions,
                'realized_gain_loss': realized_gain_loss_twd,
                'net_after_fees': net_after_fees_twd,
                'unrealized_pnl': unrealized_pnl_twd,
                'net_cash_invested': net_cash_invested,
                'true_cash_earnings': true_cash_earnings,
                'realized_pnl_breakdown': realized_pnl_breakdown,
                'alternative_views': {
                    'current_holdings_realized_pnl': current_holdings_realized_pnl,
                    'closed_positions_realized_pnl': closed_positions_realized_pnl,
                    'explanation': 'current_holdings_realized_pnl includes only gains from stocks still held; closed_positions_realized_pnl includes gains from fully sold positions'
                }
            }
    
    def _calculate_true_realized_pnl(self, filters=None):
        """Calculate true realized P&L from only SOLD quantities using matched buy/sell pairs"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        # Get position analysis query - same as portfolio_performance_analysis
        positions_query = """
            SELECT 
                t.symbol,
                t.broker,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as total_bought,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.quantity ELSE 0 END) as total_sold,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN ABS(t.net_amount) ELSE 0 END) as total_invested,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.net_amount ELSE 0 END) as total_received,
                t.currency
            FROM transactions t
            WHERE t.symbol IS NOT NULL AND t.symbol != ''
        """
        
        # Apply filters
        params = []
        if filters:
            # Apply broker filter using improved logic
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list) and len(broker_filter) > 0:
                    # For each broker, check both original name and mapped name
                    broker_conditions = []
                    for broker in broker_filter:
                        short_name = broker_mapping.get(broker, broker)
                        if broker != short_name:
                            # If there's a mapping, check both names
                            broker_conditions.append("(t.broker = ? OR t.broker = ?)")
                            params.extend([broker, short_name])
                        else:
                            # If no mapping, just check the original name
                            broker_conditions.append("t.broker = ?")
                            params.append(broker)
                    
                    if broker_conditions:
                        positions_query += f" AND ({' OR '.join(broker_conditions)})"
                elif isinstance(broker_filter, str):
                    short_name = broker_mapping.get(broker_filter, broker_filter)
                    if broker_filter != short_name:
                        # If there's a mapping, check both names
                        positions_query += " AND (t.broker = ? OR t.broker = ?)"
                        params.extend([broker_filter, short_name])
                    else:
                        # If no mapping, just check the original name
                        positions_query += " AND t.broker = ?"
                        params.append(broker_filter)
            
            # Handle multi-select symbol filter - FIXED: This was missing!
            if filters.get('symbol'):
                symbol_filter = filters['symbol']
                if isinstance(symbol_filter, list) and len(symbol_filter) > 0:
                    placeholders = ','.join(['?' for _ in symbol_filter])
                    positions_query += f" AND t.symbol IN ({placeholders})"
                    params.extend(symbol_filter)
                elif isinstance(symbol_filter, str):
                    positions_query += " AND t.symbol = ?"
                    params.append(symbol_filter)
        
        positions_query += " GROUP BY t.symbol, t.broker, t.currency ORDER BY t.symbol"
        
        total_realized_gain_loss_twd = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(positions_query, params)
            positions_data = cursor.fetchall()
            
            for row in positions_data:
                symbol, broker, bought, sold, invested, received, currency = row
                
                # Only calculate realized P&L for sold quantities
                if sold > 0:
                    # Calculate realized gain/loss: received from sales minus cost basis of sold shares
                    cost_of_sold_shares = invested * (sold / bought) if bought > 0 else 0
                    realized_gain_loss = received - cost_of_sold_shares
                    
                    # Convert to TWD
                    realized_gain_loss_twd = self.convert_to_twd(realized_gain_loss, currency or 'TWD')
                    total_realized_gain_loss_twd += realized_gain_loss_twd
        
        return total_realized_gain_loss_twd
        
    def _get_realized_pnl_breakdown(self, filters=None):
        """Get detailed breakdown of realized P&L by symbol"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        # Get position analysis query
        positions_query = """
            SELECT 
                t.symbol,
                t.broker,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as total_bought,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.quantity ELSE 0 END) as total_sold,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN ABS(t.net_amount) ELSE 0 END) as total_invested,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.net_amount ELSE 0 END) as total_received,
                t.currency
            FROM transactions t
            WHERE t.symbol IS NOT NULL AND t.symbol != ''
        """
        
        # Apply filters
        params = []
        if filters:
            # Handle multi-select broker filter
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list) and len(broker_filter) > 0:
                    short_names = [broker_mapping.get(broker, broker) for broker in broker_filter]
                    placeholders = ','.join(['?' for _ in short_names])
                    positions_query += f" AND t.broker IN ({placeholders})"
                    params.extend(short_names)
                elif isinstance(broker_filter, str):
                    short_name = broker_mapping.get(broker_filter, broker_filter)
                    positions_query += " AND t.broker = ?"
                    params.append(short_name)
        
        positions_query += " GROUP BY t.symbol, t.broker, t.currency ORDER BY t.symbol"
        
        breakdown = []
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(positions_query, params)
            positions_data = cursor.fetchall()
            
            for row in positions_data:
                symbol, broker, bought, sold, invested, received, currency = row
                
                # Only include positions that have been sold
                if sold > 0:
                    # Calculate realized gain/loss: received from sales minus cost basis of sold shares
                    cost_of_sold_shares = invested * (sold / bought) if bought > 0 else 0
                    realized_gain_loss = received - cost_of_sold_shares
                    
                    # Convert to TWD
                    realized_gain_loss_twd = self.convert_to_twd(realized_gain_loss, currency or 'TWD')
                    
                    # Calculate if this position is still held or completely sold
                    remaining_shares = bought - sold
                    
                    breakdown.append({
                        'symbol': symbol,
                        'broker': broker,
                        'total_bought': bought,
                        'total_sold': sold,
                        'remaining_shares': remaining_shares,
                        'total_invested': invested,
                        'total_received': received,
                        'cost_of_sold_shares': cost_of_sold_shares,
                        'realized_pnl': realized_gain_loss,
                        'realized_pnl_twd': realized_gain_loss_twd,
                        'currency': currency,
                        'position_status': 'CLOSED' if remaining_shares == 0 else 'PARTIAL'
                    })
        
        return breakdown

    def _get_current_holdings(self, filters=None):
        """Get current holdings (bought - sold quantities > 0)"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        holdings_query = """
            SELECT 
                t.symbol,
                t.broker,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as bought_qty,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN ABS(t.quantity) ELSE 0 END) as sold_qty,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity 
                         WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN -ABS(t.quantity) ELSE 0 END) as current_holding,
                AVG(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.price ELSE NULL END) as avg_cost,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN ABS(t.net_amount) ELSE 0 END) as total_invested,
                t.currency
            FROM transactions t 
            WHERE t.symbol IS NOT NULL AND t.symbol != ''
        """
        
        # Apply filters  
        params = []
        if filters:
            # Handle multi-select broker filter
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list) and len(broker_filter) > 0:
                    broker_conditions, broker_params = self._parse_broker_filter(broker_filter, use_account_join=False)
                    if broker_conditions:
                        holdings_query += f" AND ({' OR '.join(broker_conditions)})"
                        params.extend(broker_params)
                elif isinstance(broker_filter, str):
                    # Single broker (backward compatibility)
                    if '|' in broker_filter:
                        # Composite key: specific account
                        broker_short, account_id = broker_filter.split('|', 1)
                        holdings_query += " AND (t.broker = ? AND t.account_id = ?)"
                        params.extend([broker_short, account_id])
                    else:
                        # Regular broker - check both original name and mapped name
                        short_name = broker_mapping.get(broker_filter, broker_filter)
                        holdings_query += " AND (t.broker = ? OR t.broker = ?)"
                        params.extend([broker_filter, short_name])
            
            # Handle multi-select symbol filter
            if filters.get('symbol'):
                symbol_filter = filters['symbol']
                if isinstance(symbol_filter, list) and len(symbol_filter) > 0:
                    placeholders = ','.join(['?' for _ in symbol_filter])
                    holdings_query += f" AND t.symbol IN ({placeholders})"
                    params.extend(symbol_filter)
                elif isinstance(symbol_filter, str):
                    holdings_query += " AND t.symbol = ?"
                    params.append(symbol_filter)
            
            # Handle date filters
            if filters.get('start_date'):
                holdings_query += " AND t.transaction_date >= ?"
                params.append(filters['start_date'])
            
            if filters.get('end_date'):
                holdings_query += " AND t.transaction_date <= ?"
                params.append(filters['end_date'])
            
            if filters.get('year'):
                holdings_query += " AND strftime('%Y', t.transaction_date) = ?"
                params.append(str(filters['year']))
        
        holdings_query += " GROUP BY t.symbol, t.broker, t.currency HAVING current_holding > 0"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(holdings_query, params)
            return cursor.fetchall()

    def _get_current_prices_enhanced(self, symbols_with_brokers):
        """Fetch current prices for symbols with enhanced Yahoo Finance integration and caching"""
        import yfinance as yf
        
        prices = {}
        errors = []
        symbols_to_fetch = []
        
        # Clear cache if invalid and set new timestamp
        if not self._is_cache_valid():
            self._stock_price_cache.clear()
            self._forex_cache.clear()
            # Set new cache timestamp immediately to avoid multiple clears
            import time
            self._cache_timestamp = time.time()
        
        # Check cache first for each symbol
        for symbol_info in symbols_with_brokers:
            if isinstance(symbol_info, tuple):
                symbol, broker = symbol_info
            else:
                symbol = symbol_info
                broker = None
            
            if self._is_cache_valid() and symbol in self._stock_price_cache:
                cached_price = self._stock_price_cache[symbol]
                if cached_price is not None:
                    prices[symbol] = cached_price
                else:
                    errors.append(symbol)
            else:
                symbols_to_fetch.append(symbol_info)
        
        # Fetch only uncached symbols with rate limiting protection
        for i, symbol_info in enumerate(symbols_to_fetch):
            if isinstance(symbol_info, tuple):
                symbol, broker = symbol_info
            else:
                symbol = symbol_info
                broker = None
            
            try:
                import time
                import random
                
                # Apply request throttling only for actual API calls (not cached)
                self._throttle_requests(is_cached_request=False)
                
                # Get enhanced Yahoo symbol
                yahoo_symbol = self._get_yahoo_symbol(symbol, broker)
                ticker = yf.Ticker(yahoo_symbol)
                
                # Try multiple methods to get price
                current_price = None
                
                # Method 1: Historical data with appropriate period
                try:
                    period = "5d" if ".TW" in yahoo_symbol else "2d"  # Extended period for better data
                    hist = ticker.history(period=period, interval="1d")
                    
                    if not hist.empty:
                        current_price = hist['Close'].iloc[-1]
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                        print(f"Rate limited on historical data for {yahoo_symbol}, trying alternative methods")
                    else:
                        print(f"Historical data failed for {yahoo_symbol}: {e}")
                
                # Method 2: Try ticker info for real-time price
                if current_price is None:
                    try:
                        time.sleep(random.uniform(0.2, 0.5))  # Small delay between methods
                        info = ticker.info
                        if info and 'regularMarketPrice' in info and info['regularMarketPrice']:
                            current_price = info['regularMarketPrice']
                        elif info and 'previousClose' in info and info['previousClose']:
                            current_price = info['previousClose']
                    except Exception as e:
                        error_msg = str(e).lower()
                        if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                            print(f"Rate limited on info for {yahoo_symbol}")
                        else:
                            print(f"Ticker info failed for {yahoo_symbol}: {e}")
                
                # Method 3: Try fast_info (newer yfinance feature)
                if current_price is None:
                    try:
                        time.sleep(random.uniform(0.2, 0.5))  # Small delay between methods
                        fast_info = ticker.fast_info
                        if hasattr(fast_info, 'last_price') and fast_info.last_price:
                            current_price = fast_info.last_price
                    except Exception as e:
                        error_msg = str(e).lower()
                        if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                            print(f"Rate limited on fast_info for {yahoo_symbol}")
                        else:
                            print(f"Fast info failed for {yahoo_symbol}: {e}")
                
                if current_price is not None and current_price > 0:
                    prices[symbol] = current_price
                    # Cache the result
                    self._stock_price_cache[symbol] = current_price
                    # Update cache timestamp
                    if self._cache_timestamp is None:
                        import time
                        self._cache_timestamp = time.time()
                    print(f"Successfully fetched price for {symbol}: {current_price}")
                else:
                    prices[symbol] = None
                    # Cache the None result to avoid repeated failed requests
                    self._stock_price_cache[symbol] = None
                    errors.append({
                        'symbol': symbol,
                        'yahoo_symbol': yahoo_symbol,
                        'error': 'No price data available after trying all methods'
                    })
                    
            except Exception as e:
                yahoo_symbol = 'unknown'
                try:
                    yahoo_symbol = self._get_yahoo_symbol(symbol, broker)
                except:
                    pass
                
                error_msg = str(e).lower()
                if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                    print(f"Rate limited fetching price for {symbol} ({yahoo_symbol}), will use fallback")
                    # For rate limiting, don't cache the error to allow retry later
                    prices[symbol] = None
                else:
                    print(f"Error fetching price for {symbol} ({yahoo_symbol}): {e}")
                    prices[symbol] = None
                    # Cache the None result to avoid repeated failed requests
                    self._stock_price_cache[symbol] = None
                    
                errors.append({
                    'symbol': symbol,
                    'yahoo_symbol': yahoo_symbol,
                    'error': str(e)
                })
        
        return prices, errors

    def _get_current_prices(self, symbols):
        """Legacy method - fetch current prices for symbols from Yahoo Finance with caching"""
        import yfinance as yf
        
        prices = {}
        symbols_to_fetch = []
        
        # Check cache first for each symbol
        for symbol in symbols:
            if self._is_cache_valid() and symbol in self._stock_price_cache:
                prices[symbol] = self._stock_price_cache[symbol]
            else:
                symbols_to_fetch.append(symbol)
        
        # Clear cache if invalid and set new timestamp
        if not self._is_cache_valid():
            self._stock_price_cache.clear()
            self._forex_cache.clear()
            # Set new cache timestamp immediately to avoid multiple clears
            import time
            self._cache_timestamp = time.time()
        
        # Fetch only uncached symbols with rate limiting protection
        for i, symbol in enumerate(symbols_to_fetch):
            try:
                import time
                import random
                
                # Apply request throttling only for actual API calls (not cached)
                self._throttle_requests(is_cached_request=False)
                
                # Handle different stock markets
                yahoo_symbol = self._get_yahoo_symbol(symbol)
                ticker = yf.Ticker(yahoo_symbol)
                
                # Use longer period for Taiwan stocks for better data availability
                period = "5d" if ".TW" in yahoo_symbol else "2d"
                
                try:
                    hist = ticker.history(period=period)
                    
                    if not hist.empty:
                        current_price = hist['Close'].iloc[-1]
                        prices[symbol] = current_price
                        # Cache the result
                        self._stock_price_cache[symbol] = current_price
                    else:
                        # Try alternative method for Taiwan stocks
                        if ".TW" in yahoo_symbol:
                            try:
                                time.sleep(random.uniform(0.3, 0.7))
                                info = ticker.info
                                if info and 'regularMarketPrice' in info:
                                    prices[symbol] = info['regularMarketPrice']
                                    self._stock_price_cache[symbol] = info['regularMarketPrice']
                                else:
                                    prices[symbol] = None
                                    self._stock_price_cache[symbol] = None
                            except Exception as info_error:
                                error_msg = str(info_error).lower()
                                if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                                    print(f"Rate limited on info method for {yahoo_symbol}")
                                prices[symbol] = None
                                self._stock_price_cache[symbol] = None
                        else:
                            prices[symbol] = None
                            self._stock_price_cache[symbol] = None
                except Exception as hist_error:
                    error_msg = str(hist_error).lower()
                    if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                        print(f"Rate limited on history method for {yahoo_symbol}, skipping caching")
                        prices[symbol] = None
                        # Don't cache rate limit errors to allow retry later
                    else:
                        print(f"History fetch failed for {yahoo_symbol}: {hist_error}")
                        prices[symbol] = None
                        self._stock_price_cache[symbol] = None
                    
            except Exception as e:
                error_msg = str(e).lower()
                if 'too many requests' in error_msg or '429' in error_msg or 'rate limit' in error_msg:
                    print(f"Rate limited fetching price for {symbol}, will retry later")
                    prices[symbol] = None
                    # Don't cache rate limit errors
                else:
                    print(f"Error fetching price for {symbol}: {e}")
                    prices[symbol] = None
        return prices
    
    def _get_yahoo_symbol(self, symbol, broker=None):
        """Enhanced symbol mapping for all exchanges with comprehensive Taiwan stock support"""
        # Taiwan stocks - numeric codes (4 digits)
        if symbol.isdigit() and len(symbol) == 4:
            return f"{symbol}.TW"
        
        # Taiwan stocks - Chinese names (comprehensive mapping)
        taiwan_name_mapping = {
            '台積電': '2330.TW',     # TSMC
            '聯發科': '2454.TW',     # MediaTek
            '鴻海': '2317.TW',       # Foxconn/Hon Hai
            '中鋼': '2002.TW',       # China Steel
            '富邦台50': '006208.TW', # Fubon Taiwan 50 ETF
            '台塑': '1301.TW',       # Formosa Plastics
            '台化': '1326.TW',       # Formosa Chemicals
            '中華電': '2412.TW',     # Chunghwa Telecom
            '台達電': '2308.TW',     # Delta Electronics
            '國泰金': '2882.TW',     # Cathay Financial
            '玉山金': '2884.TW',     # E.SUN Financial
            '兆豐金': '2886.TW',     # Mega Financial
            '富邦金': '2881.TW',     # Fubon Financial
            '元大台灣50': '0050.TW', # Yuanta Taiwan 50 ETF (alternative name)
            '台泥': '1101.TW',       # Taiwan Cement
            '遠傳': '4904.TW',       # Far EasTone
            '中信金': '2891.TW',     # CTBC Financial
            '永豐金': '2890.TW',     # SinoPac Financial
            '南亞': '1303.TW',       # Nan Ya Plastics
            '華碩': '2357.TW',       # ASUSTek
            '廣達': '2382.TW',       # Quanta Computer
            '仁寶': '2324.TW',       # Compal Electronics
            '和碩': '4938.TW',       # Pegatron
            '英業達': '2356.TW',     # Inventec
            '宏碁': '2353.TW',       # Acer
            '緯創': '3231.TW',       # Wistron
            '光寶科': '2301.TW',     # Lite-On Technology
            '統一': '1216.TW',       # Uni-President
            '味全': '1201.TW',       # Wei Chuan Foods
            '長榮': '2603.TW',       # Evergreen Marine
            '陽明': '2609.TW',       # Yang Ming Marine
            '萬海': '2615.TW'        # Wan Hai Lines
        }
        
        # Check Taiwan name mapping first
        if symbol in taiwan_name_mapping:
            return taiwan_name_mapping[symbol]
        
        # US stocks - check broker context
        if broker in ['TDA', 'SCHWAB']:
            return symbol  # AAPL, MSFT, etc. (no suffix needed)
        
        # Hong Kong stocks - .HK suffix
        if symbol.isdigit() and len(symbol) in [1, 2, 3, 4, 5]:
            # Could be Hong Kong stock, but need more context
            pass
        
        # Default: return as-is for US stocks or unknown symbols
        return symbol

    def calculate_unrealized_pnl(self, filters=None):
        """Calculate unrealized P&L for current holdings using Yahoo Finance prices"""
        holdings = self._get_current_holdings(filters)
        
        if not holdings:
            return {
                'unrealized_pnl': 0,
                'total_market_value': 0,
                'total_cost_basis': 0,
                'holdings_count': 0,
                'total_shares': 0,
                'holdings_details': [],
                'price_fetch_errors': []
            }
        
        # Get unique symbols for price fetching
        symbols = list(set([holding[0] for holding in holdings]))  # symbol is first column
        current_prices = self._get_current_prices(symbols)
        
        total_unrealized_pnl_twd = 0
        total_market_value_twd = 0
        total_cost_basis_twd = 0
        total_shares = 0
        holdings_details = []
        price_fetch_errors = []
        
        for holding in holdings:
            symbol, broker, bought_qty, sold_qty, current_holding, avg_cost, total_invested, currency = holding
            
            current_price = current_prices.get(symbol)
            if current_price is None:
                price_fetch_errors.append(symbol)
                # Use avg cost as fallback
                current_price = avg_cost or 0
            
            # Calculate cost basis for remaining shares
            cost_basis = (total_invested * (current_holding / bought_qty)) if bought_qty > 0 else 0
            market_value = current_holding * current_price
            
            # Convert to TWD
            cost_basis_twd = self.convert_to_twd(cost_basis, currency or 'TWD')
            market_value_twd = self.convert_to_twd(market_value, currency or 'TWD')
            
            unrealized_pnl_twd = market_value_twd - cost_basis_twd
            
            total_unrealized_pnl_twd += unrealized_pnl_twd
            total_market_value_twd += market_value_twd
            total_cost_basis_twd += cost_basis_twd
            total_shares += current_holding
            
            # Add detailed holding information
            holdings_details.append({
                'symbol': symbol,
                'broker': broker,
                'shares': current_holding,
                'avg_cost': avg_cost,
                'current_price': current_price,
                'cost_basis': cost_basis,
                'market_value': market_value,
                'unrealized_pnl': unrealized_pnl_twd,
                'currency': currency
            })
        
        return {
            'unrealized_pnl': total_unrealized_pnl_twd,
            'total_market_value': total_market_value_twd,
            'total_cost_basis': total_cost_basis_twd,
            'holdings_count': len(holdings),
            'total_shares': total_shares,
            'holdings_details': holdings_details,
            'price_fetch_errors': price_fetch_errors
        }

    def calculate_enhanced_unrealized_pnl(self, filters=None, base_currency='TWD'):
        """Calculate unrealized P&L with enhanced forex conversion and comprehensive symbol mapping"""
        holdings = self._get_current_holdings(filters)
        
        if not holdings:
            return {
                'unrealized_pnl': 0,
                'total_market_value': 0,
                'total_cost_basis': 0,
                'holdings_count': 0,
                'total_shares': 0,
                'holdings_details': [],
                'price_fetch_errors': [],
                'forex_rates_used': {},
                'base_currency': base_currency
            }
        
        # Prepare symbols with broker information for enhanced price fetching
        symbols_with_brokers = [(holding[0], holding[1]) for holding in holdings]  # symbol, broker
        
        # Get current prices using enhanced method
        current_prices, price_errors = self._get_current_prices_enhanced(symbols_with_brokers)
        
        # Get current forex rates
        forex_rates = {
            'USDTWD': self.get_forex_rate('USD', 'TWD')
        }
        
        total_unrealized_pnl = 0
        total_market_value = 0
        total_cost_basis = 0
        total_shares = 0
        detailed_holdings = []
        
        for holding in holdings:
            symbol, broker, bought_qty, sold_qty, current_holding, avg_cost, total_invested, currency = holding
            
            # Get current price
            current_price = current_prices.get(symbol)
            yahoo_symbol = self._get_yahoo_symbol(symbol, broker)
            
            if current_price is None:
                # Fallback to average cost
                current_price = avg_cost or 0
            
            # Calculate cost basis for remaining shares
            cost_basis = (total_invested * (current_holding / bought_qty)) if bought_qty > 0 else 0
            market_value = current_holding * current_price
            
            # Convert to base currency
            if base_currency == 'TWD':
                market_value_base = self.convert_to_twd(market_value, currency or 'TWD')
                cost_basis_base = self.convert_to_twd(cost_basis, currency or 'TWD')
            else:
                # Use the generic forex conversion for other currencies
                rate = self.get_forex_rate(currency or 'TWD', base_currency)
                market_value_base = market_value * rate
                cost_basis_base = cost_basis * rate
            unrealized_pnl_base = market_value_base - cost_basis_base
            
            total_unrealized_pnl += unrealized_pnl_base
            total_market_value += market_value_base
            total_cost_basis += cost_basis_base
            total_shares += current_holding
            
            # Add detailed holding information
            detailed_holdings.append({
                'symbol': symbol,
                'broker': broker,
                'currency': currency,
                'current_holding': current_holding,
                'current_price': current_price,
                'market_value': market_value,
                'market_value_base': market_value_base,
                'cost_basis': cost_basis,
                'cost_basis_base': cost_basis_base,
                'unrealized_pnl_base': unrealized_pnl_base,
                'yahoo_symbol': yahoo_symbol,
                'avg_cost': avg_cost
            })
        
        return {
            'unrealized_pnl': total_unrealized_pnl,
            'total_market_value': total_market_value,
            'total_cost_basis': total_cost_basis,
            'holdings_count': len(holdings),
            'total_shares': total_shares,
            'holdings_details': detailed_holdings,
            'price_fetch_errors': price_errors,
            'forex_rates_used': forex_rates,
            'base_currency': base_currency
        }

    def get_portfolio_performance_analysis(self, filters=None):
        """Get portfolio performance analysis distinguishing between cash flow and investment performance"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        # Base query for position analysis
        positions_query = """
            SELECT 
                t.symbol,
                t.broker,
                SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as total_bought,
                SUM(CASE WHEN t.transaction_type = 'SELL' THEN t.quantity ELSE 0 END) as total_sold,
                SUM(CASE WHEN t.transaction_type = 'BUY' THEN ABS(t.net_amount) ELSE 0 END) as total_invested,
                SUM(CASE WHEN t.transaction_type = 'SELL' THEN t.net_amount ELSE 0 END) as total_received,
                AVG(CASE WHEN t.transaction_type = 'BUY' THEN t.price ELSE NULL END) as avg_buy_price,
                AVG(CASE WHEN t.transaction_type = 'SELL' THEN t.price ELSE NULL END) as avg_sell_price
            FROM transactions t
            WHERE t.symbol IS NOT NULL AND t.symbol != ''
        """
        
        # Cash flow analysis query
        cash_flow_query = """
            SELECT 
                SUM(CASE WHEN transaction_type = 'SELL' THEN net_amount ELSE 0 END) as total_sales_proceeds,
                SUM(CASE WHEN transaction_type = 'BUY' THEN ABS(net_amount) ELSE 0 END) as total_purchase_cost,
                SUM(CASE WHEN transaction_type = 'DIVIDEND' THEN net_amount ELSE 0 END) as total_dividends,
                SUM(CASE WHEN net_amount > 0 AND symbol IS NULL THEN net_amount ELSE 0 END) as total_deposits,
                SUM(CASE WHEN net_amount < 0 AND symbol IS NULL THEN ABS(net_amount) ELSE 0 END) as total_withdrawals,
                SUM(fee) as total_fees,
                SUM(tax) as total_taxes
            FROM transactions t
            WHERE 1=1
        """
        
        # Apply filters
        params_positions = []
        params_cash_flow = []
        if filters:
            # Handle multi-select broker filter
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list) and len(broker_filter) > 0:
                    short_names = [broker_mapping.get(broker, broker) for broker in broker_filter]
                    placeholders = ','.join(['?' for _ in short_names])
                    positions_query += f" AND t.broker IN ({placeholders})"
                    cash_flow_query += f" AND t.broker IN ({placeholders})"
                    params_positions.extend(short_names)
                    params_cash_flow.extend(short_names)
                elif isinstance(broker_filter, str):
                    short_name = broker_mapping.get(broker_filter, broker_filter)
                    positions_query += " AND t.broker = ?"
                    cash_flow_query += " AND t.broker = ?"
                    params_positions.append(short_name)
                    params_cash_flow.append(short_name)
        
        positions_query += " GROUP BY t.symbol, t.broker ORDER BY t.symbol"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get position analysis
            cursor.execute(positions_query, params_positions)
            positions_data = cursor.fetchall()
            
            # Get cash flow summary  
            cursor.execute(cash_flow_query, params_cash_flow)
            cash_flow_data = cursor.fetchone()
            
            # Process positions
            positions_summary = []
            total_current_positions_cost = 0
            total_realized_gain_loss = 0
            
            for row in positions_data:
                symbol, broker, bought, sold, invested, received, avg_buy, avg_sell = row
                
                remaining_shares = bought - sold
                realized_gain_loss = received - (invested * (sold / bought) if bought > 0 else 0)
                current_position_cost = invested * (remaining_shares / bought) if bought > 0 else 0
                
                total_current_positions_cost += current_position_cost
                total_realized_gain_loss += realized_gain_loss
                
                if remaining_shares > 0 or realized_gain_loss != 0:
                    positions_summary.append({
                        'symbol': symbol,
                        'broker': broker,
                        'total_bought': bought,
                        'total_sold': sold,
                        'remaining_shares': remaining_shares,
                        'total_invested': invested,
                        'total_received': received,
                        'current_position_cost': current_position_cost,
                        'realized_gain_loss': realized_gain_loss,
                        'avg_buy_price': avg_buy or 0,
                        'avg_sell_price': avg_sell or 0
                    })
            
            # Process cash flow data
            (sales_proceeds, purchase_cost, dividends, deposits, withdrawals, fees, taxes) = cash_flow_data or (0, 0, 0, 0, 0, 0, 0)
            
            # Calculate different metrics
            net_cash_flow = sales_proceeds - purchase_cost + dividends - fees - taxes + deposits - withdrawals
            net_invested_capital = purchase_cost + fees - sales_proceeds  # How much capital is currently "tied up" in positions
            portfolio_performance = total_realized_gain_loss  # Realized gains/losses only
            
            return {
                'cash_flow_analysis': {
                    'net_cash_flow': net_cash_flow,
                    'description': 'Net cash movement in/out of account (negative = more money spent than received)',
                    'total_purchase_cost': purchase_cost,
                    'total_sales_proceeds': sales_proceeds,
                    'total_dividends': dividends,
                    'total_deposits': deposits,
                    'total_withdrawals': withdrawals,
                    'total_fees': fees,
                    'total_taxes': taxes
                },
                'portfolio_performance_analysis': {
                    'realized_gain_loss': total_realized_gain_loss,
                    'description': 'Actual gains/losses from completed trades',
                    'current_positions_cost_basis': total_current_positions_cost,
                    'net_invested_capital': net_invested_capital,
                    'portfolio_performance_pct': (total_realized_gain_loss / purchase_cost * 100) if purchase_cost > 0 else 0
                },
                'current_positions': positions_summary,
                'summary': {
                    'total_positions': len([p for p in positions_summary if p['remaining_shares'] > 0]),
                    'total_symbols_traded': len(positions_summary),
                    'explanation': {
                        'net_cash_flow_vs_portfolio_performance': {
                            'cash_flow': 'Shows money in/out of account (like bank statement)',
                            'portfolio_performance': 'Shows investment gains/losses (like investment return)',
                            'key_difference': 'Cash flow includes cost of positions still held; portfolio performance focuses on actual gains/losses'
                        }
                    }
                }
            }
    
    def get_performance_by_year(self):
        """Get performance metrics by year with fees"""
        query = """
            SELECT 
                strftime('%Y', transaction_date) as year,
                SUM(CASE WHEN transaction_type = 'BUY' THEN ABS(net_amount) ELSE 0 END) as purchases,
                SUM(CASE WHEN transaction_type = 'SELL' THEN net_amount ELSE 0 END) as sales,
                SUM(CASE WHEN transaction_type = 'DIVIDEND' THEN net_amount ELSE 0 END) as dividends,
                SUM(fee) as fees,
                SUM(tax) as taxes,
                COUNT(*) as transactions,
                SUM(CASE WHEN net_amount > 0 AND symbol IS NULL THEN net_amount ELSE 0 END) as deposits,
                SUM(CASE WHEN net_amount < 0 AND symbol IS NULL THEN ABS(net_amount) ELSE 0 END) as withdrawals
            FROM transactions
            WHERE transaction_date IS NOT NULL
            GROUP BY strftime('%Y', transaction_date)
            ORDER BY year DESC
        """
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            results = []
            for row in cursor.fetchall():
                data = dict(zip([col[0] for col in cursor.description], row))
                data['realized_gain_loss'] = (data['sales'] or 0) - (data['purchases'] or 0)
                data['net_after_fees'] = data['realized_gain_loss'] - (data['fees'] or 0) - (data['taxes'] or 0)
                results.append(data)
            return results
    
    def get_data_freshness_status(self):
        """Get data freshness status for all brokers"""
        from scripts.data_freshness_monitor import DataFreshnessMonitor
        monitor = DataFreshnessMonitor(self.db_path)
        return monitor.get_broker_freshness_status()
    
    def generate_data_freshness_report(self):
        """Generate comprehensive data freshness report"""
        from scripts.data_freshness_monitor import DataFreshnessMonitor
        monitor = DataFreshnessMonitor(self.db_path)
        return monitor.generate_freshness_report()

# Initialize API
portfolio_api = PortfolioAPI()

# Note: Removed automatic CSV loading to prevent duplicate processing
# Use the API endpoint /api/process-all-statements to process files manually









@app.route('/')
def index():
    """Main dashboard page"""
    db_info = portfolio_api.get_database_info()
    return render_template('index.html', db_info=db_info)

@app.route('/api/accounts')
def api_accounts():
    """Get all accounts"""
    accounts = portfolio_api.get_accounts()
    return jsonify(accounts)

@app.route('/api/brokers')
def api_brokers():
    """Get all brokers with account separation"""
    broker_data = portfolio_api.get_brokers()
    # Return both broker names and keys for frontend processing
    return jsonify(broker_data)

@app.route('/api/symbols')
def api_symbols():
    """Get all symbols, optionally filtered by broker"""
    broker_filters = request.args.getlist('broker')  # Support multiple brokers
    # Remove empty values
    broker_filters = [b for b in broker_filters if b]
    
    if not broker_filters:
        broker_filters = None
        
    symbols = portfolio_api.get_symbols(broker_filters)
    return jsonify(symbols)

@app.route('/api/currencies')
def api_currencies():
    """Get all currencies"""
    currencies = portfolio_api.get_currencies()
    return jsonify(currencies)

@app.route('/api/transactions')
def api_transactions():
    """Get filtered transactions"""
    filters = {
        'account_id': request.args.get('account_id'),
        'institution': request.args.get('institution'),
        'broker': request.args.getlist('broker'),  # Support multiple brokers
        'symbol': request.args.getlist('symbol'),  # Support multiple symbols
        'transaction_type': request.args.getlist('transaction_type'),  # Support multiple transaction types
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'year': request.args.get('year'),
        'currency': request.args.get('currency'),
    }
    
    # Remove None values and empty lists
    filters = {k: v for k, v in filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
    
    transactions = portfolio_api.get_transactions(filters)
    return jsonify(transactions)

@app.route('/api/summary')
def api_summary():
    """Get portfolio summary"""
    filters = {
        'broker': request.args.getlist('broker'),  # Support multiple brokers
        'symbol': request.args.getlist('symbol'),  # Support multiple symbols  
        'transaction_type': request.args.getlist('transaction_type'),  # Support multiple transaction types
        'year': request.args.get('year'),
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
    }
    
    # Remove None values and empty lists
    filters = {k: v for k, v in filters.items() if v and (not isinstance(v, list) or len(v) > 0)}
    
    summary = portfolio_api.get_portfolio_summary(filters)
    return jsonify(summary)

@app.route('/api/performance')
def api_performance():
    """Get performance by year"""
    performance = portfolio_api.get_performance_by_year()
    return jsonify(performance)

@app.route('/api/load-csv', methods=['POST'])
def api_load_csv():
    """Load CSV data endpoint"""
    csv_path = request.json.get('csv_path')
    if not csv_path or not os.path.exists(csv_path):
        return jsonify({'error': 'CSV file not found'}), 400
    
    success = portfolio_api.load_csv_data(csv_path)
    if success:
        return jsonify({'message': 'CSV data loaded successfully'})
    else:
        return jsonify({'error': 'Failed to load CSV data'}), 500

@app.route('/api/process-all-statements', methods=['POST'])
def process_all_statements():
    """Process all broker statements (PDF and CSV)"""
    try:
        api = PortfolioAPI()
        success = api.process_all_broker_statements()
        
        if success:
            # Get processing summary
            with sqlite3.connect(api.db_path) as conn:
                cursor = conn.cursor()
                
                # Get broker counts
                cursor.execute("SELECT broker, COUNT(*) FROM accounts GROUP BY broker")
                broker_counts = dict(cursor.fetchall())
                
                # Get transaction counts
                cursor.execute("SELECT broker, COUNT(*) FROM transactions GROUP BY broker")
                transaction_counts = dict(cursor.fetchall())
                
                # Get total counts
                cursor.execute("SELECT COUNT(*) FROM accounts")
                total_accounts = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM transactions")
                total_transactions = cursor.fetchone()[0]
                
                return jsonify({
                    'success': True,
                    'message': 'All statements processed successfully',
                    'summary': {
                        'total_accounts': total_accounts,
                        'total_transactions': total_transactions,
                        'brokers': {
                            'accounts': broker_counts,
                            'transactions': transaction_counts
                        }
                    }
                })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to process statements'
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/broker-summary')
def broker_summary():
    """Get summary by broker"""
    try:
        api = PortfolioAPI()
        
        with sqlite3.connect(api.db_path) as conn:
            cursor = conn.cursor()
            
            # Get account summary by broker
            cursor.execute("""
                SELECT a.broker, a.institution, COUNT(DISTINCT a.account_id) as account_count,
                       COUNT(DISTINCT t.id) as transaction_count,
                       SUM(CASE WHEN t.transaction_type LIKE '%買%' OR t.transaction_type LIKE '%Buy%' THEN 1 ELSE 0 END) as buy_count,
                       SUM(CASE WHEN t.transaction_type LIKE '%賣%' OR t.transaction_type LIKE '%Sell%' THEN 1 ELSE 0 END) as sell_count,
                       SUM(t.net_amount) as total_net_amount
                FROM accounts a
                LEFT JOIN transactions t ON a.account_id = t.account_id
                GROUP BY a.broker, a.institution
                ORDER BY a.broker
            """)
            
            broker_summary = []
            for row in cursor.fetchall():
                broker_summary.append({
                    'broker': row[0],
                    'institution': row[1],
                    'account_count': row[2],
                    'transaction_count': row[3],
                    'buy_transactions': row[4],
                    'sell_transactions': row[5],
                    'total_net_cash_flow': row[6] or 0,  # Clarified name
                    'net_amount_explanation': 'This represents net cash flow (money in/out), not portfolio performance. Negative values indicate more money spent on purchases than received from sales.'
                })
            
            return jsonify({
                'success': True,
                'broker_summary': broker_summary
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/portfolio-performance')
def portfolio_performance():
    """Get portfolio performance analysis distinguishing between cash flow and investment returns"""
    try:
        filters = request.args.to_dict(flat=False)
        # Convert single-item lists to strings for backward compatibility
        for key, value in filters.items():
            if isinstance(value, list) and len(value) == 1:
                filters[key] = value[0]
        
        api = PortfolioAPI()
        analysis = api.get_portfolio_performance_analysis(filters)
        
        return jsonify({
            'success': True,
            'portfolio_performance': analysis
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/data-freshness')
def api_data_freshness():
    """Get data freshness status for all brokers"""
    try:
        status = portfolio_api.get_data_freshness_status()
        return jsonify({
            'success': True,
            'freshness_status': status
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/data-freshness/report')
def api_data_freshness_report():
    """Get comprehensive data freshness report"""
    try:
        report = portfolio_api.generate_data_freshness_report()
        return jsonify({
            'success': True,
            'report': report
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/unrealized-pnl')
def api_unrealized_pnl():
    """Calculate unrealized P&L for current holdings"""
    try:
        # Get filters from request parameters
        filters = {}
        if request.args.get('broker'):
            broker_list = request.args.getlist('broker')
            filters['broker'] = broker_list if len(broker_list) > 1 else broker_list[0] if broker_list else None
        
        print(f"Debug - unrealized-pnl filters: {filters}")
        result = portfolio_api.calculate_unrealized_pnl(filters)
        print(f"Debug - unrealized-pnl result: {result}")
        return jsonify(result)
    except Exception as e:
        print(f"Debug - unrealized-pnl error: {e}")
        return jsonify({
            'error': str(e),
            'unrealized_pnl': 0,
            'total_market_value': 0,
            'total_cost_basis': 0,
            'holdings_count': 0,
            'price_fetch_errors': []
        }), 500

@app.route('/api/unrealized-pnl-enhanced')
def api_unrealized_pnl_enhanced():
    """Calculate enhanced unrealized P&L with comprehensive forex and symbol mapping"""
    try:
        # Get filters from request parameters
        filters = {}
        if request.args.get('broker'):
            broker_list = request.args.getlist('broker')
            filters['broker'] = broker_list if len(broker_list) > 1 else broker_list[0] if broker_list else None
        
        # Get base currency (default to TWD)
        base_currency = request.args.get('base_currency', 'TWD')
        
        print(f"Debug - enhanced unrealized-pnl filters: {filters}, base_currency: {base_currency}")
        result = portfolio_api.calculate_enhanced_unrealized_pnl(filters, base_currency)
        print(f"Debug - enhanced unrealized-pnl result summary: unrealized_pnl={result.get('unrealized_pnl')}, errors={len(result.get('price_fetch_errors', []))}")
        return jsonify(result)
    except Exception as e:
        print(f"Debug - enhanced unrealized-pnl error: {e}")
        return jsonify({
            'error': str(e),
            'unrealized_pnl': 0,
            'total_market_value': 0,
            'total_cost_basis': 0,
            'holdings_count': 0,
            'price_fetch_errors': [],
            'forex_rates_used': {},
            'base_currency': 'TWD'
        }), 500

@app.route('/api/system-status')
def api_system_status():
    """Get system status including Yahoo Finance availability"""
    try:
        api = PortfolioAPI()
        
        # Check Yahoo Finance availability
        yahoo_available = api._check_yahoo_finance_availability()
        
        # Get cache status
        cache_valid = api._is_cache_valid()
        cache_info = {
            'valid': cache_valid,
            'timestamp': api._cache_timestamp,
            'duration': api._cache_duration,
            'forex_entries': len(api._forex_cache),
            'stock_price_entries': len(api._stock_price_cache)
        }
        
        # Get request throttling info
        throttling_info = {
            'request_count': api._request_count,
            'last_request_time': api._last_request_time,
            'yahoo_finance_available': api._yahoo_finance_available,
            'last_yahoo_check': api._last_yahoo_check
        }
        
        return jsonify({
            'success': True,
            'yahoo_finance_available': yahoo_available,
            'cache_info': cache_info,
            'throttling_info': throttling_info,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/forex-rates')
def api_forex_rates():
    """Get current forex rates"""
    try:
        api = PortfolioAPI()
        
        # Get commonly used forex rates
        rates = {
            'USDTWD': api.get_forex_rate('USD', 'TWD'),
            'TWDUSD': api.get_forex_rate('TWD', 'USD')
        }
        
        return jsonify({
            'rates': rates,
            'timestamp': datetime.now().isoformat(),
            'base_currency': 'Multiple'
        })
    except Exception as e:
        print(f"Debug - forex-rates error: {e}")
        return jsonify({
            'error': str(e),
            'rates': {},
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/symbol-mapping')
def api_symbol_mapping():
    """Get symbol mapping information for debugging"""
    try:
        api = PortfolioAPI()
        
        # Get test symbol mappings
        test_symbols = ['台積電', '2330', 'AAPL', '聯發科', '2454']
        mappings = {}
        
        for symbol in test_symbols:
            yahoo_symbol = api._get_yahoo_symbol(symbol, None)
            yahoo_symbol_with_broker = api._get_yahoo_symbol(symbol, 'CATHAY')
            mappings[symbol] = {
                'yahoo_symbol': yahoo_symbol,
                'yahoo_symbol_with_cathay': yahoo_symbol_with_broker
            }
        
        return jsonify({
            'mappings': mappings,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Debug - symbol-mapping error: {e}")
        return jsonify({
            'error': str(e),
            'mappings': {}
        }), 500

if __name__ == '__main__':
    # Ensure templates directory exists
    os.makedirs('templates', exist_ok=True)
    
    port = int(os.environ.get('FLASK_PORT', '5001'))
    app.run(debug=True, port=port)
