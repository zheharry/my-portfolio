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
        """Add currency columns to existing tables and populate based on broker"""
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
            
            # Check and add currency column to positions table
            cursor.execute("PRAGMA table_info(positions)")
            positions_columns = [row[1] for row in cursor.fetchall()]
            
            if 'currency' not in positions_columns:
                cursor.execute("ALTER TABLE positions ADD COLUMN currency TEXT DEFAULT 'USD'")
                print("Added currency column to positions table")
            
            # Update existing records with appropriate currency based on broker
            # CATHAY/國泰證券 → NTD, SCHWAB/TDA → USD
            cursor.execute("""
                UPDATE accounts 
                SET currency = CASE 
                    WHEN broker LIKE '%CATHAY%' OR broker LIKE '%國泰證券%' THEN 'NTD'
                    WHEN broker LIKE '%SCHWAB%' OR broker LIKE '%TDA%' THEN 'USD'
                    ELSE 'USD'
                END
                WHERE currency = 'USD' OR currency IS NULL
            """)
            
            cursor.execute("""
                UPDATE transactions 
                SET currency = CASE 
                    WHEN broker LIKE '%CATHAY%' OR broker LIKE '%國泰證券%' THEN 'NTD'
                    WHEN broker LIKE '%SCHWAB%' OR broker LIKE '%TDA%' THEN 'USD'
                    ELSE 'USD'
                END
                WHERE currency = 'USD' OR currency IS NULL
            """)
            
            cursor.execute("""
                UPDATE positions 
                SET currency = CASE 
                    WHEN broker LIKE '%CATHAY%' OR broker LIKE '%國泰證券%' THEN 'NTD'
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
        """Get all unique brokers with normalized names"""
        # Mapping from short names to full names
        broker_mapping = {
            'CATHAY': '國泰證券',
            'SCHWAB': 'Charles Schwab', 
            'TDA': 'TD Ameritrade'
        }
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT broker FROM transactions WHERE broker IS NOT NULL ORDER BY broker")
            raw_brokers = [row[0] for row in cursor.fetchall()]
            
            # Convert to full names and remove duplicates
            full_names = []
            for broker in raw_brokers:
                full_name = broker_mapping.get(broker, broker)
                if full_name not in full_names:
                    full_names.append(full_name)
            
            return sorted(full_names)
    
    def get_symbols(self):
        """Get all unique symbols"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT symbol FROM transactions WHERE symbol IS NOT NULL ORDER BY symbol")
            return [row[0] for row in cursor.fetchall()]
    
    def get_currencies(self):
        """Get all unique currencies"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT currency FROM transactions WHERE currency IS NOT NULL ORDER BY currency")
            return [row[0] for row in cursor.fetchall()]
    
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
            
            # Handle multi-select broker filter
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list):
                    if len(broker_filter) > 0:
                        # Map full names to short names for all brokers
                        short_names = []
                        full_names = []
                        for broker in broker_filter:
                            short_name = broker_mapping.get(broker, broker)
                            short_names.append(short_name)
                            full_names.append(broker)
                        
                        placeholders = ','.join(['?' for _ in range(len(short_names) * 3)])
                        query += f" AND (t.broker IN ({','.join(['?' for _ in short_names])}) OR a.broker IN ({','.join(['?' for _ in short_names])}) OR a.institution IN ({','.join(['?' for _ in full_names])}))"
                        params.extend(short_names + short_names + full_names)
                else:
                    # Single broker (backward compatibility)
                    short_name = broker_mapping.get(broker_filter, broker_filter)
                    query += " AND (t.broker = ? OR a.broker = ? OR a.institution = ?)"
                    params.extend([short_name, short_name, broker_filter])
            
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
            
            if filters.get('currency'):
                query += " AND t.currency = ?"
                params.append(filters['currency'])
        
        query += " ORDER BY t.transaction_date DESC, t.id DESC"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(zip([col[0] for col in cursor.description], row)) 
                   for row in cursor.fetchall()]
    
    def get_portfolio_summary(self, filters=None):
        """Get enhanced portfolio summary with fees and multi-select support"""
        # Broker mapping for filtering
        broker_mapping = {
            '國泰證券': 'CATHAY',
            'Charles Schwab': 'SCHWAB', 
            'TD Ameritrade': 'TDA'
        }
        
        query = """
            SELECT 
                SUM(CASE WHEN transaction_type = '賣出' THEN net_amount ELSE 0 END) as total_sales,
                SUM(CASE WHEN transaction_type = '買進' THEN ABS(net_amount) ELSE 0 END) as total_purchases,
                SUM(CASE WHEN transaction_type = 'DIVIDEND' THEN net_amount ELSE 0 END) as total_dividends,
                SUM(fee) as total_fees,
                SUM(tax) as total_taxes,
                COUNT(*) as total_transactions,
                SUM(CASE WHEN net_amount > 0 AND symbol IS NULL THEN net_amount ELSE 0 END) as total_deposits,
                SUM(CASE WHEN net_amount < 0 AND symbol IS NULL THEN ABS(net_amount) ELSE 0 END) as total_withdrawals
            FROM transactions t
            WHERE 1=1
        """
        
        params = []
        if filters:
            # Handle multi-select broker filter
            if filters.get('broker'):
                broker_filter = filters['broker']
                if isinstance(broker_filter, list):
                    if len(broker_filter) > 0:
                        # Map full names to short names for all brokers
                        short_names = [broker_mapping.get(broker, broker) for broker in broker_filter]
                        placeholders = ','.join(['?' for _ in short_names])
                        query += f" AND t.broker IN ({placeholders})"
                        params.extend(short_names)
                else:
                    # Single broker (backward compatibility)
                    short_name = broker_mapping.get(broker_filter, broker_filter)
                    query += " AND t.broker = ?"
                    params.append(short_name)
            
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
            
            if filters.get('currency'):
                query += " AND t.currency = ?"
                params.append(filters['currency'])
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = dict(zip([col[0] for col in cursor.description], cursor.fetchone()))
            
            # Calculate realized gain/loss
            result['realized_gain_loss'] = (result['total_sales'] or 0) - (result['total_purchases'] or 0)
            result['net_after_fees'] = result['realized_gain_loss'] - (result['total_fees'] or 0) - (result['total_taxes'] or 0)
            
            return result
    
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
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as total_bought,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.quantity ELSE 0 END) as total_sold,
                SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN ABS(t.net_amount) ELSE 0 END) as total_invested,
                SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.net_amount ELSE 0 END) as total_received,
                AVG(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.price ELSE NULL END) as avg_buy_price,
                AVG(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN t.price ELSE NULL END) as avg_sell_price
            FROM transactions t
            WHERE t.symbol IS NOT NULL AND t.symbol != ''
        """
        
        # Cash flow analysis query
        cash_flow_query = """
            SELECT 
                SUM(CASE WHEN transaction_type = '賣出' OR transaction_type = 'SELL' THEN net_amount ELSE 0 END) as total_sales_proceeds,
                SUM(CASE WHEN transaction_type = '買進' OR transaction_type = 'BUY' THEN ABS(net_amount) ELSE 0 END) as total_purchase_cost,
                SUM(CASE WHEN transaction_type = 'DIVIDEND' THEN net_amount ELSE 0 END) as total_dividends,
                SUM(CASE WHEN net_amount > 0 AND symbol IS NULL THEN net_amount ELSE 0 END) as total_deposits,
                SUM(CASE WHEN net_amount < 0 AND symbol IS NULL THEN ABS(net_amount) ELSE 0 END) as total_withdrawals,
                SUM(fee) as total_fees,
                SUM(tax) as total_taxes
            FROM transactions t
            WHERE 1=1
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
                    cash_flow_query += f" AND t.broker IN ({placeholders})"
                    params.extend(short_names)
                elif isinstance(broker_filter, str):
                    short_name = broker_mapping.get(broker_filter, broker_filter)
                    positions_query += " AND t.broker = ?"
                    cash_flow_query += " AND t.broker = ?"
                    params.extend([short_name, short_name])
        
        positions_query += " GROUP BY t.symbol, t.broker ORDER BY t.symbol"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get position analysis
            cursor.execute(positions_query, params)
            positions_data = cursor.fetchall()
            
            # Get cash flow summary  
            cursor.execute(cash_flow_query, params)
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
                SUM(CASE WHEN transaction_type = '買進' THEN ABS(net_amount) ELSE 0 END) as purchases,
                SUM(CASE WHEN transaction_type = '賣出' THEN net_amount ELSE 0 END) as sales,
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
    return render_template('index.html')

@app.route('/api/accounts')
def api_accounts():
    """Get all accounts"""
    accounts = portfolio_api.get_accounts()
    return jsonify(accounts)

@app.route('/api/brokers')
def api_brokers():
    """Get all brokers"""
    brokers = portfolio_api.get_brokers()
    return jsonify(brokers)

@app.route('/api/symbols')
def api_symbols():
    """Get all symbols"""
    symbols = portfolio_api.get_symbols()
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
        'currency': request.args.get('currency'),
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

if __name__ == '__main__':
    # Ensure templates directory exists
    os.makedirs('templates', exist_ok=True)
    
    app.run(debug=True, port=5000)