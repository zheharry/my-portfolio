#!/usr/bin/env python3
"""
Enhanced Multi-Broker Portfolio Parser
Extends the existing portfolio analysis to support TD Ameritrade, Charles Schwab, and 國泰證券
"""

import os
import sqlite3
import logging
import json
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd

class MultiBrokerPortfolioParser:
    def __init__(self, statements_dir: str = "Statements", db_path: str = "data/database/portfolio.db"):
        self.statements_dir = Path(statements_dir)
        self.db_path = Path(db_path)
        self.setup_logging()
        self.setup_database()
        
        # Broker configuration
        self.broker_configs = {
            'TDA': {
                'name': 'TD Ameritrade (Historical)',
                'account_patterns': [r'Account\s*Number:\s*(\d{3}-\d{6})', r'Account:\s*(\d{3}-\d{6})'],
                'file_pattern': r'TDA - Brokerage Statement_.*\.PDF',
                'default_account': 'TDA-HISTORICAL-001',
                'note': 'Legacy TDA statements before Schwab merger'
            },
            'SCHWAB': {
                'name': 'Charles Schwab (Post-Merger)',
                'account_patterns': [r'(\d{4}-\d{4})\s+[A-Za-z]+\s+\d+[-]\d+,\s+\d{4}'],
                'file_pattern': r'Brokerage Statement_.*\.PDF',
                'default_accounts': ['SCHWAB-ACCT-001', 'SCHWAB-ACCT-002'],  # Generic account identifiers
                'note': 'Modern Schwab statements after TDA merger'
            },
            'CATHAY': {
                'name': '國泰證券',
                'account_patterns': [],  # CSV based
                'file_pattern': r'.*\.csv',
                'default_account': 'CATHAY-001'
            }
        }
        
        # Chinese stock name to ticker symbol mapping (for CATHAY broker only)
        self.chinese_to_ticker = {
            '台積電': '2330.TW',
            '富邦台50': '006208.TW',
            '聯發科': '2454.TW', 
            '中鋼': '2002.TW'
        }
    
    def map_chinese_symbol(self, symbol: str, broker: str) -> Tuple[str, Optional[str]]:
        """
        Map Chinese stock names to ticker symbols for CATHAY broker
        Returns (ticker_symbol, chinese_name) tuple
        For non-CATHAY brokers, returns (symbol, None)
        """
        if broker == 'CATHAY' and symbol in self.chinese_to_ticker:
            return self.chinese_to_ticker[symbol], symbol
        return symbol, None
    
    def setup_logging(self):
        """Configure logging"""
        log_dir = Path("outputs/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f"portfolio_parser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_database(self):
        """Create enhanced database schema for multi-broker support"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Basic accounts table (compatible with existing)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    institution TEXT,
                    broker TEXT,
                    account_type TEXT,
                    account_holder TEXT,
                    created_date TEXT
                )
            """)
            
            # Basic transactions table (compatible with existing)
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
                    split_ratio TEXT DEFAULT NULL,
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                )
            """)
            
            # Add split_ratio column if it doesn't exist (for backward compatibility)
            try:
                cursor.execute("ALTER TABLE transactions ADD COLUMN split_ratio TEXT DEFAULT NULL")
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Enhanced positions table
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
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                )
            """)
            
            # Account balances table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS account_balances (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT,
                    statement_date TEXT,
                    cash_balance REAL DEFAULT 0,
                    total_investments REAL DEFAULT 0,
                    total_account_value REAL DEFAULT 0,
                    broker TEXT,
                    source_file TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                )
            """)
            
            # Processing log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT,
                    file_type TEXT,
                    broker TEXT,
                    status TEXT,
                    records_processed INTEGER DEFAULT 0,
                    error_message TEXT,
                    processing_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            self.logger.info("Database schema initialized successfully")
    
    def extract_text_from_pdf(self, pdf_path: Path) -> str:
        """Extract text from PDF using pdftotext"""
        try:
            result = subprocess.run(
                ['pdftotext', '-layout', str(pdf_path), '-'],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            self.logger.error(f"pdftotext failed for {pdf_path}: {e}")
            return ""
        except FileNotFoundError:
            self.logger.error("pdftotext not found. Install poppler-utils: brew install poppler")
            return ""
        except Exception as e:
            self.logger.error(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def identify_broker_from_file(self, file_path: Path) -> str:
        """Identify broker from file name and content"""
        file_name = file_path.name
        
        # TDA files (historical) - prefixed with "TDA"
        if 'TDA' in file_name or 'TD Ameritrade' in file_name:
            return 'TDA'
        # Modern Schwab files (post-merger) - "Brokerage Statement_" format
        elif file_name.startswith('Brokerage Statement_') and file_path.suffix.upper() == '.PDF':
            return 'SCHWAB'
        # Cathay Securities CSV files
        elif file_path.suffix.lower() == '.csv':
            return 'CATHAY'
        else:
            self.logger.warning(f"Could not identify broker for {file_name}")
            return 'UNKNOWN'
    
    def calculate_net_amount(self, transaction: Dict) -> None:
        """
        Calculate net_amount for a transaction based on amount, fees, and taxes.
        Net Amount = Amount - Fees - Taxes
        
        For TDA and Schwab transactions, this provides the actual net impact on the account.
        """
        amount = transaction.get('amount', 0)
        fee = transaction.get('fee', 0)
        tax = transaction.get('tax', 0)
        
        # Calculate net amount
        transaction['net_amount'] = amount - abs(fee) - abs(tax)
    
    def standardize_transaction_amount(self, transaction: Dict) -> None:
        """
        Standardize transaction amounts based on transaction type from a cash flow perspective:
        - Negative amounts (cash going out): BUY, WITHDRAWAL, TAX
        - Positive amounts (cash coming in): SELL, DEPOSIT, DIVIDEND, INTEREST, JOURNAL, OTHER
        """
        transaction_type = transaction.get('transaction_type', '')
        amount = transaction.get('amount', 0)
        
        # Define transaction types that should have negative amounts (cash going out)
        negative_types = {'BUY', 'WITHDRAWAL', 'TAX'}
        
        # Define transaction types that should have positive amounts (cash coming in)
        positive_types = {'SELL', 'DEPOSIT', 'DIVIDEND', 'INTEREST', 'JOURNAL', 'OTHER'}
        
        if transaction_type in negative_types:
            transaction['amount'] = -abs(amount)
        elif transaction_type in positive_types:
            transaction['amount'] = abs(amount)
        # For unknown types, leave amount as is but log it
        elif transaction_type and amount != 0:
            self.logger.warning(f"Unknown transaction type '{transaction_type}' - amount not standardized")
        
        # Always calculate net_amount after standardizing the amount
        self.calculate_net_amount(transaction)
    
    def parse_schwab_statement(self, text: str, file_path: Path) -> Dict:
        """Parse modern Charles Schwab statement data (post-TDA merger)"""
        data = {
            'account_info': {},
            'transactions': [],
            'positions': [],
            'balances': {}
        }
        
        # Extract account information - new Schwab format
        # Look for "Schwab One International® Account of" and account number
        account_match = re.search(r'Account Number\s+Statement Period\s*([A-Z\s]+)\s+(\d{4}-\d{4})', text)
        if not account_match:
            # Fallback pattern
            account_match = re.search(r'(\d{4}-\d{4})\s+[A-Za-z]+\s+\d+[-]\d+,\s+\d{4}', text)
        
        if account_match:
            if len(account_match.groups()) == 2:
                account_holder = account_match.group(1).strip()
                account_number = account_match.group(2)
            else:
                account_number = account_match.group(1) if len(account_match.groups()) >= 1 else account_match.group(0)
                account_holder = 'Account Holder'  # Placeholder - extracted from statements when available
            
            data['account_info']['account_id'] = f"SCHWAB-{account_number}"
            data['account_info']['account_holder'] = account_holder
        else:
            # Use generic account identification based on broker type and file position
            # This approach doesn't expose actual account numbers
            broker_prefix = "SCHWAB"
            
            # Generate a consistent but generic identifier based on filename characteristics
            # without exposing actual account numbers
            if any(suffix in file_path.name for suffix in ['_088', '_563']):
                # Legacy detection for existing files - map to generic patterns
                if '_088' in file_path.name:
                    data['account_info']['account_id'] = f'{broker_prefix}-ACCT-001'
                    data['account_info']['account_note'] = 'Account with stock trading activity'
                elif '_563' in file_path.name:
                    data['account_info']['account_id'] = f'{broker_prefix}-ACCT-002' 
                    data['account_info']['account_note'] = 'Account with simple transactions'
            else:
                # For any other files, use timestamp-based generic ID
                file_date = re.search(r'(\d{4}-\d{2}-\d{2})', file_path.name)
                if file_date:
                    date_hash = hash(file_date.group(1)) % 1000
                    data['account_info']['account_id'] = f'{broker_prefix}-ACCT-{date_hash:03d}'
                else:
                    data['account_info']['account_id'] = f'{broker_prefix}-UNKNOWN'
            data['account_info']['account_holder'] = 'Account Holder'  # Placeholder - customize for your needs
        
        data['account_info']['institution'] = 'Charles Schwab'
        data['account_info']['account_type'] = 'Schwab One International'
        data['account_info']['broker'] = 'SCHWAB'
        
        # Extract statement period
        period_match = re.search(r'Statement Period\s*([A-Za-z]+\s+\d+[-]\d+,\s+\d{4})', text)
        if period_match:
            period_str = period_match.group(1)
            # Extract end date as statement date
            end_date_match = re.search(r'([A-Za-z]+\s+\d+),\s+(\d{4})$', period_str)
            if end_date_match:
                month_day = end_date_match.group(1)
                year = end_date_match.group(2)
                try:
                    # Convert to standard format
                    date_obj = datetime.strptime(f"{month_day}, {year}", '%B %d, %Y')
                    data['account_info']['statement_date'] = date_obj.strftime('%Y-%m-%d')
                except:
                    # Fallback to filename date
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file_path.name)
                    if date_match:
                        data['account_info']['statement_date'] = date_match.group(1)
        
        # Extract account balances from Account Summary section
        summary_section = re.search(r'Account Summary(.*?)(?:Transaction Details|Manage Your Account)', text, re.DOTALL)
        if summary_section:
            summary_text = summary_section.group(1)
            
            # Extract ending account value
            ending_match = re.search(r'Ending Account Value.*?\$([0-9,]+\.?\d*)', summary_text)
            if ending_match:
                data['balances']['total_account_value'] = float(ending_match.group(1).replace(',', ''))
            
            # Extract beginning account value  
            beginning_match = re.search(r'Beginning Account Value.*?\$([0-9,]+\.?\d*)', summary_text)
            if beginning_match:
                data['balances']['beginning_account_value'] = float(beginning_match.group(1).replace(',', ''))
            
            # Extract deposits, withdrawals, etc.
            deposits_match = re.search(r'Deposits\s*([0-9,]+\.?\d*)', summary_text)
            if deposits_match:
                data['balances']['deposits'] = float(deposits_match.group(1).replace(',', ''))
            
            withdrawals_match = re.search(r'Withdrawals\s*\(([0-9,]+\.?\d*)\)', summary_text)
            if withdrawals_match:
                data['balances']['withdrawals'] = -float(withdrawals_match.group(1).replace(',', ''))
        
        # Parse transactions - new Schwab format
        data['transactions'] = self.parse_modern_schwab_transactions(text, file_path)
        
        return data
    
    def parse_modern_schwab_transactions(self, text: str, file_path: Path) -> List[Dict]:
        """Parse transactions from modern Schwab Transaction Details section"""
        transactions = []
        
        # Find Transaction Details section
        trans_section = re.search(r'Transaction Details(.*?)(?:Total Transactions|Page \d+|$)', text, re.DOTALL)
        if not trans_section:
            return transactions
        
        trans_text = trans_section.group(1)
        lines = trans_text.split('\n')
        
        # Extract year from statement period in text
        current_year = "2025"  # Default fallback
        year_match = re.search(r'Statement Period.*(\d{4})', text)
        if year_match:
            current_year = year_match.group(1)
        else:
            # Fallback: extract year from filename
            filename = file_path.name if hasattr(file_path, 'name') else str(file_path)
            filename_year_match = re.search(r'(\d{4})', filename)
            if filename_year_match:
                current_year = filename_year_match.group(1)
        
        # Detect transaction format by looking at the structure
        # Look for indicators of detailed stock transaction format
        has_symbol_column = bool(
            re.search(r'Symbol/', trans_text, re.IGNORECASE) or 
            re.search(r'Symbol\s', trans_text, re.IGNORECASE) or
            re.search(r'CUSIP', trans_text, re.IGNORECASE) or
            re.search(r'Quantity', trans_text, re.IGNORECASE) or
            re.search(r'Price', trans_text, re.IGNORECASE) or
            # Look for stock symbols in transaction data (3-5 letter codes)
            re.search(r'\b[A-Z]{3,5}\b.*\b[A-Z]{3,5}\b', trans_text) or
            # Look for buy/sell transaction patterns
            re.search(r'Sold|Bought|Sale|Purchase', trans_text, re.IGNORECASE) or
            # Look for stock transaction amounts with parentheses (indicating quantity)
            re.search(r'\(\d+(\.\d+)?\)', trans_text)
        )
        
        # Add debug logging
        self.logger.info(f"Transaction format detection - has_symbol_column: {has_symbol_column}")
        if has_symbol_column:
            self.logger.info("Using detailed transaction parser (for buy/sell transactions)")
        else:
            self.logger.info("Using simple transaction parser (for interest/dividend only)")
        
        if has_symbol_column:
            # Complex format with detailed stock transactions
            return self.parse_schwab_detailed_transactions(trans_text, current_year)
        else:
            # Simple format with basic transactions
            return self.parse_schwab_simple_transactions(trans_text, current_year)
    
    def parse_schwab_simple_transactions(self, trans_text: str, current_year: str) -> List[Dict]:
        """Parse simple Schwab transaction format (used for accounts with basic transactions)
        
        This method handles statement formats that contain simpler transaction patterns,
        typically for accounts that primarily hold cash and simple investment transactions.
        Account identification is done generically to protect privacy.
        """
        transactions = []
        lines = trans_text.split('\n')
        
        i = 0
        current_date = None  # Track the current transaction date
        
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip empty lines and headers
            if not line or 'Date' in line or 'Category' in line or 'Total Transactions' in line:
                i += 1
                continue
            
            # Look for date pattern: MM/DD at start of line
            date_match = re.match(r'^(\d{1,2}/\d{1,2})\s+(.+)', line)
            if date_match:
                date_str = date_match.group(1)
                rest_of_line = date_match.group(2).strip()
                
                # Format date as YYYY-MM-DD
                date_parts = date_str.split('/')
                if len(date_parts) == 2:
                    month, day = date_parts
                    current_date = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"
                    
                    # Validation: Check if the date is in the future (beyond today)
                    from datetime import datetime
                    try:
                        parsed_date = datetime.strptime(current_date, '%Y-%m-%d')
                        today = datetime.now()
                        if parsed_date > today:
                            prev_year = str(int(current_year) - 1)
                            current_date = f"{prev_year}-{month.zfill(2)}-{day.zfill(2)}"
                    except ValueError:
                        pass
                else:
                    current_date = f"{current_year}-01-01"
                
                # Parse the transaction
                transaction = self.parse_simple_transaction_line(rest_of_line, current_date)
                if transaction:
                    transactions.append(transaction)
                
                i += 1
                
            elif current_date and line.strip():
                # This is a continuation line without a date - use the current date
                # Look for transaction type indicators at the start
                if any(indicator in line for indicator in ['Deposit', 'Interest', 'Withdrawal', 'Dividend']):
                    transaction = self.parse_simple_transaction_line(line.strip(), current_date)
                    if transaction:
                        transactions.append(transaction)
                i += 1
            else:
                i += 1
        
        return transactions
    
    def parse_simple_transaction_line(self, line: str, transaction_date: str) -> Dict:
        """Parse a single transaction line from simple Schwab format"""
        transaction = {
            'transaction_date': transaction_date,
            'transaction_type': 'OTHER',
            'description': line,
            'symbol': '',
            'quantity': 0,
            'price': 0,
            'amount': 0,
            'fee': 0,
            'tax': 0,
            'realized_gain_loss': 0
        }
        
        # Identify transaction type
        if 'Withdrawal' in line:
            transaction['transaction_type'] = 'WITHDRAWAL'
        elif 'Deposit' in line:
            transaction['transaction_type'] = 'DEPOSIT'
        elif 'Interest' in line and 'Credit Interest' in line:
            transaction['transaction_type'] = 'INTEREST'
        elif 'Interest' in line and 'NRA Tax' in line:
            transaction['transaction_type'] = 'TAX'
        elif 'Dividend' in line:
            transaction['transaction_type'] = 'DIVIDEND'
        
        # Extract amount from the line - look for patterns like (1,690.79) or 6,000.00
        amount_match = re.search(r'\(([\d,]+\.?\d*)\)', line)
        if amount_match:
            # Amount in parentheses - negative
            extracted_amount = -float(amount_match.group(1).replace(',', ''))
        else:
            # Look for positive amount at the end
            amount_match = re.search(r'([\d,]+\.?\d*)$', line)
            if amount_match:
                extracted_amount = float(amount_match.group(1).replace(',', ''))
            else:
                extracted_amount = 0
        
        # For TAX transactions, put the amount in the tax field, not amount field
        if transaction['transaction_type'] == 'TAX':
            transaction['tax'] = abs(extracted_amount)  # Tax should be positive
            transaction['amount'] = 0
        else:
            transaction['amount'] = extracted_amount
        
        # Standardize the amount based on transaction type
        if extracted_amount != 0:
            self.standardize_transaction_amount(transaction)
        
        # Only return transaction if we found meaningful data
        return transaction if (extracted_amount != 0) else None
    
    def parse_schwab_detailed_transactions(self, trans_text: str, current_year: str) -> List[Dict]:
        """Parse detailed Schwab transaction format with stocks (used for accounts with active trading)
        
        This method handles statement formats that contain detailed stock transaction information
        including buy/sell transactions, dividends, and gains/losses. 
        Account identification is done generically to protect privacy.
        """
        transactions = []
        lines = trans_text.split('\n')
        
        i = 0
        current_date = None
        
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip empty lines, headers, and fee lines
            if (not line or 'Date' in line or 'Category' in line or 'Symbol' in line or 
                'Total Transactions' in line or 'Industry Fee' in line):
                i += 1
                continue
            
            # Look for date pattern: MM/DD at start of line
            date_match = re.match(r'^(\d{1,2}/\d{1,2})\s+(.+)', line)
            if date_match:
                date_str = date_match.group(1)
                rest_of_line = date_match.group(2).strip()
                
                # Format date as YYYY-MM-DD
                date_parts = date_str.split('/')
                if len(date_parts) == 2:
                    month, day = date_parts
                    current_date = f"{current_year}-{month.zfill(2)}-{day.zfill(2)}"
                    
                    # Validation: Check if the date is in the future
                    from datetime import datetime
                    try:
                        parsed_date = datetime.strptime(current_date, '%Y-%m-%d')
                        today = datetime.now()
                        if parsed_date > today:
                            prev_year = str(int(current_year) - 1)
                            current_date = f"{prev_year}-{month.zfill(2)}-{day.zfill(2)}"
                    except ValueError:
                        pass
                else:
                    current_date = f"{current_year}-01-01"
                
                # Special handling for multi-line account transfers and journaled shares
                combined_line = rest_of_line
                if ('other' in rest_of_line.lower() and 
                    ('account transfer' in rest_of_line.lower() or 'activity' in rest_of_line.lower())):
                    # Look ahead for continuation lines with stock transfer info
                    lookahead_lines = []
                    for j in range(1, 3):  # Look ahead up to 2 lines
                        if i + j < len(lines):
                            next_line = lines[i + j].strip()
                            if next_line:
                                lookahead_lines.append(next_line)
                                # Check if this line contains "Journaled Shares" or numeric data
                                if ('journaled shares' in next_line.lower() or
                                    (re.search(r'\d+\.\d{4}', next_line) and  # Quantity pattern
                                     re.search(r'\d+\.\d{2}', next_line))):   # Price/Amount pattern
                                    combined_line = rest_of_line + " " + " ".join(lookahead_lines)
                                    i += j  # Skip the consumed lines
                                    break
                
                # Parse detailed transaction line
                transaction = self.parse_detailed_transaction_line(combined_line, current_date)
                if transaction:
                    transactions.append(transaction)
                
                i += 1
                
            elif current_date and line.strip():
                # This might be a continuation line without a date - use the current date
                # Enhanced filtering for banner lines, headers, and non-transaction content
                skip_patterns = [
                    'Industry Fee', 'NAME_PART', 'Page', 'Total',
                    'Transaction Details', 'Date Category Action', 'Symbol/', 'CUSIP', 'Description',
                    'Quantity', 'Price/Rate', 'Charges/', 'Interest($)', 'Amount($)', 'Realized',
                    'Gain/(Loss)', 'per Share', 'Statement Period', 'continued', 'Schwab One',
                    'International Account', 'of', 'YUAN JUNG CHENG',  # Account holder info
                    'BROKERAGE', 'STATEMENT', 'ACCOUNT', 'PERIOD',
                    'Date\s+Category\s+Action',  # Column headers
                ]
                
                # Check if line contains skip patterns
                should_skip = any(
                    skip_pattern.lower() in line.lower() if not skip_pattern.startswith('Date\\s+') 
                    else re.search(skip_pattern, line, re.IGNORECASE)
                    for skip_pattern in skip_patterns
                )
                
                # Also skip lines that are just column headers or formatting
                if (re.match(r'^[A-Za-z\s/()$]+$', line) and 
                    len(line.split()) <= 10 and 
                    not re.search(r'\d', line)):
                    should_skip = True
                
                if not should_skip:
                    # For new Schwab format, check if this looks like a transaction line
                    # Look for Category indicators (Sale, Purchase, Interest, Withdrawal, etc.)
                    # Even if Action column is empty
                    line_has_transaction = (
                        any(indicator in line.lower() for indicator in [
                            'sale', 'purchase', 'buy', 'sold', 'bought', 'withdrawal', 'deposit', 
                            'interest', 'dividend', 'nra tax', 'credit'
                        ]) or
                        re.search(r'\b[A-Z]{3,5}\b', line) or  # Stock symbol pattern
                        re.search(r'\(\d+', line) or  # Quantity in parentheses pattern (legacy)
                        re.search(r'\d+\.\d{4}', line)  # Price pattern like 179.7500
                    )
                    
                    if line_has_transaction:
                        transaction = self.parse_detailed_transaction_line(line.strip(), current_date)
                        if transaction:
                            transactions.append(transaction)
                i += 1
            else:
                i += 1
        
        return transactions
    
    def parse_detailed_transaction_line(self, line: str, transaction_date: str) -> Dict:
        """Parse a single detailed transaction line from Schwab format"""
        
        # Early validation: Skip obvious header/banner lines
        line_lower = line.lower()
        header_indicators = [
            'transaction details', 'date category action', 'symbol/cusip', 'description',
            'quantity', 'price/rate', 'charges/', 'interest($)', 'amount($)', 'realized',
            'gain/(loss)', 'per share', 'brokerage statement', 'account period',
            'statement period', 'schwab one', 'international account'
        ]
        
        # Check if this line is clearly a header or banner
        if any(indicator in line_lower for indicator in header_indicators):
            return None
            
        # Skip lines that are just formatting or contain only text without numbers
        if re.match(r'^[A-Za-z\s/()$-]+$', line) and len(line.split()) <= 6:
            return None
        
        transaction = {
            'transaction_date': transaction_date,
            'transaction_type': 'OTHER',
            'description': line,
            'symbol': '',
            'quantity': 0,
            'price': 0,
            'amount': 0,
            'fee': 0,
            'tax': 0,
            'realized_gain_loss': 0
        }
        
        # Identify transaction type - expand pattern matching for the new Schwab format
        
        # For new Schwab format, Category and Action are in separate columns
        # Category: Sale, Purchase, Interest, Withdrawal, etc.
        # Action: empty for stock transactions, "Buy", "NRA Tax", "Credit", etc. for others
        
        if ('sale' in line_lower or 'sold' in line_lower or 
            'sell' in line_lower or 'redemption' in line_lower):
            transaction['transaction_type'] = 'SELL'
        elif ('purchase' in line_lower or 'buy' in line_lower or 
              'bought' in line_lower or 'subscription' in line_lower):
            transaction['transaction_type'] = 'BUY'
        elif ('purchase' in line_lower and 'reinvested shares' in line_lower):
            # Special case: Dividend reinvestment should be treated as BUY
            # Example: "10/01 Purchase Reinvested Shares VOO VANGUARD S&P 500 ETF 0.4415 520.8799 (229.95)"
            transaction['transaction_type'] = 'BUY'
        elif 'withdrawal' in line_lower:
            transaction['transaction_type'] = 'WITHDRAWAL'
        elif 'deposit' in line_lower:
            transaction['transaction_type'] = 'DEPOSIT'
        elif 'interest' in line_lower:
            # For interest transactions, check for NRA Tax or Credit
            if 'nra tax' in line_lower or ('tax' in line_lower and 'interest' in line_lower):
                transaction['transaction_type'] = 'TAX'
            elif 'credit' in line_lower:
                transaction['transaction_type'] = 'INTEREST'
            else:
                # Default to interest for other interest-related transactions
                transaction['transaction_type'] = 'INTEREST'
        elif 'dividend' in line_lower:
            # Similar handling for dividend transactions
            if 'nra tax' in line_lower or ('tax' in line_lower and 'dividend' in line_lower):
                transaction['transaction_type'] = 'TAX'
            else:
                transaction['transaction_type'] = 'DIVIDEND'
        elif ('other' in line_lower and ('account transfer' in line_lower or 'journaled shares' in line_lower)):
            # Special case: Stock transfers into account should be treated as BUY
            # Examples: 
            # - "Other Account Transfer INTC INTEL CORP 174.0000 19.8700 3,457.38"
            # - "Other Activity Journaled Shares INTC INTEL CORP 433.0000 21.4100 9,270.53"
            # Check if this involves a stock symbol (has quantity and price)
            if (re.search(r'\b[A-Z]{3,5}\b', line) and 
                re.search(r'\d+\.\d{4}', line) and  # Has quantity pattern
                re.search(r'\d+\.\d{2}', line)):    # Has price pattern
                transaction['transaction_type'] = 'BUY'
            else:
                # If not a stock transfer, keep as OTHER
                transaction['transaction_type'] = 'OTHER'
        elif ('other' in line_lower and ('forward split' in line_lower or 'stock split' in line_lower)):
            # Special case: Stock splits vs dividend reinvestments
            # The key distinction is PRICE, not quantity:
            # - Stock splits (including fractional shares): price = 0.0 (no cost)
            # - Dividend reinvestments: price > 0.0 (actual purchase price)
            
            # Extract quantity first
            quantity_match = re.search(r'\(?([\d,]+\.?\d{0,4})\)?', line)
            quantity = 0
            if quantity_match:
                quantity = float(quantity_match.group(1).replace(',', ''))
                # If quantity was in parentheses, make it negative
                if f'({quantity_match.group(1)})' in line:
                    quantity = -quantity
            
            # All "Stock Split" and "Forward Split" transactions should remain as SPLIT regardless of quantity
            # because they represent share allocations, not purchases (no price)
            if 'stock split' in line_lower or 'forward split' in line_lower:
                # Actual stock splits and forward splits (including fractional shares from split events)
                # Examples:
                # - "Other Activity Forward Split SMCI SUPER MICRO COMPUTER INC 1,070.0000"
                # - "Other Activity Forward Split SUPER MICRO COMPUTER INC FORWARD SPLIT (107.0000)"
                # - "Other Activity Stock Split NVDA NVIDIA CORP 1,127.0000"
                transaction['transaction_type'] = 'SPLIT'
        
        # Special handling for SPLIT transactions
        if transaction['transaction_type'] == 'SPLIT':
            # Extract symbol for split transactions - look for known stock symbols
            # Handle specific cases first
            if 'SMCI' in line.upper() or 'SUPER MICRO COMPUTER' in line.upper():
                transaction['symbol'] = 'SMCI'
            elif 'NVDA' in line.upper() or 'NVIDIA' in line.upper():
                transaction['symbol'] = 'NVDA'
            else:
                # Try to find other stock symbols
                symbol_match = re.search(r'\b([A-Z]{3,5})\b', line)
                if symbol_match:
                    potential_symbol = symbol_match.group(1)
                    if potential_symbol not in ['CORP', 'INC', 'LLC', 'CLASS', 'FUND', 'SCHWAB', 'BANK', 'OTHER', 'SPLIT', 'FORWARD', 'SUPER', 'STOCK']:
                        transaction['symbol'] = potential_symbol
            
            # Extract quantity for split transactions (can be positive or negative)
            quantity_match = re.search(r'\(?([\d,]+\.?\d{4})\)?', line)
            if quantity_match:
                quantity = float(quantity_match.group(1).replace(',', ''))
                # If quantity was in parentheses, make it negative
                if f'({quantity_match.group(1)})' in line:
                    quantity = -quantity
                transaction['quantity'] = quantity
            
            # For splits, there's no price or monetary amount typically
            transaction['price'] = 0
            transaction['amount'] = 0
            
            # Calculate split ratio based on split type
            if 'forward split' in line.lower():
                if transaction['quantity'] > 0:
                    transaction['split_ratio'] = 'forward_split_positive'
                elif transaction['quantity'] < 0:
                    transaction['split_ratio'] = 'forward_split_negative'
                else:
                    transaction['split_ratio'] = 'forward_split'
            elif 'stock split' in line.lower():
                # For regular stock splits like NVDA 10:1, the quantity represents shares added
                # NVDA: 1,127 additional shares from 10:1 split means ~112.7 original shares
                if transaction['quantity'] > 0:
                    transaction['split_ratio'] = 'stock_split_10_for_1'  # Common ratio
                else:
                    transaction['split_ratio'] = 'stock_split'
            else:
                transaction['split_ratio'] = None
            
        # For regular stock transactions, parse the structured format
        elif transaction['transaction_type'] in ['SELL', 'BUY']:
            # Extract symbol (3-5 uppercase letters) - handle new Schwab format
            # In new format, symbol appears after Category and Action columns
            symbol_match = re.search(r'\b([A-Z]{3,5})\b', line)
            if symbol_match:
                potential_symbol = symbol_match.group(1)
                # Filter out common non-symbol words
                if potential_symbol not in ['CORP', 'INC', 'LLC', 'CLASS', 'FUND', 'SCHWAB', 'BANK', 'TFRD', 'NAME_PART', 'AMEX']:
                    transaction['symbol'] = potential_symbol
                    
            # If no symbol found using standard method, try parsing the new Schwab table format
            # Format: "Date | Category | Action | Symbol/CUSIP | Description | Quantity | Price/Rate | Charges/Int. | Amount"
            if not transaction['symbol']:
                # Split by multiple spaces or tabs to identify columns
                parts = re.split(r'\s{2,}|\t+', line.strip())
                if len(parts) >= 4:
                    # In new format, symbol is typically in the 3rd or 4th column (0-indexed)
                    for part in parts[2:5]:  # Check columns that might contain symbol
                        symbol_in_part = re.search(r'\b([A-Z]{3,5})\b', part)
                        if symbol_in_part:
                            potential_symbol = symbol_in_part.group(1)
                            if potential_symbol not in ['CORP', 'INC', 'LLC', 'CLASS', 'FUND', 'SCHWAB', 'BANK']:
                                transaction['symbol'] = potential_symbol
                                break
            
            # Extract company name (between symbol and quantity)
            if transaction['symbol']:
                # Look for company name pattern after symbol
                company_match = re.search(rf"{re.escape(transaction['symbol'])}\s+([A-Z\s&,]+?)(?:\s+\(|\s+A\s+)?\s*\(", line)
                if company_match:
                    transaction['description'] = f"{transaction['symbol']} {company_match.group(1).strip()}"
                else:
                    # Fallback: look for company name patterns
                    company_match = re.search(r"([A-Z][A-Z\s&,]+?(?:CORP|INC|CLASS))", line)
                    if company_match:
                        transaction['description'] = f"{transaction['symbol']} {company_match.group(1).strip()}"
            
            # Parse numeric values using the new Schwab format understanding (only for BUY/SELL)
            # Example line: "06/12 Purchase    GOOGL    ALPHABET INC CLASS A    360.0000    179.7500    (64,710.00)"
            
            # Try the new Schwab format first (most common now)
            parsed_values = self._parse_new_schwab_format(line)
            
            if not parsed_values:
                # Try legacy format with quantity in parentheses
                parsed_values = self._parse_legacy_schwab_format(line)
            
            # If we successfully parsed values, use them
            if parsed_values:
                try:
                    transaction['quantity'] = parsed_values['quantity']
                    transaction['price'] = parsed_values['price']
                    transaction['fee'] = parsed_values['fee']
                    transaction['amount'] = parsed_values['amount']
                    transaction['realized_gain_loss'] = parsed_values.get('realized_gain_loss', 0)
                    
                    # Set quantity sign based on transaction type
                    if transaction['transaction_type'] == 'SELL':
                        transaction['quantity'] = -abs(transaction['quantity'])
                    elif transaction['transaction_type'] == 'BUY':
                        transaction['quantity'] = abs(transaction['quantity'])
                    
                    # Use standardized amount handling
                    self.standardize_transaction_amount(transaction)
                    
                    # Add debug logging for successful stock transaction parsing
                    self.logger.debug(f"Parsed stock transaction: {transaction['transaction_type']} {transaction['symbol']} {transaction['quantity']} @ {transaction['price']} = {transaction['amount']}")
                    
                except (ValueError, KeyError) as e:
                    # If parsing fails, try to extract just the amount for a basic transaction
                    self.logger.warning(f"Failed to parse stock transaction numbers: {e}")
                    amount_match = re.search(r'([\d,]+\.?\d*)', line)
                    if amount_match:
                        transaction['amount'] = float(amount_match.group(1).replace(',', ''))
                        if transaction['transaction_type'] == 'BUY':
                            transaction['amount'] = -abs(transaction['amount'])
                        elif transaction['transaction_type'] == 'SELL':
                            transaction['amount'] = abs(transaction['amount'])
        
        else:
            # For non-stock transactions (Interest, Tax, Withdrawal, etc.), symbol should be empty
            # But we might find description patterns for interest/tax transactions
            if transaction['transaction_type'] in ['INTEREST', 'TAX']:
                # Look for interest account description like "SCHWAB1 INT"
                schwab_int_match = re.search(r'(SCHWAB\d+\s+INT)', line, re.IGNORECASE)
                if schwab_int_match:
                    transaction['description'] = schwab_int_match.group(1)
                # For these transactions, symbol remains empty as expected
                transaction['symbol'] = ''
            
            # Look for amounts in parentheses (negative) first
            amount_match = re.search(r'\(([\d,]+\.?\d*)\)', line)
            if amount_match:
                extracted_amount = -float(amount_match.group(1).replace(',', ''))
            else:
                # Look for positive amount at the end
                amount_match = re.search(r'([\d,]+\.?\d*)$', line)
                if amount_match:
                    extracted_amount = float(amount_match.group(1).replace(',', ''))
                else:
                    extracted_amount = 0
            
            # For TAX transactions, put the amount in the tax field, not amount field
            if transaction['transaction_type'] == 'TAX':
                transaction['tax'] = abs(extracted_amount)  # Tax should be positive
                transaction['amount'] = 0
            else:
                transaction['amount'] = extracted_amount
        
        # Always calculate net_amount for all transactions
        self.calculate_net_amount(transaction)
        
        # Only return transaction if we found meaningful data (amount, symbol, or tax)
        return transaction if (transaction['amount'] != 0 or transaction['symbol'] or transaction['tax'] != 0) else None
    
    def parse_tda_statement(self, text: str, file_path: Path) -> Dict:
        """Parse TD Ameritrade statement data"""
        data = {
            'account_info': {},
            'transactions': [],
            'positions': [],
            'balances': {}
        }
        
        # Extract account information
        data['account_info'] = {
            'account_id': 'TDA-HISTORICAL-001',  # Generic identifier for historical TDA account
            'institution': 'TD Ameritrade',
            'account_type': 'Brokerage',
            'broker': 'TDA'
        }
        
        # Extract account holder from TDA format
        lines = text.split('\n')[:15]  # Check first 15 lines
        for line in lines:
            clean_line = line.strip()
            if clean_line and not any(word in clean_line.lower() for word in [
                'trade', 'settlement', 'industry', 'box', 'omaha', 'po box'
            ]):
                # Look for name pattern (all caps, letters and spaces)
                if re.match(r'^[A-Z][A-Z\s]{5,}$', clean_line) and len(clean_line.split()) >= 2:
                    data['account_info']['account_holder'] = clean_line
                    break
        
        # Extract statement date from filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', file_path.name)
        if date_match:
            data['account_info']['statement_date'] = date_match.group(1)
        
        # Extract portfolio summary section
        portfolio_section = re.search(r'Portfolio Summary(.*?)(?:Account Activity|Margin Information)', text, re.DOTALL)
        if portfolio_section:
            portfolio_text = portfolio_section.group(1)
            # Parse portfolio balances
            data['balances'] = self.parse_tda_balances(portfolio_text)
        
        # Extract positions
        positions_section = re.search(r'Long Positions(.*?)(?:Short Positions|Account Activity)', text, re.DOTALL)
        if positions_section:
            positions_text = positions_section.group(1)
            data['positions'] = self.parse_tda_positions(positions_text)
        
        # Extract transactions - look for Opening Balance line and transaction data that follows
        # TDA transactions appear after "Opening Balance" line
        opening_balance_match = re.search(r'Opening Balance.*?\$([\d,]+\.?\d*)', text, re.DOTALL)
        if opening_balance_match:
            # Find all text after the Opening Balance
            opening_balance_index = text.find('Opening Balance')
            if opening_balance_index != -1:
                activity_text = text[opening_balance_index:]
                # Limit to reasonable transaction section (before terms and conditions)
                end_match = re.search(r'(?:Terms and Conditions|Accuracy of Reports|TD Ameritrade does not provide)', activity_text)
                if end_match:
                    activity_text = activity_text[:end_match.start()]
                data['transactions'] = self.parse_tda_transactions(activity_text)
        else:
            # Fallback: look for any transaction-like patterns in the entire text
            data['transactions'] = self.parse_tda_transactions(text)
        
        return data
    
    def parse_tda_balances(self, portfolio_text: str) -> Dict:
        """Parse TDA portfolio balance information"""
        balances = {}
        
        # Look for balance patterns in TDA Portfolio Summary format
        cash_match = re.search(r'Cash\s+\$\s*([\d,]+\.?\d*)', portfolio_text)
        if cash_match:
            balances['cash_balance'] = float(cash_match.group(1).replace(',', ''))
        
        # Look for stock investments
        stocks_match = re.search(r'Stocks\s+[\d,]+\.?\d*\s+[\d,]+\.?\d*\s+.*?\s+([\d,]+\.?\d*)', portfolio_text)
        if stocks_match:
            balances['total_investments'] = float(stocks_match.group(1).replace(',', ''))
        
        # Look for total account value
        total_match = re.search(r'Total\s+\$\s*([\d,]+\.?\d*)', portfolio_text)
        if total_match:
            balances['total_account_value'] = float(total_match.group(1).replace(',', ''))
        
        return balances
    
    def parse_tda_positions(self, positions_text: str) -> List[Dict]:
        """Parse positions from TDA Long Positions section"""
        positions = []
        # TDA positions would be in a tabular format, but since we saw limited data
        # in the sample, this is a basic implementation
        return positions
    
    def parse_tda_transactions(self, activity_text: str) -> List[Dict]:
        """Parse transactions from TDA Account Activity section - based on actual observed format"""
        transactions = []
        
        # Split text into lines and look for transaction patterns
        lines = activity_text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # Look for transaction lines with date pattern MM/DD/YY MM/DD/YY Cash TransactionType
            # Format: "12/08/22 12/08/22 Cash Div/Int - Income MICROSOFT CORP MSFT - $ 0.00 $ 136.23 117,922.94"
            date_transaction_match = re.search(
                r'^(\d{2}/\d{2}/\d{2})\s+(\d{2}/\d{2}/\d{2})\s+(Cash|Margin)\s+(.+)', 
                line
            )
            
            if date_transaction_match:
                trade_date = date_transaction_match.group(1)
                settle_date = date_transaction_match.group(2)
                account_type = date_transaction_match.group(3)
                transaction_details = date_transaction_match.group(4).strip()
                
                # Convert MM/DD/YY to YYYY-MM-DD (assuming 20XX)
                trade_year = "20" + trade_date.split('/')[-1]
                trade_month = trade_date.split('/')[0].zfill(2)
                trade_day = trade_date.split('/')[1].zfill(2)
                trade_date_formatted = f"{trade_year}-{trade_month}-{trade_day}"
                
                settle_year = "20" + settle_date.split('/')[-1]
                settle_month = settle_date.split('/')[0].zfill(2)
                settle_day = settle_date.split('/')[1].zfill(2)
                settle_date_formatted = f"{settle_year}-{settle_month}-{settle_day}"
                
                # Initialize transaction
                transaction = {
                    'transaction_date': trade_date_formatted,
                    'settle_date': settle_date_formatted,
                    'account_type': account_type,
                    'transaction_type': 'OTHER',
                    'description': transaction_details,
                    'symbol': '',
                    'quantity': 0,
                    'price': 0,
                    'amount': 0,
                    'fee': 0,
                    'tax': 0,
                    'realized_gain_loss': 0
                }
                
                # Parse transaction type
                if 'Div/Int - Income' in transaction_details:
                    transaction['transaction_type'] = 'DIVIDEND'
                elif 'Buy - Securities Purchased' in transaction_details:
                    transaction['transaction_type'] = 'BUY'
                elif 'Sell - Securities Sold' in transaction_details:
                    transaction['transaction_type'] = 'SELL'
                elif 'Journal - Other' in transaction_details:
                    transaction['transaction_type'] = 'JOURNAL'
                elif 'Transfer' in transaction_details:
                    transaction['transaction_type'] = 'TRANSFER'
                
                # Extract company name and symbol from transaction_details
                # Pattern: "TransactionType COMPANY NAME SYMBOL quantity price amount"
                symbol_match = re.search(r'\b([A-Z]{3,5})\b', transaction_details)
                if symbol_match:
                    potential_symbol = symbol_match.group(1)
                    # Filter out common non-symbols
                    if potential_symbol not in ['CORP', 'COM', 'INC', 'LLC', 'FUND', 'CASH', 'OTHER']:
                        transaction['symbol'] = potential_symbol
                        
                        # Extract company name (everything before the symbol)
                        company_match = re.search(rf'([A-Z\s&]+?)\s+{re.escape(potential_symbol)}', transaction_details)
                        if company_match:
                            company_name = company_match.group(1).strip()
                            # Clean up transaction type from company name
                            for tx_type in ['Div/Int - Income', 'Buy - Securities Purchased', 'Sell - Securities Sold', 'Journal - Other']:
                                company_name = company_name.replace(tx_type, '').strip()
                            transaction['description'] = company_name
                
                # Extract amounts from the line - TDA format has amounts at the end
                # Pattern: "... $ amount1 $ amount2 balance"  or "... - $ amount balance"
                amount_matches = re.findall(r'\$\s*([\d,]+\.?\d*)', line)
                if amount_matches:
                    try:
                        # Usually the last non-zero amount is the transaction amount (not the balance)
                        transaction_amounts = [float(amt.replace(',', '')) for amt in amount_matches]
                        # Filter out 0.00 amounts and take the first meaningful amount
                        meaningful_amounts = [amt for amt in transaction_amounts if amt != 0.0]
                        if meaningful_amounts:
                            transaction['amount'] = meaningful_amounts[0]
                        elif transaction_amounts:
                            transaction['amount'] = transaction_amounts[0]
                        
                        # Look for negative amounts in parentheses first
                        if '(' in line and ')' in line:
                            paren_match = re.search(r'\((\d+\.?\d*)\)', line)
                            if paren_match:
                                transaction['amount'] = float(paren_match.group(1))  # Get absolute value, let standardization handle sign
                        
                        # Use standardized amount handling
                        self.standardize_transaction_amount(transaction)
                    
                    except (ValueError, IndexError):
                        pass
                
                # Extract quantity and price if it's a BUY/SELL transaction
                if transaction['transaction_type'] in ['BUY', 'SELL']:
                    # Look for quantity pattern before the amounts
                    qty_match = re.search(r'(\d+\.?\d+)\s+\(?([\d,]+\.?\d*)\)?\s*$', line)
                    if qty_match:
                        try:
                            quantity = float(qty_match.group(1).replace(',', ''))
                            # Make SELL quantities negative for consistency
                            if transaction['transaction_type'] == 'SELL':
                                quantity = -abs(quantity)
                            transaction['quantity'] = quantity
                            transaction['price'] = float(qty_match.group(2).replace(',', ''))
                        except ValueError:
                            pass
                
                transactions.append(transaction)
        
        return transactions
    
    def parse_cathay_csv(self, csv_path: Path) -> Dict:
        """Parse 國泰證券 CSV data (existing logic)"""
        try:
            # Read CSV file, skipping the first summary row
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
            
            # Skip the first summary line and use the second line as header
            if len(lines) > 1 and '根據您篩選的結果' in lines[0]:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as tmp:
                    tmp.writelines(lines[1:])  # Skip first line
                    tmp_path = tmp.name
                
                df = pd.read_csv(tmp_path, encoding='utf-8')
                os.unlink(tmp_path)  # Clean up temp file
            else:
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            # Convert to standard format
            data = {
                'account_info': {
                    'account_id': 'CATHAY-001',
                    'institution': '國泰證券',
                    'account_type': '證券戶',
                    'broker': 'CATHAY',
                    'account_holder': 'Default'
                },
                'transactions': [],
                'positions': [],
                'balances': {}
            }
            
            # Process transactions
            for idx, row in df.iterrows():
                try:
                    symbol = str(row.get('股名', '')).strip()
                    date_str = str(row.get('日期', '')).strip()
                    
                    if not symbol or not date_str or symbol == 'nan' or date_str == 'nan':
                        continue
                    
                    # Parse transaction data
                    quantity_str = str(row.get('成交股數', '0')).replace(',', '').replace('"', '')
                    quantity = int(float(quantity_str)) if quantity_str and quantity_str != 'nan' else 0
                    
                    net_amount_str = str(row.get('淨收付金額', '0')).replace(',', '').replace('"', '')
                    net_amount = float(net_amount_str) if net_amount_str and net_amount_str != 'nan' else 0
                    
                    transaction_type = 'BUY' if '現買' in str(row.get('買賣別', '')) else 'SELL'
                    price = float(str(row.get('成交價', '0')).replace(',', '')) if row.get('成交價') else 0
                    cost = float(str(row.get('成本', '0')).replace(',', '')) if row.get('成本') else 0
                    fee = float(str(row.get('手續費', '0')).replace(',', '')) if pd.notna(row.get('手續費')) else 0
                    tax = float(str(row.get('交易稅', '0')).replace(',', '')) if pd.notna(row.get('交易稅')) else 0
                    order_id = str(row.get('委託書號', ''))
                    
                    # Make SELL quantities negative for consistency
                    if transaction_type == 'SELL':
                        quantity = -abs(quantity)
                    
                    # Convert date format
                    if '/' in date_str:
                        date_obj = datetime.strptime(date_str, '%Y/%m/%d')
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                    else:
                        formatted_date = date_str
                    
                    # Map Chinese symbol to ticker for CATHAY broker
                    ticker_symbol, chinese_name = self.map_chinese_symbol(symbol, 'CATHAY')
                    
                    transaction = {
                        'transaction_date': formatted_date,
                        'symbol': ticker_symbol,
                        'chinese_name': chinese_name,
                        'transaction_type': transaction_type,
                        'quantity': quantity,
                        'price': price,
                        'amount': cost,
                        'fee': fee,
                        'tax': tax,
                        'net_amount': net_amount,
                        'order_id': order_id
                    }
                    
                    # Apply standardized amount handling
                    self.standardize_transaction_amount(transaction)
                    
                    data['transactions'].append(transaction)
                    
                except Exception as e:
                    self.logger.error(f"Error processing CSV row {idx}: {e}")
                    continue
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error parsing CSV {csv_path}: {e}")
            return None
    
    def store_account_data(self, data: Dict, file_path: Path, broker: str):
        """Store account information in database"""
        if not data.get('account_info'):
            return
        
        account_info = data['account_info']
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if currency column exists, if not add it
            cursor.execute("PRAGMA table_info(accounts)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'currency' not in columns:
                cursor.execute("ALTER TABLE accounts ADD COLUMN currency TEXT DEFAULT 'USD'")
            if 'status' not in columns:
                cursor.execute("ALTER TABLE accounts ADD COLUMN status TEXT DEFAULT 'ACTIVE'")
            
            # Determine currency based on broker
            currency = 'NTD' if broker == 'CATHAY' else 'USD'
            
            cursor.execute("""
                INSERT OR REPLACE INTO accounts 
                (account_id, institution, broker, account_type, account_holder, created_date, currency)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                account_info.get('account_id'),
                account_info.get('institution'),
                broker,
                account_info.get('account_type'),
                account_info.get('account_holder'),
                account_info.get('statement_date', datetime.now().isoformat()),
                currency
            ))
            
            conn.commit()
    
    def store_transactions(self, data: Dict, file_path: Path, broker: str):
        """Store transaction data in database"""
        if not data.get('transactions'):
            return
        
        account_id = data.get('account_info', {}).get('account_id')
        if not account_id:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if new columns exist
            cursor.execute("PRAGMA table_info(transactions)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'source_file' not in columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN source_file TEXT")
            if 'created_at' not in columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP")
            if 'currency' not in columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN currency TEXT DEFAULT 'USD'")
            if 'chinese_name' not in columns:
                cursor.execute("ALTER TABLE transactions ADD COLUMN chinese_name TEXT")
            
            # Determine currency and display broker name based on broker type
            if broker == 'CATHAY':
                currency = 'NTD'
                display_broker = '國泰證券'
            else:
                currency = 'USD'
                display_broker = broker
            
            for transaction in data['transactions']:
                cursor.execute("""
                    INSERT OR REPLACE INTO transactions 
                    (account_id, transaction_date, symbol, chinese_name, transaction_type, quantity, price, 
                     amount, fee, tax, net_amount, broker, order_id, description, source_file, currency, split_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    account_id,
                    transaction.get('transaction_date'),
                    transaction.get('symbol'),
                    transaction.get('chinese_name'),  # Will be None for non-CATHAY brokers
                    transaction.get('transaction_type'),
                    transaction.get('quantity', 0),
                    transaction.get('price', 0),
                    transaction.get('amount', 0),
                    transaction.get('fee', 0),
                    transaction.get('tax', 0),
                    transaction.get('net_amount', 0),
                    display_broker,
                    transaction.get('order_id'),
                    transaction.get('description'),
                    str(file_path),
                    currency,
                    transaction.get('split_ratio')
                ))
            
            conn.commit()
            self.logger.info(f"Stored {len(data['transactions'])} transactions from {file_path.name}")
    
    def store_balances(self, data: Dict, file_path: Path, broker: str):
        """Store account balance data in database"""
        if not data.get('balances'):
            return
        
        account_id = data.get('account_info', {}).get('account_id')
        statement_date = data.get('account_info', {}).get('statement_date')
        
        if not account_id:
            return
        
        balances = data['balances']
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO account_balances 
                (account_id, statement_date, cash_balance, total_investments, total_account_value, broker, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                account_id,
                statement_date,
                balances.get('cash_balance', 0),
                balances.get('total_investments', 0),
                balances.get('total_account_value', 0),
                broker,
                str(file_path)
            ))
            
            conn.commit()
    
    def process_file(self, file_path: Path) -> bool:
        """Process a single statement file"""
        try:
            broker = self.identify_broker_from_file(file_path)
            self.logger.info(f"Processing {file_path.name} as {broker} statement")
            
            # Log processing attempt
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO processing_log (file_path, file_type, broker, status)
                    VALUES (?, ?, ?, 'PROCESSING')
                """, (str(file_path), file_path.suffix, broker))
                conn.commit()
            
            data = None
            
            if broker == 'SCHWAB':
                text = self.extract_text_from_pdf(file_path)
                if text:
                    data = self.parse_schwab_statement(text, file_path)
                    
            elif broker == 'TDA':
                text = self.extract_text_from_pdf(file_path)
                if text:
                    data = self.parse_tda_statement(text, file_path)
                    
            elif broker == 'CATHAY':
                data = self.parse_cathay_csv(file_path)
            
            if data:
                # Store data
                self.store_account_data(data, file_path, broker)
                self.store_transactions(data, file_path, broker)
                self.store_balances(data, file_path, broker)
                
                # Update processing log
                transaction_count = len(data.get('transactions', []))
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE processing_log 
                        SET status = 'SUCCESS', records_processed = ?
                        WHERE file_path = ? AND status = 'PROCESSING'
                    """, (transaction_count, str(file_path)))
                    conn.commit()
                
                return True
            else:
                # Update processing log with failure
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE processing_log 
                        SET status = 'FAILED', error_message = 'No data extracted'
                        WHERE file_path = ? AND status = 'PROCESSING'
                    """, (str(file_path),))
                    conn.commit()
                
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {e}")
            
            # Update processing log with error
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE processing_log 
                    SET status = 'ERROR', error_message = ?
                    WHERE file_path = ? AND status = 'PROCESSING'
                """, (str(e), str(file_path)))
                conn.commit()
            
            return False
    
    def update_missing_net_amounts(self):
        """Update existing transactions that are missing net_amount values"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Find transactions without net_amount
                cursor.execute("""
                    SELECT id, amount, fee, tax 
                    FROM transactions 
                    WHERE net_amount IS NULL OR net_amount = 0
                """)
                
                transactions_to_update = cursor.fetchall()
                
                if not transactions_to_update:
                    self.logger.info("All transactions already have net_amount calculated")
                    return
                
                self.logger.info(f"Found {len(transactions_to_update)} transactions missing net_amount")
                
                # Update each transaction
                for transaction_id, amount, fee, tax in transactions_to_update:
                    # Calculate net_amount using the same logic
                    net_amount = (amount or 0) - abs(fee or 0) - abs(tax or 0)
                    
                    cursor.execute("""
                        UPDATE transactions 
                        SET net_amount = ? 
                        WHERE id = ?
                    """, (net_amount, transaction_id))
                
                conn.commit()
                self.logger.info(f"Updated net_amount for {len(transactions_to_update)} transactions")
                
        except Exception as e:
            self.logger.error(f"Error updating missing net_amounts: {e}")

    def process_all_statements(self):
        """Process all PDF and CSV statements"""
        if not self.statements_dir.exists():
            self.logger.error(f"Statements directory not found: {self.statements_dir}")
            return
        
        processed = 0
        failed = 0
        
        # Process CSV files (國泰證券)
        for csv_file in self.statements_dir.rglob("*.csv"):
            if self.process_file(csv_file):
                processed += 1
            else:
                failed += 1
        
        # Process PDF files (TDA and Schwab)
        for pdf_file in self.statements_dir.rglob("*.PDF"):
            if self.process_file(pdf_file):
                processed += 1
            else:
                failed += 1
        
        for pdf_file in self.statements_dir.rglob("*.pdf"):
            if self.process_file(pdf_file):
                processed += 1
            else:
                failed += 1
        
        self.logger.info(f"Processing complete: {processed} successful, {failed} failed")
        
        # Generate summary report
        self.generate_processing_report()
        
        # Update any existing transactions that might be missing net_amount values
        self.update_missing_net_amounts()
    
    def generate_processing_report(self):
        """Generate a summary report of processing results"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get processing summary
            cursor.execute("""
                SELECT broker, status, COUNT(*) as count, SUM(records_processed) as total_records
                FROM processing_log
                GROUP BY broker, status
                ORDER BY broker, status
            """)
            
            results = cursor.fetchall()
            
            report = {
                'processing_summary': {},
                'broker_counts': {},
                'total_accounts': 0,
                'total_transactions': 0,
                'holdings_by_broker': {},
                'generated_at': datetime.now().isoformat()
            }
            
            for broker, status, count, total_records in results:
                if broker not in report['processing_summary']:
                    report['processing_summary'][broker] = {}
                report['processing_summary'][broker][status] = {
                    'files': count,
                    'records': total_records or 0
                }
            
            # Get account counts
            cursor.execute("SELECT broker, COUNT(*) FROM accounts GROUP BY broker")
            for broker, count in cursor.fetchall():
                report['broker_counts'][broker] = count
                report['total_accounts'] += count
            
            # Get transaction counts
            cursor.execute("SELECT COUNT(*) FROM transactions")
            report['total_transactions'] = cursor.fetchone()[0]
            
            # Get holdings by broker and symbol (including all holdings, positive or negative)
            cursor.execute("""
                SELECT 
                    t.broker,
                    t.symbol,
                    t.chinese_name,
                    SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as bought_qty,
                    SUM(CASE WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN ABS(t.quantity) ELSE 0 END) as sold_qty,
                    SUM(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.quantity 
                             WHEN t.transaction_type = '賣出' OR t.transaction_type = 'SELL' THEN -ABS(t.quantity) ELSE 0 END) as current_holding,
                    AVG(CASE WHEN t.transaction_type = '買進' OR t.transaction_type = 'BUY' THEN t.price ELSE NULL END) as avg_cost,
                    t.currency
                FROM transactions t 
                WHERE t.symbol IS NOT NULL AND t.symbol != ''
                GROUP BY t.symbol, t.broker, t.chinese_name, t.currency
                ORDER BY t.broker, t.symbol
            """)
            
            holdings_results = cursor.fetchall()
            
            # Organize holdings by broker
            for broker, symbol, chinese_name, bought_qty, sold_qty, current_holding, avg_cost, currency in holdings_results:
                if broker not in report['holdings_by_broker']:
                    report['holdings_by_broker'][broker] = []
                
                holding_info = {
                    'symbol': symbol,
                    'bought_qty': bought_qty,
                    'sold_qty': sold_qty,
                    'current_holding': current_holding,
                    'avg_cost': round(avg_cost, 2) if avg_cost else 0,
                    'currency': currency
                }
                
                # Add chinese_name only if it exists (CATHAY broker)
                if chinese_name:
                    holding_info['chinese_name'] = chinese_name
                
                report['holdings_by_broker'][broker].append(holding_info)
            
            # Save report
            report_path = Path("outputs/reports/processing_report.json")
            report_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Processing report saved to {report_path}")
            print(f"\\n=== Processing Report ===")
            print(f"Total accounts: {report['total_accounts']}")
            print(f"Total transactions: {report['total_transactions']}")
            for broker, counts in report['processing_summary'].items():
                print(f"\\n{broker}:")
                for status, stats in counts.items():
                    print(f"  {status}: {stats['files']} files, {stats['records']} records")
            
            # Print holdings summary
            print(f"\\n=== Holdings by Broker ===")
            for broker, holdings in report['holdings_by_broker'].items():
                print(f"\\n{broker}:")
                total_positions = len(holdings)
                positive_holdings = len([h for h in holdings if h['current_holding'] > 0])
                negative_holdings = len([h for h in holdings if h['current_holding'] < 0])
                zero_holdings = len([h for h in holdings if h['current_holding'] == 0])
                
                print(f"  Total symbols: {total_positions}")
                print(f"  Active positions (>0): {positive_holdings}")
                print(f"  Short positions (<0): {negative_holdings}")
                print(f"  Closed positions (=0): {zero_holdings}")
                print(f"\\n  Symbol Details:")
                
                for holding in holdings:
                    symbol_display = holding['symbol']
                    if 'chinese_name' in holding:
                        symbol_display = f"{holding['symbol']} ({holding['chinese_name']})"
                    
                    position_type = ""
                    if holding['current_holding'] > 0:
                        position_type = "[LONG]"
                    elif holding['current_holding'] < 0:
                        position_type = "[SHORT]"
                    else:
                        position_type = "[CLOSED]"
                    
                    print(f"    {position_type} {symbol_display}: {holding['current_holding']:.2f} shares @ avg ${holding['avg_cost']:.2f} {holding['currency']}")
    
    def _parse_new_schwab_format(self, line: str) -> Dict:
        """Parse the new Schwab transaction format where quantity is not in parentheses
        
        Example line format (new tabular structure):
        "Sale    GOOGL    ALPHABET INC CLASS A    (350.0000)    188.9350    0.06    66,127.19    4,665.30 (ST)"
        OR
        "Purchase    Buy    GOOGL    ALPHABET INC CLASS A    360.0000    179.7500        (64,710.00)"
        OR 
        "Sale    VOO    VANGUARD S&P 500 ETF    (18.0000)    541.9272         9,754.69    3,337.38,(LT)"
        """
        # First check: if this is clearly legacy format with quantity in parentheses, reject
        legacy_pattern = re.search(r'\([0-9,]+\.?\d*\)\s+[0-9,]+\.?\d*\s+[0-9,]+\.?\d*\s+[0-9,]+\.?\d*', line)
        if legacy_pattern:
            return None
        
        # Remove extra spaces and normalize the line
        normalized_line = ' '.join(line.split())
        
        # Extract symbol to better position the parsing
        symbol_match = re.search(r'\b([A-Z]{3,5})\b', line)
        if not symbol_match:
            return None
        
        symbol = symbol_match.group(1)
        if symbol in ['CORP', 'INC', 'LLC', 'CLASS', 'FUND', 'SCHWAB', 'BANK']:
            return None
        
        # Find symbol position and extract the part after company description
        symbol_pos = line.find(symbol)
        after_symbol = line[symbol_pos + len(symbol):]
        
        # Look for company name and then numeric values
        # Skip company description and find the numeric part - be careful with ETF names containing numbers
        # Use a more precise pattern that looks for decimal numbers (which are likely quantity/price)
        company_end = re.search(r'([A-Z\s&,0-9]+?)\s+(\d+\.\d{3,4})', after_symbol)
        if not company_end:
            # Fallback to original pattern if decimal pattern doesn't work
            company_end = re.search(r'([A-Z\s&,]+?)\s+([0-9,]+\.?\d*)', after_symbol)
            if not company_end:
                return None
            
        # Extract all numbers after the company description
        numeric_part = after_symbol[company_end.start(2):]
        numbers = re.findall(r'[0-9,]+\.?\d*', numeric_part)
        
        if len(numbers) < 2:  # Need at least quantity and price
            return None
        
        try:
            # For new format: quantity, price, [fee], amount, [realized_gain_loss]
            quantity = float(numbers[0].replace(',', ''))
            price = float(numbers[1].replace(',', ''))
            
            # Initialize defaults
            amount = 0
            fee = 0
            realized_gain_loss = 0
            
            # Check for amount in parentheses (indicates outgoing cash flow)
            amount_in_parens = re.search(r'\(([\d,]+\.?\d*)\)', numeric_part)
            if amount_in_parens:
                amount = float(amount_in_parens.group(1).replace(',', ''))
            
            # Parse based on number of numeric values found
            if len(numbers) == 2:
                # Only quantity and price - no amount or fees (e.g., reinvestment)
                amount = 0
                fee = 0
            elif len(numbers) == 3:
                # quantity, price, amount (no fees - regular buy/sell)
                if not amount_in_parens:  # If amount not already found in parentheses
                    amount = float(numbers[2].replace(',', ''))
                fee = 0  # Regular buy/sell transactions have no fees
            elif len(numbers) == 4:
                # quantity, price, amount, realized_gain_loss (no fees - regular sale)
                if not amount_in_parens:
                    amount = float(numbers[2].replace(',', ''))
                realized_gain_loss = float(numbers[3].replace(',', ''))
                fee = 0  # Regular buy/sell transactions have no fees
            elif len(numbers) >= 5:
                # quantity, price, fee, amount, realized_gain_loss (with fees)
                if not amount_in_parens:
                    fee = float(numbers[2].replace(',', ''))
                    amount = float(numbers[3].replace(',', ''))
                    if len(numbers) > 4:
                        realized_gain_loss = float(numbers[4].replace(',', ''))
                else:
                    # If amount was in parentheses, adjust parsing
                    fee = float(numbers[2].replace(',', ''))
                    if len(numbers) > 3:
                        realized_gain_loss = float(numbers[3].replace(',', ''))
            
            return {
                'quantity': quantity,
                'price': price,
                'fee': fee,
                'amount': amount,
                'realized_gain_loss': realized_gain_loss
            }
            
        except (ValueError, IndexError) as e:
            self.logger.debug(f"Failed to parse new Schwab format numbers: {e}")
            return None
    
    def _parse_legacy_schwab_format(self, line: str) -> Dict:
        """Parse the legacy Schwab transaction format with quantity in parentheses
        
        Example line formats:
        With fee: "08/04 Sale    GOOGL    ALPHABET INC CLASS A    (350.0000)    188.9350    0.06    66,127.19    4,665.30,(ST)"
        No fee: "Sale    VOO    VANGUARD S&P 500 ETF    (18.0000)    541.9272         9,754.69    3,337.38,(LT)"
        """
        
        # First try: Standard format with fee: (quantity) price fee amount [gain/loss]
        numeric_match = re.search(r'\((\d+(?:,\d+)*(?:\.\d+)?)\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s*([\d,]+\.?\d*)?', line)
        
        if numeric_match:
            try:
                quantity = float(numeric_match.group(1).replace(',', ''))
                price = float(numeric_match.group(2).replace(',', ''))
                third_num = float(numeric_match.group(3).replace(',', ''))
                fourth_num = float(numeric_match.group(4).replace(',', ''))
                fifth_num = float(numeric_match.group(5).replace(',', '')) if numeric_match.group(5) else 0
                
                # Determine if third number is fee or amount based on magnitude
                # Fees are typically small (< $100), amounts are typically larger
                # Also, check if we have a fifth number (gain/loss)
                if third_num < 100 and fourth_num > third_num and fifth_num > 0:
                    # Format: (quantity) price fee amount gain/loss
                    return {
                        'quantity': quantity,
                        'price': price,
                        'fee': third_num,
                        'amount': fourth_num,
                        'realized_gain_loss': fifth_num
                    }
                else:
                    # Format: (quantity) price amount gain/loss (no fee)
                    return {
                        'quantity': quantity,
                        'price': price,
                        'fee': 0,  # No fee for regular stock transactions
                        'amount': third_num,
                        'realized_gain_loss': fourth_num
                    }
            except (ValueError, IndexError):
                pass
        
        # Alternative format with different spacing - more flexible
        numeric_match = re.search(r'\((\d+(?:,\d+)*(?:\.\d+)?)\)[^0-9]*([\d,]+\.?\d*)[^0-9]+([\d,]+\.?\d*)[^0-9]+([\d,]+\.?\d*)', line)
        if numeric_match:
            try:
                quantity = float(numeric_match.group(1).replace(',', ''))
                price = float(numeric_match.group(2).replace(',', ''))
                third_num = float(numeric_match.group(3).replace(',', ''))
                fourth_num = float(numeric_match.group(4).replace(',', ''))
                
                # For this pattern, assume no fee (regular stock transaction)
                return {
                    'quantity': quantity,
                    'price': price,
                    'fee': 0,  # Regular stock transactions have no fees
                    'amount': third_num,
                    'realized_gain_loss': fourth_num
                }
            except (ValueError, IndexError):
                pass
        
        return None


if __name__ == "__main__":
    # Run from project root directory to access Statements folder
    parser = MultiBrokerPortfolioParser(statements_dir="Statements")
    parser.process_all_statements()