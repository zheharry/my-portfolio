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
                'default_account': 'TDA-088',
                'note': 'Legacy TDA statements before Schwab merger'
            },
            'SCHWAB': {
                'name': 'Charles Schwab (Post-Merger)',
                'account_patterns': [r'(\d{4}-\d{4})\s+[A-Za-z]+\s+\d+[-]\d+,\s+\d{4}'],
                'file_pattern': r'Brokerage Statement_.*\.PDF',
                'default_accounts': ['SCHWAB-9740-7088', 'SCHWAB-2530-2563'],
                'note': 'Modern Schwab statements after TDA merger'
            },
            'CATHAY': {
                'name': '國泰證券',
                'account_patterns': [],  # CSV based
                'file_pattern': r'.*\.csv',
                'default_account': 'CATHAY-001'
            }
        }
    
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
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                )
            """)
            
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
    
    def standardize_transaction_amount(self, transaction: Dict) -> None:
        """
        Standardize transaction amounts based on transaction type from a cash flow perspective:
        - Negative amounts (cash going out): BUY, 買進, WITHDRAWAL, TAX
        - Positive amounts (cash coming in): SELL, 賣出, DEPOSIT, DIVIDEND, INTEREST, JOURNAL, OTHER
        """
        transaction_type = transaction.get('transaction_type', '')
        amount = transaction.get('amount', 0)
        
        # Define transaction types that should have negative amounts (cash going out)
        negative_types = {'BUY', '買進', 'WITHDRAWAL', 'TAX'}
        
        # Define transaction types that should have positive amounts (cash coming in)
        positive_types = {'SELL', '賣出', 'DEPOSIT', 'DIVIDEND', 'INTEREST', 'JOURNAL', 'OTHER'}
        
        if transaction_type in negative_types:
            transaction['amount'] = -abs(amount)
        elif transaction_type in positive_types:
            transaction['amount'] = abs(amount)
        # For unknown types, leave amount as is but log it
        elif transaction_type and amount != 0:
            self.logger.warning(f"Unknown transaction type '{transaction_type}' - amount not standardized")
    
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
                account_holder = 'YUAN JUNG CHENG'  # Default from statements
            
            data['account_info']['account_id'] = f"SCHWAB-{account_number}"
            data['account_info']['account_holder'] = account_holder
        else:
            # Fallback to filename-based account identification
            if '_088' in file_path.name:
                data['account_info']['account_id'] = 'SCHWAB-9740-7088'
            elif '_563' in file_path.name:
                data['account_info']['account_id'] = 'SCHWAB-2530-2563'
            else:
                data['account_info']['account_id'] = 'SCHWAB-UNKNOWN'
            data['account_info']['account_holder'] = 'YUAN JUNG CHENG'
        
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
        has_symbol_column = bool(re.search(r'Symbol/', trans_text) and re.search(r'CUSIP', trans_text))
        
        if has_symbol_column:
            # Complex format with detailed stock transactions
            return self.parse_schwab_detailed_transactions(trans_text, current_year)
        else:
            # Simple format with basic transactions
            return self.parse_schwab_simple_transactions(trans_text, current_year)
    
    def parse_schwab_simple_transactions(self, trans_text: str, current_year: str) -> List[Dict]:
        """Parse simple Schwab transaction format (like 563 account)"""
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
        """Parse detailed Schwab transaction format with stocks (like 088 account)"""
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
                
                # Parse detailed transaction line
                transaction = self.parse_detailed_transaction_line(rest_of_line, current_date)
                if transaction:
                    transactions.append(transaction)
                
                i += 1
                
            elif current_date and line.strip():
                # This might be a continuation line without a date - use the current date
                # Skip lines that are just industry fees or other non-transaction info
                if not any(skip_word in line for skip_word in ['Industry Fee', 'JUNG CHE']):
                    # Check if this looks like a transaction line (has category/action)
                    if any(indicator in line for indicator in ['Sale', 'Purchase', 'Buy', 'Withdrawal', 'Deposit', 'Interest', 'Dividend']):
                        transaction = self.parse_detailed_transaction_line(line.strip(), current_date)
                        if transaction:
                            transactions.append(transaction)
                i += 1
            else:
                i += 1
        
        return transactions
    
    def parse_detailed_transaction_line(self, line: str, transaction_date: str) -> Dict:
        """Parse a single detailed transaction line from Schwab format"""
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
        if 'Sale' in line:
            transaction['transaction_type'] = 'SELL'
        elif 'Purchase' in line or 'Buy' in line:
            transaction['transaction_type'] = 'BUY'
        elif 'Withdrawal' in line:
            transaction['transaction_type'] = 'WITHDRAWAL'
        elif 'Deposit' in line:
            transaction['transaction_type'] = 'DEPOSIT'
        elif 'Interest' in line and 'Credit Interest' in line:
            transaction['transaction_type'] = 'INTEREST'
        elif 'Interest' in line and 'NRA Tax' in line:
            transaction['transaction_type'] = 'TAX'
        elif 'Dividend' in line:
            transaction['transaction_type'] = 'DIVIDEND'
        
        # For stock transactions, parse the structured format
        if transaction['transaction_type'] in ['SELL', 'BUY']:
            # Extract symbol (3-5 uppercase letters)
            symbol_match = re.search(r'\b([A-Z]{3,5})\b', line)
            if symbol_match:
                potential_symbol = symbol_match.group(1)
                # Filter out common non-symbol words
                if potential_symbol not in ['CORP', 'INC', 'LLC', 'CLASS', 'FUND', 'SCHWAB', 'BANK', 'TFRD', 'JUNG', 'AMEX']:
                    transaction['symbol'] = potential_symbol
            
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
            
            # Parse numeric values: (quantity) price fee amount gain/loss
            # Example: "(45.0000) 536.6201 0.01 24,147.89 13,122.89,(LT)"
            numeric_match = re.search(r'\((\d+\.?\d*)\)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)', line)
            if numeric_match:
                try:
                    transaction['quantity'] = float(numeric_match.group(1).replace(',', ''))
                    transaction['price'] = float(numeric_match.group(2).replace(',', ''))
                    transaction['fee'] = float(numeric_match.group(3).replace(',', ''))
                    transaction['amount'] = float(numeric_match.group(4).replace(',', ''))
                    
                    # Extract realized gain/loss (may have ,LT or ,ST suffix)
                    gain_loss_str = numeric_match.group(5).replace(',', '')
                    gain_loss_match = re.search(r'(\d+\.?\d*)', gain_loss_str)
                    if gain_loss_match:
                        transaction['realized_gain_loss'] = float(gain_loss_match.group(1))
                    
                    # Set quantity sign based on transaction type
                    if transaction['transaction_type'] == 'SELL':
                        transaction['quantity'] = -abs(transaction['quantity'])
                    elif transaction['transaction_type'] == 'BUY':
                        transaction['quantity'] = abs(transaction['quantity'])
                    
                    # Use standardized amount handling
                    self.standardize_transaction_amount(transaction)
                    
                except (ValueError, IndexError) as e:
                    # If parsing fails, still return the transaction but with zero values
                    pass
        
        else:
            # For non-stock transactions, look for simple amounts
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
            'account_id': 'TDA-088',
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
                    'amount': 0
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
                            transaction['quantity'] = float(qty_match.group(1).replace(',', ''))
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
                    
                    transaction_type = '買進' if '現買' in str(row.get('買賣別', '')) else '賣出'
                    price = float(str(row.get('成交價', '0')).replace(',', '')) if row.get('成交價') else 0
                    cost = float(str(row.get('成本', '0')).replace(',', '')) if row.get('成本') else 0
                    fee = float(str(row.get('手續費', '0')).replace(',', '')) if pd.notna(row.get('手續費')) else 0
                    tax = float(str(row.get('交易稅', '0')).replace(',', '')) if pd.notna(row.get('交易稅')) else 0
                    order_id = str(row.get('委託書號', ''))
                    
                    # Convert date format
                    if '/' in date_str:
                        date_obj = datetime.strptime(date_str, '%Y/%m/%d')
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                    else:
                        formatted_date = date_str
                    
                    transaction = {
                        'transaction_date': formatted_date,
                        'symbol': symbol,
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
            
            cursor.execute("""
                INSERT OR REPLACE INTO accounts 
                (account_id, institution, broker, account_type, account_holder, created_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                account_info.get('account_id'),
                account_info.get('institution'),
                broker,
                account_info.get('account_type'),
                account_info.get('account_holder'),
                account_info.get('statement_date', datetime.now().isoformat())
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
            
            for transaction in data['transactions']:
                cursor.execute("""
                    INSERT OR REPLACE INTO transactions 
                    (account_id, transaction_date, symbol, transaction_type, quantity, price, 
                     amount, fee, tax, net_amount, broker, order_id, description, source_file)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    account_id,
                    transaction.get('transaction_date'),
                    transaction.get('symbol'),
                    transaction.get('transaction_type'),
                    transaction.get('quantity', 0),
                    transaction.get('price', 0),
                    transaction.get('amount', 0),
                    transaction.get('fee', 0),
                    transaction.get('tax', 0),
                    transaction.get('net_amount', 0),
                    broker,
                    transaction.get('order_id'),
                    transaction.get('description'),
                    str(file_path)
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


if __name__ == "__main__":
    # Run from project root directory to access Statements folder
    parser = MultiBrokerPortfolioParser(statements_dir="Statements")
    parser.process_all_statements()