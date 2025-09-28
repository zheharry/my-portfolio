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
                'name': 'TD Ameritrade',
                'account_patterns': [r'Account\s*Number:\s*(\d{3}-\d{6})', r'Account:\s*(\d{3}-\d{6})'],
                'file_pattern': r'TDA - Brokerage Statement_.*\.PDF',
                'default_account': 'TDA-088'
            },
            'SCHWAB': {
                'name': 'Charles Schwab',
                'account_patterns': [r'Account Number:\s*(\d{4}-\d{4})'],
                'file_pattern': r'Brokerage Statement_.*\.PDF',
                'default_account': 'SCHWAB-2563'
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
        
        if 'TDA' in file_name or 'TD Ameritrade' in file_name:
            return 'TDA'
        elif file_name.startswith('Brokerage Statement_') and file_path.suffix.upper() == '.PDF':
            return 'SCHWAB'
        elif file_path.suffix.lower() == '.csv':
            return 'CATHAY'
        else:
            self.logger.warning(f"Could not identify broker for {file_name}")
            return 'UNKNOWN'
    
    def parse_schwab_statement(self, text: str, file_path: Path) -> Dict:
        """Parse Charles Schwab statement data"""
        data = {
            'account_info': {},
            'transactions': [],
            'positions': [],
            'balances': {}
        }
        
        # Extract account information
        account_match = re.search(r'Account Number:\s*(\d{4}-\d{4})', text)
        if account_match:
            data['account_info']['account_id'] = f"SCHWAB-{account_match.group(1)}"
            data['account_info']['institution'] = 'Charles Schwab'
            data['account_info']['account_type'] = 'International Brokerage'
            data['account_info']['broker'] = 'SCHWAB'
        
        # Extract account holder
        holder_match = re.search(r'Account Of\s+([A-Z\s]+)', text, re.MULTILINE)
        if holder_match:
            data['account_info']['account_holder'] = holder_match.group(1).strip()
        
        # Extract statement period
        period_match = re.search(r'Statement Period:\s*([A-Za-z]+\s+\d+,\s+\d{4})\s+to\s+([A-Za-z]+\s+\d+,\s+\d{4})', text)
        if period_match:
            data['account_info']['start_date'] = period_match.group(1)
            data['account_info']['end_date'] = period_match.group(2)
            data['account_info']['statement_date'] = period_match.group(2)
        
        # Extract account value summary
        value_section = re.search(r'Account Value Summary(.*?)(?:Change in Account Value|Transactions & Fees)', text, re.DOTALL)
        if value_section:
            values_text = value_section.group(1)
            
            # Parse cash balance
            cash_match = re.search(r'Cash & Sweep Money Market Funds[^\$]*\$\s*([\d,]+\.?\d*)', values_text)
            if cash_match:
                data['balances']['cash_balance'] = float(cash_match.group(1).replace(',', ''))
            
            # Parse total investments
            investments_match = re.search(r'Total Investments Long[^\$]*\$\s*([\d,]+\.?\d*)', values_text)
            if investments_match:
                data['balances']['total_investments'] = float(investments_match.group(1).replace(',', ''))
            
            # Parse total account value
            total_match = re.search(r'Total Account Value[^\$]*\$\s*([\d,]+\.?\d*)', values_text)
            if total_match:
                data['balances']['total_account_value'] = float(total_match.group(1).replace(',', ''))
        
        # Extract positions section
        positions_section = re.search(r'Holdings Detail - Long Positions(.*?)(?:Transactions & Fees|Page \d+)', text, re.DOTALL)
        if positions_section:
            positions_text = positions_section.group(1)
            # Parse individual positions (this would need more detailed parsing based on actual format)
            data['positions'] = self.parse_schwab_positions(positions_text)
        
        # Extract transactions section - look for Transaction Detail
        transactions_section = re.search(r'Transaction Detail(.*?)(?:Terms and Conditions|Page \d+)', text, re.DOTALL)
        if transactions_section:
            transactions_text = transactions_section.group(1)
            data['transactions'] = self.parse_schwab_transactions(transactions_text)
        else:
            # Fallback: look for any transaction patterns in the text
            data['transactions'] = self.parse_schwab_transactions(text)
        
        return data
    
    def parse_schwab_positions(self, positions_text: str) -> List[Dict]:
        """Parse positions from Schwab holdings section"""
        positions = []
        # Schwab positions appear to be in Investment Detail section
        # This is a simplified parser - positions might not be present in all statements
        return positions
    
    def parse_schwab_transactions(self, transactions_text: str) -> List[Dict]:
        """Parse transactions from Schwab Transaction Detail section"""
        transactions = []
        
        # Look for Transaction Detail section
        lines = transactions_text.split('\n')
        current_year = "2022"  # Default year, will try to extract from statement
        
        # Try to extract year from the statement text
        year_match = re.search(r'Statement Period:.*(\d{4})', transactions_text)
        if year_match:
            current_year = year_match.group(1)
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip empty lines and headers
            if not line or 'Settle' in line or 'Date' in line or 'Transaction' in line:
                i += 1
                continue
            
            # Look for date patterns MM/DD MM/DD (Settle Date, Trade Date)
            date_match = re.search(r'^(\d{1,2}/\d{1,2})\s+(\d{1,2}/\d{1,2})\s+(.+)', line)
            if date_match:
                settle_date = date_match.group(1)
                trade_date = date_match.group(2)
                rest_of_line = date_match.group(3)
                
                # Format dates as YYYY-MM-DD
                settle_parts = settle_date.split('/')
                trade_parts = trade_date.split('/')
                settle_formatted = f"{current_year}-{settle_parts[0].zfill(2)}-{settle_parts[1].zfill(2)}"
                trade_formatted = f"{current_year}-{trade_parts[0].zfill(2)}-{trade_parts[1].zfill(2)}"
                
                # Parse transaction details
                transaction = {
                    'transaction_date': trade_formatted,
                    'settle_date': settle_formatted,
                    'transaction_type': 'OTHER',
                    'description': rest_of_line,
                    'symbol': '',
                    'quantity': 0,
                    'price': 0,
                    'amount': 0
                }
                
                # Identify transaction type
                line_lower = rest_of_line.lower()
                desc_lower = rest_of_line.lower()
                
                if 'dividend' in line_lower:
                    transaction['transaction_type'] = 'DIVIDEND'
                elif ('funds received' in desc_lower or 
                      'wired funds received' in desc_lower):
                    transaction['transaction_type'] = 'DEPOSIT'
                elif 'nra tax' in desc_lower or 'nra' in line_lower:
                    transaction['transaction_type'] = 'TAX'
                elif ('credit interest' in desc_lower or 
                      ('int' in desc_lower and 'interest' in desc_lower) or
                      ('schwab1 int' in desc_lower)):
                    transaction['transaction_type'] = 'INTEREST'
                elif 'debit' in line_lower:
                    if 'amex' in line_lower:
                        transaction['transaction_type'] = 'AMEX_DEBIT'
                    elif 'capital one' in line_lower:
                        transaction['transaction_type'] = 'C1_DEBIT'
                    elif 'td ameritrade' in line_lower:
                        transaction['transaction_type'] = 'TRANSFER_DEBIT'
                    else:
                        transaction['transaction_type'] = 'DEBIT'
                elif 'credit' in line_lower:
                    transaction['transaction_type'] = 'CREDIT'
                elif 'auto' in line_lower and 'debit' in line_lower:
                    transaction['transaction_type'] = 'AUTO_DEBIT'
                elif 'redeemed' in line_lower:
                    transaction['transaction_type'] = 'REDEMPTION'
                elif 'purchased' in line_lower or 'buy' in line_lower:
                    transaction['transaction_type'] = 'BUY'
                elif 'sold' in line_lower or 'sell' in line_lower:
                    transaction['transaction_type'] = 'SELL'
                else:
                    transaction['transaction_type'] = 'OTHER'
                
                # Extract amounts - look for patterns like (95.00) or 95.00
                amount_matches = re.findall(r'\(?([\d,]+\.?\d*)\)?', rest_of_line)
                if amount_matches:
                    try:
                        amount_str = amount_matches[-1].replace(',', '')  # Take the last number as amount
                        transaction['amount'] = float(amount_str)
                        # Check if amount should be negative
                        if '(' in rest_of_line or 'debit' in line_lower:
                            transaction['amount'] = -abs(transaction['amount'])
                    except (ValueError, IndexError):
                        pass
                
                # Look for symbol in the description or following lines
                symbol_match = re.search(r'\b([A-Z]{3,5})\b', rest_of_line)
                if symbol_match:
                    potential_symbol = symbol_match.group(1)
                    # Filter out common non-symbol words
                    if potential_symbol not in ['AUTO', 'DEBIT', 'AMEX', 'USD', 'FUND', 'SCH', 'LIQ']:
                        transaction['symbol'] = potential_symbol
                
                # Look ahead for additional transaction details on next lines
                j = i + 1
                while j < len(lines) and j < i + 3:  # Check next few lines
                    next_line = lines[j].strip()
                    if not next_line:
                        j += 1
                        continue
                    
                    # Stop if we hit another date line
                    if re.match(r'^\d{1,2}/\d{1,2}\s+\d{1,2}/\d{1,2}', next_line):
                        break
                    
                    # Look for symbol/description on continuation lines
                    if not transaction['symbol']:
                        symbol_match = re.search(r'\b([A-Z]{3,5})\b', next_line)
                        if symbol_match:
                            potential_symbol = symbol_match.group(1)
                            if potential_symbol not in ['AUTO', 'DEBIT', 'AMEX', 'USD', 'FUND', 'SCH', 'LIQ']:
                                transaction['symbol'] = potential_symbol
                    
                    # Look for quantity and price in format: Quantity Price Total
                    qty_price_match = re.search(r'(\d+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)', next_line)
                    if qty_price_match:
                        try:
                            transaction['quantity'] = float(qty_price_match.group(1).replace(',', ''))
                            transaction['price'] = float(qty_price_match.group(2).replace(',', ''))
                            total_amount = float(qty_price_match.group(3).replace(',', ''))
                            if transaction['amount'] == 0:
                                transaction['amount'] = total_amount
                        except ValueError:
                            pass
                    
                    j += 1
                
                transactions.append(transaction)
                i = j  # Continue from where we left off
            else:
                i += 1
        
        return transactions
    
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
                        
                        # Determine if amount should be negative based on transaction type
                        if transaction['transaction_type'] == 'BUY':
                            transaction['amount'] = -abs(transaction['amount'])
                        elif transaction['transaction_type'] in ['DIVIDEND', 'SELL']:
                            transaction['amount'] = abs(transaction['amount'])
                        
                        # Look for negative amounts in parentheses
                        if '(' in line and ')' in line:
                            paren_match = re.search(r'\((\d+\.?\d*)\)', line)
                            if paren_match:
                                transaction['amount'] = -float(paren_match.group(1))
                    
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
                    
                    # Standardize to English transaction types for consistency across all brokers
                    transaction_type = 'BUY' if '現買' in str(row.get('買賣別', '')) else 'SELL'
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