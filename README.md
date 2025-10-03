# My Portfolio

A personal portfolio analysis and visualization tool that processes broker statements from multiple sources and provides a web interface for viewing portfolio performance.

## Features

- Multi-broker support (TD Ameritrade, Charles Schwab, 國泰證券)
- Automated parsing of PDF and CSV statements
- Transaction analysis and portfolio performance tracking
- Web dashboard with filtering and visualization
- SQLite database for data storage

## Getting Started

### Prerequisites

- Python 3.7+
- pip (Python package installer)

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd my-portfolio
   ```

2. Install required Python packages:
   ```bash
   pip install flask pandas sqlite3 pathlib
   ```

### Configuration

The parser uses generic placeholders for sensitive information by default and works without any configuration. Account identification is done automatically from PDF content when possible.

**Optional customization:**

1. **Copy the configuration template (optional):**
   ```bash
   cp config/user_config_template.json config/user_config.json
   ```

2. **Edit `config/user_config.json` for display preferences (optional):**
   - Customize account display names and labels
   - Set preferences for account detection methods
   - All actual account numbers and sensitive data are handled generically

3. **Note:** The `user_config.json` file is automatically ignored by git to protect your privacy, but is not required for the parser to function.

### Running Portfolio Analysis

#### Process All Broker Statements

To analyze all broker statements (PDF and CSV files) in the `Statements/` directory:

```bash
.venv/bin/python -c "
from scripts.multi_broker_parser import MultiBrokerPortfolioParser
parser = MultiBrokerPortfolioParser()
parser.process_all_statements()
"
```

#### Process Individual CSV File

To process a specific CSV file:

```bash
python -c "
from app import PortfolioAPI
api = PortfolioAPI()
api.load_csv_data('path/to/your/file.csv')
"
```

### Starting the Web Application

To start the web dashboard:

```bash
python app.py
```

The web application will be available at: `http://localhost:5000`

#### Web Dashboard Features

- **Main Dashboard**: Overview of portfolio performance
- **Transactions**: View and filter all transactions by broker, date, symbol, etc.
- **Performance**: Year-over-year performance analysis
- **Accounts**: View all connected broker accounts
- **API Endpoints**: RESTful API for programmatic access

#### Available API Endpoints

- `/api/accounts` - Get all accounts
- `/api/brokers` - Get all brokers
- `/api/transactions` - Get filtered transactions
- `/api/summary` - Get portfolio summary
- `/api/performance` - Get performance by year
- `/api/process-all-statements` - Process all statements (POST)

## File Structure

```
Statements/          # Broker statements (PDF/CSV) - ignored by git
├── 2017/
├── 2018/
└── ...

data/
├── database/        # SQLite database files - ignored by git
├── processed/       # Processed data files - ignored by git
└── raw_extracts/    # Raw extracted data - ignored by git

scripts/
├── multi_broker_parser.py    # Main parsing logic
└── ...

templates/           # Flask HTML templates
static/             # Web assets (CSS, JS)
config/             # Configuration files
```

## Data Privacy & Security

This application processes sensitive financial data with strong privacy protections:

### Built-in Privacy Features
- **No hardcoded personal data**: All sensitive information uses generic placeholders by default
- **Local data only**: All personal financial information is stored locally and never transmitted externally
- **Configuration protection**: User-specific settings are kept in ignored config files that never get committed to version control
- **Secure defaults**: Parser works with anonymized data out of the box

### Protected Data Types
The `.gitignore` file is configured to prevent accidental upload of:
- Broker statements and documents (`*.pdf`, `*.PDF`, `*.csv`)
- Database files (`*.db`, `*.sqlite`, `*.sqlite3`)
- Personal configuration (`user_config.json`)
- Personal financial data patterns
- Cache and temporary files

### Privacy Best Practices
1. **Never commit sensitive configuration files**
2. **Use the provided template for customization**
3. **Keep account numbers and personal names in local config only**
4. **Review git status before committing to ensure no sensitive files are staged**

## Data Privacy

This application processes sensitive financial data. All personal financial information is stored locally and never transmitted externally.

## Broker Transaction Mapping Rules

This section documents how different broker transaction types are mapped to standardized Categories and Actions for consistent display across the web interface.

### 1. 國泰證券 (Cathay Securities) - Taiwan

**File Format:** CSV  
**Currency:** NTD (New Taiwan Dollar)  
**Account Format:** CATHAY-{account_number}

| Original Transaction Type | Category | Action | Description |
|--------------------------|----------|---------|-------------|
| `買進`, `現買` | Buy | Securities Purchased | Stock purchase transactions |
| `賣出`, `現賣` | Sell | Securities Sold | Stock sale transactions |

**Special Features:**
- Chinese stock names are mapped to US ticker symbols where applicable
- Automatic currency detection (NTD)
- Fee and tax extraction from CSV columns
- Support for quantity, price, and net amount calculations

### 2. Charles Schwab (Post-TDA Merger)

**File Format:** PDF  
**Currency:** USD  
**Account Format:** SCHWAB-{account_number}

| Original Transaction Type | Category | Action | Description |
|--------------------------|----------|---------|-------------|
| `BUY`, `Buy`, `Purchase` | Buy | Securities Purchased | Stock/security purchases |
| `SELL`, `Sale`, `Sell` | Sell | Securities Sold | Stock/security sales |
| `INTEREST` | Interest | Credit | Interest income |
| `WITHDRAWAL` | Withdrawal | Cash Debit | Money withdrawn from account |
| `DEPOSIT` | Deposit | Cash Credit | Money deposited to account |
| `DIVIDEND` | Dividend | Income | Dividend payments |
| `TAX` | Interest | NRA Tax | Non-resident alien tax withheld |
| `JOURNAL` | Withdrawal | Journaled | Journal transfers between accounts |
| `MONEYLINK` | Withdrawal | MoneyLink | MoneyLink transfer transactions |

**Transaction Detection Patterns:**
- **Buy:** `purchase`, `buy`, `bought`, `subscription`
- **Sell:** `sale`, `sold`, `redemption`
- **Interest:** `interest`, `credit interest`, `SCHWAB1 INT`
- **Withdrawal:** `withdrawal`, `MoneyLink Txn`
- **Deposit:** `deposit`, `funds received`
- **Tax:** `NRA tax`, `tax`

### 3. TD Ameritrade (Legacy/Historical)

**File Format:** PDF  
**Currency:** USD  
**Account Format:** TDA-{account_number}

| Original Transaction Type | Category | Action | Description |
|--------------------------|----------|---------|-------------|
| `BUY` | Buy | Securities Purchased | Stock/security purchases |
| `SELL` | Sell | Securities Sold | Stock/security sales |
| `DIVIDEND` | Dividend | Income | Dividend payments via "Div/Int - Income" |
| `INTEREST` | Interest | Credit | Interest income |
| `JOURNAL` | Withdrawal | Journaled | Journal transfers ("Journal - Other") |
| `TRANSFER` | Withdrawal | Cash Debit | Account transfers |

**Transaction Detection Patterns:**
- **Buy:** `Buy - Securities Purchased`
- **Sell:** `Sell - Securities Sold`
- **Dividend:** `Div/Int - Income`, `QUALIFIED DIVIDENDS`
- **Journal:** `Journal - Other`, `Journal - Funds Disbursed`
- **Transfer:** `TDA TO CS&CO TRANSFER`

### 4. Unknown/Other Brokers

**Fallback Rules:**

| Input Condition | Category | Action | Description |
|----------------|----------|---------|-------------|
| `net_amount > 0` AND `symbol = null` | Deposit | Cash Credit | Generic positive cash flow |
| `net_amount < 0` AND `symbol = null` | Withdrawal | Cash Debit | Generic negative cash flow |
| Unrecognized transaction_type | [transaction_type] | - | Pass-through original type |

### Transaction Type Standardization

The parser applies consistent cash flow logic:

**Negative Amounts (Cash Out):**
- `BUY` - Purchasing securities
- `WITHDRAWAL` - Money leaving account  
- `TAX` - Tax payments

**Positive Amounts (Cash In):**
- `SELL` - Selling securities
- `DEPOSIT` - Money entering account
- `DIVIDEND` - Dividend income
- `INTEREST` - Interest income
- `JOURNAL` - Journal credits
- `OTHER` - Other income

### Configuration Files

Parsing rules are defined in:
- `/config/parsing_rules.json` - Main parsing patterns and keywords
- `/archive/development-tools/parsing_rules.json` - Development/legacy rules
- `/scripts/multi_broker_parser.py` - Core parsing logic
- `/app.py` - Web UI mapping function (`map_transaction_to_category_action`)

### Adding New Brokers

To add support for a new broker:

1. **Update broker configuration** in `multi_broker_parser.py`:
   ```python
   'NEW_BROKER': {
       'name': 'New Broker Name',
       'account_patterns': [r'Account:\s*(\d+-\d+)'],
       'file_pattern': r'Statement_.*\.PDF',
       'default_account': 'NEWBROKER-001'
   }
   ```

2. **Add transaction type mapping** in `app.py`:
   ```python
   elif transaction_type in ['NEW_BUY_TYPE']:
       return {'category': 'Buy', 'action': 'Securities Purchased'}
   ```

3. **Update parsing patterns** in `config/parsing_rules.json`:
   ```json
   "transaction_keywords": {
       "buy_patterns": ["BUY", "PURCHASE", "NEW_BUY_KEYWORD"]
   }
   ```

## Testing

### End-to-End (E2E) Tests

This project includes comprehensive Playwright E2E tests for the web dashboard.

#### Test Coverage
- **Filter Tests**: Year, date, broker, symbol, and transaction type filters
- **Data Validation**: Realized P&L, fees, taxes, net amounts
- **Export Functionality**: Export to "My Stocks" app CSV format

#### Quick Start

```bash
# Install dependencies
./test-runner.sh install

# Start server on port 5001
./test-runner.sh start

# Run all tests
./test-runner.sh test-all

# Run tests in headed mode (visible browser)
./test-runner.sh test-all --headed

# Show test report
./test-runner.sh report

# Stop server
./test-runner.sh stop
```

#### Test Runner Commands

The `test-runner.sh` script provides convenient commands for managing the test server and running tests:

```bash
./test-runner.sh start           # Start Flask server on port 5001
./test-runner.sh stop            # Stop Flask server
./test-runner.sh install         # Install all dependencies
./test-runner.sh test-all        # Run all E2E tests
./test-runner.sh test-filters    # Run filter tests only
./test-runner.sh test-data       # Run data validation tests only
./test-runner.sh test-export     # Run export tests only
./test-runner.sh test FILE       # Run specific test file
./test-runner.sh report          # Show test report
./test-runner.sh help            # Show help message
```

For more details, see [tests/README.md](tests/README.md).

## Development Status

This project is actively in development. Current features include multi-broker statement processing and basic web dashboard functionality.