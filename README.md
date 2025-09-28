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

The parser uses generic placeholders for sensitive information by default. To customize for your specific accounts:

1. **Copy the configuration template:**
   ```bash
   cp config/user_config_template.json config/user_config.json
   ```

2. **Edit `config/user_config.json` with your account details:**
   - Replace `"Your Account Holder Name"` with your actual name
   - Replace `"XXXX-XXXX"` and `"YYYY-YYYY"` with your actual account numbers
   - Update filename patterns if your statements use different naming conventions

3. **Note:** The `user_config.json` file is automatically ignored by git to protect your privacy.

### Running Portfolio Analysis

#### Process All Broker Statements

To analyze all broker statements (PDF and CSV files) in the `Statements/` directory:

```bash
python -c "
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

## Development Status

This project is actively in development. Current features include multi-broker statement processing and basic web dashboard functionality.