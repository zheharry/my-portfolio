# Scripts Directory

This directory contains utility scripts for portfolio data processing and maintenance.

## Files

### `multi_broker_parser.py`
Main parser class for processing broker statements from multiple sources.

**Key Features:**
- Enhanced Schwab transaction categorization
- Support for AMEX, Capital One, and TD Ameritrade debit detection
- Database migration functionality

**Enhanced Transaction Types:**
- `AMEX_DEBIT` - AMEX credit card payments
- `C1_DEBIT` - Capital One credit card payments  
- `TRANSFER_DEBIT` - TD Ameritrade account transfers
- `DEBIT` - Generic debit transactions (fallback)

### `migrate_schwab_debits.py`
Standalone migration script for updating existing database records.

**Usage:**
```bash
# Preview changes (recommended first)
python scripts/migrate_schwab_debits.py --dry-run

# Execute migration  
python scripts/migrate_schwab_debits.py --force

# Custom database path
python scripts/migrate_schwab_debits.py --db-path /path/to/database.db --dry-run
```

**What it does:**
1. Identifies existing Schwab DEBIT transactions
2. Categorizes them based on description patterns:
   - AMEX payments → `AMEX_DEBIT`
   - Capital One payments → `C1_DEBIT`
   - TD Ameritrade transfers → `TRANSFER_DEBIT`
3. Provides detailed reporting of changes

## Testing

Test scripts are available in `/tmp/` during development:
- `/tmp/test_parser.py` - Unit tests for parser logic
- `/tmp/test_migration.py` - Database migration testing

## Integration

These scripts integrate with the main Flask application (`app.py`) and support:
- Web-based transaction filtering by new categories
- RESTful API endpoints for programmatic access
- Backwards compatibility with existing data