# Playwright E2E Testing Implementation

This document provides an overview of the E2E testing implementation for the My Portfolio application.

## Quick Start

```bash
# 1. Install dependencies
./test-runner.sh install

# 2. Start the server
./test-runner.sh start

# 3. Run all tests
./test-runner.sh test-all

# 4. View test report
./test-runner.sh report

# 5. Stop the server
./test-runner.sh stop
```

## What Was Implemented

### 1. Test Runner Script (`test-runner.sh`)

A comprehensive bash script that manages the entire test lifecycle:

- **Server Management**
  - Start Flask server on port 5001
  - Stop server and clean up processes
  - Check server status and health

- **Dependency Management**
  - Install Python dependencies (if requirements.txt exists)
  - Install Node.js dependencies
  - Install Playwright browsers

- **Test Execution**
  - Run all tests: `./test-runner.sh test-all`
  - Run specific suites: `./test-runner.sh test-filters`
  - Run individual files: `./test-runner.sh test tests/filters.spec.js`
  - Headed mode support: `./test-runner.sh test-all --headed`

- **Reporting**
  - Generate HTML, JSON, and JUnit reports
  - View reports: `./test-runner.sh report`

### 2. Playwright Configuration (`playwright.config.js`)

Configured with:
- **Base URL**: http://127.0.0.1:5001
- **Browsers**: Chromium, WebKit, Microsoft Edge (desktop only)
- **Reports**: HTML, JSON, JUnit XML
- **Features**: Screenshots on failure, video on retry, trace collection
- **No mobile browsers** (per requirements)

### 3. Test Suites

#### Filter Tests (`tests/filters.spec.js`) - 11 Tests

Tests all filter functionality as required:

1. ✅ **Year Filter**: Select year and verify date range updates
2. ✅ **Date Range Filter**: Set custom date ranges
3. ✅ **Broker Filter**: Filter by broker with checkbox selection
4. ✅ **Symbol Filter**: Filter by stock symbol with checkbox selection
5. ✅ **Transaction Type Filter**: Filter by transaction type
6. ✅ **Symbol Search**: Search and filter symbols dynamically
7. ✅ **Clear Filters**: Reset all filters to default
8. ✅ **Multiple Filters**: Combine filters together
9. ✅ **Select/Deselect All Brokers**: Bulk selection controls
10. ✅ **Select/Deselect All Symbols**: Bulk selection controls
11. ✅ **Dashboard Load**: Verify page loads correctly

#### Data Validation Tests (`tests/data.spec.js`) - 16 Tests

Tests data display and calculations as required:

1. ✅ **Realized P&L Display**: Verify realized profit/loss data
2. ✅ **Fee Information**: Check fee display in UI
3. ✅ **Tax Information**: Check tax information display
4. ✅ **Net Amount Display**: Verify net amount calculations
5. ✅ **Transaction Count**: Validate transaction counting
6. ✅ **Portfolio Summary Metrics**: Check summary cards
7. ✅ **Transaction Details**: Verify table data
8. ✅ **Currency Formatting**: Check currency display format
9. ✅ **Date Formatting**: Verify date formats
10. ✅ **Empty Data Handling**: Test with no data
11. ✅ **Performance Data**: Check performance displays
12. ✅ **Numerical Consistency**: Validate calculations
13. ✅ **Broker Information**: Verify broker data display
14. ✅ **Symbol Information**: Verify symbol data display
15. ✅ **Data Updates**: Test filter effects on data
16. ✅ **Transaction Count Calculation**: Count accuracy

#### Export Tests (`tests/export.spec.js`) - 11 Tests

Tests export functionality to "My Stocks" app format as required:

1. ✅ **Export Button**: Verify button is visible
2. ✅ **"My Stocks" Format**: Correct CSV format for app
3. ✅ **Date Format**: Export dates in "YYYY-MM-DD GMT+0800" format
4. ✅ **Transaction Types**: Map types correctly (Buy/Sell)
5. ✅ **Filtered Data Export**: Export only filtered data
6. ✅ **Broker Information**: Include broker in export
7. ✅ **Empty Data Export**: Handle no data gracefully
8. ✅ **Currency Information**: Include currency codes
9. ✅ **Exchange Information**: Include exchange data (TAI, NYQ, etc.)
10. ✅ **Unique Filenames**: Generate unique names with date
11. ✅ **Complete Data**: Verify all columns are present

#### Smoke Tests (`tests/smoke.spec.js`) - 3 Tests

Infrastructure validation tests:

1. ✅ **Framework Configuration**: Playwright setup
2. ✅ **Server Connection**: Can connect to port 5001
3. ✅ **Page Structure**: Basic HTML structure present

## Test Statistics

- **Total Test Files**: 4
- **Total Test Cases**: 41
- **Filter Tests**: 11
- **Data Validation Tests**: 16
- **Export Tests**: 11
- **Smoke Tests**: 3

## Browser Coverage

✅ **Desktop Browsers Only** (per requirements):
- Chromium (Chrome-like browsers)
- WebKit (Safari)
- Microsoft Edge

❌ **Mobile Browsers**: Explicitly excluded

## Running Tests

### Using test-runner.sh (Recommended)

```bash
# Full workflow
./test-runner.sh install
./test-runner.sh start
./test-runner.sh test-all
./test-runner.sh report
./test-runner.sh stop

# Individual commands
./test-runner.sh test-filters          # Run only filter tests
./test-runner.sh test-data             # Run only data tests
./test-runner.sh test-export           # Run only export tests
./test-runner.sh test-all --headed     # Run with visible browser
./test-runner.sh test tests/smoke.spec.js  # Run specific file
```

### Using npm scripts

```bash
# Start server manually first
python app.py  # In another terminal

# Run tests
npm test                    # All tests
npm run test:headed         # Headed mode
npm run test:filters        # Filter tests only
npm run test:data           # Data tests only
npm run test:export         # Export tests only
npm run test:report         # Show report
```

### Using Playwright directly

```bash
npx playwright test                      # All tests
npx playwright test --headed             # Headed mode
npx playwright test --ui                 # Interactive UI
npx playwright test tests/filters.spec.js  # Specific file
npx playwright test --debug              # Debug mode
```

## Test Reports

Reports are generated in `test-results/`:
- **HTML Report**: `test-results/html-report/index.html`
- **JSON Report**: `test-results/results.json`
- **JUnit XML**: `test-results/results.xml`

View HTML report:
```bash
./test-runner.sh report
# or
npx playwright show-report
```

## Requirements Mapping

All requirements from the issue have been implemented:

| Requirement | Status | Implementation |
|------------|--------|----------------|
| test-runner.sh for start/stop server | ✅ | Script with start/stop commands on port 5001 |
| Install dependencies | ✅ | `install` command in test-runner.sh |
| Test all cases | ✅ | `test-all` command |
| Test individual cases | ✅ | `test-filters`, `test-data`, `test-export` commands |
| Test with headed mode | ✅ | `--headed` flag support |
| Report generation | ✅ | HTML, JSON, JUnit reports + `report` command |
| Chrome/Edge/Safari desktop only | ✅ | Configured in playwright.config.js |
| No mobile browsers | ✅ | Mobile browsers excluded |
| Test all filters | ✅ | Year, date, broker, symbol, type filters tested |
| Test realized P&L | ✅ | Data validation suite includes P&L tests |
| Test fee data | ✅ | Fee information display tests |
| Test tax data | ✅ | Tax information display tests |
| Test net project data | ✅ | Net amount calculation tests |
| Test export function | ✅ | Full export suite for "My Stocks" app format |

## Documentation

- **Main README**: Updated with testing section
- **tests/README.md**: Comprehensive testing documentation
- **This file**: Implementation overview and quick reference

## Notes

- Server runs on **port 5001** (configured via FLASK_PORT environment variable)
- Tests work with both populated and empty databases
- Download tests save files to `test-results/downloads/`
- Browser installation requires internet connection (~500MB download)
- Tests are designed to be resilient and handle various data states

## Troubleshooting

See `tests/README.md` for detailed troubleshooting information, including:
- Server startup issues
- Browser installation problems
- Test timeout issues
- CI/CD integration examples

## Next Steps

1. Install dependencies: `./test-runner.sh install`
2. Start server: `./test-runner.sh start`
3. Run tests: `./test-runner.sh test-all`
4. Review report: `./test-runner.sh report`
5. Integrate into CI/CD pipeline (optional)

For more details, see `tests/README.md`.
