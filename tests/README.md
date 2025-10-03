# E2E Tests for My Portfolio

This directory contains Playwright E2E tests for the My Portfolio web application.

## Test Structure

- **filters.spec.js** - Tests for all filter functionality (year, date, broker, symbol, type)
- **data.spec.js** - Tests for data validation (realized P&L, fees, taxes, net amounts)
- **export.spec.js** - Tests for export functionality to "My Stocks" app format

## Prerequisites

- Node.js (v16 or higher)
- Python 3.7+ with Flask
- Portfolio application set up and ready to run

## Installation

Install dependencies using the test runner script:

```bash
./test-runner.sh install
```

Or manually:

```bash
npm install
npx playwright install chromium webkit
```

## Running Tests

### Using test-runner.sh (Recommended)

The `test-runner.sh` script provides a convenient way to manage the server and run tests:

```bash
# Full workflow: install, start server, run all tests, show report, stop server
./test-runner.sh install && ./test-runner.sh start && ./test-runner.sh test-all && ./test-runner.sh report && ./test-runner.sh stop

# Start the server
./test-runner.sh start

# Run all tests
./test-runner.sh test-all

# Run tests in headed mode (visible browser)
./test-runner.sh test-all --headed

# Run specific test suites
./test-runner.sh test-filters
./test-runner.sh test-data
./test-runner.sh test-export

# Run specific test file
./test-runner.sh test tests/filters.spec.js

# Show test report
./test-runner.sh report

# Stop the server
./test-runner.sh stop

# Show help
./test-runner.sh help
```

### Using npm scripts

```bash
# Start server separately (in another terminal)
python app.py

# Run all tests
npm test

# Run in headed mode (visible browser)
npm run test:headed

# Run specific test suite
npm run test:filters
npm run test:data
npm run test:export

# Show test report
npm run test:report
```

### Using Playwright directly

```bash
# Run all tests
npx playwright test

# Run in headed mode
npx playwright test --headed

# Run with UI mode (interactive)
npx playwright test --ui

# Run specific test file
npx playwright test tests/filters.spec.js

# Run in debug mode
npx playwright test --debug
```

## Browser Support

Tests are configured to run on:
- **Chromium** (Chrome-like browsers)
- **WebKit** (Safari)
- **Microsoft Edge**

Mobile browsers are explicitly excluded as per requirements.

## Test Coverage

### Filter Tests (filters.spec.js)
- ✅ Dashboard loads successfully
- ✅ Year filter functionality
- ✅ Date range filter functionality
- ✅ Broker filter functionality
- ✅ Symbol filter functionality
- ✅ Symbol search functionality
- ✅ Transaction type filter functionality
- ✅ Clear all filters
- ✅ Combine multiple filters
- ✅ Select/deselect all brokers
- ✅ Select/deselect all symbols

### Data Validation Tests (data.spec.js)
- ✅ Realized P&L display
- ✅ Fee information display
- ✅ Tax information display
- ✅ Net amount display
- ✅ Transaction count calculation
- ✅ Portfolio summary metrics
- ✅ Transaction details in table
- ✅ Currency value formatting
- ✅ Date value formatting
- ✅ Empty data handling
- ✅ Performance data display
- ✅ Numerical calculation consistency
- ✅ Broker information display
- ✅ Symbol information display
- ✅ Data updates with filters

### Export Tests (export.spec.js)
- ✅ Export button visibility
- ✅ Export in "My Stocks" app format
- ✅ Correct date format in export
- ✅ Correct transaction types in export
- ✅ Export filtered data only
- ✅ Broker information in export
- ✅ Export with no data
- ✅ Currency information in export
- ✅ Exchange information in export
- ✅ Unique filename generation
- ✅ Complete transaction data export

## Test Reports

After running tests, reports are generated in:
- **HTML Report**: `test-results/html-report/index.html`
- **JSON Report**: `test-results/results.json`
- **JUnit XML**: `test-results/results.xml`

View the HTML report:
```bash
npx playwright show-report
# or
./test-runner.sh report
```

## Configuration

Test configuration is in `playwright.config.js`:
- Base URL: `http://127.0.0.1:5001`
- Screenshots: On failure
- Videos: On first retry
- Trace: On first retry

## Troubleshooting

### Server not starting
- Ensure port 5001 is not in use: `lsof -i :5001`
- Check server logs: `cat /tmp/portfolio-server.log`

### Tests timing out
- Ensure server is running before tests
- Increase timeout in test configuration if needed
- Check network connectivity

### Browser installation issues
- Run: `npx playwright install-deps`
- Ensure you have sufficient disk space

### Tests failing
- Check if application has data to test with
- Verify database exists and has transactions
- Review test screenshots in `test-results/` directory

## CI/CD Integration

Tests can be integrated into CI/CD pipelines. Example:

```yaml
# GitHub Actions example
- name: Install dependencies
  run: ./test-runner.sh install

- name: Start server
  run: ./test-runner.sh start

- name: Run tests
  run: ./test-runner.sh test-all

- name: Stop server
  run: ./test-runner.sh stop
```

## Development

### Adding New Tests

1. Create a new `.spec.js` file in the `tests/` directory
2. Import Playwright test utilities
3. Write your test cases
4. Add npm script in `package.json` if needed
5. Update this README

### Test Best Practices

- Use meaningful test descriptions
- Wait for elements before interacting
- Use appropriate timeouts
- Clean up after tests (downloads, etc.)
- Test both happy and error paths
- Keep tests independent and isolated

## Notes

- Server runs on port **5001** (configured via FLASK_PORT environment variable)
- Tests use the desktop-only configuration (no mobile browsers)
- Download tests save files to `test-results/downloads/`
- Tests are designed to work with real data or empty database
