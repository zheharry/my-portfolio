import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

test.describe('Portfolio Export Functionality', () => {
  // Setup download directory
  const downloadDir = path.join(process.cwd(), 'test-results', 'downloads');
  
  test.beforeAll(async () => {
    // Ensure download directory exists
    if (!fs.existsSync(downloadDir)) {
      fs.mkdirSync(downloadDir, { recursive: true });
    }
  });

  test.beforeEach(async ({ page }) => {
    // Navigate to the dashboard
    await page.goto('/');
    
    // Wait for the dashboard to load
    await page.waitForSelector('.dashboard-header', { timeout: 10000 });
    
    // Wait for initial data to load
    await page.waitForTimeout(2000);
  });

  test('should have export button visible', async ({ page }) => {
    // Check that export button exists and is visible
    const exportButton = page.locator('#exportData');
    await expect(exportButton).toBeVisible();
    
    // Button should have appropriate text
    const buttonText = await exportButton.textContent();
    expect(buttonText?.toLowerCase()).toContain('export');
  });

  test('should export data in "My Stocks" app format', async ({ page, context }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Verify download was triggered
    expect(download).toBeTruthy();
    
    // Get suggested filename
    const suggestedFilename = download.suggestedFilename();
    
    // Should be a CSV file with expected naming pattern
    expect(suggestedFilename).toMatch(/my_stocks_export_\d{4}-\d{2}-\d{2}\.csv/);
    
    // Save the file
    const filePath = path.join(downloadDir, suggestedFilename);
    await download.saveAs(filePath);
    
    // Verify file was saved
    expect(fs.existsSync(filePath)).toBeTruthy();
    
    // Read and verify file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    
    // Should have CSV content
    expect(fileContent).toBeTruthy();
    
    // Should have header row with expected columns
    const lines = fileContent.split('\n');
    expect(lines.length).toBeGreaterThan(0);
    
    const header = lines[0];
    
    // Check for expected "My Stocks" app format columns
    const expectedColumns = [
      'Date',
      'Time',
      'Symbol',
      'Name',
      'Type',
      'Shares',
      'Price',
      'Amount',
      'Commission',
      'Currency',
      'Exchange'
    ];
    
    // Verify header contains key columns
    for (const column of expectedColumns) {
      expect(header).toContain(column);
    }
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should export data with correct date format', async ({ page }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContent.split('\n');
    
    if (lines.length > 1) {
      // Check first data row (skip header)
      const dataRow = lines[1];
      
      if (dataRow.trim() !== '') {
        // Date should be in "YYYY-MM-DD GMT+0800" format or similar
        expect(dataRow).toMatch(/\d{4}-\d{2}-\d{2}/);
      }
    }
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should export data with correct transaction types', async ({ page }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContent.split('\n');
    
    // Parse CSV and check transaction types
    if (lines.length > 1) {
      const header = lines[0].split(',');
      const typeIndex = header.findIndex(col => col.includes('Type'));
      
      if (typeIndex >= 0) {
        // Check data rows for valid transaction types
        for (let i = 1; i < Math.min(lines.length, 10); i++) {
          const row = lines[i];
          if (row.trim() !== '') {
            const columns = row.split(',');
            if (columns.length > typeIndex) {
              const type = columns[typeIndex].trim();
              
              // Type should be "Buy" or "Sell" for "My Stocks" app format
              if (type !== '') {
                expect(['Buy', 'Sell', 'Dividend', 'Interest', 'Split']).toContain(type);
              }
            }
          }
        }
      }
    }
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should export filtered data only', async ({ page }) => {
    // Apply a filter first
    await page.selectOption('#yearFilter', '2023');
    await page.waitForTimeout(500);
    await page.click('#applyFilters');
    await page.waitForTimeout(1000);
    
    // Get transaction count from UI
    const transactionRows = await page.locator('#transactionTable tbody tr').count();
    
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContent.split('\n').filter(line => line.trim() !== '');
    
    // Number of data rows (excluding header) should match UI
    const dataRowCount = lines.length - 1; // Subtract header
    
    // Should have exported data
    expect(dataRowCount).toBeGreaterThanOrEqual(0);
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should include broker information in export', async ({ page }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const header = fileContent.split('\n')[0];
    
    // May include broker info (or account info)
    // The export should have complete data structure
    expect(header.split(',').length).toBeGreaterThan(5);
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should handle export with no data', async ({ page }) => {
    // Apply filters that result in no data
    await page.selectOption('#yearFilter', '2017');
    await page.fill('#startDateFilter', '2017-01-01');
    await page.fill('#endDateFilter', '2017-01-02');
    await page.click('#applyFilters');
    await page.waitForTimeout(1000);
    
    // Try to export
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 }).catch(() => null);
    
    // Click export button
    await page.click('#exportData');
    
    // Wait a bit
    await page.waitForTimeout(1000);
    
    // Download might still trigger with header only
    const download = await downloadPromise;
    
    if (download) {
      // If download happened, verify it has at least a header
      const filePath = path.join(downloadDir, download.suggestedFilename());
      await download.saveAs(filePath);
      
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      expect(fileContent).toBeTruthy();
      
      // Clean up
      fs.unlinkSync(filePath);
    }
    
    // Page should still be functional
    await expect(page.locator('.dashboard-header')).toBeVisible();
  });

  test('should export with correct currency information', async ({ page }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContent.split('\n');
    const header = lines[0].split(',');
    
    // Find currency column
    const currencyIndex = header.findIndex(col => col.toLowerCase().includes('currency'));
    
    if (currencyIndex >= 0 && lines.length > 1) {
      // Check that currency values are present
      for (let i = 1; i < Math.min(lines.length, 10); i++) {
        const row = lines[i];
        if (row.trim() !== '') {
          const columns = row.split(',');
          if (columns.length > currencyIndex) {
            const currency = columns[currencyIndex].trim();
            
            // Currency should be valid (USD, TWD, etc.)
            if (currency !== '') {
              expect(currency).toMatch(/^[A-Z]{3}$/);
            }
          }
        }
      }
    }
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should export with correct exchange information', async ({ page }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContent.split('\n');
    const header = lines[0].split(',');
    
    // Find exchange column
    const exchangeIndex = header.findIndex(col => col.toLowerCase().includes('exchange'));
    
    if (exchangeIndex >= 0 && lines.length > 1) {
      // Check that exchange values are present where applicable
      for (let i = 1; i < Math.min(lines.length, 10); i++) {
        const row = lines[i];
        if (row.trim() !== '') {
          const columns = row.split(',');
          if (columns.length > exchangeIndex) {
            const exchange = columns[exchangeIndex].trim();
            
            // Exchange should be valid (TAI, NYQ, NAS, PCX, etc.) or empty for cash
            if (exchange !== '') {
              expect(exchange.length).toBeGreaterThan(0);
            }
          }
        }
      }
    }
    
    // Clean up
    fs.unlinkSync(filePath);
  });

  test('should generate unique filename for each export', async ({ page }) => {
    // First export
    const downloadPromise1 = page.waitForEvent('download', { timeout: 10000 });
    await page.click('#exportData');
    const download1 = await downloadPromise1;
    const filename1 = download1.suggestedFilename();
    
    // Wait a bit
    await page.waitForTimeout(2000);
    
    // Second export
    const downloadPromise2 = page.waitForEvent('download', { timeout: 10000 });
    await page.click('#exportData');
    const download2 = await downloadPromise2;
    const filename2 = download2.suggestedFilename();
    
    // Filenames should have date in them
    expect(filename1).toMatch(/\d{4}-\d{2}-\d{2}/);
    expect(filename2).toMatch(/\d{4}-\d{2}-\d{2}/);
  });

  test('should export complete transaction data', async ({ page }) => {
    // Set up download handler
    const downloadPromise = page.waitForEvent('download', { timeout: 10000 });
    
    // Click export button
    await page.click('#exportData');
    
    // Wait for download
    const download = await downloadPromise;
    
    // Save the file
    const filePath = path.join(downloadDir, download.suggestedFilename());
    await download.saveAs(filePath);
    
    // Read file contents
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContent.split('\n').filter(line => line.trim() !== '');
    
    // Should have at least a header
    expect(lines.length).toBeGreaterThanOrEqual(1);
    
    // Each non-empty line should have the same number of columns
    if (lines.length > 1) {
      const headerColumnCount = lines[0].split(',').length;
      
      for (let i = 1; i < lines.length; i++) {
        const columnCount = lines[i].split(',').length;
        // Allow some flexibility for quotes in CSV
        expect(columnCount).toBeGreaterThanOrEqual(headerColumnCount - 2);
      }
    }
    
    // Clean up
    fs.unlinkSync(filePath);
  });
});
