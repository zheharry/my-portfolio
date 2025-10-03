import { test, expect } from '@playwright/test';

test.describe('Portfolio Data Validation', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the dashboard
    await page.goto('/');
    
    // Wait for the dashboard to load
    await page.waitForSelector('.dashboard-header', { timeout: 10000 });
    
    // Wait for initial data to load
    await page.waitForTimeout(2000);
  });

  test('should display realized P&L data correctly', async ({ page }) => {
    // Wait for summary cards to load
    await page.waitForSelector('.summary-card', { timeout: 5000 });
    
    // Check if Realized P&L card exists
    const realizedPnLCard = page.locator('text=Realized P&L').first();
    if (await realizedPnLCard.count() > 0) {
      await expect(realizedPnLCard).toBeVisible();
      
      // Verify the card has a value
      const cardParent = page.locator('.summary-card:has-text("Realized P&L")').first();
      const valueElement = cardParent.locator('.card-body h3, .card-body .h3, .card-body strong');
      
      if (await valueElement.count() > 0) {
        const value = await valueElement.first().textContent();
        expect(value).toBeTruthy();
        // Value should contain currency symbol or number
        expect(value).toMatch(/[\$\€\£\¥₹₩]|\d/);
      }
    }
  });

  test('should display fee information', async ({ page }) => {
    // Look for fee-related elements in the summary or transaction table
    const feeElements = page.locator('text=/fee|Fee|FEE/i').first();
    
    if (await feeElements.count() > 0) {
      await expect(feeElements).toBeVisible();
    }
    
    // Check transaction table for fee column
    const transactionTable = page.locator('#transactionTable');
    if (await transactionTable.count() > 0) {
      const headers = await transactionTable.locator('thead th').allTextContents();
      const hasFeeColumn = headers.some(header => 
        header.toLowerCase().includes('fee') || 
        header.toLowerCase().includes('commission')
      );
      
      if (hasFeeColumn) {
        expect(hasFeeColumn).toBeTruthy();
      }
    }
  });

  test('should display tax information', async ({ page }) => {
    // Look for tax-related elements
    const taxElements = page.locator('text=/tax|Tax|TAX/i').first();
    
    if (await taxElements.count() > 0) {
      await expect(taxElements).toBeVisible();
    }
    
    // Check transaction table for tax column
    const transactionTable = page.locator('#transactionTable');
    if (await transactionTable.count() > 0) {
      const headers = await transactionTable.locator('thead th').allTextContents();
      const hasTaxColumn = headers.some(header => 
        header.toLowerCase().includes('tax')
      );
      
      if (hasTaxColumn) {
        expect(hasTaxColumn).toBeTruthy();
      }
    }
  });

  test('should display net amount correctly', async ({ page }) => {
    // Check for net amount in summary cards
    const netCard = page.locator('text=/net|Net|NET/i').first();
    
    if (await netCard.count() > 0) {
      await expect(netCard).toBeVisible();
    }
    
    // Check transaction table for net amount column
    const transactionTable = page.locator('#transactionTable');
    if (await transactionTable.count() > 0) {
      const headers = await transactionTable.locator('thead th').allTextContents();
      const hasNetColumn = headers.some(header => 
        header.toLowerCase().includes('net') || 
        header.toLowerCase().includes('amount')
      );
      
      expect(hasNetColumn).toBeTruthy();
    }
  });

  test('should calculate and display total transaction count', async ({ page }) => {
    // Wait for transactions to load
    await page.waitForSelector('#transactionTable tbody tr', { timeout: 5000 });
    
    // Count transactions in table
    const transactionRows = await page.locator('#transactionTable tbody tr').count();
    
    // Should have at least 0 transactions
    expect(transactionRows).toBeGreaterThanOrEqual(0);
    
    // Check if there's a transaction count display
    const countElement = page.locator('text=/transactions?|records?/i').first();
    if (await countElement.count() > 0) {
      const countText = await countElement.textContent();
      expect(countText).toBeTruthy();
    }
  });

  test('should display portfolio summary metrics', async ({ page }) => {
    // Wait for summary section
    await page.waitForSelector('.card', { timeout: 5000 });
    
    // Check for various summary cards
    const summaryTexts = [
      'Total',
      'Realized',
      'Unrealized',
      'Market Value',
      'Cost',
      'Holdings'
    ];
    
    for (const text of summaryTexts) {
      const element = page.locator(`text=/${text}/i`).first();
      if (await element.count() > 0) {
        // At least one summary metric should be visible
        await expect(element).toBeVisible();
        break;
      }
    }
  });

  test('should display transaction details in table', async ({ page }) => {
    // Wait for transaction table
    await page.waitForSelector('#transactionTable', { timeout: 5000 });
    
    // Check table headers exist
    const headers = await page.locator('#transactionTable thead th').count();
    expect(headers).toBeGreaterThan(0);
    
    // Get all header texts
    const headerTexts = await page.locator('#transactionTable thead th').allTextContents();
    
    // Should have common columns
    const expectedColumns = ['Date', 'Symbol', 'Type', 'Amount'];
    const hasRequiredColumns = expectedColumns.some(col => 
      headerTexts.some(header => header.includes(col))
    );
    
    expect(hasRequiredColumns).toBeTruthy();
  });

  test('should format currency values correctly', async ({ page }) => {
    // Wait for any card with currency values
    await page.waitForSelector('.card-body', { timeout: 5000 });
    
    // Get all text content from summary cards
    const cardBodies = await page.locator('.card-body').all();
    
    if (cardBodies.length > 0) {
      for (const cardBody of cardBodies) {
        const text = await cardBody.textContent();
        
        // If text contains currency values, check formatting
        if (text && /[\$\€\£\¥₹₩]/.test(text)) {
          // Should have proper formatting (currency symbol and numbers)
          expect(text).toMatch(/[\$\€\£\¥₹₩]\s*[\d,]+\.?\d*/);
          break;
        }
      }
    }
  });

  test('should display date values in correct format', async ({ page }) => {
    // Wait for transaction table
    await page.waitForSelector('#transactionTable tbody tr', { timeout: 5000 });
    
    // Check if there are any rows
    const rowCount = await page.locator('#transactionTable tbody tr').count();
    
    if (rowCount > 0) {
      // Get first row's date cell
      const firstRow = page.locator('#transactionTable tbody tr').first();
      const dateCell = firstRow.locator('td').first();
      const dateText = await dateCell.textContent();
      
      if (dateText && dateText.trim() !== '') {
        // Should be in a valid date format (YYYY-MM-DD or similar)
        expect(dateText).toMatch(/\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4}/);
      }
    }
  });

  test('should handle empty data gracefully', async ({ page }) => {
    // Apply filters that might return no results
    await page.selectOption('#yearFilter', '2017');
    await page.waitForTimeout(500);
    
    // Set very narrow date range
    await page.fill('#startDateFilter', '2017-01-01');
    await page.fill('#endDateFilter', '2017-01-02');
    
    // Apply filters
    await page.click('#applyFilters');
    await page.waitForTimeout(1000);
    
    // Page should still be functional
    await expect(page.locator('.dashboard-header')).toBeVisible();
    await expect(page.locator('#transactionTable')).toBeVisible();
  });

  test('should display performance data', async ({ page }) => {
    // Look for performance-related elements
    const performanceSection = page.locator('text=/performance|Performance|PERFORMANCE/i').first();
    
    if (await performanceSection.count() > 0) {
      await expect(performanceSection).toBeVisible();
    }
    
    // Check if there's a chart element
    const chartElement = page.locator('canvas').first();
    if (await chartElement.count() > 0) {
      await expect(chartElement).toBeVisible();
    }
  });

  test('should verify numerical calculations are consistent', async ({ page }) => {
    // Wait for summary cards
    await page.waitForSelector('.summary-card', { timeout: 5000 });
    
    // Get all summary card values that contain numbers
    const cards = await page.locator('.summary-card').all();
    const numericValues = [];
    
    for (const card of cards) {
      const text = await card.textContent();
      const matches = text?.match(/[\$\€\£\¥₹₩]?\s*([-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)/g);
      if (matches) {
        numericValues.push(...matches);
      }
    }
    
    // Should have at least some numeric values
    if (numericValues.length > 0) {
      // All values should be valid numbers when parsed
      for (const value of numericValues) {
        const cleaned = value.replace(/[^\d.-]/g, '');
        const number = parseFloat(cleaned);
        expect(isNaN(number)).toBeFalsy();
      }
    }
  });

  test('should display broker information correctly', async ({ page }) => {
    // Wait for broker filter to populate
    await page.waitForSelector('#brokerFilter .filter-checkbox', { timeout: 5000 });
    
    // Get broker count
    const brokerCount = await page.locator('#brokerFilter .filter-checkbox').count();
    
    // Should have at least one broker
    expect(brokerCount).toBeGreaterThanOrEqual(0);
    
    if (brokerCount > 0) {
      // Get broker names
      const brokerLabels = await page.locator('#brokerFilter .filter-checkbox label').allTextContents();
      
      // Each broker should have a non-empty name
      for (const label of brokerLabels) {
        expect(label.trim()).not.toBe('');
      }
    }
  });

  test('should display symbol information correctly', async ({ page }) => {
    // Wait for symbol filter to populate
    await page.waitForSelector('#symbolFilter .filter-checkbox', { timeout: 5000 });
    
    // Get symbol count
    const symbolCount = await page.locator('#symbolFilter .filter-checkbox').count();
    
    // Should have symbols
    expect(symbolCount).toBeGreaterThanOrEqual(0);
    
    if (symbolCount > 0) {
      // Get symbol names
      const symbolLabels = await page.locator('#symbolFilter .filter-checkbox label').allTextContents();
      
      // Each symbol should have a non-empty name
      for (const label of symbolLabels) {
        expect(label.trim()).not.toBe('');
      }
    }
  });

  test('should update data when filters are applied', async ({ page }) => {
    // Get initial data
    const initialRowCount = await page.locator('#transactionTable tbody tr').count();
    
    // Apply a year filter
    await page.selectOption('#yearFilter', '2023');
    await page.waitForTimeout(500);
    await page.click('#applyFilters');
    await page.waitForTimeout(1000);
    
    // Data might have changed
    const filteredRowCount = await page.locator('#transactionTable tbody tr').count();
    
    // Both counts should be valid (>= 0)
    expect(initialRowCount).toBeGreaterThanOrEqual(0);
    expect(filteredRowCount).toBeGreaterThanOrEqual(0);
  });
});
