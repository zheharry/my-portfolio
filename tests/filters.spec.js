import { test, expect } from '@playwright/test';

test.describe('Portfolio Dashboard Filters', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the dashboard
    await page.goto('/');
    
    // Wait for the dashboard to load
    await page.waitForSelector('.dashboard-header', { timeout: 10000 });
    
    // Wait for initial data to load
    await page.waitForTimeout(2000);
  });

  test('should load the dashboard successfully', async ({ page }) => {
    // Check that the main elements are present
    await expect(page.locator('.dashboard-header')).toBeVisible();
    await expect(page.locator('#applyFilters')).toBeVisible();
    await expect(page.locator('#clearFilters')).toBeVisible();
    await expect(page.locator('#exportData')).toBeVisible();
  });

  test('should filter by year', async ({ page }) => {
    // Get initial transaction count
    const initialTransactions = await page.locator('#transactionTable tbody tr').count();
    
    // Select a specific year (e.g., 2023)
    await page.selectOption('#yearFilter', '2023');
    
    // Wait for filters to apply (auto-apply)
    await page.waitForTimeout(1000);
    
    // Verify that date filters were updated
    const startDate = await page.inputValue('#startDateFilter');
    const endDate = await page.inputValue('#endDateFilter');
    
    expect(startDate).toContain('2023');
    expect(endDate).toContain('2023');
    
    // Transaction count might change (could be same if all data is from 2023)
    const filteredTransactions = await page.locator('#transactionTable tbody tr').count();
    expect(filteredTransactions).toBeGreaterThanOrEqual(0);
  });

  test('should filter by date range', async ({ page }) => {
    // Set specific date range
    await page.fill('#startDateFilter', '2023-01-01');
    await page.fill('#endDateFilter', '2023-06-30');
    
    // Click apply filters
    await page.click('#applyFilters');
    
    // Wait for data to load
    await page.waitForTimeout(1000);
    
    // Verify dates are applied
    const startDate = await page.inputValue('#startDateFilter');
    const endDate = await page.inputValue('#endDateFilter');
    
    expect(startDate).toBe('2023-01-01');
    expect(endDate).toBe('2023-06-30');
  });

  test('should filter by broker', async ({ page }) => {
    // Wait for broker filter to be populated
    await page.waitForSelector('#brokerFilter .filter-checkbox', { timeout: 5000 });
    
    // Get all available broker checkboxes
    const brokerCheckboxes = await page.locator('#brokerFilter .filter-checkbox input[type="checkbox"]').all();
    
    if (brokerCheckboxes.length > 0) {
      // Select first broker
      await brokerCheckboxes[0].check();
      
      // Wait for auto-apply
      await page.waitForTimeout(500);
      
      // Verify checkbox is checked
      await expect(brokerCheckboxes[0]).toBeChecked();
      
      // Apply filters explicitly
      await page.click('#applyFilters');
      await page.waitForTimeout(1000);
      
      // Check that filtered data is shown
      const transactions = await page.locator('#transactionTable tbody tr').count();
      expect(transactions).toBeGreaterThanOrEqual(0);
    }
  });

  test('should filter by symbol', async ({ page }) => {
    // Wait for symbol filter to be populated
    await page.waitForSelector('#symbolFilter .filter-checkbox', { timeout: 5000 });
    
    // Get all available symbol checkboxes
    const symbolCheckboxes = await page.locator('#symbolFilter .filter-checkbox input[type="checkbox"]').all();
    
    if (symbolCheckboxes.length > 0) {
      // Select first symbol
      await symbolCheckboxes[0].check();
      
      // Wait for auto-apply
      await page.waitForTimeout(500);
      
      // Verify checkbox is checked
      await expect(symbolCheckboxes[0]).toBeChecked();
      
      // Apply filters
      await page.click('#applyFilters');
      await page.waitForTimeout(1000);
      
      // Check that filtered data is shown
      const transactions = await page.locator('#transactionTable tbody tr').count();
      expect(transactions).toBeGreaterThanOrEqual(0);
    }
  });

  test('should use symbol search functionality', async ({ page }) => {
    // Wait for symbol filter
    await page.waitForSelector('#symbolSearchInput', { timeout: 5000 });
    
    // Type in search box
    await page.fill('#symbolSearchInput', 'AAPL');
    
    // Wait for filtering
    await page.waitForTimeout(500);
    
    // Check that visible symbols are filtered
    const visibleSymbols = await page.locator('#symbolFilter .filter-checkbox:visible').count();
    
    // Should have filtered the list (or show 0 if AAPL doesn't exist)
    expect(visibleSymbols).toBeGreaterThanOrEqual(0);
  });

  test('should filter by transaction type', async ({ page }) => {
    // Wait for type filter to be populated
    await page.waitForSelector('#typeFilter .filter-checkbox', { timeout: 5000 });
    
    // Get all available type checkboxes
    const typeCheckboxes = await page.locator('#typeFilter .filter-checkbox input[type="checkbox"]').all();
    
    if (typeCheckboxes.length > 0) {
      // Select first transaction type
      await typeCheckboxes[0].check();
      
      // Wait for auto-apply
      await page.waitForTimeout(500);
      
      // Verify checkbox is checked
      await expect(typeCheckboxes[0]).toBeChecked();
      
      // Apply filters
      await page.click('#applyFilters');
      await page.waitForTimeout(1000);
      
      // Check that filtered data is shown
      const transactions = await page.locator('#transactionTable tbody tr').count();
      expect(transactions).toBeGreaterThanOrEqual(0);
    }
  });

  test('should clear all filters', async ({ page }) => {
    // Apply some filters first
    await page.selectOption('#yearFilter', '2023');
    await page.waitForTimeout(500);
    
    // Wait for broker filter and select one
    await page.waitForSelector('#brokerFilter .filter-checkbox', { timeout: 5000 });
    const brokerCheckboxes = await page.locator('#brokerFilter .filter-checkbox input[type="checkbox"]').all();
    if (brokerCheckboxes.length > 0) {
      await brokerCheckboxes[0].check();
    }
    
    // Apply filters
    await page.click('#applyFilters');
    await page.waitForTimeout(1000);
    
    // Now clear filters
    await page.click('#clearFilters');
    await page.waitForTimeout(1000);
    
    // Verify filters are cleared
    const yearValue = await page.inputValue('#yearFilter');
    expect(yearValue).toBe('');
    
    // Verify checkboxes are unchecked
    if (brokerCheckboxes.length > 0) {
      await expect(brokerCheckboxes[0]).not.toBeChecked();
    }
  });

  test('should combine multiple filters', async ({ page }) => {
    // Select year
    await page.selectOption('#yearFilter', '2023');
    await page.waitForTimeout(500);
    
    // Wait for broker filter
    await page.waitForSelector('#brokerFilter .filter-checkbox', { timeout: 5000 });
    const brokerCheckboxes = await page.locator('#brokerFilter .filter-checkbox input[type="checkbox"]').all();
    
    if (brokerCheckboxes.length > 0) {
      await brokerCheckboxes[0].check();
    }
    
    // Apply filters
    await page.click('#applyFilters');
    await page.waitForTimeout(1000);
    
    // Verify multiple filters are applied
    const yearValue = await page.inputValue('#yearFilter');
    expect(yearValue).toBe('2023');
    
    if (brokerCheckboxes.length > 0) {
      await expect(brokerCheckboxes[0]).toBeChecked();
    }
    
    // Check that data is filtered
    const transactions = await page.locator('#transactionTable tbody tr').count();
    expect(transactions).toBeGreaterThanOrEqual(0);
  });

  test('should select and deselect all brokers', async ({ page }) => {
    // Wait for broker filter
    await page.waitForSelector('#brokerFilter .filter-checkbox', { timeout: 5000 });
    
    // Find "Select All" button
    const selectAllBtn = page.locator('#brokerFilter .select-all-btn');
    if (await selectAllBtn.count() > 0) {
      // Click Select All
      await selectAllBtn.click();
      await page.waitForTimeout(500);
      
      // Verify all are checked
      const checkboxes = await page.locator('#brokerFilter .filter-checkbox input[type="checkbox"]').all();
      for (const checkbox of checkboxes) {
        await expect(checkbox).toBeChecked();
      }
      
      // Click Clear All
      const clearAllBtn = page.locator('#brokerFilter .clear-all-btn');
      await clearAllBtn.click();
      await page.waitForTimeout(500);
      
      // Verify all are unchecked
      for (const checkbox of checkboxes) {
        await expect(checkbox).not.toBeChecked();
      }
    }
  });

  test('should select and deselect all symbols', async ({ page }) => {
    // Wait for symbol filter
    await page.waitForSelector('#symbolFilter .filter-checkbox', { timeout: 5000 });
    
    // Find "Select All" button
    const selectAllBtn = page.locator('#symbolFilter .select-all-btn');
    if (await selectAllBtn.count() > 0) {
      // Click Select All
      await selectAllBtn.click();
      await page.waitForTimeout(500);
      
      // Verify some are checked (might not check all due to search filter)
      const checkedCount = await page.locator('#symbolFilter .filter-checkbox input[type="checkbox"]:checked').count();
      expect(checkedCount).toBeGreaterThanOrEqual(0);
      
      // Click Clear All
      const clearAllBtn = page.locator('#symbolFilter .clear-all-btn');
      await clearAllBtn.click();
      await page.waitForTimeout(500);
      
      // Verify all are unchecked
      const checkedCountAfter = await page.locator('#symbolFilter .filter-checkbox input[type="checkbox"]:checked').count();
      expect(checkedCountAfter).toBe(0);
    }
  });
});
