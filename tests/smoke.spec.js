import { test, expect } from '@playwright/test';

/**
 * Smoke tests to verify basic E2E test infrastructure
 * These tests validate the test setup and configuration
 */

test.describe('Smoke Tests - Infrastructure Validation', () => {
  test('test framework is configured correctly', async ({ page }) => {
    // This test verifies that:
    // 1. Playwright is properly installed
    // 2. Test configuration is valid
    // 3. Browser can be launched
    
    expect(page).toBeTruthy();
    expect(page.url()).toBeDefined();
  });

  test('can connect to base URL', async ({ page }) => {
    // Verify the base URL is accessible
    // This will check if the server is running
    
    try {
      const response = await page.goto('/', { 
        waitUntil: 'domcontentloaded',
        timeout: 10000 
      });
      
      // If we get here, connection was successful
      expect(response).toBeTruthy();
      
      if (response) {
        // Server should return a valid response
        const status = response.status();
        expect(status).toBeLessThan(500); // Not a server error
      }
    } catch (error) {
      // If connection fails, provide helpful error message
      console.error('Failed to connect to server. Please ensure:');
      console.error('1. Flask server is running on port 5001');
      console.error('2. Run: ./test-runner.sh start');
      console.error('3. Or manually: python app.py');
      throw error;
    }
  });

  test('page has expected basic structure', async ({ page }) => {
    await page.goto('/');
    
    // Verify basic HTML structure
    const html = await page.content();
    expect(html).toContain('html');
    expect(html).toContain('body');
  });
});
