// Dashboard JavaScript for Portfolio Analysis
class PortfolioDashboard {
    constructor() {
        console.log('Initializing PortfolioDashboard...');
        this.transactions = [];
        this.performanceChart = null;
        this.distributionChart = null;
        this.currentFilters = {};
        this.brokerKeys = {}; // Store mapping from display names to backend keys
        
        try {
            this.initializeEventListeners();
            console.log('Event listeners initialized');
            this.loadInitialData();
        } catch (error) {
            console.error('Error in constructor:', error);
        }
    }

    // Initialize event listeners
    initializeEventListeners() {
        document.getElementById('applyFilters').addEventListener('click', () => this.applyFilters());
        document.getElementById('clearFilters').addEventListener('click', () => this.clearFilters());
        document.getElementById('exportData').addEventListener('click', () => this.exportData());
        
        // Auto-apply filters on change for better UX
        const filterElements = [
            'yearFilter', 'startDateFilter', 'endDateFilter'
        ];
        
        filterElements.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.addEventListener('change', () => this.applyFilters());
            }
        });
    }

    // Load initial data
    async loadInitialData() {
        console.log('Starting to load initial data...');
        this.showLoading(true);
        try {
            console.log('Checking data freshness...');
            await this.checkDataFreshness();
            console.log('Data freshness checked');
            
            console.log('Loading filter options...');
            await this.loadFilterOptions();
            console.log('Filter options loaded');
            
            // Initialize multi-select controls after options are loaded
            this.initializeMultiSelectControls();
            console.log('Multi-select controls initialized');
            
            console.log('Loading transactions...');
            await this.loadTransactions();
            console.log('Transactions loaded');
            
            console.log('Loading summary...');
            await this.loadSummary();
            console.log('Summary loaded');
            
            console.log('Loading performance data...');
            await this.loadPerformanceData();
            console.log('Performance data loaded');
            
            console.log('All initial data loaded successfully!');
        } catch (error) {
            console.error('Error loading initial data:', error);
            console.error('Stack trace:', error.stack);
            this.showError('載入資料時發生錯誤，請重新整理頁面: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }

    // Load filter options
    async loadFilterOptions() {
        try {
            // Load symbols
            const symbols = await this.fetchAPI('/api/symbols');
            this.populateSelect('symbolFilter', symbols);

            // Load brokers - API returns array directly now
            const brokers = await this.fetchAPI('/api/brokers');
            this.populateSelect('brokerFilter', brokers);

            // Populate years (2017-2025)
            const currentYear = new Date().getFullYear();
            const years = [];
            for (let year = 2017; year <= currentYear; year++) {
                years.push(year.toString());
            }
            this.populateSelect('yearFilter', years);

        } catch (error) {
            console.error('Error loading filter options:', error);
        }
    }

    // Populate select element
    populateSelect(selectId, options) {
        const select = document.getElementById(selectId);
        if (!select) return;

        // Check if this is a multi-select checkbox container
        if (select.classList.contains('multi-select-list')) {
            this.populateMultiSelectCheckboxes(selectId, options);
            return;
        }

        // Handle traditional single selects (year, currency, etc.)
        const firstOption = select.options[0];
        select.innerHTML = '';
        select.appendChild(firstOption);

        options.forEach(option => {
            const optionElement = document.createElement('option');
            optionElement.value = option;
            optionElement.textContent = option;
            select.appendChild(optionElement);
        });
    }

    // Populate multi-select checkbox container
    populateMultiSelectCheckboxes(containerId, options) {
        const container = document.getElementById(containerId);
        if (!container) return;

        // Clear existing checkboxes except for transaction type which has static options
        if (containerId !== 'transactionTypeFilter') {
            container.innerHTML = '';
        }

        options.forEach((option, index) => {
            const checkboxItem = document.createElement('div');
            checkboxItem.className = 'checkbox-item';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `${containerId}_${index}`;
            checkbox.value = option;
            checkbox.checked = true; // Select all by default

            const label = document.createElement('label');
            label.htmlFor = checkbox.id;
            label.textContent = option;

            checkboxItem.appendChild(checkbox);
            checkboxItem.appendChild(label);
            container.appendChild(checkboxItem);

            // Add change event listener
            checkbox.addEventListener('change', () => {
                this.updateSelectionCount(containerId);
                
                // If this is the broker filter, refresh symbols
                if (containerId === 'brokerFilter') {
                    this.onBrokerFilterChange();
                }
                
                this.applyFilters();
            });
        });

        // Initialize count display
        this.updateSelectionCount(containerId);
    }

    // Initialize multi-select controls
    initializeMultiSelectControls() {
        // Add event listeners for select-all buttons
        document.querySelectorAll('.select-all-btn').forEach(button => {
            button.addEventListener('click', (e) => {
                const targetId = e.target.getAttribute('data-target');
                this.selectAllItems(targetId);
            });
        });

        // Add event listeners for deselect-all buttons
        document.querySelectorAll('.deselect-all-btn').forEach(button => {
            button.addEventListener('click', (e) => {
                const targetId = e.target.getAttribute('data-target');
                this.deselectAllItems(targetId);
            });
        });

        // Initialize transaction type count (since it has static options)
        this.updateSelectionCount('transactionTypeFilter');
        
        // Add change listeners to transaction type checkboxes
        document.querySelectorAll('#transactionTypeFilter input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                this.updateSelectionCount('transactionTypeFilter');
                this.applyFilters();
            });
        });
    }

    // Select all items in a multi-select container
    selectAllItems(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const checkboxes = container.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            checkbox.checked = true;
        });

        this.updateSelectionCount(containerId);
        
        // If this is the broker filter, refresh symbols
        if (containerId === 'brokerFilter') {
            this.onBrokerFilterChange();
        }
        
        this.applyFilters();
    }

    // Deselect all items in a multi-select container
    deselectAllItems(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const checkboxes = container.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            checkbox.checked = false;
        });

        this.updateSelectionCount(containerId);
        
        // If this is the broker filter, refresh symbols
        if (containerId === 'brokerFilter') {
            this.onBrokerFilterChange();
        }
        
        this.applyFilters();
    }

    // Update selection count display
    updateSelectionCount(containerId) {
        const container = document.getElementById(containerId);
        const countElement = document.getElementById(containerId.replace('Filter', 'Count'));
        
        if (!container || !countElement) return;

        const checkboxes = container.querySelectorAll('input[type="checkbox"]');
        const checkedBoxes = container.querySelectorAll('input[type="checkbox"]:checked');
        
        countElement.textContent = `${checkedBoxes.length} / ${checkboxes.length}`;
    }

    // Apply filters
    async applyFilters() {
        this.showLoading(true);
        this.currentFilters = this.getFilterValues();
        
        try {
            await Promise.all([
                this.loadTransactions(),
                this.loadSummary(),
                this.updateCharts()
            ]);
        } catch (error) {
            console.error('Error applying filters:', error);
            this.showError('篩選資料時發生錯誤');
        } finally {
            this.showLoading(false);
        }
    }

    // Clear filters
    clearFilters() {
        const singleFilterElements = [
            'yearFilter', 'startDateFilter', 'endDateFilter'
        ];
        
        // Clear traditional single selects
        singleFilterElements.forEach(id => {
            const element = document.getElementById(id);
            if (element) {
                element.value = '';
            }
        });

        // Clear multi-select checkboxes (select all by default)
        const multiSelectContainers = ['brokerFilter', 'symbolFilter', 'transactionTypeFilter'];
        multiSelectContainers.forEach(containerId => {
            this.selectAllItems(containerId);
        });
        
        this.currentFilters = {};
        this.applyFilters();
    }

    // Get current filter values
    getFilterValues() {
        const filters = {};
        
        const filterMappings = {
            'yearFilter': 'year',
            'startDateFilter': 'start_date',
            'endDateFilter': 'end_date'
        };

        // Handle traditional single selects
        Object.entries(filterMappings).forEach(([elementId, filterKey]) => {
            const element = document.getElementById(elementId);
            if (element && element.value) {
                filters[filterKey] = element.value;
            }
        });

        // Handle multi-select checkboxes
        const multiSelectMappings = {
            'brokerFilter': 'broker',
            'symbolFilter': 'symbol',
            'transactionTypeFilter': 'transaction_type'
        };

        Object.entries(multiSelectMappings).forEach(([containerId, filterKey]) => {
            const container = document.getElementById(containerId);
            if (container) {
                const checkedBoxes = container.querySelectorAll('input[type="checkbox"]:checked');
                let selectedValues = Array.from(checkedBoxes).map(checkbox => checkbox.value);
                
                // For broker filter, convert display names to backend keys
                if (containerId === 'brokerFilter' && this.brokerKeys) {
                    selectedValues = selectedValues.map(displayName => 
                        this.brokerKeys[displayName] || displayName
                    );
                }
                
                if (selectedValues.length > 0) {
                    filters[filterKey] = selectedValues;
                }
            }
        });

        return filters;
    }

    // Handle broker filter changes - refresh symbols and clear symbol selection
    async onBrokerFilterChange() {
        try {
            // Get selected brokers
            const selectedBrokers = this.getSelectedBrokers();
            
            // Refresh symbols based on selected brokers
            await this.refreshSymbols(selectedBrokers);
            
        } catch (error) {
            console.error('Error handling broker filter change:', error);
        }
    }

    // Get currently selected brokers (converted to backend keys)
    getSelectedBrokers() {
        const container = document.getElementById('brokerFilter');
        if (!container) return [];
        
        const checkedBoxes = container.querySelectorAll('input[type="checkbox"]:checked');
        const displayNames = Array.from(checkedBoxes).map(checkbox => checkbox.value);
        
        // Convert display names to backend keys
        return displayNames.map(displayName => 
            this.brokerKeys[displayName] || displayName
        );
    }

    // Refresh symbols based on selected brokers
    async refreshSymbols(selectedBrokers) {
        try {
            // Build URL with broker filters
            let url = '/api/symbols';
            if (selectedBrokers.length > 0) {
                const params = new URLSearchParams();
                selectedBrokers.forEach(broker => {
                    params.append('broker', broker);
                });
                url += `?${params}`;
            }
            
            // Fetch filtered symbols
            const symbols = await this.fetchAPI(url);
            
            // Clear current symbol selection
            this.clearSymbolSelection();
            
            // Repopulate symbol filter with new symbols
            this.populateMultiSelectCheckboxes('symbolFilter', symbols);
            
        } catch (error) {
            console.error('Error refreshing symbols:', error);
        }
    }

    // Clear symbol selection
    clearSymbolSelection() {
        const container = document.getElementById('symbolFilter');
        if (!container) return;
        
        // Remove all current symbol checkboxes
        container.innerHTML = '';
        
        // Update selection count
        this.updateSelectionCount('symbolFilter');
    }

    // Load transactions with current filters
    async loadTransactions() {
        try {
            const params = new URLSearchParams();
            
            // Handle filters, including arrays
            Object.entries(this.currentFilters).forEach(([key, value]) => {
                if (Array.isArray(value)) {
                    // For arrays, add each value separately
                    value.forEach(v => params.append(key, v));
                } else {
                    params.append(key, value);
                }
            });
            
            this.transactions = await this.fetchAPI(`/api/transactions?${params}`);
            this.updateTransactionsTable();
        } catch (error) {
            console.error('Error loading transactions:', error);
            this.showError('載入交易資料時發生錯誤');
        }
    }

    // Load summary data
    async loadSummary() {
        try {
            const params = new URLSearchParams();
            
            // Handle filters, including arrays
            Object.entries(this.currentFilters).forEach(([key, value]) => {
                if (Array.isArray(value)) {
                    // For arrays, add each value separately
                    value.forEach(v => params.append(key, v));
                } else {
                    params.append(key, value);
                }
            });
            
            const summary = await this.fetchAPI(`/api/summary?${params}`);
            this.updateSummaryCards(summary);
        } catch (error) {
            console.error('Error loading summary:', error);
        }
    }

    // Load performance data
    async loadPerformanceData() {
        try {
            const performance = await this.fetchAPI('/api/performance');
            this.updatePerformanceDisplay(performance);
            this.createPerformanceChart(performance);
        } catch (error) {
            console.error('Error loading performance data:', error);
        }
    }

    // Update transactions table
    updateTransactionsTable() {
        const tbody = document.getElementById('transactionsTableBody');
        const countElement = document.getElementById('transactionCount');
        
        if (!tbody) return;

        tbody.innerHTML = '';
        countElement.textContent = `${this.transactions.length} 筆交易`;

        this.transactions.forEach(transaction => {
            const row = document.createElement('tr');
            const typeClass = transaction.transaction_type === '買進' ? 'transaction-type-buy' : 'transaction-type-sell';
            row.className = typeClass;

            row.innerHTML = `
                <td>${this.formatDate(transaction.transaction_date)}</td>
                <td><strong>${transaction.symbol || '-'}</strong></td>
                <td><span class="badge ${transaction.transaction_type === '買進' ? 'bg-success' : 'bg-success'}">${transaction.transaction_type}</span></td>
                <td>${this.formatNumber(transaction.quantity)}</td>
                <td>$${this.formatNumber(transaction.price)}</td>
                <td>$${this.formatNumber(Math.abs(transaction.amount))}</td>
                <td><span class="text-warning">$${this.formatNumber(transaction.fee)}</span></td>
                <td><span class="text-info">$${this.formatNumber(transaction.tax)}</span></td>
                <td class="${transaction.net_amount >= 0 ? 'gain' : 'loss'}">${this.formatNetAmount(transaction.net_amount)}</td>
                <td><span class="badge bg-secondary">${transaction.broker}</span></td>
                <td><span class="badge bg-primary">${transaction.currency || 'USD'}</span></td>
                <td><small>${transaction.order_id || ''}</small></td>
            `;
            
            tbody.appendChild(row);
        });
    }

    // Update summary cards (all amounts converted to NTD)
    updateSummaryCards(summary) {
        document.getElementById('totalInvestment').textContent = `NT$${this.formatNumber(summary.total_purchases || 0)}`;
        
        const realizedPL = summary.realized_gain_loss || 0;
        const realizedPLElement = document.getElementById('realizedPL');
        realizedPLElement.innerHTML = this.formatNetAmount(realizedPL, 'NTD');
        realizedPLElement.className = realizedPL >= 0 ? 'gain' : 'loss';
        
        document.getElementById('totalFees').textContent = `NT$${this.formatNumber(summary.total_fees || 0)}`;
        
        const netProfit = summary.net_after_fees || 0;
        const netProfitElement = document.getElementById('netProfit');
        netProfitElement.innerHTML = this.formatNetAmount(netProfit, 'NTD');
        netProfitElement.className = netProfit >= 0 ? 'gain' : 'loss';
        
        // Load unrealized P&L data
        this.loadUnrealizedPnL();
    }

    // Load unrealized P&L data
    async loadUnrealizedPnL() {
        try {
            const params = new URLSearchParams();
            
            // Handle filters, including arrays
            Object.entries(this.currentFilters).forEach(([key, value]) => {
                if (Array.isArray(value)) {
                    // For arrays, add each value separately
                    value.forEach(v => params.append(key, v));
                } else {
                    params.append(key, value);
                }
            });
            
            const unrealizedData = await this.fetchAPI(`/api/unrealized-pnl?${params}`);
            this.updateUnrealizedPnLCards(unrealizedData);
        } catch (error) {
            console.error('Error loading unrealized P&L:', error);
            // Set default values on error
            this.updateUnrealizedPnLCards({
                unrealized_pnl: 0,
                total_market_value: 0,
                total_cost_basis: 0,
                holdings_count: 0,
                price_fetch_errors: []
            });
        }
    }

    // Update unrealized P&L cards
    updateUnrealizedPnLCards(data) {
        // Update unrealized P&L
        const unrealizedPL = data.unrealized_pnl || 0;
        const unrealizedPLElement = document.getElementById('unrealizedPL');
        unrealizedPLElement.innerHTML = this.formatNetAmount(unrealizedPL, 'NTD');
        unrealizedPLElement.className = unrealizedPL >= 0 ? 'gain' : 'loss';
        
        // Update market value
        document.getElementById('marketValue').textContent = `NT$${this.formatNumber(data.total_market_value || 0)}`;
        
        // Update cost basis
        document.getElementById('costBasis').textContent = `NT$${this.formatNumber(data.total_cost_basis || 0)}`;
        
        // Update holdings count
        document.getElementById('holdingsCount').textContent = data.holdings_count || 0;
        
        // Show warning for price fetch errors if any
        if (data.price_fetch_errors && data.price_fetch_errors.length > 0) {
            console.warn('Failed to fetch prices for symbols:', data.price_fetch_errors);
            // You could add a visual indicator here for price fetch failures
        }
    }

    // Update performance display
    updatePerformanceDisplay(performance) {
        const container = document.getElementById('yearPerformance');
        if (!container) return;

        container.innerHTML = '';

        performance.forEach(yearData => {
            const yearDiv = document.createElement('div');
            yearDiv.className = 'year-performance';
            
            const netGainLoss = yearData.net_after_fees || 0;
            const gainLossClass = netGainLoss >= 0 ? 'gain' : 'loss';
            
            yearDiv.innerHTML = `
                <div class="row">
                    <div class="col-md-2">
                        <h5>${yearData.year}年</h5>
                    </div>
                    <div class="col-md-2">
                        <small>投資額</small><br>
                        <strong>$${this.formatNumber(yearData.purchases || 0)}</strong>
                    </div>
                    <div class="col-md-2">
                        <small>賣出額</small><br>
                        <strong>$${this.formatNumber(yearData.sales || 0)}</strong>
                    </div>
                    <div class="col-md-2">
                        <small>手續費</small><br>
                        <strong class="text-warning">$${this.formatNumber(yearData.fees || 0)}</strong>
                    </div>
                    <div class="col-md-2">
                        <small>稅費</small><br>
                        <strong class="text-info">$${this.formatNumber(yearData.taxes || 0)}</strong>
                    </div>
                    <div class="col-md-2">
                        <small>淨收益</small><br>
                        <strong class="${gainLossClass}">${this.formatNetAmount(netGainLoss)}</strong>
                    </div>
                </div>
            `;
            
            container.appendChild(yearDiv);
        });
    }

    // Create performance chart
    createPerformanceChart(performance) {
        const ctx = document.getElementById('performanceChart');
        if (!ctx) return;

        if (this.performanceChart) {
            this.performanceChart.destroy();
        }

        // Reverse the data arrays to show 2025 on the right (ascending order for chart display)
        const reversedPerformance = performance.slice().reverse();
        const labels = reversedPerformance.map(p => p.year + '年');
        const purchases = reversedPerformance.map(p => p.purchases || 0);
        const sales = reversedPerformance.map(p => p.sales || 0);
        const fees = reversedPerformance.map(p => p.fees || 0);
        const netGains = reversedPerformance.map(p => p.net_after_fees || 0);

        this.performanceChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: '投資額 (Investment)',
                        data: purchases,
                        backgroundColor: 'rgba(54, 162, 235, 0.8)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    },
                    {
                        label: '賣出額 (Sales)',
                        data: sales,
                        backgroundColor: 'rgba(75, 192, 192, 0.8)',
                        borderColor: 'rgba(75, 192, 192, 1)',
                        borderWidth: 1
                    },
                    {
                        label: '手續費 (Fees)',
                        data: fees,
                        backgroundColor: 'rgba(255, 206, 86, 0.8)',
                        borderColor: 'rgba(255, 206, 86, 1)',
                        borderWidth: 1
                    },
                    {
                        label: '淨收益 (Net Profit)',
                        data: netGains,
                        backgroundColor: netGains.map(value => value >= 0 ? 'rgba(75, 192, 192, 0.8)' : 'rgba(255, 99, 132, 0.8)'),
                        borderColor: netGains.map(value => value >= 0 ? 'rgba(75, 192, 192, 1)' : 'rgba(255, 99, 132, 1)'),
                        borderWidth: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: '年度績效分析'
                    },
                    legend: {
                        display: true
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toLocaleString();
                            }
                        }
                    }
                }
            }
        });

        // Create distribution chart
        this.createDistributionChart();
    }

    // Create distribution chart
    createDistributionChart() {
        const ctx = document.getElementById('distributionChart');
        if (!ctx) return;

        if (this.distributionChart) {
            this.distributionChart.destroy();
        }

        const buyTransactions = this.transactions.filter(t => t.transaction_type === '買進').length;
        const sellTransactions = this.transactions.filter(t => t.transaction_type === '賣出').length;

        this.distributionChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['買進 (Buy)', '賣出 (Sell)'],
                datasets: [{
                    data: [buyTransactions, sellTransactions],
                    backgroundColor: [
                        'rgba(54, 162, 235, 0.8)',
                        'rgba(255, 99, 132, 0.8)'
                    ],
                    borderColor: [
                        'rgba(54, 162, 235, 1)',
                        'rgba(255, 99, 132, 1)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: '交易類型分布'
                    },
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    // Update charts with current data
    async updateCharts() {
        try {
            const performance = await this.fetchAPI('/api/performance');
            this.createPerformanceChart(performance);
            this.createDistributionChart();
        } catch (error) {
            console.error('Error updating charts:', error);
        }
    }

    // Export data to CSV
    exportData() {
        if (this.transactions.length === 0) {
            alert('沒有資料可匯出');
            return;
        }

        const headers = [
            'Date', 'Symbol', 'Type', 'Quantity', 'Price', 'Amount', 
            'Fee', 'Tax', 'Net Amount', 'Broker', 'Order ID'
        ];

        const csvContent = [
            headers.join(','),
            ...this.transactions.map(t => [
                t.transaction_date,
                t.symbol || '',
                t.transaction_type,
                t.quantity || '',
                t.price || '',
                t.amount || '',
                t.fee || '',
                t.tax || '',
                t.net_amount || '',
                t.broker || '',
                t.order_id || ''
            ].join(','))
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `portfolio_transactions_${new Date().toISOString().split('T')[0]}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    // Utility functions
    async fetchAPI(url) {
        console.log(`Fetching ${url}...`);
        try {
            const response = await fetch(url);
            console.log(`Response status for ${url}: ${response.status}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            console.log(`Data received from ${url}:`, data);
            return data;
        } catch (error) {
            console.error(`Error fetching ${url}:`, error);
            throw error;
        }
    }

    formatNumber(num) {
        if (num === null || num === undefined) return '0';
        return parseFloat(num).toLocaleString('en-US', { 
            minimumFractionDigits: 0, 
            maximumFractionDigits: 2 
        });
    }

    formatNetAmount(num, currency = 'USD') {
        if (num === null || num === undefined) return currency === 'NTD' ? 'NT$0' : '$0';
        const formatted = parseFloat(num).toLocaleString('en-US', { 
            minimumFractionDigits: 0, 
            maximumFractionDigits: 2 
        });
        
        const currencySymbol = currency === 'NTD' ? 'NT$' : '$';
        
        if (num < 0) {
            return `<span class="net-loss">-${currencySymbol}${formatted.replace('-', '')}</span>`;
        }
        return `${currencySymbol}${formatted}`;
    }

    formatDate(dateStr) {
        if (!dateStr) return '-';
        const date = new Date(dateStr);
        return date.toLocaleDateString('zh-TW');
    }

    showLoading(show) {
        const loadingElement = document.getElementById('loadingIndicator');
        if (loadingElement) {
            loadingElement.style.display = show ? 'block' : 'none';
        }
    }

    showError(message) {
        // Simple error display - you could enhance this with a proper notification system
        console.error(message);
        alert(message);
    }
    
    // Check data freshness and display alerts
    async checkDataFreshness() {
        try {
            const response = await this.fetchAPI('/api/data-freshness/report');
            if (response && response.success && response.report) {
                this.displayDataFreshnessAlerts(response.report);
            }
        } catch (error) {
            console.error('Error checking data freshness:', error);
            // Don't show error to user for freshness check failures
        }
    }
    
    // Display data freshness alerts in the UI
    displayDataFreshnessAlerts(report) {
        const alertsContainer = document.getElementById('dataFreshnessAlerts');
        if (!alertsContainer || !report.alerts || report.alerts.length === 0) {
            return;
        }
        
        let alertsHTML = '';
        
        // Overall status indicator
        if (report.overall_status === 'DEGRADED') {
            alertsHTML += `
                <div class="alert alert-warning alert-dismissible fade show" role="alert">
                    <i class="fas fa-exclamation-triangle"></i>
                    <strong>Data Quality Notice:</strong> Some broker data may be outdated. Please review the details below.
                    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                </div>
            `;
        }
        
        // Individual broker alerts
        report.alerts.forEach(alert => {
            const severity = alert.severity === 'HIGH' ? 'danger' : 
                           alert.severity === 'MEDIUM' ? 'warning' : 'info';
            const icon = alert.severity === 'HIGH' ? 'fa-exclamation-circle' : 
                        alert.severity === 'MEDIUM' ? 'fa-exclamation-triangle' : 'fa-info-circle';
            
            alertsHTML += `
                <div class="alert alert-${severity} alert-dismissible fade show" role="alert">
                    <i class="fas ${icon}"></i>
                    <strong>${alert.broker}:</strong> ${alert.message}
                    ${alert.type === 'DATA_LIMITATION' ? 
                      '<br><small>This may be due to the TDA-Schwab merger completed in October 2023. ' +
                      'Consider checking for updated statement files or account consolidation.</small>' : 
                      ''}
                    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                </div>
            `;
        });
        
        // Recommendations
        if (report.recommendations && report.recommendations.length > 0) {
            alertsHTML += `
                <div class="alert alert-info alert-dismissible fade show" role="alert">
                    <i class="fas fa-lightbulb"></i>
                    <strong>Recommendations:</strong>
                    <ul class="mb-0 mt-2">
                        ${report.recommendations.map(rec => `<li>${rec}</li>`).join('')}
                    </ul>
                    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                </div>
            `;
        }
        
        alertsContainer.innerHTML = alertsHTML;
        alertsContainer.style.display = alertsHTML ? 'block' : 'none';
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM Content Loaded, initializing dashboard...');
    try {
        window.dashboard = new PortfolioDashboard();
    } catch (error) {
        console.error('Error initializing dashboard:', error);
    }
});