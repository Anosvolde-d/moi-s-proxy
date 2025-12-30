
// Global variables
let ws = null;
let currentEditKeyId = null;
let reconnectAttempts = 0;
const maxReconnectAttempts = 5;
let requestLogInterval = null;
let keyUsageRefreshInterval = null;
let requestsChart = null;
let tokensChart = null;
let allKeys = []; // Store all keys for filtering
let currentFilter = 'all';
let currentSearchTerm = '';

// Chart time range state - persists user selection across refreshes
// **Validates: Requirements 6.1, 6.2**
let selectedRequestsChartPeriod = 'hourly'; // 'hourly' = 24h, 'daily' = 7d
let selectedTokensChartPeriod = 'hourly';   // 'hourly' = 24h, 'daily' = 7d

// Store full API keys temporarily (keyed by key_prefix for lookup)
// These are only available during the session when the key was created
const fullKeyStorage = {};

// Authentication configuration
const ADMIN_PASSWORD_KEY = 'admin_password';
const ACCESS_CODE_KEY = 'dashboard_access_verified';

// Helper for authenticated API calls
async function apiFetch(url, options = {}) {
    const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
    const headers = {
        ...options.headers,
        'X-Admin-Password': password
    };

    const response = await fetch(url, { ...options, headers });

    if (response.status === 401) {
        // Password invalid or expired
        sessionStorage.removeItem(ADMIN_PASSWORD_KEY);
        sessionStorage.removeItem(ACCESS_CODE_KEY);
        window.location.reload();
        throw new Error('Unauthorized');
    }

    return response;
}

// Check access on page load (before DOMContentLoaded)
(function checkAccess() {
    // Check if already verified in this session
    if (sessionStorage.getItem(ACCESS_CODE_KEY) === 'true' && sessionStorage.getItem(ADMIN_PASSWORD_KEY)) {
        // Hide overlay immediately if already verified
        document.addEventListener('DOMContentLoaded', () => {
            const overlay = document.getElementById('accessCodeOverlay');
            if (overlay) {
                overlay.style.display = 'none';
            }
        });
    }
})();

// Verify admin password
async function verifyAccessCode() {
    const input = document.getElementById('accessCodeInput');
    const error = document.getElementById('accessCodeError');
    const password = input.value.trim();
    const btn = document.querySelector('.access-btn');

    if (!password) {
        error.textContent = 'Please enter a password.';
        return;
    }

    try {
        btn.disabled = true;
        btn.textContent = 'Verifying...';

        // Test password against a protected endpoint
        const response = await fetch('/api/admin/settings/global', {
            headers: { 'X-Admin-Password': password }
        });

        if (response.ok) {
            // Store verification in session storage
            sessionStorage.setItem(ADMIN_PASSWORD_KEY, password);
            sessionStorage.setItem(ACCESS_CODE_KEY, 'true');

            // Hide overlay with animation
            const overlay = document.getElementById('accessCodeOverlay');
            overlay.style.opacity = '0';
            setTimeout(() => {
                overlay.style.display = 'none';
            }, 300);

            // Initialize dashboard
            initializeDashboard();
        } else {
            throw new Error('Invalid password');
        }
    } catch (err) {
        // Show error
        error.textContent = 'Invalid admin password. Please try again.';
        input.value = '';
        input.focus();

        // Shake animation
        const modal = document.querySelector('.access-card');
        if (modal) {
            modal.classList.add('shake');
            setTimeout(() => {
                modal.classList.remove('shake');
            }, 500);
        }
    } finally {
        btn.disabled = false;
        btn.textContent = '🔓 Unlock Dashboard';
    }
}

// Handle Enter key on access code input
document.addEventListener('DOMContentLoaded', () => {
    const input = document.getElementById('accessCodeInput');
    if (input) {
        input.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                verifyAccessCode();
            }
        });

        // Focus input if overlay is visible
        const overlay = document.getElementById('accessCodeOverlay');
        if (overlay && overlay.style.display !== 'none') {
            input.focus();
        }
    }
});

// Initialize dashboard function
function initializeDashboard() {
    loadAPIKeys();
    connectWebSocket();
    loadAnalytics();
    loadRecentRequests();
    loadModelCosts(); // New
    startRequestLogAutoRefresh();
    startKeyUsageAutoRefresh();
    startAnalyticsAutoRefresh();
    startSubKeysAutoRefresh(); // Auto-refresh sub-keys list every 3 seconds
    initializeCharts();
    loadChartData('24h');

}

// Initialize dashboard on page load
document.addEventListener('DOMContentLoaded', () => {
    // Check if already verified
    if (sessionStorage.getItem(ACCESS_CODE_KEY) === 'true') {
        initializeDashboard();
    }
    // If not verified, wait for access code entry
});

// ==================== TOAST NOTIFICATION SYSTEM ====================

function showToast(message, type = 'info', duration = 4000) {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    const icons = {
        success: '✓',
        error: '✕',
        warning: '⚠',
        info: 'ℹ'
    };

    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <span class="toast-message">${escapeHtml(message)}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;

    container.appendChild(toast);

    // Trigger animation
    requestAnimationFrame(() => {
        toast.classList.add('show');
    });

    // Auto remove
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, duration);
}

// ==================== SEARCH & FILTER FUNCTIONS ====================

function filterKeys() {
    const searchInput = document.getElementById('keySearch');
    currentSearchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';
    renderFilteredKeys();
}

function setFilter(filter, event) {
    currentFilter = filter;

    // Update active button
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Use event.target if event is provided, otherwise find the button by filter value
    if (event && event.target) {
        event.target.classList.add('active');
    } else {
        const btn = document.querySelector(`.filter-btn[data-filter="${filter}"]`);
        if (btn) btn.classList.add('active');
    }

    renderFilteredKeys();
}

function renderFilteredKeys() {
    const container = document.getElementById('keysContainer');
    if (!container) {
        console.error('keysContainer element not found');
        return;
    }

    let filteredKeys = allKeys;

    // Apply status filter
    if (currentFilter === 'active') {
        filteredKeys = filteredKeys.filter(key => key.enabled);
    } else if (currentFilter === 'disabled') {
        filteredKeys = filteredKeys.filter(key => !key.enabled);
    }

    // Apply search filter
    if (currentSearchTerm) {
        filteredKeys = filteredKeys.filter(key => {
            const name = (key.name || '').toLowerCase();
            const prefix = (key.key_prefix || '').toLowerCase();
            return name.includes(currentSearchTerm) || prefix.includes(currentSearchTerm);
        });
    }

    // Update key count
    const keyCount = document.getElementById('keyCount');
    if (keyCount) {
        keyCount.textContent = `${filteredKeys.length} key${filteredKeys.length !== 1 ? 's' : ''}`;
    }

    console.log(`Rendering ${filteredKeys.length} keys after filtering`);

    if (filteredKeys.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">🔑</div>
                <p>${currentSearchTerm || currentFilter !== 'all' ? 'No keys match your filter' : 'No API keys yet. Generate one to get started!'}</p>
            </div>
        `;
        return;
    }

    container.innerHTML = '';
    let successCount = 0;
    let errorCount = 0;
    
    filteredKeys.forEach((key, index) => {
        try {
            const keyCard = createKeyCard(key);
            container.appendChild(keyCard);
            successCount++;
        } catch (error) {
            console.error(`Error creating key card for key ${key.id}:`, error, key);
            errorCount++;
        }
    });
    
    console.log(`Rendered ${successCount} key cards, ${errorCount} errors`);
}

// ==================== CHART FUNCTIONS ====================
// **Validates: Requirements 6.3** - Improved visual styling for charts

function initializeCharts() {
    const requestsCtx = document.getElementById('requestsChart');
    const tokensCtx = document.getElementById('tokensChart');

    if (!requestsCtx || !tokensCtx) return;

    // Enhanced chart options with improved styling
    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            intersect: false,
            mode: 'index'
        },
        plugins: {
            legend: {
                display: false
            },
            tooltip: {
                backgroundColor: 'rgba(15, 23, 42, 0.95)',
                titleColor: '#f1f5f9',
                titleFont: { size: 13, weight: '600' },
                bodyColor: '#cbd5e1',
                bodyFont: { size: 12 },
                borderColor: 'rgba(99, 102, 241, 0.4)',
                borderWidth: 1,
                padding: 14,
                cornerRadius: 10,
                displayColors: true,
                boxPadding: 6,
                caretSize: 8,
                caretPadding: 10
            }
        },
        scales: {
            x: {
                grid: {
                    color: 'rgba(148, 163, 184, 0.08)',
                    drawBorder: false,
                    tickLength: 0
                },
                ticks: {
                    color: '#64748b',
                    font: { size: 11, weight: '500' },
                    padding: 8,
                    maxRotation: 0
                },
                border: {
                    display: false
                }
            },
            y: {
                grid: {
                    color: 'rgba(148, 163, 184, 0.08)',
                    drawBorder: false,
                    tickLength: 0
                },
                ticks: {
                    color: '#64748b',
                    font: { size: 11, weight: '500' },
                    padding: 12,
                    callback: function(value) {
                        // Format large numbers with K suffix
                        if (value >= 1000) {
                            return (value / 1000).toFixed(1) + 'K';
                        }
                        return value;
                    }
                },
                border: {
                    display: false
                },
                beginAtZero: true
            }
        },
        animation: {
            duration: 750,
            easing: 'easeOutQuart'
        }
    };

    // Requests chart with gradient fill
    requestsChart = new Chart(requestsCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Requests',
                data: [],
                borderColor: '#6366f1',
                backgroundColor: function(context) {
                    const chart = context.chart;
                    const {ctx, chartArea} = chart;
                    if (!chartArea) return 'rgba(99, 102, 241, 0.1)';
                    const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
                    gradient.addColorStop(0, 'rgba(99, 102, 241, 0.25)');
                    gradient.addColorStop(0.5, 'rgba(99, 102, 241, 0.1)');
                    gradient.addColorStop(1, 'rgba(99, 102, 241, 0.02)');
                    return gradient;
                },
                borderWidth: 2.5,
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 7,
                pointHoverBackgroundColor: '#6366f1',
                pointHoverBorderColor: '#fff',
                pointHoverBorderWidth: 3,
                pointHitRadius: 20
            }]
        },
        options: chartOptions
    });

    // Tokens chart with improved bar styling
    tokensChart = new Chart(tokensCtx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Input Tokens',
                    data: [],
                    backgroundColor: 'rgba(99, 102, 241, 0.85)',
                    hoverBackgroundColor: 'rgba(99, 102, 241, 1)',
                    borderRadius: 6,
                    borderSkipped: false,
                    barPercentage: 0.7,
                    categoryPercentage: 0.8
                },
                {
                    label: 'Output Tokens',
                    data: [],
                    backgroundColor: 'rgba(168, 85, 247, 0.85)',
                    hoverBackgroundColor: 'rgba(168, 85, 247, 1)',
                    borderRadius: 6,
                    borderSkipped: false,
                    barPercentage: 0.7,
                    categoryPercentage: 0.8
                }
            ]
        },
        options: {
            ...chartOptions,
            plugins: {
                ...chartOptions.plugins,
                legend: {
                    display: true,
                    position: 'top',
                    align: 'end',
                    labels: {
                        color: '#94a3b8',
                        usePointStyle: true,
                        pointStyle: 'rectRounded',
                        padding: 16,
                        font: { size: 11, weight: '500' },
                        boxWidth: 8,
                        boxHeight: 8
                    }
                }
            }
        }
    });
}

async function loadChartData(period) {
    // Update active button
    document.querySelectorAll('.chart-period-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.textContent.toLowerCase().includes(period.replace('h', 'H').replace('d', 'D'))) {
            btn.classList.add('active');
        }
    });

    try {
        const response = await apiFetch('/admin/analytics');
        const data = await response.json();

        const hourlyData = data.hourly_usage || [];

        if (hourlyData.length === 0) {
            // No data - show empty state
            if (requestsChart) {
                requestsChart.data.labels = ['No data'];
                requestsChart.data.datasets[0].data = [0];
                requestsChart.update();
            }
            if (tokensChart) {
                tokensChart.data.labels = ['No data'];
                tokensChart.data.datasets[0].data = [0];
                tokensChart.data.datasets[1].data = [0];
                tokensChart.update();
            }
            return;
        }

        // Process data based on period
        let processedData;
        if (period === '24h') {
            // Last 24 hours
            processedData = hourlyData.slice(-24);
        } else {
            // Last 7 days - aggregate by day
            processedData = aggregateByDay(hourlyData.slice(-168)); // 7 days * 24 hours
        }

        const labels = processedData.map(d => {
            if (period === '24h') {
                return d.hour ? d.hour.split(' ')[1] || d.hour : 'N/A';
            } else {
                return d.day || d.hour?.split(' ')[0] || 'N/A';
            }
        });

        const requests = processedData.map(d => d.request_count || d.requests || 0);
        const inputTokens = processedData.map(d => d.input_tokens || 0);
        const outputTokens = processedData.map(d => d.output_tokens || 0);

        // Update charts
        if (requestsChart) {
            requestsChart.data.labels = labels;
            requestsChart.data.datasets[0].data = requests;
            requestsChart.update();
        }

        if (tokensChart) {
            tokensChart.data.labels = labels;
            tokensChart.data.datasets[0].data = inputTokens;
            tokensChart.data.datasets[1].data = outputTokens;
            tokensChart.update();
        }

    } catch (error) {
        console.error('Error loading chart data:', error);
        showToast('Failed to load chart data', 'error');
    }
}

function aggregateByDay(hourlyData) {
    const dayMap = new Map();

    hourlyData.forEach(item => {
        const day = item.hour ? item.hour.split(' ')[0] : 'Unknown';
        if (!dayMap.has(day)) {
            dayMap.set(day, {
                day,
                requests: 0,
                input_tokens: 0,
                output_tokens: 0
            });
        }
        const dayData = dayMap.get(day);
        dayData.requests += item.request_count || 0;
        dayData.input_tokens += item.input_tokens || 0;
        dayData.output_tokens += item.output_tokens || 0;
    });

    return Array.from(dayMap.values());
}

// Chart period selection functions
// **Validates: Requirements 6.1, 6.2** - Store selection in state to persist across refreshes
function setChartPeriod(period) {
    // Store the selection in state variable to persist across refreshes
    selectedRequestsChartPeriod = period;
    
    // Update active button for requests chart
    const requestsChartCard = document.querySelector('#requestsChart')?.closest('.chart-card');
    if (requestsChartCard) {
        requestsChartCard.querySelectorAll('.period-btn').forEach(btn => {
            btn.classList.remove('active');
            if (btn.dataset.period === period) {
                btn.classList.add('active');
            }
        });
    }
    
    // Load chart data with the selected period
    const chartPeriod = period === 'hourly' ? '24h' : '7d';
    loadChartData(chartPeriod);
}

// **Validates: Requirements 6.1, 6.2** - Store selection in state to persist across refreshes
function setTokenChartPeriod(period) {
    // Store the selection in state variable to persist across refreshes
    selectedTokensChartPeriod = period;
    
    // Update active button for tokens chart
    const tokensChartCard = document.querySelector('#tokensChart')?.closest('.chart-card');
    if (tokensChartCard) {
        tokensChartCard.querySelectorAll('.period-btn').forEach(btn => {
            btn.classList.remove('active');
            if (btn.dataset.period === period) {
                btn.classList.add('active');
            }
        });
    }
    
    // Load chart data with the selected period
    const chartPeriod = period === 'hourly' ? '24h' : '7d';
    loadChartData(chartPeriod);
}

// WebSocket connection for real-time logs
let wsReconnectTimeout = null;
let wsHeartbeatInterval = null;
let lastPongTime = Date.now();
const WS_HEARTBEAT_INTERVAL = 15000; // 15 seconds
const WS_HEARTBEAT_TIMEOUT = 45000; // 45 seconds without pong = reconnect

function connectWebSocket() {
    // Clear any pending reconnect timeout
    if (wsReconnectTimeout) {
        clearTimeout(wsReconnectTimeout);
        wsReconnectTimeout = null;
    }
    
    // Clear heartbeat interval
    if (wsHeartbeatInterval) {
        clearInterval(wsHeartbeatInterval);
        wsHeartbeatInterval = null;
    }
    
    // Close existing connection if any
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        ws.close();
    }
    
    const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
    if (!password) {
        console.error('No admin password found for WebSocket authentication');
        return;
    }
    
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    // Don't pass password in URL - send it via message after connection
    const wsUrl = `${protocol}//${window.location.host}/admin/logs/stream`;

    try {
        ws = new WebSocket(wsUrl);
    } catch (error) {
        console.error('Failed to create WebSocket:', error);
        scheduleReconnect();
        return;
    }

    ws.onopen = () => {
        console.log('WebSocket connected, authenticating...');
        // Send authentication message instead of URL parameter
        try {
            ws.send(JSON.stringify({ type: 'auth', password: password }));
        } catch (error) {
            console.error('Failed to send auth message:', error);
        }
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);

            // Handle authentication response
            if (data.type === 'auth_success') {
                console.log('WebSocket authenticated');
                updateWSStatus(true);
                reconnectAttempts = 0;
                lastPongTime = Date.now();
                startHeartbeatMonitor();
                addConsoleLog('INFO', 'Connected to real-time log stream');
                return;
            }

            if (data.type === 'auth_failed' || data.error === 'Unauthorized') {
                console.error('WebSocket authentication failed');
                addConsoleLog('ERROR', 'WebSocket authentication failed');
                ws.close();
                return;
            }
            
            // Handle log history from server (sent on new connection)
            if (data.type === 'log_history' && data.logs) {
                console.log(`Received ${data.count} historical log entries`);
                // Add historical logs to console (oldest first)
                data.logs.forEach(log => {
                    if (log.level !== 'PING') {
                        addConsoleLog(log.level, log.message, log.timestamp, true);
                    }
                });
                addConsoleLog('INFO', `Loaded ${data.count} recent log entries`);
                return;
            }

            // Update last pong time for any message received
            lastPongTime = Date.now();

            if (data.level !== 'PING') {
                addConsoleLog(data.level, data.message, data.timestamp);
            }
        } catch (error) {
            console.error('Error parsing WebSocket message:', error);
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        // Don't add console log here as it may cause issues if console isn't ready
    };

    ws.onclose = (event) => {
        console.log('WebSocket disconnected, code:', event.code);
        updateWSStatus(false);
        
        // Clear heartbeat monitor
        if (wsHeartbeatInterval) {
            clearInterval(wsHeartbeatInterval);
            wsHeartbeatInterval = null;
        }
        
        // Only log and attempt reconnect if we were previously connected
        if (reconnectAttempts === 0) {
            addConsoleLog('WARNING', 'Disconnected from log stream');
        }

        // Attempt to reconnect with exponential backoff
        scheduleReconnect();
    };
}

function startHeartbeatMonitor() {
    // Clear any existing heartbeat interval
    if (wsHeartbeatInterval) {
        clearInterval(wsHeartbeatInterval);
    }
    
    // Monitor connection health
    wsHeartbeatInterval = setInterval(() => {
        const timeSinceLastPong = Date.now() - lastPongTime;
        
        if (timeSinceLastPong > WS_HEARTBEAT_TIMEOUT) {
            console.warn('WebSocket heartbeat timeout, reconnecting...');
            addConsoleLog('WARNING', 'Connection timeout, reconnecting...');
            
            // Force close and reconnect
            if (ws) {
                ws.close();
            }
        }
    }, WS_HEARTBEAT_INTERVAL);
}

function scheduleReconnect() {
    // Don't schedule if already scheduled
    if (wsReconnectTimeout) {
        return;
    }
    
    if (reconnectAttempts < maxReconnectAttempts) {
        reconnectAttempts++;
        const delay = Math.min(3000 * Math.pow(1.5, reconnectAttempts - 1), 30000); // Exponential backoff, max 30 seconds
        
        wsReconnectTimeout = setTimeout(() => {
            wsReconnectTimeout = null;
            addConsoleLog('INFO', `Reconnecting... (attempt ${reconnectAttempts}/${maxReconnectAttempts})`);
            connectWebSocket();
        }, delay);
    } else {
        addConsoleLog('ERROR', 'Max reconnection attempts reached. Click "Reconnect" to try again.');
    }
}

// Manual reconnect function
function reconnectWebSocket() {
    reconnectAttempts = 0;
    
    // Clear any pending timeout
    if (wsReconnectTimeout) {
        clearTimeout(wsReconnectTimeout);
        wsReconnectTimeout = null;
    }
    
    addConsoleLog('INFO', 'Manual reconnection initiated...');
    connectWebSocket();
}

// Update WebSocket status indicator
function updateWSStatus(connected) {
    const statusIndicator = document.getElementById('wsStatus');
    const statusText = document.getElementById('wsStatusText');

    if (connected) {
        statusIndicator.classList.add('connected');
        statusText.textContent = 'Connected';
    } else {
        statusIndicator.classList.remove('connected');
        statusText.textContent = 'Disconnected';
    }
}

// Add log to console
// isHistorical: if true, don't scroll to bottom (for batch loading historical logs)
function addConsoleLog(level, message, timestamp = null, isHistorical = false) {
    const consoleEl = document.getElementById('console');
    if (!consoleEl) return;

    const line = document.createElement('div');

    const levelClass = level.toLowerCase();
    line.className = `console-line ${levelClass}`;

    const time = timestamp ? new Date(timestamp.includes('Z') ? timestamp : timestamp + 'Z').toLocaleTimeString() : new Date().toLocaleTimeString();

    line.innerHTML = `
        <span class="timestamp">[${time}]</span>
        <span class="level">[${level}]</span>
        <span class="message">${escapeHtml(message)}</span>
    `;

    consoleEl.appendChild(line);
    
    // Only auto-scroll for new (non-historical) logs
    if (!isHistorical) {
        consoleEl.scrollTop = consoleEl.scrollHeight;
    }

    // Memory management: Limit console to last 500 lines
    // Use more efficient removal for large batches
    const maxLines = 500;
    const linesToRemove = consoleEl.children.length - maxLines;
    if (linesToRemove > 0) {
        // Remove in batch for better performance
        for (let i = 0; i < linesToRemove; i++) {
            consoleEl.removeChild(consoleEl.firstChild);
        }
    }
}

// Clear console
function clearConsole() {
    const consoleEl = document.getElementById('console');
    if (consoleEl) {
        consoleEl.innerHTML = '';
        addConsoleLog('INFO', 'Console cleared');
    }
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Copy to clipboard helper
async function copyToClipboard(text, description = 'API Key') {
    try {
        await navigator.clipboard.writeText(text);
        showToast(`${description} copied to clipboard!`, 'success');
        addConsoleLog('INFO', `${description} copied to clipboard`);
    } catch (err) {
        console.error('Failed to copy text: ', err);
        // Fallback for non-HTTPS or other issues
        const textArea = document.createElement("textarea");
        textArea.value = text;
        document.body.appendChild(textArea);
        textArea.select();
        try {
            document.execCommand('copy');
            showToast(`${description} copied to clipboard!`, 'success');
        } catch (err) {
            showToast('Failed to copy to clipboard', 'error');
        }
        document.body.removeChild(textArea);
    }
}

// Copy MOI proxy key - tries to get full key from session storage, falls back to prefix
async function copyMoiKey(keyPrefix) {
    // First check in-memory storage
    let fullKey = fullKeyStorage[keyPrefix];

    // If not in memory, check sessionStorage
    if (!fullKey) {
        try {
            const storedKeys = JSON.parse(sessionStorage.getItem('moi_full_keys') || '{}');
            fullKey = storedKeys[keyPrefix];
        } catch (e) {
            console.warn('Could not read from sessionStorage:', e);
        }
    }

    if (fullKey) {
        // We have the full key from this session
        await copyToClipboard(fullKey, 'Full MOI Proxy Key');
    } else {
        // Full key not available - copy just the prefix with a note
        await copyToClipboard(keyPrefix, 'MOI Proxy Key Prefix');
        showToast('Note: Full key only available immediately after creation. Prefix copied.', 'warning', 5000);
    }
}

// Load API keys
async function loadAPIKeys() {
    const container = document.getElementById('keysContainer');
    
    // Show loading skeleton before making API call
    if (container) {
        container.innerHTML = `
            <div class="key-card skeleton">
                <div class="skeleton-line" style="width: 60%; height: 24px;"></div>
                <div class="skeleton-line" style="width: 40%; height: 16px; margin-top: 8px;"></div>
                <div class="skeleton-line" style="width: 100%; height: 8px; margin-top: 16px;"></div>
                <div class="skeleton-line" style="width: 100%; height: 8px; margin-top: 8px;"></div>
            </div>
        `;
    }
    
    try {
        console.log('Loading API keys...');
        const response = await apiFetch('/admin/keys/list');
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log('API keys response:', data);

        // Validate response structure
        if (!data || !Array.isArray(data.keys)) {
            console.error('Invalid response structure:', data);
            throw new Error('Invalid response: expected { keys: [...] }');
        }

        console.log(`Loaded ${data.keys.length} API keys`);

        // Store all keys for filtering IMMEDIATELY (don't wait for usage stats)
        allKeys = data.keys;
        
        // Set default usage values
        allKeys.forEach(key => {
            key.current_rpm = key.current_rpm || 0;
            key.current_rpd = key.current_rpd || 0;
            key.refresh_hour = key.refresh_hour || null;
        });

        console.log('Calling renderFilteredKeys...');
        // Render filtered keys immediately
        renderFilteredKeys();
        console.log('renderFilteredKeys completed');
        
        // Load usage stats in background (don't block rendering)
        loadUsageStatsInBackground(data.keys);
        
        // Load aggregated usage for all keys (updates display with sub-key totals)
        // **Validates: Requirements 10.6**
        loadAllAggregatedUsage();

    } catch (error) {
        console.error('Error loading API keys:', error);
        addConsoleLog('ERROR', `Failed to load API keys: ${error.message}`);
        showToast('Failed to load API keys', 'error');
        document.getElementById('keysContainer').innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">⚠️</div>
                <p>Failed to load API keys: ${escapeHtml(error.message)}</p>
            </div>
        `;
    }
}

// Load usage stats in background without blocking UI
async function loadUsageStatsInBackground(keys) {
    console.log('Loading usage stats in background...');
    
    // Process in batches of 10 to avoid overwhelming the server
    const batchSize = 10;
    for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        await Promise.all(batch.map(async (key) => {
            try {
                const usageResponse = await apiFetch(`/admin/keys/${key.id}/usage`);
                if (usageResponse.ok) {
                    const usageData = await usageResponse.json();
                    key.current_rpm = usageData.current_rpm || 0;
                    key.current_rpd = usageData.current_rpd || 0;
                    key.refresh_hour = usageData.refresh_hour;
                    
                    // Update the UI for this key
                    updateKeyUsageDisplay(key.id, key.current_rpm, key.max_rpm, key.current_rpd, key.max_rpd);
                }
            } catch (e) {
                // Silently ignore errors for background loading
            }
        }));
        
        // Small delay between batches
        if (i + batchSize < keys.length) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }
    
    console.log('Background usage stats loading complete');
}

// Update key usage display without re-rendering the whole card
function updateKeyUsageDisplay(keyId, currentRpm, maxRpm, currentRpd, maxRpd) {
    const rpmEl = document.getElementById(`rpm-${keyId}`);
    const rpdEl = document.getElementById(`rpd-${keyId}`);
    const rpmBar = document.getElementById(`rpm-bar-${keyId}`);
    const rpdBar = document.getElementById(`rpd-bar-${keyId}`);
    
    if (rpmEl) {
        const rpmPercent = (currentRpm / maxRpm) * 100;
        const rpmColorClass = rpmPercent >= 95 ? 'danger' : rpmPercent >= 80 ? 'warning' : '';
        rpmEl.textContent = `${currentRpm} / ${maxRpm}`;
        rpmEl.className = `usage-value ${rpmColorClass}`;
        if (rpmBar) {
            rpmBar.style.width = `${Math.min(rpmPercent, 100)}%`;
            rpmBar.className = `usage-bar ${rpmColorClass}`;
        }
    }
    
    if (rpdEl) {
        const rpdPercent = (currentRpd / maxRpd) * 100;
        const rpdColorClass = rpdPercent >= 95 ? 'danger' : rpdPercent >= 80 ? 'warning' : '';
        rpdEl.textContent = `${currentRpd} / ${maxRpd}`;
        rpdEl.className = `usage-value ${rpdColorClass}`;
        if (rpdBar) {
            rpdBar.style.width = `${Math.min(rpdPercent, 100)}%`;
            rpdBar.className = `usage-bar ${rpdColorClass}`;
        }
    }
}

// Create API key card element
function createKeyCard(key) {
    const card = document.createElement('div');
    card.className = `key-card ${key.enabled ? '' : 'disabled'}`;
    card.id = `key-card-${key.id}`;

    const statusClass = key.enabled ? 'status-enabled' : 'status-disabled';
    const statusText = key.enabled ? 'Active' : 'Disabled';
    const toggleText = key.enabled ? 'Disable' : 'Enable';

    const lastUsed = key.last_used_at ? new Date(key.last_used_at.includes('Z') ? key.last_used_at : key.last_used_at + 'Z').toLocaleString() : 'Never';
    const created = new Date(key.created_at.includes('Z') ? key.created_at : key.created_at + 'Z').toLocaleString();

    // Calculate usage percentages for color coding
    const rpmPercent = (key.current_rpm / key.max_rpm) * 100;
    const rpdPercent = (key.current_rpd / key.max_rpd) * 100;

    // Determine color classes based on usage
    const rpmColorClass = rpmPercent >= 95 ? 'danger' : rpmPercent >= 80 ? 'warning' : '';
    const rpdColorClass = rpdPercent >= 95 ? 'danger' : rpdPercent >= 80 ? 'warning' : '';

    // Check for expiration
    const hasExpiration = key.expires_at && key.expires_at !== '';
    const expirationDate = hasExpiration ? new Date(key.expires_at.includes('Z') ? key.expires_at : key.expires_at + 'Z') : null;
    const isExpired = hasExpiration && expirationDate < new Date();
    const expirationText = hasExpiration ? expirationDate.toLocaleDateString() : '';

    // Check for model mappings
    const hasModelMappings = key.model_mappings && Object.keys(key.model_mappings).length > 0;
    const mappingsCount = hasModelMappings ? Object.keys(key.model_mappings).length : 0;

    // Check for IP restrictions
    const hasIpRestrictions = (key.ip_whitelist && key.ip_whitelist.trim()) || (key.ip_blacklist && key.ip_blacklist.trim());

    // Sub-key count (will be loaded dynamically)
    const subKeyCount = key.sub_key_count || 0;

    // Store key data for edit function
    window[`keyData_${key.id}`] = key;

    card.innerHTML = `
        <div class="key-header">
            <div class="key-info">
                <div class="key-prefix">${escapeHtml(key.key_prefix)}</div>
                ${key.name ? `<div class="key-name">${escapeHtml(key.name)}</div>` : '<div class="key-name">Unnamed Key</div>'}
            </div>
            <div class="key-badges">
                <span class="key-status ${statusClass}">
                    <span class="status-dot"></span>
                    ${statusText}
                </span>
                ${hasExpiration ? `<span class="key-badge ${isExpired ? 'expired' : 'expires'}" title="Expiration date">
                    ⏳ ${isExpired ? 'Expired' : expirationText}
                </span>` : ''}
                ${hasModelMappings ? `<span class="key-badge mappings" title="Custom model mappings configured">
                    🔄 ${mappingsCount} mapping${mappingsCount > 1 ? 's' : ''}
                </span>` : ''}
                ${hasIpRestrictions ? `<span class="key-badge ip-restricted" title="IP restrictions configured">
                    🔒 IP Restricted
                </span>` : ''}
                ${key.refresh_hour !== null ?
            `<span class="schedule-badge" title="Auto-refresh at ${key.refresh_hour}:00 UTC">
                        ⏰ ${key.refresh_hour}:00 UTC
                    </span>` : ''}
            </div>
        </div>
        
        <div class="key-usage">
            <div class="usage-item">
                <div class="usage-header">
                    <span class="usage-label">RPM (Requests/Min)</span>
                    <span class="usage-value ${rpmColorClass}" id="rpm-${key.id}">${key.current_rpm || 0} / ${key.max_rpm}</span>
                </div>
                <div class="usage-bar-container">
                    <div class="usage-bar ${rpmColorClass}" id="rpm-bar-${key.id}" style="width: ${Math.min(rpmPercent, 100)}%"></div>
                </div>
            </div>
            <div class="usage-item">
                <div class="usage-header">
                    <span class="usage-label">RPD (Requests/Day)</span>
                    <span class="usage-value ${rpdColorClass}" id="rpd-${key.id}">${key.current_rpd || 0} / ${key.max_rpd}</span>
                </div>
                <div class="usage-bar-container">
                    <div class="usage-bar ${rpdColorClass}" id="rpd-bar-${key.id}" style="width: ${Math.min(rpdPercent, 100)}%"></div>
                </div>
            </div>
            ${key.max_total_tokens ? `
            <div class="usage-item">
                <div class="usage-header">
                    <span class="usage-label">Tokens (Total Quota)</span>
                    <span class="usage-value ${key.total_tokens_used >= key.max_total_tokens * 0.9 ? 'danger' : key.total_tokens_used >= key.max_total_tokens * 0.8 ? 'warning' : ''}" id="tokens-${key.id}">${formatNumber(key.total_tokens_used || 0)} / ${formatNumber(key.max_total_tokens)}</span>
                </div>
                <div class="usage-bar-container">
                    <div class="usage-bar ${key.total_tokens_used >= key.max_total_tokens * 0.9 ? 'danger' : key.total_tokens_used >= key.max_total_tokens * 0.8 ? 'warning' : ''}" id="tokens-bar-${key.id}" style="width: ${Math.min((key.total_tokens_used / key.max_total_tokens) * 100, 100)}%"></div>
                </div>
            </div>
            ` : ''}
        </div>
        
        <div class="key-meta">
            <div class="meta-item">
                <span class="meta-label">Created</span>
                <span class="meta-value">${created}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Last Used</span>
                <span class="meta-value">${lastUsed}</span>
            </div>
            ${key.max_context_tokens ? `
            <div class="meta-item">
                <span class="meta-label">Context Limit</span>
                <span class="meta-value">${formatNumber(key.max_context_tokens)} tokens</span>
            </div>
            ` : ''}
        </div>
        
        <!-- Sub-Keys Section -->
        <div class="sub-keys-section" id="sub-keys-section-${key.id}">
            <button class="btn btn-secondary sub-keys-toggle" onclick="toggleSubKeys(${key.id})" title="Show/hide Discord user sub-keys">
                <span class="sub-keys-icon" id="sub-keys-icon-${key.id}">▼</span>
                <span>Sub-Keys</span>
                <span class="sub-keys-count" id="sub-keys-count-${key.id}">(loading...)</span>
            </button>
            <div class="sub-keys-container" id="sub-keys-container-${key.id}" style="display: none;">
                <div class="sub-keys-loading">Loading sub-keys...</div>
            </div>
        </div>
        
        <div class="key-actions">
            <button class="btn btn-info" onclick="copyMoiKey('${escapeHtml(key.key_prefix)}')" title="Copy the full MOI proxy key (if available from this session) or the prefix">
                <span>🔑</span> Copy Key
            </button>
            <button class="btn btn-secondary" onclick="editKey(${key.id}, ${key.max_rpm}, ${key.max_rpd}, window.keyData_${key.id})" title="Edit key settings">
                <span>✏️</span> Edit
            </button>
            <button class="btn btn-secondary" onclick="showScheduleModal(${key.id}, ${key.refresh_hour || 0})" title="Set auto-refresh schedule">
                <span>⏰</span> Schedule
            </button>
            <button class="btn btn-secondary" onclick="refreshKeyLimits(${key.id})" title="Reset rate limit counters">
                <span>🔄</span> Reset
            </button>
            <button class="btn ${key.enabled ? 'btn-warning' : 'btn-success'}" onclick="toggleKey(${key.id})">
                <span>${key.enabled ? '⏸️' : '▶️'}</span> ${toggleText}
            </button>
            <button class="btn btn-danger" onclick="deleteKey(${key.id})" title="Delete this API key">
                <span>🗑️</span> Delete
            </button>
        </div>
    `;

    return card;
}

// Show generate modal
function showGenerateModal(event) {
    if (event) {
        event.stopPropagation();
    }
    const modal = document.getElementById('generateModal');
    if (modal) {
        modal.classList.add('active');
        // Reset form - with null checks for safety
        const keyName = document.getElementById('keyName');
        const claimCode = document.getElementById('claimCode');
        const maxRpm = document.getElementById('maxRpm');
        const maxRpd = document.getElementById('maxRpd');
        const targetUrl = document.getElementById('targetUrl');
        const targetApiKey = document.getElementById('targetApiKey');
        const httpReferer = document.getElementById('httpReferer');
        const noAuth = document.getElementById('noAuth');
        const disableModelFetch = document.getElementById('disableModelFetch');
        const expiresAt = document.getElementById('expiresAt');
        const ipWhitelist = document.getElementById('ipWhitelist');
        const ipBlacklist = document.getElementById('ipBlacklist');
        const modelMappings = document.getElementById('modelMappings');
        const maxTotalTokens = document.getElementById('maxTotalTokens');
        const maxContextTokens = document.getElementById('maxContextTokens');
        const customPrefills = document.getElementById('customPrefills');
        
        if (keyName) keyName.value = '';
        if (claimCode) claimCode.value = '';
        if (maxRpm) maxRpm.value = '60';
        if (maxRpd) maxRpd.value = '1000';
        if (targetUrl) targetUrl.value = '';
        if (targetApiKey) targetApiKey.value = '';
        if (httpReferer) httpReferer.value = '';
        if (noAuth) noAuth.checked = false;
        if (disableModelFetch) disableModelFetch.checked = false;
        if (expiresAt) expiresAt.value = '';
        if (ipWhitelist) ipWhitelist.value = '';
        if (ipBlacklist) ipBlacklist.value = '';
        if (modelMappings) modelMappings.value = '';
        if (maxTotalTokens) maxTotalTokens.value = '';
        if (maxContextTokens) maxContextTokens.value = '';
        if (customPrefills) customPrefills.value = '';
        
        // Clear model mappings builder
        clearModelMappingsBuilder();
        console.log('Generate modal opened');
    } else {
        console.error('Generate modal element not found');
    }
}

// Add event listener for generate key button
document.addEventListener('DOMContentLoaded', () => {
    const btnGenerateKey = document.getElementById('btnGenerateKey');
    if (btnGenerateKey) {
        btnGenerateKey.addEventListener('click', (e) => {
            console.log('Generate Key button clicked');
            showGenerateModal(e);
        });
    } else {
        console.error('Generate Key button not found');
    }
});


// Close generate modal
function closeGenerateModal() {
    document.getElementById('generateModal').classList.remove('active');
}

// Generate new API key
async function generateKey() {
    const name = document.getElementById('keyName').value.trim() || null;
    const claimCode = document.getElementById('claimCode').value.trim() || null;  // **Validates: Requirements 3.1, 3.2**
    const maxRpm = parseInt(document.getElementById('maxRpm').value);
    const maxRpd = parseInt(document.getElementById('maxRpd').value);
    const targetUrl = document.getElementById('targetUrl').value.trim() || null;
    const targetApiKey = document.getElementById('targetApiKey').value.trim() || null;
    const httpReferer = document.getElementById('httpReferer').value.trim() || null;
    const noAuth = document.getElementById('noAuth').checked;

    // New fields
    const disableModelFetch = document.getElementById('disableModelFetch').checked;
    const expiresAt = document.getElementById('expiresAt').value || null;
    // IP whitelist/blacklist removed - Discord-based access control is now used
    const modelMappingsStr = document.getElementById('modelMappings').value.trim();

    // New fields
    const maxTotalTokens = parseInt(document.getElementById('maxTotalTokens').value) || null;
    const maxContextTokens = parseInt(document.getElementById('maxContextTokens').value) || null;
    const customPrefillsStr = document.getElementById('customPrefills').value.trim();
    let customPrefills = null;
    if (customPrefillsStr) {
        try {
            customPrefills = JSON.parse(customPrefillsStr);
        } catch (e) {
            showToast('Invalid JSON format for custom prefills', 'error');
            return;
        }
    }

    // Get providers and rotation frequency
    const providers = getProviders();
    const providerRotationFrequency = parseInt(document.getElementById('providerRotationFrequency').value) || 1;

    // Parse model mappings JSON
    let modelMappings = null;
    if (modelMappingsStr) {
        try {
            modelMappings = JSON.parse(modelMappingsStr);
        } catch (e) {
            showToast('Invalid JSON format for model mappings', 'error');
            return;
        }
    }

    if (maxRpm < 1 || maxRpm > 1000) {
        showToast('Max RPM must be between 1 and 1000', 'warning');
        return;
    }

    if (maxRpd < 1 || maxRpd > 100000) {
        showToast('Max RPD must be between 1 and 100000', 'warning');
        return;
    }

    try {
        const response = await apiFetch('/admin/keys/generate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name,
                claim_code: claimCode,  // **Validates: Requirements 3.1, 3.2**
                max_rpm: maxRpm,
                max_rpd: maxRpd,
                target_url: targetUrl,
                target_api_key: targetApiKey,
                http_referer: httpReferer,
                no_auth: noAuth,
                disable_model_fetch: disableModelFetch,
                expires_at: expiresAt,
                // IP whitelist/blacklist removed - Discord-based access control is now used
                model_mappings: modelMappings,
                providers: providers,
                provider_rotation_frequency: providerRotationFrequency,
                max_total_tokens: maxTotalTokens,
                max_context_tokens: maxContextTokens,
                custom_prefills: customPrefills
            })
        });

        if (!response.ok) {
            throw new Error('Failed to generate API key');
        }

        const data = await response.json();

        closeGenerateModal();

        // Store the full key for later copying (keyed by prefix)
        fullKeyStorage[data.prefix] = data.api_key;

        // Also store in sessionStorage for persistence during browser session
        try {
            const storedKeys = JSON.parse(sessionStorage.getItem('moi_full_keys') || '{}');
            storedKeys[data.prefix] = data.api_key;
            sessionStorage.setItem('moi_full_keys', JSON.stringify(storedKeys));
        } catch (e) {
            console.warn('Could not store key in sessionStorage:', e);
        }

        // Show the generated key
        document.getElementById('generatedKey').textContent = data.api_key;
        document.getElementById('keyDisplayModal').classList.add('active');

        // Reload keys list
        loadAPIKeys();

        addConsoleLog('SUCCESS', `New API key generated: ${data.prefix}`);
        showToast('API key generated successfully!', 'success');
    } catch (error) {
        console.error('Error generating API key:', error);
        addConsoleLog('ERROR', `Failed to generate API key: ${error.message}`);
        showToast('Failed to generate API key', 'error');
    }
}

// Close key display modal
function closeKeyDisplayModal() {
    document.getElementById('keyDisplayModal').classList.remove('active');
    document.getElementById('generatedKey').textContent = '';
}

// Copy generated key to clipboard
function copyKey() {
    const keyText = document.getElementById('generatedKey').textContent;
    navigator.clipboard.writeText(keyText).then(() => {
        addConsoleLog('INFO', 'API key copied to clipboard');
        showToast('API key copied to clipboard!', 'success');

        // Visual feedback
        const btn = event.target;
        const originalText = btn.textContent;
        btn.textContent = '✓ Copied!';
        btn.style.background = 'var(--success-color)';

        setTimeout(() => {
            btn.textContent = originalText;
            btn.style.background = '';
        }, 2000);
    }).catch(err => {
        console.error('Failed to copy:', err);
        addConsoleLog('ERROR', 'Failed to copy to clipboard');
        showToast('Failed to copy to clipboard', 'error');
    });
}

// Copy model mappings from generate modal
function copyModelMappings() {
    const textarea = document.getElementById('modelMappings');
    const text = textarea.value.trim();

    if (!text) {
        showToast('No model mappings to copy', 'warning');
        return;
    }

    navigator.clipboard.writeText(text).then(() => {
        addConsoleLog('INFO', 'Model mappings copied to clipboard');
        showToast('Model mappings copied!', 'success');
    }).catch(err => {
        console.error('Failed to copy:', err);
        showToast('Failed to copy to clipboard', 'error');
    });
}

// Copy model mappings from edit modal
function copyEditModelMappings() {
    const textarea = document.getElementById('editModelMappings');
    const text = textarea.value.trim();

    if (!text) {
        showToast('No model mappings to copy', 'warning');
        return;
    }

    navigator.clipboard.writeText(text).then(() => {
        addConsoleLog('INFO', 'Model mappings copied to clipboard');
        showToast('Model mappings copied!', 'success');
    }).catch(err => {
        console.error('Failed to copy:', err);
        showToast('Failed to copy to clipboard', 'error');
    });
}

// ==================== PROVIDER MANAGEMENT FUNCTIONS ====================

// Counter for unique provider IDs
let providerCounter = 0;

// Add a new provider row to the generate modal
function addProvider(name = '', url = '', apiKey = '') {
    const container = document.getElementById('providersContainer');
    const providerId = `provider-${providerCounter++}`;

    const row = document.createElement('div');
    row.className = 'provider-row';
    row.id = providerId;
    row.innerHTML = `
        <input type="text" class="form-input provider-name" placeholder="Provider name" value="${escapeHtml(name)}">
        <input type="text" class="form-input provider-url mono" placeholder="https://api.example.com/v1" value="${escapeHtml(url)}">
        <input type="text" class="form-input provider-key mono" placeholder="API Key (sk-...)" value="${escapeHtml(apiKey)}">
        <button type="button" class="btn-remove" onclick="removeProvider('${providerId}')">✕</button>
    `;

    container.appendChild(row);
}

// Remove a provider row from the generate modal
function removeProvider(providerId) {
    const row = document.getElementById(providerId);
    if (row) {
        row.remove();
    }
}

// Get all providers from the generate modal
function getProviders() {
    const container = document.getElementById('providersContainer');
    const rows = container.querySelectorAll('.provider-row');
    const providers = [];

    rows.forEach(row => {
        const name = row.querySelector('.provider-name').value.trim();
        const url = row.querySelector('.provider-url').value.trim();
        const apiKey = row.querySelector('.provider-key').value.trim();

        // Only include if at least name and URL are provided
        if (name && url) {
            providers.push({ name, url, api_key: apiKey });
        }
    });

    return providers.length > 0 ? providers : null;
}

// Clear all providers from the generate modal
function clearProviders() {
    const container = document.getElementById('providersContainer');
    container.innerHTML = '';
}

// Update rotation frequency label for generate modal
function updateRotationFrequencyLabel(value) {
    const label = document.getElementById('rotationFrequencyLabel');
    if (label) {
        label.textContent = `${value} request(s)`;
    }
}

// Counter for unique edit provider IDs
let editProviderCounter = 0;

// Add a new provider row to the edit modal
function addEditProvider(name = '', url = '', apiKey = '') {
    const container = document.getElementById('editProvidersContainer');
    const providerId = `edit-provider-${editProviderCounter++}`;

    const row = document.createElement('div');
    row.className = 'provider-row';
    row.id = providerId;
    row.innerHTML = `
        <input type="text" class="form-input provider-name" placeholder="Provider name" value="${escapeHtml(name)}">
        <input type="text" class="form-input provider-url mono" placeholder="https://api.example.com/v1" value="${escapeHtml(url)}">
        <input type="text" class="form-input provider-key mono" placeholder="API Key (sk-...)" value="${escapeHtml(apiKey)}">
        <button type="button" class="btn-remove" onclick="removeEditProvider('${providerId}')">✕</button>
    `;

    container.appendChild(row);
}

// Remove a provider row from the edit modal
function removeEditProvider(providerId) {
    const row = document.getElementById(providerId);
    if (row) {
        row.remove();
    }
}

// Get all providers from the edit modal
function getEditProviders() {
    const container = document.getElementById('editProvidersContainer');
    const rows = container.querySelectorAll('.provider-row');
    const providers = [];

    rows.forEach(row => {
        const name = row.querySelector('.provider-name').value.trim();
        const url = row.querySelector('.provider-url').value.trim();
        const apiKey = row.querySelector('.provider-key').value.trim();

        // Only include if at least name and URL are provided
        if (name && url) {
            providers.push({ name, url, api_key: apiKey });
        }
    });

    return providers.length > 0 ? providers : null;
}

// Clear all providers from the edit modal
function clearEditProviders() {
    const container = document.getElementById('editProvidersContainer');
    container.innerHTML = '';
}

// Update rotation frequency label for edit modal
function updateEditRotationFrequencyLabel(value) {
    const label = document.getElementById('editRotationFrequencyLabel');
    if (label) {
        label.textContent = `${value} request(s)`;
    }
}

// Populate providers in the edit modal from key data
function populateEditProviders(providers) {
    clearEditProviders();
    if (providers && Array.isArray(providers)) {
        providers.forEach(p => {
            addEditProvider(p.name || '', p.url || '', p.api_key || '');
        });
    }
}

// ==================== END PROVIDER MANAGEMENT ====================
// Edit key - now accepts full key object
function editKey(keyId, currentRpm, currentRpd, keyData) {
    try {
        currentEditKeyId = keyId;
        document.getElementById('editMaxRpm').value = currentRpm;
        document.getElementById('editMaxRpd').value = currentRpd;

        // Populate new fields if keyData is provided
        if (keyData) {
            const editClaimCode = document.getElementById('editClaimCode');
            const editTargetUrl = document.getElementById('editTargetUrl');
            const editTargetApiKey = document.getElementById('editTargetApiKey');
            const editHttpReferer = document.getElementById('editHttpReferer');
            const editNoAuth = document.getElementById('editNoAuth');
            const editDisableModelFetch = document.getElementById('editDisableModelFetch');
            const editExpiresAt = document.getElementById('editExpiresAt');
            const editIpWhitelist = document.getElementById('editIpWhitelist');
            const editIpBlacklist = document.getElementById('editIpBlacklist');
            const editModelMappings = document.getElementById('editModelMappings');
            const editMaxTotalTokens = document.getElementById('editMaxTotalTokens');
            const editMaxContextTokens = document.getElementById('editMaxContextTokens');
            const editCustomPrefills = document.getElementById('editCustomPrefills');
            const editProviderRotationFrequency = document.getElementById('editProviderRotationFrequency');

            if (editClaimCode) editClaimCode.value = keyData.claim_code || '';
            if (editTargetUrl) editTargetUrl.value = keyData.target_url || '';
            if (editTargetApiKey) editTargetApiKey.value = keyData.target_api_key || '';
            if (editHttpReferer) editHttpReferer.value = keyData.http_referer || '';
            if (editNoAuth) editNoAuth.checked = keyData.no_auth || false;
            if (editDisableModelFetch) editDisableModelFetch.checked = keyData.disable_model_fetch || false;
            if (editExpiresAt) editExpiresAt.value = keyData.expires_at || '';
            if (editIpWhitelist) editIpWhitelist.value = keyData.ip_whitelist || '';
            if (editIpBlacklist) editIpBlacklist.value = keyData.ip_blacklist || '';
            if (editModelMappings) editModelMappings.value = keyData.model_mappings ? JSON.stringify(keyData.model_mappings, null, 2) : '';
            if (editMaxTotalTokens) editMaxTotalTokens.value = keyData.max_total_tokens || '';
            if (editMaxContextTokens) editMaxContextTokens.value = keyData.max_context_tokens || '';
            if (editCustomPrefills) editCustomPrefills.value = keyData.custom_prefills ? (typeof keyData.custom_prefills === 'string' ? keyData.custom_prefills : JSON.stringify(keyData.custom_prefills, null, 2)) : '';

            // Load model mappings into builder
            if (typeof loadModelMappingsToBuilder === 'function') {
                loadModelMappingsToBuilder(keyData.model_mappings ? JSON.stringify(keyData.model_mappings, null, 2) : '', true);
            }

            // Populate providers
            if (typeof populateEditProviders === 'function') {
                populateEditProviders(keyData.providers);
            }

            // Set rotation frequency
            const rotationFreq = keyData.provider_rotation_frequency || 1;
            if (editProviderRotationFrequency) editProviderRotationFrequency.value = rotationFreq;
            if (typeof updateEditRotationFrequencyLabel === 'function') {
                updateEditRotationFrequencyLabel(rotationFreq);
            }
        } else {
            // Reset to defaults if no keyData
            const editClaimCode = document.getElementById('editClaimCode');
            const editTargetUrl = document.getElementById('editTargetUrl');
            const editTargetApiKey = document.getElementById('editTargetApiKey');
            const editHttpReferer = document.getElementById('editHttpReferer');
            const editNoAuth = document.getElementById('editNoAuth');
            const editDisableModelFetch = document.getElementById('editDisableModelFetch');
            const editExpiresAt = document.getElementById('editExpiresAt');
            const editIpWhitelist = document.getElementById('editIpWhitelist');
            const editIpBlacklist = document.getElementById('editIpBlacklist');
            const editModelMappings = document.getElementById('editModelMappings');
            const editMaxTotalTokens = document.getElementById('editMaxTotalTokens');
            const editMaxContextTokens = document.getElementById('editMaxContextTokens');
            const editCustomPrefills = document.getElementById('editCustomPrefills');
            const editProviderRotationFrequency = document.getElementById('editProviderRotationFrequency');

            if (editClaimCode) editClaimCode.value = '';
            if (editTargetUrl) editTargetUrl.value = '';
            if (editTargetApiKey) editTargetApiKey.value = '';
            if (editHttpReferer) editHttpReferer.value = '';
            if (editNoAuth) editNoAuth.checked = false;
            if (editDisableModelFetch) editDisableModelFetch.checked = false;
            if (editExpiresAt) editExpiresAt.value = '';
            if (editIpWhitelist) editIpWhitelist.value = '';
            if (editIpBlacklist) editIpBlacklist.value = '';
            if (editModelMappings) editModelMappings.value = '';
            if (editMaxTotalTokens) editMaxTotalTokens.value = '';
            if (editMaxContextTokens) editMaxContextTokens.value = '';
            if (editCustomPrefills) editCustomPrefills.value = '';
            if (typeof clearEditModelMappingsBuilder === 'function') clearEditModelMappingsBuilder();
            if (typeof clearEditProviders === 'function') clearEditProviders();
            if (editProviderRotationFrequency) editProviderRotationFrequency.value = 1;
            if (typeof updateEditRotationFrequencyLabel === 'function') updateEditRotationFrequencyLabel(1);
        }

        document.getElementById('editModal').classList.add('active');
        console.log('Edit modal opened for key:', keyId);
    } catch (error) {
        console.error('Error opening edit modal:', error);
        showToast('Failed to open edit modal: ' + error.message, 'error');
    }
}

// Close edit modal
function closeEditModal() {
    document.getElementById('editModal').classList.remove('active');
    currentEditKeyId = null;
}

// Save edited limits and settings
async function saveEdits() {
    const claimCode = document.getElementById('editClaimCode').value.trim() || null;  // **Validates: Requirements 3.1, 3.3**
    const maxRpm = parseInt(document.getElementById('editMaxRpm').value);
    const maxRpd = parseInt(document.getElementById('editMaxRpd').value);
    const httpReferer = document.getElementById('editHttpReferer').value.trim() || null;

    // Target URL and API key fields (CRITICAL - these were missing!)
    const targetUrl = document.getElementById('editTargetUrl').value.trim() || null;
    const targetApiKey = document.getElementById('editTargetApiKey').value.trim() || null;
    const noAuth = document.getElementById('editNoAuth').checked;

    // New fields
    const disableModelFetch = document.getElementById('editDisableModelFetch').checked;
    const expiresAt = document.getElementById('editExpiresAt').value || null;
    // IP whitelist/blacklist removed - Discord-based access control is now used
    const modelMappingsStr = document.getElementById('editModelMappings').value.trim();

    // Get providers and rotation frequency
    const providers = getEditProviders();
    const providerRotationFrequency = parseInt(document.getElementById('editProviderRotationFrequency').value) || 1;

    // Token limits and custom prefills - **Validates: Requirements 8.1, 8.3, 8.5**
    const maxTotalTokensStr = document.getElementById('editMaxTotalTokens').value.trim();
    const maxContextTokensStr = document.getElementById('editMaxContextTokens').value.trim();
    const customPrefillsStr = document.getElementById('editCustomPrefills').value.trim();
    
    // Parse token limits - use -1 to indicate "remove limit", null to indicate "no change"
    // Empty string means remove the limit (set to null in backend)
    const maxTotalTokens = maxTotalTokensStr === '' ? -1 : (parseInt(maxTotalTokensStr) || null);
    const maxContextTokens = maxContextTokensStr === '' ? -1 : (parseInt(maxContextTokensStr) || null);
    
    // Parse custom prefills - can be JSON object or plain string
    let customPrefills = null;
    if (customPrefillsStr) {
        try {
            // Try to parse as JSON first
            customPrefills = JSON.parse(customPrefillsStr);
        } catch (e) {
            // If not valid JSON, use as plain string
            customPrefills = customPrefillsStr;
        }
    }

    // Parse model mappings JSON
    let modelMappings = null;
    if (modelMappingsStr) {
        try {
            modelMappings = JSON.parse(modelMappingsStr);
        } catch (e) {
            showToast('Invalid JSON format for model mappings', 'error');
            return;
        }
    }

    if (maxRpm < 1 || maxRpm > 1000) {
        showToast('Max RPM must be between 1 and 1000', 'warning');
        return;
    }

    if (maxRpd < 1 || maxRpd > 100000) {
        showToast('Max RPD must be between 1 and 100000', 'warning');
        return;
    }

    // Validate context token limits - **Validates: Requirements 8.1**
    if (maxContextTokens !== null && maxContextTokens !== -1 && maxContextTokens < 1) {
        showToast('Max context tokens must be a positive number or empty to remove limit', 'warning');
        return;
    }

    if (maxTotalTokens !== null && maxTotalTokens !== -1 && maxTotalTokens < 1) {
        showToast('Max total tokens must be a positive number or empty to remove limit', 'warning');
        return;
    }

    try {
        const response = await apiFetch(`/admin/keys/${currentEditKeyId}/limits`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                claim_code: claimCode,  // **Validates: Requirements 3.1, 3.3**
                max_rpm: maxRpm,
                max_rpd: maxRpd,
                target_url: targetUrl,
                target_api_key: targetApiKey,
                no_auth: noAuth,
                http_referer: httpReferer,
                disable_model_fetch: disableModelFetch,
                expires_at: expiresAt,
                // IP whitelist/blacklist removed - Discord-based access control is now used
                model_mappings: modelMappings,
                providers: providers,
                provider_rotation_frequency: providerRotationFrequency,
                max_total_tokens: maxTotalTokens,
                max_context_tokens: maxContextTokens,
                custom_prefills: customPrefills
            })
        });

        if (!response.ok) {
            throw new Error('Failed to update settings');
        }

        closeEditModal();
        loadAPIKeys();

        addConsoleLog('SUCCESS', `Updated settings for key ID ${currentEditKeyId}`);
        showToast('Key settings updated successfully!', 'success');
    } catch (error) {
        console.error('Error updating settings:', error);
        addConsoleLog('ERROR', `Failed to update settings: ${error.message}`);
        showToast('Failed to update key settings', 'error');
    }
}

// Toggle API key enabled/disabled
async function toggleKey(keyId) {
    try {
        const response = await apiFetch(`/admin/keys/${keyId}/toggle`, {
            method: 'PUT'
        });

        if (!response.ok) {
            throw new Error('Failed to toggle API key');
        }

        loadAPIKeys();
        addConsoleLog('SUCCESS', `Toggled API key ID ${keyId}`);
        showToast('API key status updated', 'success');
    } catch (error) {
        console.error('Error toggling API key:', error);
        addConsoleLog('ERROR', `Failed to toggle API key: ${error.message}`);
        showToast('Failed to toggle API key', 'error');
    }
}

// ==================== SUB-KEYS MANAGEMENT ====================

// Store for sub-keys data and expansion state
const subKeysCache = {};
const subKeysExpanded = {};

/**
 * Toggle sub-keys expansion for a main key
 * Fetches sub-keys on first expand, uses cache on subsequent toggles
 * 
 * **Validates: Requirements 9.3, 9.4, 9.5**
 */
async function toggleSubKeys(keyId) {
    const container = document.getElementById(`sub-keys-container-${keyId}`);
    const icon = document.getElementById(`sub-keys-icon-${keyId}`);
    const countElement = document.getElementById(`sub-keys-count-${keyId}`);
    
    if (!container || !icon) return;
    
    const isExpanded = subKeysExpanded[keyId] || false;
    
    if (isExpanded) {
        // Collapse
        container.style.display = 'none';
        icon.classList.remove('expanded');
        subKeysExpanded[keyId] = false;
    } else {
        // Expand
        container.style.display = 'block';
        icon.classList.add('expanded');
        subKeysExpanded[keyId] = true;
        
        // Fetch sub-keys if not cached
        if (!subKeysCache[keyId]) {
            await loadSubKeys(keyId);
        } else {
            renderSubKeys(keyId, subKeysCache[keyId]);
        }
    }
}

/**
 * Load sub-keys for a main key from the API
 * 
 * **Validates: Requirements 9.3, 9.4**
 */
async function loadSubKeys(keyId) {
    const container = document.getElementById(`sub-keys-container-${keyId}`);
    const countElement = document.getElementById(`sub-keys-count-${keyId}`);
    
    if (!container) return;
    
    // Show loading state
    container.innerHTML = '<div class="sub-keys-loading">Loading sub-keys...</div>';
    
    try {
        const response = await apiFetch(`/api/admin/keys/${keyId}/sub-keys`);
        
        if (!response.ok) {
            throw new Error('Failed to load sub-keys');
        }
        
        const data = await response.json();
        
        // Cache the data
        subKeysCache[keyId] = data.sub_keys || [];
        
        // Update count
        if (countElement) {
            countElement.textContent = `(${data.count || 0})`;
        }
        
        // Render sub-keys
        renderSubKeys(keyId, data.sub_keys || []);
        
    } catch (error) {
        console.error('Error loading sub-keys:', error);
        container.innerHTML = `<div class="sub-keys-empty">Failed to load sub-keys: ${escapeHtml(error.message)}</div>`;
        showToast('Failed to load sub-keys', 'error');
    }
}

/**
 * Render sub-keys list in the container
 * Shows Discord username as sub-key name, sub-key prefix, request count, and last used time
 * 
 * **Validates: Requirements 9.3, 9.4, 9.5**
 */
function renderSubKeys(keyId, subKeys) {
    const container = document.getElementById(`sub-keys-container-${keyId}`);
    const countElement = document.getElementById(`sub-keys-count-${keyId}`);
    
    if (!container) return;
    
    // Update count
    if (countElement) {
        countElement.textContent = `(${subKeys.length})`;
    }
    
    if (subKeys.length === 0) {
        container.innerHTML = '<div class="sub-keys-empty">No sub-keys yet. Sub-keys are created when Discord users claim this key with a claim code.</div>';
        return;
    }
    
    const html = subKeys.map(subKey => {
        const lastUsed = subKey.last_used 
            ? formatRelativeTime(subKey.last_used)
            : 'Never';
        
        const statusClass = subKey.enabled !== false ? 'active' : 'disabled';
        const statusText = subKey.enabled !== false ? '✓' : '✗';
        
        // Use sub_key_id (from API) or id as fallback
        const subKeyId = subKey.sub_key_id || subKey.id;
        
        // Add click handler to open user details modal
        // **Validates: Requirements 1.1**
        return `
            <div class="sub-key-item ${statusClass} clickable" 
                 onclick="showUserDetailsModal(${subKeyId})"
                 title="Click to view user details">
                <img class="sub-key-avatar" 
                     src="${escapeHtml(subKey.discord_avatar_url)}" 
                     alt="${escapeHtml(subKey.discord_username)}"
                     onerror="this.src='https://cdn.discordapp.com/embed/avatars/0.png'">
                <div class="sub-key-info">
                    <div class="sub-key-username">👤 ${escapeHtml(subKey.discord_username)}</div>
                    <div class="sub-key-prefix">${escapeHtml(subKey.sub_key_prefix || 'Moi-sub-...')}</div>
                </div>
                <div class="sub-key-stats">
                    <div class="sub-key-requests">${subKey.usage_count || 0} requests</div>
                    <div class="sub-key-last-used">Last: ${lastUsed}</div>
                </div>
                <div class="sub-key-view-icon">👁️</div>
            </div>
        `;
    }).join('');
    
    container.innerHTML = html;
}

/**
 * Format a timestamp as relative time (e.g., "2h ago", "5m ago")
 */
function formatRelativeTime(timestamp) {
    if (!timestamp) return 'Never';
    
    try {
        const date = new Date(timestamp.includes('Z') ? timestamp : timestamp + 'Z');
        const now = new Date();
        const diffMs = now - date;
        const diffSec = Math.floor(diffMs / 1000);
        const diffMin = Math.floor(diffSec / 60);
        const diffHour = Math.floor(diffMin / 60);
        const diffDay = Math.floor(diffHour / 24);
        
        if (diffDay > 0) return `${diffDay}d ago`;
        if (diffHour > 0) return `${diffHour}h ago`;
        if (diffMin > 0) return `${diffMin}m ago`;
        return 'Just now';
    } catch (e) {
        return timestamp;
    }
}

/**
 * Refresh sub-keys for a specific key (clears cache and reloads)
 */
async function refreshSubKeys(keyId) {
    delete subKeysCache[keyId];
    if (subKeysExpanded[keyId]) {
        await loadSubKeys(keyId);
    }
}

// ==================== SUB-KEYS AUTO-REFRESH ====================

// Interval for sub-keys auto-refresh
let subKeysRefreshInterval = null;

/**
 * Start auto-refresh for sub-keys list
 * Refreshes every 3 seconds, preserving scroll position and expanded state
 * 
 * **Validates: Requirements 5.1, 5.3**
 */
function startSubKeysAutoRefresh() {
    // Clear any existing interval
    if (subKeysRefreshInterval) {
        clearInterval(subKeysRefreshInterval);
    }

    // Refresh every 3 seconds
    subKeysRefreshInterval = setInterval(() => {
        refreshExpandedSubKeysSilently();
    }, 3000);
}

/**
 * Stop auto-refresh for sub-keys list
 */
function stopSubKeysAutoRefresh() {
    if (subKeysRefreshInterval) {
        clearInterval(subKeysRefreshInterval);
        subKeysRefreshInterval = null;
    }
}

/**
 * Silently refresh all expanded sub-key lists
 * Updates data without full re-render, preserving scroll position
 * 
 * **Validates: Requirements 5.2, 5.3**
 */
async function refreshExpandedSubKeysSilently() {
    // Get all expanded key IDs
    const expandedKeyIds = Object.keys(subKeysExpanded).filter(keyId => subKeysExpanded[keyId]);
    
    if (expandedKeyIds.length === 0) return;
    
    // Refresh each expanded sub-keys list silently
    for (const keyId of expandedKeyIds) {
        await refreshSubKeysSilently(parseInt(keyId));
    }
}

/**
 * Silently refresh sub-keys for a specific key
 * Updates data in place without flicker, preserving scroll position
 * 
 * **Validates: Requirements 5.2, 5.3**
 */
async function refreshSubKeysSilently(keyId) {
    const container = document.getElementById(`sub-keys-container-${keyId}`);
    const countElement = document.getElementById(`sub-keys-count-${keyId}`);
    
    if (!container || !subKeysExpanded[keyId]) return;
    
    // Store scroll position before update
    const scrollTop = container.scrollTop;
    
    try {
        const response = await apiFetch(`/api/admin/keys/${keyId}/sub-keys`);
        
        if (!response.ok) {
            // Silently fail - don't disrupt the UI
            return;
        }
        
        const data = await response.json();
        const newSubKeys = data.sub_keys || [];
        const oldSubKeys = subKeysCache[keyId] || [];
        
        // Update cache
        subKeysCache[keyId] = newSubKeys;
        
        // Update count
        if (countElement) {
            countElement.textContent = `(${data.count || 0})`;
        }
        
        // Check if data has changed to avoid unnecessary re-renders
        if (hasSubKeysDataChanged(oldSubKeys, newSubKeys)) {
            // Update individual sub-key items in place
            updateSubKeysInPlace(keyId, newSubKeys);
        }
        
        // Restore scroll position
        container.scrollTop = scrollTop;
        
    } catch (error) {
        // Silently fail - don't disrupt the UI
        console.debug('Silent sub-keys refresh failed:', error);
    }
}

/**
 * Check if sub-keys data has changed
 * Compares relevant fields to determine if update is needed
 */
function hasSubKeysDataChanged(oldSubKeys, newSubKeys) {
    if (oldSubKeys.length !== newSubKeys.length) return true;
    
    for (let i = 0; i < newSubKeys.length; i++) {
        const oldKey = oldSubKeys[i];
        const newKey = newSubKeys[i];
        
        if (!oldKey || !newKey) return true;
        if (oldKey.id !== newKey.id) return true;
        if (oldKey.usage_count !== newKey.usage_count) return true;
        if (oldKey.last_used !== newKey.last_used) return true;
        if (oldKey.enabled !== newKey.enabled) return true;
        if (oldKey.discord_username !== newKey.discord_username) return true;
    }
    
    return false;
}

/**
 * Update sub-key items in place without full re-render
 * Only updates changed elements to prevent flicker
 * 
 * **Validates: Requirements 5.2**
 */
function updateSubKeysInPlace(keyId, subKeys) {
    const container = document.getElementById(`sub-keys-container-${keyId}`);
    if (!container) return;
    
    // If structure changed significantly, do a full re-render
    const existingItems = container.querySelectorAll('.sub-key-item');
    if (existingItems.length !== subKeys.length) {
        renderSubKeys(keyId, subKeys);
        return;
    }
    
    // Update each item in place
    subKeys.forEach((subKey, index) => {
        const item = existingItems[index];
        if (!item) return;
        
        // Update request count
        const requestsEl = item.querySelector('.sub-key-requests');
        if (requestsEl) {
            const newText = `${subKey.usage_count || 0} requests`;
            if (requestsEl.textContent !== newText) {
                requestsEl.textContent = newText;
            }
        }
        
        // Update last used time
        const lastUsedEl = item.querySelector('.sub-key-last-used');
        if (lastUsedEl) {
            const lastUsed = subKey.last_used 
                ? formatRelativeTime(subKey.last_used)
                : 'Never';
            const newText = `Last: ${lastUsed}`;
            if (lastUsedEl.textContent !== newText) {
                lastUsedEl.textContent = newText;
            }
        }
        
        // Update status class
        const statusClass = subKey.enabled !== false ? 'active' : 'disabled';
        if (item.classList.contains('active') && statusClass === 'disabled') {
            item.classList.remove('active');
            item.classList.add('disabled');
        } else if (item.classList.contains('disabled') && statusClass === 'active') {
            item.classList.remove('disabled');
            item.classList.add('active');
        }
        
        // Update username if changed
        const usernameEl = item.querySelector('.sub-key-username');
        if (usernameEl) {
            const newUsername = `👤 ${escapeHtml(subKey.discord_username)}`;
            if (usernameEl.innerHTML !== newUsername) {
                usernameEl.innerHTML = newUsername;
            }
        }
    });
}

// ==================== AGGREGATED USAGE ====================

// Store for aggregated usage data
const aggregatedUsageCache = {};

/**
 * Load aggregated usage for a main key from the API
 * This shows the total usage across all sub-keys
 * 
 * **Validates: Requirements 10.6**
 */
async function loadAggregatedUsage(keyId) {
    try {
        const response = await apiFetch(`/api/admin/keys/${keyId}/aggregated-usage`);
        
        if (!response.ok) {
            throw new Error('Failed to load aggregated usage');
        }
        
        const data = await response.json();
        
        // Cache the data
        aggregatedUsageCache[keyId] = data;
        
        // Update the UI
        updateAggregatedUsageDisplay(keyId, data);
        
        return data;
    } catch (error) {
        console.error('Error loading aggregated usage:', error);
        return null;
    }
}

/**
 * Update the aggregated usage display on a key card
 * Shows RPD, RPM, and tokens used across all sub-keys
 * 
 * **Validates: Requirements 10.6**
 */
function updateAggregatedUsageDisplay(keyId, usage) {
    // Update RPM display
    const rpmElement = document.getElementById(`rpm-${keyId}`);
    const rpmBarElement = document.getElementById(`rpm-bar-${keyId}`);
    if (rpmElement && usage.rpm_used !== undefined) {
        const rpmPercent = (usage.rpm_used / usage.max_rpm) * 100;
        const rpmColorClass = rpmPercent >= 95 ? 'danger' : rpmPercent >= 80 ? 'warning' : '';
        rpmElement.textContent = `${usage.rpm_used} / ${usage.max_rpm}`;
        rpmElement.className = `usage-value ${rpmColorClass}`;
        if (rpmBarElement) {
            rpmBarElement.style.width = `${Math.min(rpmPercent, 100)}%`;
            rpmBarElement.className = `usage-bar ${rpmColorClass}`;
        }
    }
    
    // Update RPD display
    const rpdElement = document.getElementById(`rpd-${keyId}`);
    const rpdBarElement = document.getElementById(`rpd-bar-${keyId}`);
    if (rpdElement && usage.rpd_used !== undefined) {
        const rpdPercent = (usage.rpd_used / usage.max_rpd) * 100;
        const rpdColorClass = rpdPercent >= 95 ? 'danger' : rpdPercent >= 80 ? 'warning' : '';
        rpdElement.textContent = `${usage.rpd_used} / ${usage.max_rpd}`;
        rpdElement.className = `usage-value ${rpdColorClass}`;
        if (rpdBarElement) {
            rpdBarElement.style.width = `${Math.min(rpdPercent, 100)}%`;
            rpdBarElement.className = `usage-bar ${rpdColorClass}`;
        }
    }
    
    // Update tokens display if max_total_tokens is set
    const tokensElement = document.getElementById(`tokens-${keyId}`);
    const tokensBarElement = document.getElementById(`tokens-bar-${keyId}`);
    if (tokensElement && usage.max_total_tokens && usage.tokens_used !== undefined) {
        const tokensPercent = (usage.tokens_used / usage.max_total_tokens) * 100;
        const tokensColorClass = tokensPercent >= 90 ? 'danger' : tokensPercent >= 80 ? 'warning' : '';
        tokensElement.textContent = `${formatNumber(usage.tokens_used)} / ${formatNumber(usage.max_total_tokens)}`;
        tokensElement.className = `usage-value ${tokensColorClass}`;
        if (tokensBarElement) {
            tokensBarElement.style.width = `${Math.min(tokensPercent, 100)}%`;
            tokensBarElement.className = `usage-bar ${tokensColorClass}`;
        }
    }
    
    // Update sub-key count if available
    const countElement = document.getElementById(`sub-keys-count-${keyId}`);
    if (countElement && usage.sub_key_count !== undefined) {
        countElement.textContent = `(${usage.sub_key_count})`;
    }
}

/**
 * Load aggregated usage for all keys
 * Called after keys are loaded to update the display with aggregated data
 */
async function loadAllAggregatedUsage() {
    if (!allKeys || allKeys.length === 0) return;
    
    // Load aggregated usage for each key in parallel
    const promises = allKeys.map(key => loadAggregatedUsage(key.id));
    await Promise.all(promises);
}

// Delete API key
// Delete API key
window.deleteKey = async function (keyId) {
    console.log('Delete key requested for ID:', keyId);
    if (!confirm('Are you sure you want to delete this API key? This action cannot be undone.')) {
        return;
    }

    try {
        const response = await apiFetch(`/admin/keys/${keyId}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            throw new Error('Failed to delete API key');
        }

        loadAPIKeys();
        addConsoleLog('SUCCESS', `Deleted API key ID ${keyId}`);
        showToast('API key deleted', 'info');
    } catch (error) {
        console.error('Error deleting API key:', error);
        addConsoleLog('ERROR', `Failed to delete API key: ${error.message}`);
        showToast('Failed to delete API key', 'error');
    }
}

// Refresh key limits (reset RPM/RPD counters)
async function refreshKeyLimits(keyId) {
    const card = document.getElementById(`key-card-${keyId}`);
    if (card) {
        card.classList.add('updating');
    }

    try {
        const response = await apiFetch(`/admin/keys/${keyId}/refresh`, {
            method: 'POST'
        });

        if (!response.ok) {
            throw new Error('Failed to refresh key limits');
        }

        const result = await response.json();

        // Update the display
        const rpmElement = document.getElementById(`rpm-${keyId}`);
        const rpdElement = document.getElementById(`rpd-${keyId}`);
        const rpmBar = document.getElementById(`rpm-bar-${keyId}`);
        const rpdBar = document.getElementById(`rpd-bar-${keyId}`);

        if (rpmElement && rpdElement) {
            // Reset to 0/X format
            const maxRpm = parseInt(rpmElement.textContent.split('/')[1]);
            const maxRpd = parseInt(rpdElement.textContent.split('/')[1]);

            rpmElement.textContent = `0 / ${maxRpm}`;
            rpdElement.textContent = `0 / ${maxRpd}`;

            // Remove warning/danger classes
            rpmElement.classList.remove('warning', 'danger');
            rpdElement.classList.remove('warning', 'danger');
        }

        // Reset usage bars
        if (rpmBar) {
            rpmBar.style.width = '0%';
            rpmBar.classList.remove('warning', 'danger');
        }
        if (rpdBar) {
            rpdBar.style.width = '0%';
            rpdBar.classList.remove('warning', 'danger');
        }

        addConsoleLog('SUCCESS', `Refreshed rate limits for key ID ${keyId}`);
        showToast('Rate limits reset successfully!', 'success');

        if (card) {
            setTimeout(() => {
                card.classList.remove('updating');
            }, 600);
        }

    } catch (error) {
        console.error('Error refreshing limits:', error);
        addConsoleLog('ERROR', `Failed to refresh limits: ${error.message}`);
        showToast('Failed to reset rate limits', 'error');
        if (card) {
            card.classList.remove('updating');
        }
    }
}

// Show schedule modal
let currentScheduleKeyId = null;
function showScheduleModal(keyId, currentHour) {
    currentScheduleKeyId = keyId;
    document.getElementById('refreshHour').value = currentHour || 0;
    document.getElementById('scheduleModal').classList.add('active');
}

// Close schedule modal
function closeScheduleModal() {
    document.getElementById('scheduleModal').classList.remove('active');
    currentScheduleKeyId = null;
}

// Save schedule
async function saveSchedule() {
    const hour = parseInt(document.getElementById('refreshHour').value);

    if (hour < 0 || hour > 23) {
        showToast('Please enter a valid hour between 0 and 23', 'warning');
        return;
    }

    try {
        const response = await apiFetch(`/admin/keys/${currentScheduleKeyId}/schedule`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ refresh_hour: hour })
        });

        if (!response.ok) {
            throw new Error('Failed to set schedule');
        }

        closeScheduleModal();
        loadAPIKeys(); // Reload to update the display

        addConsoleLog('SUCCESS', `Set auto-refresh schedule for key ID ${currentScheduleKeyId} at ${hour}:00 UTC`);
        showToast(`Auto-refresh scheduled for ${hour}:00 UTC`, 'success');

    } catch (error) {
        console.error('Error setting schedule:', error);
        addConsoleLog('ERROR', `Failed to set schedule: ${error.message}`);
        showToast('Failed to set schedule', 'error');
    }
}

// Close modals when clicking outside (only when clicking directly on the modal backdrop)
window.onclick = (event) => {
    if (event.target.classList.contains('modal-overlay') && event.target.classList.contains('active')) {
        event.target.classList.remove('active');
    }
};

// Load analytics data
async function loadAnalytics() {
    try {
        const response = await apiFetch('/admin/analytics');
        const data = await response.json();

        // Update total stats
        document.getElementById('totalRequests').textContent = formatNumber(data.total_requests || 0);
        document.getElementById('totalInputTokens').textContent = formatNumber(data.total_input_tokens || 0);
        document.getElementById('totalOutputTokens').textContent = formatNumber(data.total_output_tokens || 0);
        document.getElementById('totalCost').textContent = `$${(data.total_cost || 0).toFixed(4)}`;
        document.getElementById('totalTokens').textContent = formatNumber(data.total_tokens || 0);

        // Update models list
        const modelsList = document.getElementById('modelsList');
        const models = data.models_usage || [];

        if (models.length === 0) {
            modelsList.innerHTML = '<p class="loading">No model usage data yet</p>';
        } else {
            modelsList.innerHTML = models.map(model => `
                <div class="model-item">
                    <span class="model-name">${escapeHtml(model.model || 'Unknown')}</span>
                    <div class="model-stats">
                        <div class="model-stat">
                            <span class="model-stat-value">${formatNumber(model.request_count)}</span>
                            <span class="model-stat-label">Requests</span>
                        </div>
                        <div class="model-stat">
                            <span class="model-stat-value">${formatNumber(model.input_tokens)}</span>
                            <span class="model-stat-label">Input</span>
                        </div>
                        <div class="model-stat">
                            <span class="model-stat-value">${formatNumber(model.output_tokens)}</span>
                            <span class="model-stat-label">Output</span>
                        </div>
                        <div class="model-stat">
                            <span class="model-stat-value">${formatNumber(model.total_tokens)}</span>
                            <span class="model-stat-label">Total</span>
                        </div>
                    </div>
                </div>
            `).join('');
        }

    } catch (error) {
        console.error('Error loading analytics:', error);
        addConsoleLog('ERROR', `Failed to load analytics: ${error.message}`);
    }
}

// Refresh analytics
function refreshAnalytics() {
    loadAnalytics();
    loadChartData('24h');
    addConsoleLog('INFO', 'Analytics refreshed');
    showToast('Analytics refreshed!', 'success');
}

// Load recent requests
async function loadRecentRequests() {
    try {
        const response = await apiFetch('/admin/logs/recent?limit=10');
        const data = await response.json();

        const tbody = document.getElementById('requestLogBody');
        const logs = data.logs || [];

        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" class="loading">No recent requests</td></tr>';
        } else {
            tbody.innerHTML = logs.map(log => {
                const time = new Date(log.request_time.includes('Z') ? log.request_time : log.request_time + 'Z').toLocaleTimeString();
                const totalTokens = log.tokens_used || (log.input_tokens || 0) + (log.output_tokens || 0);
                const cost = log.cost !== undefined && log.cost !== null ? `$${log.cost.toFixed(4)}` : '-';
                const clientIp = log.client_ip || 'N/A';
                const clientApp = log.client_app || 'Unknown';
                
                // User attribution: show Discord username or "Master Key"
                // **Validates: Requirements 4.1, 4.2, 4.3**
                let userDisplay = 'Master Key';
                let userClass = 'user-master';
                if (log.discord_id) {
                    // Prefer global_name (display name), then username, then truncated ID
                    if (log.discord_global_name) {
                        userDisplay = log.discord_global_name;
                    } else if (log.discord_username) {
                        userDisplay = log.discord_username;
                    } else {
                        // Fallback to showing truncated Discord ID
                        userDisplay = `Discord: ${log.discord_id.substring(0, 8)}...`;
                    }
                    userClass = 'user-discord';
                }
                
                return `
                    <tr>
                        <td class="time-cell">${time}</td>
                        <td class="user-cell ${userClass}">${escapeHtml(userDisplay)}</td>
                        <td class="model-cell">${escapeHtml(log.model || 'Unknown')}</td>
                        <td class="token-cell input">${formatNumber(log.input_tokens || 0)}</td>
                        <td class="token-cell output">${formatNumber(log.output_tokens || 0)}</td>
                        <td class="token-cell total">${formatNumber(totalTokens)}</td>
                        <td class="cost-cell">${cost}</td>
                        <td class="client-app-cell">${escapeHtml(clientApp)}</td>
                        <td class="ip-cell">${escapeHtml(clientIp)}</td>
                    </tr>
                `;
            }).join('');
        }

    } catch (error) {
        console.error('Error loading recent requests:', error);
        const tbody = document.getElementById('requestLogBody');
        tbody.innerHTML = '<tr><td colspan="9" class="loading" style="color: var(--danger-color);">Failed to load</td></tr>';
    }
}

// Start auto-refresh for request log
function startRequestLogAutoRefresh() {
    // Clear any existing interval
    if (requestLogInterval) {
        clearInterval(requestLogInterval);
    }

    // Refresh every 3 seconds
    requestLogInterval = setInterval(() => {
        loadRecentRequests();
    }, 3000);
}

// Start auto-refresh for API key usage stats
function startKeyUsageAutoRefresh() {
    // Clear any existing interval
    if (keyUsageRefreshInterval) {
        clearInterval(keyUsageRefreshInterval);
    }

    // Refresh every 3 seconds (as requested)
    keyUsageRefreshInterval = setInterval(() => {
        refreshAllKeyUsage();
    }, 3000);
}

// Global variable for analytics interval
let analyticsRefreshInterval = null;

// Start auto-refresh for analytics
// **Validates: Requirements 6.1, 6.2** - Use stored state variables to preserve time range selection
function startAnalyticsAutoRefresh() {
    if (analyticsRefreshInterval) {
        clearInterval(analyticsRefreshInterval);
    }

    // Refresh every 3 seconds
    analyticsRefreshInterval = setInterval(() => {
        loadAnalytics();
        // Also refresh chart occasionally, but maybe not every 3s to stay smooth
        // Monthly/Weekly charts don't change that fast
    }, 3000);

    // Refresh chart data every 10 seconds using stored state variables
    // This preserves the user's time range selection instead of reverting to default
    setInterval(() => {
        // Use stored state variables instead of querying DOM
        const period = selectedRequestsChartPeriod === 'hourly' ? '24h' : '7d';
        loadChartData(period);
    }, 10000);
}

// Refresh usage stats for all keys without reloading the entire list
async function refreshAllKeyUsage() {
    try {
        const response = await apiFetch('/admin/keys/list');
        if (!response.ok) {
            console.error('Failed to fetch keys list');
            return;
        }
        const data = await response.json();
        
        if (!data.keys || !Array.isArray(data.keys)) {
            console.error('Invalid keys data:', data);
            return;
        }

        // Update allKeys with fresh data from the server
        allKeys = data.keys;

        for (const key of data.keys) {
            try {
                const usageResponse = await apiFetch(`/admin/keys/${key.id}/usage`);
                
                // Skip if key not found (404) - key may have been deleted
                if (usageResponse.status === 404) {
                    continue;
                }
                
                if (!usageResponse.ok) {
                    continue;
                }
                
                const usageData = await usageResponse.json();

                // Update the display elements if they exist
                const rpmElement = document.getElementById(`rpm-${key.id}`);
                const rpdElement = document.getElementById(`rpd-${key.id}`);
                const rpmBar = document.getElementById(`rpm-bar-${key.id}`);
                const rpdBar = document.getElementById(`rpd-bar-${key.id}`);
                const card = document.getElementById(`key-card-${key.id}`);

                if (rpmElement && rpdElement && card) {
                    const currentRpm = usageData.current_rpm || 0;
                    const currentRpd = usageData.current_rpd || 0;
                    const lastUsed = key.last_used_at ? new Date(key.last_used_at).toLocaleString() : 'Never';

                    // Update last used meta value if it exists
                    const metaValues = card.querySelectorAll('.meta-value');
                    if (metaValues.length >= 2) {
                        metaValues[1].textContent = lastUsed;
                    }

                    // Calculate percentages
                    const rpmPercent = (currentRpm / key.max_rpm) * 100;
                    const rpdPercent = (currentRpd / key.max_rpd) * 100;

                    // Determine color classes
                    const rpmColorClass = rpmPercent >= 95 ? 'danger' : rpmPercent >= 80 ? 'warning' : '';
                    const rpdColorClass = rpdPercent >= 95 ? 'danger' : rpdPercent >= 80 ? 'warning' : '';

                    // Update text
                    rpmElement.textContent = `${currentRpm} / ${key.max_rpm}`;
                    rpdElement.textContent = `${currentRpd} / ${key.max_rpd}`;

                    // Update color classes
                    rpmElement.className = `usage-value ${rpmColorClass}`;
                    rpdElement.className = `usage-value ${rpdColorClass}`;

                    // Update usage bars
                    if (rpmBar) {
                        const rpmBarInner = rpmBar.querySelector('.usage-bar') || rpmBar;
                        rpmBarInner.style.width = `${Math.min(rpmPercent, 100)}%`;
                        rpmBarInner.className = `usage-bar ${rpmColorClass}`;
                    }
                    if (rpdBar) {
                        const rpdBarInner = rpdBar.querySelector('.usage-bar') || rpdBar;
                        rpdBarInner.style.width = `${Math.min(rpdPercent, 100)}%`;
                        rpdBarInner.className = `usage-bar ${rpdColorClass}`;
                    }

                    // New: Token Quota refresh
                    const tokensElement = document.getElementById(`tokens-${key.id}`);
                    // Note: tokens-bar-id is now on the inner bar
                    const tokensBar = document.getElementById(`tokens-bar-${key.id}`);
                    if (tokensElement && tokensBar && key.max_total_tokens) {
                        const totalTokensUsed = key.total_tokens_used || 0;
                        const tokenPercent = (totalTokensUsed / key.max_total_tokens) * 100;
                        const tokenColorClass = tokenPercent >= 90 ? 'danger' : tokenPercent >= 80 ? 'warning' : '';

                        tokensElement.textContent = `${formatNumber(totalTokensUsed)} / ${formatNumber(key.max_total_tokens)}`;
                        tokensElement.className = `usage-value ${tokenColorClass}`;
                        tokensBar.style.width = `${Math.min(tokenPercent, 100)}%`;
                        tokensBar.className = `usage-bar ${tokenColorClass}`;
                    }
                }
            } catch (e) {
                // Silently ignore individual key errors
            }
        }
    } catch (error) {
        // Silently ignore errors during auto-refresh
        console.error('Error refreshing key usage:', error);
    }
}

// Format large numbers with K/M suffixes
function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    num = parseInt(num);
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

// ==================== Model Mapping Builder Functions ====================

// Counter for unique IDs
let modelMappingCounter = 0;
let editModelMappingCounter = 0;

// Add a new model mapping entry (Generate modal)
function addModelMapping(aliasName = '', mappings = null) {
    const container = document.getElementById('modelMappingsBuilder');
    const entryId = `mapping-${modelMappingCounter++}`;

    const entry = document.createElement('div');
    entry.className = 'model-mapping-entry';
    entry.id = entryId;
    entry.style.cssText = 'border: 1px solid var(--border-color); border-radius: 8px; padding: 12px; margin-bottom: 8px; background: var(--bg-secondary);';

    // Determine if it's a simple mapping or per-provider mapping
    const isPerProvider = mappings && typeof mappings === 'object';
    const simpleValue = !isPerProvider && mappings ? mappings : '';

    entry.innerHTML = `
        <div style="display: flex; gap: 8px; align-items: center; margin-bottom: 8px;">
            <input type="text" class="form-input mapping-alias" placeholder="Alias (e.g., gpt-4)" value="${escapeHtml(aliasName)}" style="flex: 1;">
            <button type="button" class="btn btn-danger btn-sm" onclick="removeModelMapping('${entryId}')" title="Remove">✕</button>
        </div>
        <div class="mapping-type-toggle" style="margin-bottom: 8px;">
            <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
                <input type="checkbox" class="mapping-per-provider" ${isPerProvider ? 'checked' : ''} onchange="toggleMappingType('${entryId}')">
                <span style="font-size: 12px; color: var(--text-secondary);">Per-provider mapping</span>
            </label>
        </div>
        <div class="simple-mapping" style="${isPerProvider ? 'display: none;' : ''}">
            <input type="text" class="form-input mapping-simple-value" placeholder="Real model name (e.g., gpt-4-turbo-preview)" value="${escapeHtml(simpleValue)}">
        </div>
        <div class="provider-mappings" style="${isPerProvider ? '' : 'display: none;'}">
            <div class="provider-mapping-list"></div>
            <button type="button" class="btn btn-secondary btn-sm" onclick="addProviderMapping('${entryId}')" style="margin-top: 4px;">+ Add Provider</button>
        </div>
    `;

    container.appendChild(entry);

    // If per-provider, populate the provider mappings
    if (isPerProvider) {
        const providerList = entry.querySelector('.provider-mapping-list');
        for (const [providerName, modelName] of Object.entries(mappings)) {
            addProviderMappingEntry(providerList, providerName, modelName);
        }
    }

    // Sync to JSON
    syncMappingsToJson();
}

// Add a new model mapping entry (Edit modal)
function addEditModelMapping(aliasName = '', mappings = null) {
    const container = document.getElementById('editModelMappingsBuilder');
    const entryId = `edit-mapping-${editModelMappingCounter++}`;

    const entry = document.createElement('div');
    entry.className = 'model-mapping-entry';
    entry.id = entryId;
    entry.style.cssText = 'border: 1px solid var(--border-color); border-radius: 8px; padding: 12px; margin-bottom: 8px; background: var(--bg-secondary);';

    // Determine if it's a simple mapping or per-provider mapping
    const isPerProvider = mappings && typeof mappings === 'object';
    const simpleValue = !isPerProvider && mappings ? mappings : '';

    entry.innerHTML = `
        <div style="display: flex; gap: 8px; align-items: center; margin-bottom: 8px;">
            <input type="text" class="form-input mapping-alias" placeholder="Alias (e.g., gpt-4)" value="${escapeHtml(aliasName)}" style="flex: 1;">
            <button type="button" class="btn btn-danger btn-sm" onclick="removeModelMapping('${entryId}')" title="Remove">✕</button>
        </div>
        <div class="mapping-type-toggle" style="margin-bottom: 8px;">
            <label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">
                <input type="checkbox" class="mapping-per-provider" ${isPerProvider ? 'checked' : ''} onchange="toggleMappingType('${entryId}')">
                <span style="font-size: 12px; color: var(--text-secondary);">Per-provider mapping</span>
            </label>
        </div>
        <div class="simple-mapping" style="${isPerProvider ? 'display: none;' : ''}">
            <input type="text" class="form-input mapping-simple-value" placeholder="Real model name (e.g., gpt-4-turbo-preview)" value="${escapeHtml(simpleValue)}">
        </div>
        <div class="provider-mappings" style="${isPerProvider ? '' : 'display: none;'}">
            <div class="provider-mapping-list"></div>
            <button type="button" class="btn btn-secondary btn-sm" onclick="addProviderMapping('${entryId}')" style="margin-top: 4px;">+ Add Provider</button>
        </div>
    `;

    container.appendChild(entry);

    // If per-provider, populate the provider mappings
    if (isPerProvider) {
        const providerList = entry.querySelector('.provider-mapping-list');
        for (const [providerName, modelName] of Object.entries(mappings)) {
            addProviderMappingEntry(providerList, providerName, modelName);
        }
    }

    // Sync to JSON
    syncEditMappingsToJson();
}

// Add a provider mapping entry to a mapping
function addProviderMapping(entryId) {
    const entry = document.getElementById(entryId);
    const providerList = entry.querySelector('.provider-mapping-list');
    addProviderMappingEntry(providerList, '', '');

    // Sync to JSON based on which modal
    if (entryId.startsWith('edit-')) {
        syncEditMappingsToJson();
    } else {
        syncMappingsToJson();
    }
}

// Add a provider mapping entry element
function addProviderMappingEntry(container, providerName = '', modelName = '') {
    const providerEntry = document.createElement('div');
    providerEntry.className = 'provider-mapping-entry';
    providerEntry.style.cssText = 'display: flex; gap: 8px; align-items: center; margin-bottom: 4px;';

    providerEntry.innerHTML = `
        <input type="text" class="form-input provider-name" placeholder="Provider name (or 'default')" value="${escapeHtml(providerName)}" style="flex: 1;">
        <span style="color: var(--text-secondary);">→</span>
        <input type="text" class="form-input provider-model" placeholder="Model name" value="${escapeHtml(modelName)}" style="flex: 1;">
        <button type="button" class="btn btn-danger btn-sm" onclick="this.parentElement.remove(); syncMappingsFromBuilder();" title="Remove">✕</button>
    `;

    container.appendChild(providerEntry);
}

// Toggle between simple and per-provider mapping
function toggleMappingType(entryId) {
    const entry = document.getElementById(entryId);
    const checkbox = entry.querySelector('.mapping-per-provider');
    const simpleMapping = entry.querySelector('.simple-mapping');
    const providerMappings = entry.querySelector('.provider-mappings');

    if (checkbox.checked) {
        simpleMapping.style.display = 'none';
        providerMappings.style.display = '';

        // If no provider mappings exist, add a default one
        const providerList = entry.querySelector('.provider-mapping-list');
        if (providerList.children.length === 0) {
            addProviderMappingEntry(providerList, 'default', '');
        }
    } else {
        simpleMapping.style.display = '';
        providerMappings.style.display = 'none';
    }

    // Sync to JSON based on which modal
    if (entryId.startsWith('edit-')) {
        syncEditMappingsToJson();
    } else {
        syncMappingsToJson();
    }
}

// Remove a model mapping entry
function removeModelMapping(entryId) {
    const entry = document.getElementById(entryId);
    if (entry) {
        entry.remove();

        // Sync to JSON based on which modal
        if (entryId.startsWith('edit-')) {
            syncEditMappingsToJson();
        } else {
            syncMappingsToJson();
        }
    }
}

// Sync builder to JSON textarea (Generate modal)
function syncMappingsToJson() {
    const container = document.getElementById('modelMappingsBuilder');
    const textarea = document.getElementById('modelMappings');
    const mappings = buildMappingsFromContainer(container);

    if (Object.keys(mappings).length > 0) {
        textarea.value = JSON.stringify(mappings, null, 2);
    } else {
        textarea.value = '';
    }
}

// Sync builder to JSON textarea (Edit modal)
function syncEditMappingsToJson() {
    const container = document.getElementById('editModelMappingsBuilder');
    const textarea = document.getElementById('editModelMappings');
    const mappings = buildMappingsFromContainer(container);

    if (Object.keys(mappings).length > 0) {
        textarea.value = JSON.stringify(mappings, null, 2);
    } else {
        textarea.value = '';
    }
}

// Build mappings object from container
function buildMappingsFromContainer(container) {
    const mappings = {};
    const entries = container.querySelectorAll('.model-mapping-entry');

    entries.forEach(entry => {
        const alias = entry.querySelector('.mapping-alias').value.trim();
        if (!alias) return;

        const isPerProvider = entry.querySelector('.mapping-per-provider').checked;

        if (isPerProvider) {
            const providerMappings = {};
            const providerEntries = entry.querySelectorAll('.provider-mapping-entry');

            providerEntries.forEach(pe => {
                const providerName = pe.querySelector('.provider-name').value.trim();
                const modelName = pe.querySelector('.provider-model').value.trim();
                if (providerName && modelName) {
                    providerMappings[providerName] = modelName;
                }
            });

            if (Object.keys(providerMappings).length > 0) {
                mappings[alias] = providerMappings;
            }
        } else {
            const simpleValue = entry.querySelector('.mapping-simple-value').value.trim();
            if (simpleValue) {
                mappings[alias] = simpleValue;
            }
        }
    });

    return mappings;
}

// Sync JSON textarea to builder (Generate modal)
function syncMappingsFromJson() {
    const container = document.getElementById('modelMappingsBuilder');
    const textarea = document.getElementById('modelMappings');

    // Clear existing entries
    container.innerHTML = '';
    modelMappingCounter = 0;

    const jsonStr = textarea.value.trim();
    if (!jsonStr) return;

    try {
        const mappings = JSON.parse(jsonStr);
        if (typeof mappings === 'object' && mappings !== null) {
            for (const [alias, value] of Object.entries(mappings)) {
                addModelMapping(alias, value);
            }
        }
        showToast('Loaded mappings from JSON', 'success');
    } catch (e) {
        showToast('Invalid JSON format', 'error');
    }
}

// Sync JSON textarea to builder (Edit modal)
function syncEditMappingsFromJson() {
    const container = document.getElementById('editModelMappingsBuilder');
    const textarea = document.getElementById('editModelMappings');

    // Clear existing entries
    container.innerHTML = '';
    editModelMappingCounter = 0;

    const jsonStr = textarea.value.trim();
    if (!jsonStr) return;

    try {
        const mappings = JSON.parse(jsonStr);
        if (typeof mappings === 'object' && mappings !== null) {
            for (const [alias, value] of Object.entries(mappings)) {
                addEditModelMapping(alias, value);
            }
        }
        showToast('Loaded mappings from JSON', 'success');
    } catch (e) {
        showToast('Invalid JSON format', 'error');
    }
}

// Helper to sync from builder (called by remove button)
function syncMappingsFromBuilder() {
    // Determine which modal is active and sync accordingly
    const generateModal = document.getElementById('generateModal');
    const editModal = document.getElementById('editModal');

    if (generateModal && generateModal.classList.contains('active')) {
        syncMappingsToJson();
    }
    if (editModal && editModal.classList.contains('active')) {
        syncEditMappingsToJson();
    }
}

// Clear model mappings builder (Generate modal)
function clearModelMappingsBuilder() {
    const container = document.getElementById('modelMappingsBuilder');
    container.innerHTML = '';
    modelMappingCounter = 0;
    document.getElementById('modelMappings').value = '';
}

// Clear model mappings builder (Edit modal)
function clearEditModelMappingsBuilder() {
    const container = document.getElementById('editModelMappingsBuilder');
    container.innerHTML = '';
    editModelMappingCounter = 0;
    document.getElementById('editModelMappings').value = '';
}

// Load model mappings into builder from JSON string
function loadModelMappingsToBuilder(jsonStr, isEdit = false) {
    if (isEdit) {
        clearEditModelMappingsBuilder();
        document.getElementById('editModelMappings').value = jsonStr || '';
        if (jsonStr) {
            syncEditMappingsFromJson();
        }
    } else {
        clearModelMappingsBuilder();
        document.getElementById('modelMappings').value = jsonStr || '';
        if (jsonStr) {
            syncMappingsFromJson();
        }
    }
}

// ==================== LARGE CONTEXT REQUESTS SECTION ====================

// State for large context section
let largeContextExpanded = false;
let largeContextRefreshInterval = null;

// Toggle the large context section expand/collapse
function toggleLargeContextSection() {
    const section = document.getElementById('largeContextSection');
    const icon = document.getElementById('largeContextCollapseIcon');

    largeContextExpanded = !largeContextExpanded;

    if (largeContextExpanded) {
        // Remove collapsed class to expand
        section.classList.remove('collapsed');
        icon.textContent = '▲';
        // Load data when expanding
        loadLargeContextLogs();
        loadLargeContextStats();
        // Start auto-refresh
        startLargeContextAutoRefresh();
    } else {
        // Add collapsed class to collapse
        section.classList.add('collapsed');
        icon.textContent = '▼';
        // Stop auto-refresh when collapsed
        stopLargeContextAutoRefresh();
    }
}

// Load large context logs from API
async function loadLargeContextLogs() {
    try {
        const response = await apiFetch('/admin/large-context-logs?limit=50');
        const data = await response.json();

        const tbody = document.getElementById('largeContextTableBody');
        const countElement = document.getElementById('largeContextCount');
        const logs = data.logs || [];

        // Update count badge
        if (countElement) {
            countElement.textContent = `${logs.length} request${logs.length !== 1 ? 's' : ''}`;
        }

        if (logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="loading">No large context requests yet</td></tr>';
        } else {
            tbody.innerHTML = logs.map(log => {
                const time = new Date(log.request_time.includes('Z') ? log.request_time : log.request_time + 'Z').toLocaleString();
                const inputTokens = log.input_tokens || 0;
                const outputTokens = log.output_tokens || 0;
                const totalTokens = log.total_tokens || 0;

                // Determine token class based on size
                let tokenClass = 'high';
                if (totalTokens >= 100000) {
                    tokenClass = 'extreme';
                } else if (totalTokens >= 60000) {
                    tokenClass = 'very-high';
                }

                return `
                    <tr>
                        <td class="time-cell">${escapeHtml(time)}</td>
                        <td class="key-cell">${escapeHtml(log.key_name || 'Unknown')}</td>
                        <td class="model-cell">${escapeHtml(log.model || 'Unknown')}</td>
                        <td class="token-cell input">${formatNumber(inputTokens)}</td>
                        <td class="token-cell output">${formatNumber(outputTokens)}</td>
                        <td class="token-cell total ${tokenClass}">${formatNumber(totalTokens)}</td>
                        <td class="ip-cell">${escapeHtml(log.client_ip || 'N/A')}</td>
                    </tr>
                `;
            }).join('');
        }

    } catch (error) {
        console.error('Error loading large context logs:', error);
        const tbody = document.getElementById('largeContextTableBody');
        tbody.innerHTML = '<tr><td colspan="7" class="loading" style="color: var(--danger-color);">Failed to load</td></tr>';
    }
}

// Load large context stats from API
async function loadLargeContextStats() {
    try {
        const response = await apiFetch('/admin/large-context-stats');
        const stats = await response.json();

        // Update stat cards
        const totalRequestsEl = document.getElementById('lcTotalRequests');
        const avgTokensEl = document.getElementById('lcAvgTokens');
        const maxTokensEl = document.getElementById('lcMaxTokens');
        const totalTokensEl = document.getElementById('lcTotalTokens');

        if (totalRequestsEl) totalRequestsEl.textContent = formatNumber(stats.total_requests || 0);
        if (avgTokensEl) avgTokensEl.textContent = formatNumber(Math.round(stats.avg_tokens || 0));
        if (maxTokensEl) maxTokensEl.textContent = formatNumber(stats.max_tokens || 0);
        if (totalTokensEl) totalTokensEl.textContent = formatNumber(stats.total_tokens || 0);

    } catch (error) {
        console.error('Error loading large context stats:', error);
    }
}

// Start auto-refresh for large context logs
function startLargeContextAutoRefresh() {
    // Clear any existing interval
    if (largeContextRefreshInterval) {
        clearInterval(largeContextRefreshInterval);
    }

    // Refresh every 3 seconds
    largeContextRefreshInterval = setInterval(() => {
        if (largeContextExpanded) {
            loadLargeContextLogs();
            loadLargeContextStats();
        }
    }, 3000);
}

// Stop auto-refresh for large context logs
function stopLargeContextAutoRefresh() {
    if (largeContextRefreshInterval) {
        clearInterval(largeContextRefreshInterval);
        largeContextRefreshInterval = null;
    }
}

// Format number with commas for display
function formatNumberWithCommas(num) {
    if (num === null || num === undefined) return '0';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// ==================== MODEL COSTS MANAGEMENT ====================

// Global variable for model costs
let modelCosts = [];

// Load model costs from API
async function loadModelCosts() {
    try {
        const response = await apiFetch('/admin/costs');
        const data = await response.json();
        modelCosts = data.costs || [];
        renderModelCosts();
    } catch (error) {
        console.error('Error loading model costs:', error);
        // Don't show error toast on initial load - costs section may not exist yet
    }
}

// Render model costs in the UI (using table format)
function renderModelCosts() {
    const tableBody = document.getElementById('modelCostsTableBody');
    if (!tableBody) return;

    if (modelCosts.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="4" class="loading-text text-center">No model costs configured yet. Add pricing to track costs.</td>
            </tr>
        `;
        return;
    }

    tableBody.innerHTML = modelCosts.map(cost => `
        <tr>
            <td><code>${escapeHtml(cost.model_pattern)}</code></td>
            <td>$${cost.input_cost_per_1m.toFixed(4)}</td>
            <td>$${cost.output_cost_per_1m.toFixed(4)}</td>
            <td>
                <button class="btn btn-danger btn-sm" onclick="deleteModelCost(${cost.id})" title="Delete this cost entry">
                    <span>🗑️</span> Delete
                </button>
            </td>
        </tr>
    `).join('');
}

// Show add model cost modal (uses costModal from HTML)
function showAddCostModal() {
    const modal = document.getElementById('costModal');
    if (modal) {
        modal.classList.add('active');
        document.getElementById('costId').value = '';
        document.getElementById('modelPattern').value = '';
        document.getElementById('inputCost').value = '0.00';
        document.getElementById('outputCost').value = '0.00';
    }
}

// Close add model cost modal
function closeAddCostModal() {
    const modal = document.getElementById('costModal');
    if (modal) {
        modal.classList.remove('active');
    }
}

// Alias for closing cost modal (used by HTML)
function closeCostModal() {
    closeAddCostModal();
}

// Save model cost (add or update)
async function saveModelCost() {
    const costId = document.getElementById('costId').value;
    const modelPattern = document.getElementById('modelPattern').value.trim();
    const inputCost = parseFloat(document.getElementById('inputCost').value);
    const outputCost = parseFloat(document.getElementById('outputCost').value);

    if (!modelPattern) {
        showToast('Please enter a model pattern', 'warning');
        return;
    }

    if (isNaN(inputCost) || inputCost < 0) {
        showToast('Please enter a valid input cost', 'warning');
        return;
    }

    if (isNaN(outputCost) || outputCost < 0) {
        showToast('Please enter a valid output cost', 'warning');
        return;
    }

    try {
        const response = await apiFetch('/admin/costs', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                model_pattern: modelPattern,
                input_cost_per_1m: inputCost,
                output_cost_per_1m: outputCost
            })
        });

        if (!response.ok) {
            throw new Error('Failed to save model cost');
        }

        closeCostModal();
        loadModelCosts();
        addConsoleLog('SUCCESS', `Saved cost for model pattern: ${modelPattern}`);
        showToast('Model cost saved successfully!', 'success');
    } catch (error) {
        console.error('Error saving model cost:', error);
        addConsoleLog('ERROR', `Failed to save model cost: ${error.message}`);
        showToast('Failed to save model cost', 'error');
    }
}

// Legacy function name for compatibility
async function addModelCost() {
    await saveModelCost();
}

// Delete a model cost
async function deleteModelCost(costId) {
    if (!confirm('Are you sure you want to delete this model cost entry?')) {
        return;
    }

    try {
        const response = await apiFetch(`/admin/costs/${costId}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            throw new Error('Failed to delete model cost');
        }

        loadModelCosts();
        addConsoleLog('SUCCESS', `Deleted model cost ID ${costId}`);
        showToast('Model cost deleted', 'info');
    } catch (error) {
        console.error('Error deleting model cost:', error);
        addConsoleLog('ERROR', `Failed to delete model cost: ${error.message}`);
        showToast('Failed to delete model cost', 'error');
    }
}

// ==================== DATA MANAGEMENT FUNCTIONS ====================

async function exportData() {
    try {
        showToast('Generating backup...', 'info');
        const response = await apiFetch('/admin/export');

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || 'Export failed');
        }

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;

        // Extract filename from Content-Disposition header if possible
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'db_backup.json';
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="?([^"]+)"?/);
            if (match && match[1]) filename = match[1];
        }

        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);

        showToast('Backup downloaded successfully', 'success');
    } catch (err) {
        console.error('Export error:', err);
        showToast('Failed to download backup: ' + err.message, 'error');
    }
}

async function importData(input) {
    const file = input.files[0];
    if (!file) return;

    // Confirm first
    if (!confirm('⚠️ WARNING: This will OVERWRITE all existing data (keys, logs, costs) with the backup content.\n\nAre you sure you want to proceed?')) {
        input.value = ''; // Reset
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
        showToast('Restoring database...', 'info');

        const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
        // We use raw fetch here to handle FormData correctly (browser sets Content-Type)
        const response = await fetch('/admin/import', {
            method: 'POST',
            headers: {
                'X-Admin-Password': password
            },
            body: formData
        });

        if (response.status === 401) {
            sessionStorage.removeItem(ADMIN_PASSWORD_KEY);
            sessionStorage.removeItem(ACCESS_CODE_KEY);
            window.location.reload();
            throw new Error('Unauthorized');
        }

        const data = await response.json();

        if (response.ok) {
            showToast('Database restored successfully! Reloading...', 'success');
            setTimeout(() => window.location.reload(), 1500);
        } else {
            throw new Error(data.detail || 'Import failed');
        }
    } catch (err) {
        console.error('Import error:', err);
        showToast('Restore failed: ' + err.message, 'error');
    } finally {
        input.value = ''; // Reset input
    }
}

// ==================== CLIENT APP STATISTICS ====================

async function loadClientStats() {
    const grid = document.getElementById('clientStatsGrid');
    if (!grid) return;

    grid.innerHTML = `
        <div class="loading-container">
            <div class="spinner"></div>
            <p class="loading-text">Loading client statistics...</p>
        </div>
    `;

    try {
        const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
        const response = await fetch('/api/client-stats?days=30', {
            headers: { 'Authorization': `Bearer ${password}` }
        });

        if (!response.ok) throw new Error('Failed to load client stats');

        const data = await response.json();
        const stats = data.client_stats || [];

        if (stats.length === 0) {
            grid.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">📱</div>
                    <p>No client app data available yet</p>
                </div>
            `;
            return;
        }

        // Calculate total for percentages
        const totalRequests = stats.reduce((sum, s) => sum + (s.request_count || 0), 0);

        grid.innerHTML = stats.map(stat => {
            const percent = totalRequests > 0 ? ((stat.request_count / totalRequests) * 100).toFixed(1) : 0;
            const tokens = formatNumber(stat.total_tokens || 0);
            return `
                <div class="client-stat-card">
                    <div class="client-stat-header">
                        <span class="client-stat-name">${escapeHtml(stat.client_app || 'Unknown')}</span>
                        <span class="client-stat-percent">${percent}%</span>
                    </div>
                    <div class="client-stat-bar">
                        <div class="client-stat-fill" style="width: ${percent}%"></div>
                    </div>
                    <div class="client-stat-details">
                        <span>${formatNumber(stat.request_count)} requests</span>
                        <span>${tokens} tokens</span>
                    </div>
                </div>
            `;
        }).join('');

    } catch (error) {
        console.error('Error loading client stats:', error);
        grid.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">⚠️</div>
                <p>Failed to load client statistics</p>
            </div>
        `;
    }
}

// ==================== CLIENT BLACKLIST MANAGEMENT ====================

async function loadBlacklist() {
    const tbody = document.getElementById('blacklistTableBody');
    const countEl = document.getElementById('blacklistCount');
    if (!tbody) return;

    tbody.innerHTML = '<tr><td colspan="4" class="loading-text text-center">Loading blacklist...</td></tr>';

    try {
        const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
        const response = await fetch('/api/client-blacklist', {
            headers: { 'Authorization': `Bearer ${password}` }
        });

        if (!response.ok) throw new Error('Failed to load blacklist');

        const data = await response.json();
        const blacklist = data.blacklist || [];

        if (countEl) {
            countEl.textContent = `${blacklist.length} blocked`;
        }

        if (blacklist.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="loading-text text-center">No blocked clients</td></tr>';
            return;
        }

        tbody.innerHTML = blacklist.map(item => {
            const blockedAt = item.created_at ? 
                new Date(item.created_at.includes('Z') ? item.created_at : item.created_at + 'Z').toLocaleString() : 
                'Unknown';
            return `
                <tr>
                    <td class="client-app-cell">${escapeHtml(item.client_app)}</td>
                    <td class="reason-cell">${escapeHtml(item.reason || 'No reason')}</td>
                    <td class="time-cell">${blockedAt}</td>
                    <td class="actions-cell">
                        <button class="btn btn-danger btn-sm" onclick="removeFromBlacklist('${escapeHtml(item.client_app)}')">
                            🗑️ Unblock
                        </button>
                    </td>
                </tr>
            `;
        }).join('');

    } catch (error) {
        console.error('Error loading blacklist:', error);
        tbody.innerHTML = '<tr><td colspan="4" class="loading-text text-center" style="color: var(--danger-color);">Failed to load</td></tr>';
    }
}

function showAddBlacklistModal() {
    document.getElementById('blacklistClientApp').value = '';
    document.getElementById('blacklistReason').value = '';
    document.getElementById('blacklistModal').classList.add('active');
}

function closeBlacklistModal() {
    document.getElementById('blacklistModal').classList.remove('active');
}

async function addToBlacklist() {
    const clientApp = document.getElementById('blacklistClientApp').value.trim();
    const reason = document.getElementById('blacklistReason').value.trim() || 'Blocked by admin';

    if (!clientApp) {
        showToast('Please enter a client app name', 'error');
        return;
    }

    try {
        const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
        const response = await fetch('/api/client-blacklist', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${password}`
            },
            body: JSON.stringify({ client_app: clientApp, reason: reason })
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || 'Failed to add to blacklist');
        }

        showToast(`Client '${clientApp}' has been blocked`, 'success');
        closeBlacklistModal();
        loadBlacklist();

    } catch (error) {
        console.error('Error adding to blacklist:', error);
        showToast(error.message, 'error');
    }
}

async function removeFromBlacklist(clientApp) {
    if (!confirm(`Are you sure you want to unblock '${clientApp}'?`)) {
        return;
    }

    try {
        const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
        const response = await fetch(`/api/client-blacklist/${encodeURIComponent(clientApp)}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${password}` }
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || 'Failed to remove from blacklist');
        }

        showToast(`Client '${clientApp}' has been unblocked`, 'success');
        loadBlacklist();

    } catch (error) {
        console.error('Error removing from blacklist:', error);
        showToast(error.message, 'error');
    }
}

// Load client tracking data on dashboard init
document.addEventListener('DOMContentLoaded', () => {
    // Only load if already verified (these will be called after initializeDashboard)
    if (sessionStorage.getItem(ACCESS_CODE_KEY) === 'true') {
        setTimeout(() => {
            loadClientStats();
            loadBlacklist();
        }, 500);
    }
});

// ==================== DISCORD USER MANAGEMENT ====================

function getDiscordAvatarUrl(discordId, avatarHash) {
    if (avatarHash) {
        return `https://cdn.discordapp.com/avatars/${discordId}/${avatarHash}.png`;
    }
    // Default avatar based on discriminator (use ID mod 5 for new usernames)
    return `https://cdn.discordapp.com/embed/avatars/${parseInt(discordId) % 5}.png`;
}

async function loadTopDiscordUsers() {
    const tbody = document.getElementById('topDiscordUsersTableBody');
    if (!tbody) return;

    tbody.innerHTML = '<tr><td colspan="5" class="loading-text text-center">Loading top Discord users...</td></tr>';

    try {
        const response = await apiFetch('/admin/top-discord-users?limit=10&days=30');

        if (!response.ok) throw new Error('Failed to load top Discord users');

        const data = await response.json();
        const users = data.users || [];

        if (users.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="loading-text text-center">No Discord user data available</td></tr>';
            return;
        }

        tbody.innerHTML = users.map((user, index) => {
            const avatarUrl = getDiscordAvatarUrl(user.discord_id, user.avatar);
            const displayName = user.global_name || user.username || 'Unknown';
            
            return `
                <tr>
                    <td class="rank-cell">#${index + 1}</td>
                    <td class="discord-user-cell">
                        <div class="discord-user-info">
                            <img src="${avatarUrl}" alt="${escapeHtml(displayName)}" class="discord-avatar" onerror="this.src='https://cdn.discordapp.com/embed/avatars/0.png'">
                            <div class="discord-user-details">
                                <span class="discord-username">${escapeHtml(displayName)}</span>
                                <span class="discord-id">${escapeHtml(user.discord_id)}</span>
                            </div>
                        </div>
                    </td>
                    <td class="token-cell">${formatNumber(user.total_tokens || 0)}</td>
                    <td class="request-cell">${formatNumber(user.request_count || 0)}</td>
                    <td class="actions-cell">
                        <button class="btn btn-danger btn-sm" onclick="showBanDiscordModal('${escapeHtml(user.discord_id)}', '${escapeHtml(displayName)}')">
                            🚫 Ban
                        </button>
                    </td>
                </tr>
            `;
        }).join('');

    } catch (error) {
        console.error('Error loading top Discord users:', error);
        tbody.innerHTML = '<tr><td colspan="5" class="loading-text text-center" style="color: var(--danger-color);">Failed to load</td></tr>';
    }
}

// Cleanup test Discord users (users with IDs that are all same digits or test usernames)
window.cleanupTestUsers = async function() {
    if (!confirm('This will delete all test Discord users (IDs with repeated digits like 111111111111111111, or usernames starting with user_, banned_, normal_). Continue?')) {
        return;
    }

    try {
        showToast('Cleaning up test users...', 'info');
        
        const response = await apiFetch('/api/admin/cleanup-test-users', {
            method: 'DELETE'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }

        const data = await response.json();
        
        if (data.deleted_count > 0) {
            showToast(`Deleted ${data.deleted_count} test user(s)`, 'success');
            addConsoleLog('SUCCESS', `Cleaned up ${data.deleted_count} test Discord users`);
        } else {
            showToast('No test users found to delete', 'info');
        }
        
        // Refresh the Discord users list
        loadTopDiscordUsers();
        loadBannedDiscordUsers();
        
    } catch (error) {
        console.error('Error cleaning up test users:', error);
        showToast('Failed to cleanup test users: ' + error.message, 'error');
        addConsoleLog('ERROR', `Failed to cleanup test users: ${error.message}`);
    }
};

// Cleanup test API keys (keys with test-like names)
window.cleanupTestKeys = async function() {
    if (!confirm('This will delete all test API keys (keys with names containing: test, property, hypothesis, mock, sample, demo, example, tmp, temp). Continue?')) {
        return;
    }

    try {
        showToast('Cleaning up test API keys...', 'info');
        
        const response = await apiFetch('/api/admin/cleanup-test-keys', {
            method: 'DELETE'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }

        const data = await response.json();
        
        if (data.deleted_count > 0) {
            showToast(`Deleted ${data.deleted_count} test API key(s)`, 'success');
            addConsoleLog('SUCCESS', `Cleaned up ${data.deleted_count} test API keys`);
        } else {
            showToast('No test API keys found to delete', 'info');
        }
        
        // Refresh the keys list
        loadAPIKeys();
        
    } catch (error) {
        console.error('Error cleaning up test keys:', error);
        showToast('Failed to cleanup test keys: ' + error.message, 'error');
        addConsoleLog('ERROR', `Failed to cleanup test keys: ${error.message}`);
    }
};

// Delete ALL API keys (dangerous - use with caution)
window.deleteAllKeys = async function() {
    if (!confirm('⚠️ WARNING: This will delete ALL API keys from the database!\n\nThis action cannot be undone.\n\nAre you absolutely sure?')) {
        return;
    }
    
    // Double confirmation for safety
    if (!confirm('FINAL WARNING: You are about to delete ALL keys. Type "yes" in the next prompt to confirm.')) {
        return;
    }
    
    const confirmation = prompt('Type "DELETE ALL" to confirm deletion of all API keys:');
    if (confirmation !== 'DELETE ALL') {
        showToast('Deletion cancelled - confirmation text did not match', 'info');
        return;
    }

    try {
        showToast('Deleting ALL API keys...', 'warning');
        
        const response = await apiFetch('/api/admin/delete-all-keys', {
            method: 'DELETE'
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP ${response.status}`);
        }

        const data = await response.json();
        
        showToast(`Deleted ${data.deleted_count} API key(s)`, 'success');
        addConsoleLog('WARNING', `Deleted ALL ${data.deleted_count} API keys`);
        
        if (data.errors && data.errors.length > 0) {
            addConsoleLog('WARNING', `${data.errors.length} keys failed to delete`);
        }
        
        // Refresh the keys list
        loadAPIKeys();
        
    } catch (error) {
        console.error('Error deleting all keys:', error);
        showToast('Failed to delete all keys: ' + error.message, 'error');
        addConsoleLog('ERROR', `Failed to delete all keys: ${error.message}`);
    }
};

async function loadBannedDiscordUsers() {
    const tbody = document.getElementById('bannedDiscordTableBody');
    const countEl = document.getElementById('discordBanCount');
    if (!tbody) return;

    tbody.innerHTML = '<tr><td colspan="4" class="loading-text text-center">Loading banned users...</td></tr>';

    try {
        const response = await apiFetch('/api/admin/banned-discord');

        if (!response.ok) throw new Error('Failed to load banned Discord users');

        const data = await response.json();
        const bannedUsers = data.banned_users || [];

        if (countEl) {
            countEl.textContent = `${bannedUsers.length} banned`;
        }

        if (bannedUsers.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="loading-text text-center">No banned Discord users</td></tr>';
            return;
        }

        tbody.innerHTML = bannedUsers.map(user => {
            const avatarUrl = getDiscordAvatarUrl(user.discord_id, user.avatar);
            const displayName = user.global_name || user.username || 'Unknown';
            const bannedAt = user.banned_at ? 
                new Date(user.banned_at.includes('Z') ? user.banned_at : user.banned_at + 'Z').toLocaleString() : 
                'Unknown';
            
            return `
                <tr>
                    <td class="discord-user-cell">
                        <div class="discord-user-info">
                            <img src="${avatarUrl}" alt="${escapeHtml(displayName)}" class="discord-avatar" onerror="this.src='https://cdn.discordapp.com/embed/avatars/0.png'">
                            <div class="discord-user-details">
                                <span class="discord-username">${escapeHtml(displayName)}</span>
                                <span class="discord-id">${escapeHtml(user.discord_id)}</span>
                            </div>
                        </div>
                    </td>
                    <td class="reason-cell">${escapeHtml(user.reason || 'No reason')}</td>
                    <td class="time-cell">${bannedAt}</td>
                    <td class="actions-cell">
                        <button class="btn btn-success btn-sm" onclick="unbanDiscordUser('${escapeHtml(user.discord_id)}', '${escapeHtml(displayName)}')">
                            ✓ Unban
                        </button>
                    </td>
                </tr>
            `;
        }).join('');

    } catch (error) {
        console.error('Error loading banned Discord users:', error);
        tbody.innerHTML = '<tr><td colspan="4" class="loading-text text-center" style="color: var(--danger-color);">Failed to load</td></tr>';
    }
}

function showBanDiscordModal(discordId, username) {
    document.getElementById('banDiscordId').value = discordId;
    document.getElementById('banDiscordUsername').value = username;
    document.getElementById('banDiscordReason').value = '';
    document.getElementById('discordBanModal').classList.add('active');
}

function closeDiscordBanModal() {
    document.getElementById('discordBanModal').classList.remove('active');
}

async function confirmBanDiscordUser() {
    const discordId = document.getElementById('banDiscordId').value;
    const username = document.getElementById('banDiscordUsername').value;
    const reason = document.getElementById('banDiscordReason').value.trim() || 'Banned by admin';

    if (!discordId) {
        showToast('Discord ID is required', 'error');
        return;
    }

    try {
        const response = await apiFetch('/api/admin/ban-discord', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ discord_id: discordId, reason: reason })
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || 'Failed to ban user');
        }

        showToast(`Discord user '${username}' has been banned`, 'success');
        closeDiscordBanModal();
        loadTopDiscordUsers();
        loadBannedDiscordUsers();

    } catch (error) {
        console.error('Error banning Discord user:', error);
        showToast(error.message, 'error');
    }
}

async function unbanDiscordUser(discordId, username) {
    if (!confirm(`Are you sure you want to unban '${username}'?`)) {
        return;
    }

    try {
        const response = await apiFetch(`/api/admin/ban-discord/${encodeURIComponent(discordId)}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || 'Failed to unban user');
        }

        showToast(`Discord user '${username}' has been unbanned`, 'success');
        loadTopDiscordUsers();
        loadBannedDiscordUsers();

    } catch (error) {
        console.error('Error unbanning Discord user:', error);
        showToast(error.message, 'error');
    }
}

// Load Discord data on dashboard init
(function() {
    const originalInit = window.initializeDashboard;
    if (originalInit) {
        window.initializeDashboard = function() {
            originalInit();
            setTimeout(() => {
                loadTopDiscordUsers();
                loadBannedDiscordUsers();
            }, 600);
        };
    }
})();


// ==================== SUB-KEY USER DETAILS MODAL ====================

// Store for current user details modal state
let currentUserDetailsSubKeyId = null;
let currentUserDetailsLogsLoaded = 0;
const USER_DETAILS_LOGS_INITIAL = 5;
const USER_DETAILS_LOGS_MAX = 20;

/**
 * Show the user details modal for a sub-key
 * Fetches user info, usage stats, and recent logs
 * 
 * @param {number} subKeyId - The sub-key ID to show details for
 * 
 * **Validates: Requirements 1.1, 1.2, 1.3**
 */
async function showUserDetailsModal(subKeyId) {
    currentUserDetailsSubKeyId = subKeyId;
    currentUserDetailsLogsLoaded = 0;
    
    // Show modal with loading state
    const modal = document.getElementById('userDetailsModal');
    if (!modal) return;
    
    modal.classList.add('active');
    
    // Reset UI to loading state
    document.getElementById('userDetailsUsername').textContent = 'Loading...';
    document.getElementById('userDetailsSubKey').textContent = '';
    document.getElementById('userDetailsDiscordId').textContent = '';
    document.getElementById('userDetailsAvatar').src = 'https://cdn.discordapp.com/embed/avatars/0.png';
    document.getElementById('userDetailsDollarUsage').textContent = '$0.00';
    document.getElementById('userDetailsDollarLimit').textContent = 'of $20.00';
    document.getElementById('userDetailsTotalRequests').textContent = '0';
    document.getElementById('userDetailsLastUsed').textContent = 'Never';
    document.getElementById('userDetailsCreatedAt').textContent = '-';
    document.getElementById('userDetailsLogsBody').innerHTML = '<tr><td colspan="7" class="loading-text text-center">Loading logs...</td></tr>';
    document.getElementById('userDetailsLogsCount').textContent = '(loading...)';
    document.getElementById('userDetailsLogsFooter').style.display = 'none';
    
    try {
        // Fetch user details with initial logs
        const response = await apiFetch(`/api/admin/sub-key/${subKeyId}/details?limit=${USER_DETAILS_LOGS_INITIAL}`);
        
        if (!response.ok) {
            throw new Error('Failed to load user details');
        }
        
        const data = await response.json();
        
        // Populate user info
        populateUserDetailsModal(data);
        
    } catch (error) {
        console.error('Error loading user details:', error);
        showToast('Failed to load user details', 'error');
        document.getElementById('userDetailsUsername').textContent = 'Error loading user';
        document.getElementById('userDetailsLogsBody').innerHTML = `<tr><td colspan="7" class="loading-text text-center" style="color: var(--danger-color);">Failed to load logs</td></tr>`;
    }
}

/**
 * Populate the user details modal with data
 * 
 * @param {Object} data - User details data from API
 * 
 * **Validates: Requirements 1.3, 1.4**
 */
function populateUserDetailsModal(data) {
    const user = data.user;
    const usage = data.usage;
    const logs = data.logs;
    
    // User info
    document.getElementById('userDetailsUsername').textContent = user.username || 'Unknown';
    document.getElementById('userDetailsSubKey').textContent = user.sub_key_prefix || '';
    document.getElementById('userDetailsDiscordId').textContent = `Discord ID: ${user.discord_id || 'Unknown'}`;
    document.getElementById('userDetailsAvatar').src = user.avatar_url || 'https://cdn.discordapp.com/embed/avatars/0.png';
    document.getElementById('userDetailsAvatar').onerror = function() {
        this.src = 'https://cdn.discordapp.com/embed/avatars/0.png';
    };
    
    // Status badge
    const statusContainer = document.getElementById('userDetailsStatus');
    if (user.enabled !== false) {
        statusContainer.innerHTML = '<span class="badge badge-enabled"><span class="status-dot"></span> Active</span>';
    } else {
        statusContainer.innerHTML = '<span class="badge badge-disabled"><span class="status-dot"></span> Disabled</span>';
    }
    
    // Usage stats
    const dollarUsage = usage.daily_dollar_usage || 0;
    const dollarLimit = usage.daily_dollar_limit || 20.0;
    document.getElementById('userDetailsDollarUsage').textContent = `$${dollarUsage.toFixed(2)}`;
    document.getElementById('userDetailsDollarLimit').textContent = `of $${dollarLimit.toFixed(2)}`;
    document.getElementById('userDetailsTotalRequests').textContent = formatNumber(usage.total_requests || 0);
    document.getElementById('userDetailsLastUsed').textContent = user.last_used_at ? formatRelativeTime(user.last_used_at) : 'Never';
    document.getElementById('userDetailsCreatedAt').textContent = user.created_at ? formatDateShort(user.created_at) : '-';
    
    // Set dollar limit input
    document.getElementById('userDetailsLimitInput').value = dollarLimit.toFixed(2);
    
    // Render logs
    currentUserDetailsLogsLoaded = logs.length;
    renderUserDetailsLogs(logs);
    
    // Show/hide load more button
    const logsFooter = document.getElementById('userDetailsLogsFooter');
    if (logs.length >= USER_DETAILS_LOGS_INITIAL && logs.length < USER_DETAILS_LOGS_MAX) {
        logsFooter.style.display = 'flex';
    } else {
        logsFooter.style.display = 'none';
    }
    
    // Update logs count
    document.getElementById('userDetailsLogsCount').textContent = `(${logs.length} logs)`;
}

/**
 * Render usage logs in the modal table
 * 
 * @param {Array} logs - Array of log entries
 * 
 * **Validates: Requirements 1.3**
 */
function renderUserDetailsLogs(logs) {
    const tbody = document.getElementById('userDetailsLogsBody');
    
    if (!logs || logs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="loading-text text-center">No usage logs yet</td></tr>';
        return;
    }
    
    tbody.innerHTML = logs.map(log => {
        const timestamp = log.timestamp ? formatTimestamp(log.timestamp) : '-';
        const model = log.model || 'Unknown';
        const inputTokens = formatNumber(log.input_tokens || 0);
        const outputTokens = formatNumber(log.output_tokens || 0);
        const totalTokens = formatNumber(log.total_tokens || 0);
        const clientApp = log.client_app || 'Unknown';
        const cost = log.cost ? `$${log.cost.toFixed(4)}` : '$0.0000';
        
        return `
            <tr>
                <td class="timestamp">${timestamp}</td>
                <td class="model-cell">${escapeHtml(model)}</td>
                <td class="tokens-cell">${inputTokens}</td>
                <td class="tokens-cell">${outputTokens}</td>
                <td class="tokens-cell">${totalTokens}</td>
                <td>${escapeHtml(clientApp)}</td>
                <td class="cost-cell">${cost}</td>
            </tr>
        `;
    }).join('');
}

/**
 * Load more logs for the current user
 * Fetches additional logs up to the maximum limit
 * 
 * **Validates: Requirements 1.2, 1.3**
 */
async function loadMoreUserLogs() {
    if (!currentUserDetailsSubKeyId) return;
    
    const loadMoreBtn = document.getElementById('loadMoreLogsBtn');
    if (loadMoreBtn) {
        loadMoreBtn.disabled = true;
        loadMoreBtn.textContent = 'Loading...';
    }
    
    try {
        // Fetch more logs
        const response = await apiFetch(`/api/admin/sub-key/${currentUserDetailsSubKeyId}/details?limit=${USER_DETAILS_LOGS_MAX}`);
        
        if (!response.ok) {
            throw new Error('Failed to load more logs');
        }
        
        const data = await response.json();
        
        // Update logs
        currentUserDetailsLogsLoaded = data.logs.length;
        renderUserDetailsLogs(data.logs);
        
        // Update logs count
        document.getElementById('userDetailsLogsCount').textContent = `(${data.logs.length} logs)`;
        
        // Hide load more button if we've loaded all available or reached max
        const logsFooter = document.getElementById('userDetailsLogsFooter');
        if (data.logs.length >= USER_DETAILS_LOGS_MAX || data.logs.length < USER_DETAILS_LOGS_MAX) {
            logsFooter.style.display = 'none';
        }
        
    } catch (error) {
        console.error('Error loading more logs:', error);
        showToast('Failed to load more logs', 'error');
    } finally {
        if (loadMoreBtn) {
            loadMoreBtn.disabled = false;
            loadMoreBtn.textContent = 'Load More Logs';
        }
    }
}

/**
 * Update the dollar limit for the current user
 * 
 * **Validates: Requirements 3.3**
 */
async function updateUserDollarLimit() {
    if (!currentUserDetailsSubKeyId) return;
    
    const limitInput = document.getElementById('userDetailsLimitInput');
    const newLimit = parseFloat(limitInput.value);
    
    if (isNaN(newLimit) || newLimit < 0) {
        showToast('Please enter a valid dollar limit', 'error');
        return;
    }
    
    try {
        const response = await apiFetch(`/api/admin/sub-key/${currentUserDetailsSubKeyId}/limit`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ daily_dollar_limit: newLimit })
        });
        
        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || 'Failed to update limit');
        }
        
        // Update the display
        document.getElementById('userDetailsDollarLimit').textContent = `of $${newLimit.toFixed(2)}`;
        
        showToast(`Dollar limit updated to $${newLimit.toFixed(2)}`, 'success');
        
        // Refresh sub-keys cache to reflect the change
        if (currentUserDetailsSubKeyId) {
            // Find the master key ID for this sub-key and refresh
            // This is a bit tricky, so we'll just invalidate all caches
            Object.keys(subKeysCache).forEach(keyId => {
                delete subKeysCache[keyId];
            });
        }
        
    } catch (error) {
        console.error('Error updating dollar limit:', error);
        showToast(error.message, 'error');
    }
}

/**
 * Close the user details modal
 */
function closeUserDetailsModal() {
    const modal = document.getElementById('userDetailsModal');
    if (modal) {
        modal.classList.remove('active');
    }
    currentUserDetailsSubKeyId = null;
    currentUserDetailsLogsLoaded = 0;
}

/**
 * Format a timestamp for display in logs
 * Automatically converts UTC to user's local timezone
 * 
 * @param {string} timestamp - ISO timestamp string (UTC)
 * @returns {string} Formatted timestamp in user's local timezone
 */
function formatTimestamp(timestamp) {
    if (!timestamp) return '-';
    try {
        const date = new Date(timestamp.includes('Z') ? timestamp : timestamp + 'Z');
        // Use user's browser locale for proper localization
        return date.toLocaleString(undefined, {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    } catch (e) {
        return timestamp;
    }
}

/**
 * Format a date for short display
 * Automatically converts UTC to user's local timezone
 * 
 * @param {string} dateStr - ISO date string (UTC)
 * @returns {string} Formatted date in user's local timezone
 */
function formatDateShort(dateStr) {
    if (!dateStr) return '-';
    try {
        const date = new Date(dateStr.includes('Z') ? dateStr : dateStr + 'Z');
        // Use user's browser locale for proper localization
        return date.toLocaleDateString(undefined, {
            month: 'short',
            day: 'numeric',
            year: 'numeric'
        });
    } catch (e) {
        return dateStr;
    }
}

// ==================== GLOBAL SETTINGS ====================

// Default daily dollar limit for new sub-keys
let globalDefaultDollarLimit = 20.0;

/**
 * Open the global settings modal
 */
function openGlobalSettingsModal() {
    const modal = document.getElementById('globalSettingsModal');
    if (modal) {
        // Load current settings
        loadGlobalSettings();
        modal.classList.add('active');
    }
}

/**
 * Close the global settings modal
 */
function closeGlobalSettingsModal() {
    const modal = document.getElementById('globalSettingsModal');
    if (modal) {
        modal.classList.remove('active');
    }
}

/**
 * Load global settings from server
 */
async function loadGlobalSettings() {
    try {
        const response = await apiFetch('/api/admin/settings/global');
        if (response.ok) {
            const settings = await response.json();
            globalDefaultDollarLimit = settings.default_daily_dollar_limit || 20.0;
            
            // Update form fields
            const globalLimitInput = document.getElementById('globalDollarLimit');
            const bulkLimitInput = document.getElementById('bulkDollarLimit');
            if (globalLimitInput) globalLimitInput.value = globalDefaultDollarLimit.toFixed(2);
            if (bulkLimitInput) bulkLimitInput.value = globalDefaultDollarLimit.toFixed(2);
        }
    } catch (error) {
        console.error('Error loading global settings:', error);
    }
}

/**
 * Save global settings
 */
async function saveGlobalSettings() {
    const globalLimitInput = document.getElementById('globalDollarLimit');
    const newLimit = parseFloat(globalLimitInput.value);
    
    if (isNaN(newLimit) || newLimit < 0) {
        showToast('Please enter a valid dollar limit', 'error');
        return;
    }
    
    try {
        const response = await apiFetch('/api/admin/settings/global', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ default_daily_dollar_limit: newLimit })
        });
        
        if (response.ok) {
            globalDefaultDollarLimit = newLimit;
            showToast(`Default daily limit set to $${newLimit.toFixed(2)}`, 'success');
            closeGlobalSettingsModal();
        } else {
            const error = await response.json();
            showToast(error.detail || 'Failed to save settings', 'error');
        }
    } catch (error) {
        console.error('Error saving global settings:', error);
        showToast('Failed to save settings', 'error');
    }
}

/**
 * Apply a dollar limit to ALL existing sub-keys
 */
async function applyBulkDollarLimit() {
    const bulkLimitInput = document.getElementById('bulkDollarLimit');
    const newLimit = parseFloat(bulkLimitInput.value);
    
    if (isNaN(newLimit) || newLimit < 0) {
        showToast('Please enter a valid dollar limit', 'error');
        return;
    }
    
    // Confirm with user
    if (!confirm(`Are you sure you want to set the daily dollar limit to $${newLimit.toFixed(2)} for ALL existing sub-keys?`)) {
        return;
    }
    
    try {
        const response = await apiFetch('/api/admin/sub-keys/bulk-limit', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ daily_dollar_limit: newLimit })
        });
        
        if (response.ok) {
            const result = await response.json();
            showToast(`Updated ${result.updated_count} sub-keys to $${newLimit.toFixed(2)} daily limit`, 'success');
        } else {
            const error = await response.json();
            showToast(error.detail || 'Failed to update sub-keys', 'error');
        }
    } catch (error) {
        console.error('Error applying bulk dollar limit:', error);
        showToast('Failed to update sub-keys', 'error');
    }
}
