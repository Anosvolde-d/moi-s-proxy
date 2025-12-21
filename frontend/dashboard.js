
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
        const response = await fetch('/admin/settings/webscrapingapi', {
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
    initializeCharts();
    loadChartData('24h');
    loadWebScrapingAPIStatus();
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

function setFilter(filter) {
    currentFilter = filter;

    // Update active button
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    event.target.classList.add('active');

    renderFilteredKeys();
}

function renderFilteredKeys() {
    const container = document.getElementById('keysContainer');
    if (!container) return;

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
    filteredKeys.forEach(key => {
        const keyCard = createKeyCard(key);
        container.appendChild(keyCard);
    });
}

// ==================== CHART FUNCTIONS ====================

function initializeCharts() {
    const requestsCtx = document.getElementById('requestsChart');
    const tokensCtx = document.getElementById('tokensChart');

    if (!requestsCtx || !tokensCtx) return;

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
                backgroundColor: 'rgba(15, 23, 42, 0.9)',
                titleColor: '#e2e8f0',
                bodyColor: '#94a3b8',
                borderColor: 'rgba(99, 102, 241, 0.3)',
                borderWidth: 1,
                padding: 12,
                cornerRadius: 8
            }
        },
        scales: {
            x: {
                grid: {
                    color: 'rgba(148, 163, 184, 0.1)',
                    drawBorder: false
                },
                ticks: {
                    color: '#64748b',
                    font: { size: 11 }
                }
            },
            y: {
                grid: {
                    color: 'rgba(148, 163, 184, 0.1)',
                    drawBorder: false
                },
                ticks: {
                    color: '#64748b',
                    font: { size: 11 }
                },
                beginAtZero: true
            }
        }
    };

    requestsChart = new Chart(requestsCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Requests',
                data: [],
                borderColor: '#6366f1',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: '#6366f1',
                pointHoverBorderColor: '#fff',
                pointHoverBorderWidth: 2
            }]
        },
        options: chartOptions
    });

    tokensChart = new Chart(tokensCtx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Input Tokens',
                    data: [],
                    backgroundColor: 'rgba(99, 102, 241, 0.8)',
                    borderRadius: 4
                },
                {
                    label: 'Output Tokens',
                    data: [],
                    backgroundColor: 'rgba(168, 85, 247, 0.8)',
                    borderRadius: 4
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
                    labels: {
                        color: '#94a3b8',
                        usePointStyle: true,
                        padding: 20
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

// WebSocket connection for real-time logs
function connectWebSocket() {
    const password = sessionStorage.getItem(ADMIN_PASSWORD_KEY);
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    // Don't pass password in URL - send it via message after connection
    const wsUrl = `${protocol}//${window.location.host}/admin/logs/stream`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('WebSocket connected, authenticating...');
        // Send authentication message instead of URL parameter
        ws.send(JSON.stringify({ type: 'auth', password: password }));
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        
        // Handle authentication response
        if (data.type === 'auth_success') {
            console.log('WebSocket authenticated');
            updateWSStatus(true);
            reconnectAttempts = 0;
            addConsoleLog('INFO', 'Connected to real-time log stream');
            return;
        }
        
        if (data.type === 'auth_failed' || data.error === 'Unauthorized') {
            console.error('WebSocket authentication failed');
            addConsoleLog('ERROR', 'WebSocket authentication failed');
            ws.close();
            return;
        }
        
        if (data.level !== 'PING') {
            addConsoleLog(data.level, data.message, data.timestamp);
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        addConsoleLog('ERROR', 'WebSocket connection error');
    };

    ws.onclose = () => {
        console.log('WebSocket disconnected');
        updateWSStatus(false);
        addConsoleLog('WARNING', 'Disconnected from log stream');

        // Attempt to reconnect
        if (reconnectAttempts < maxReconnectAttempts) {
            reconnectAttempts++;
            setTimeout(() => {
                addConsoleLog('INFO', `Reconnecting... (attempt ${reconnectAttempts}/${maxReconnectAttempts})`);
                connectWebSocket();
            }, 3000 * reconnectAttempts);
        }
    };
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
function addConsoleLog(level, message, timestamp = null) {
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
    consoleEl.scrollTop = consoleEl.scrollHeight;

    // Limit console to last 500 lines
    while (consoleEl.children.length > 500) {
        consoleEl.removeChild(consoleEl.firstChild);
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

// Load API keys
async function loadAPIKeys() {
    try {
        const response = await apiFetch('/admin/keys/list');
        const data = await response.json();

        const container = document.getElementById('keysContainer');

        // Show loading skeleton
        container.innerHTML = `
            <div class="key-card skeleton">
                <div class="skeleton-line" style="width: 60%; height: 24px;"></div>
                <div class="skeleton-line" style="width: 40%; height: 16px; margin-top: 8px;"></div>
                <div class="skeleton-line" style="width: 100%; height: 8px; margin-top: 16px;"></div>
                <div class="skeleton-line" style="width: 100%; height: 8px; margin-top: 8px;"></div>
            </div>
        `;

        // Load usage stats for each key
        for (const key of data.keys) {
            try {
                const usageResponse = await apiFetch(`/admin/keys/${key.id}/usage`);
                const usageData = await usageResponse.json();
                key.current_rpm = usageData.current_rpm || 0;
                key.current_rpd = usageData.current_rpd || 0;
                key.refresh_hour = usageData.refresh_hour;
            } catch (e) {
                key.current_rpm = 0;
                key.current_rpd = 0;
                key.refresh_hour = null;
            }
        }

        // Store all keys for filtering
        allKeys = data.keys;

        // Render filtered keys
        renderFilteredKeys();

    } catch (error) {
        console.error('Error loading API keys:', error);
        addConsoleLog('ERROR', `Failed to load API keys: ${error.message}`);
        showToast('Failed to load API keys', 'error');
        document.getElementById('keysContainer').innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">⚠️</div>
                <p>Failed to load API keys</p>
            </div>
        `;
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

    // Determine proxy status (default to true if not set)
    // Note: SQLite stores booleans as integers (0/1), so we use Boolean() for proper conversion
    const useProxy = key.use_proxy === undefined || key.use_proxy === null ? true : Boolean(key.use_proxy);
    const proxyStatusClass = useProxy ? 'proxy-enabled' : 'proxy-disabled';
    const proxyStatusText = useProxy ? 'Proxy On' : 'Proxy Off';
    const proxyToggleText = useProxy ? 'Disable Proxy' : 'Enable Proxy';

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
                <span class="key-badge ${useProxy ? 'proxy' : 'no-proxy'}" title="WebScrapingAPI proxy status for this key">
                    🌐 ${proxyStatusText}
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
        
        <div class="key-actions">
            <button class="btn btn-info" onclick="copyToClipboard('${escapeHtml(key.key_prefix)}...', 'MOI Proxy Key Prefix')" title="Copy the MOI's proxy key prefix (full key only available at creation)">
                <span>🔑</span> Copy Prefix
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
            <button class="btn ${useProxy ? 'btn-info' : 'btn-secondary'}" onclick="toggleKeyProxy(${key.id})" title="Toggle WebScrapingAPI proxy for this key">
                <span>🌐</span> ${proxyToggleText}
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
        // Reset form
        document.getElementById('keyName').value = '';
        document.getElementById('maxRpm').value = '60';
        document.getElementById('maxRpd').value = '1000';
        document.getElementById('targetUrl').value = '';
        document.getElementById('targetApiKey').value = '';
        document.getElementById('httpReferer').value = '';
        document.getElementById('noAuth').checked = false;
        // New fields
        document.getElementById('useProxy').checked = true; // Default ON
        document.getElementById('disableModelFetch').checked = false; // Default OFF
        document.getElementById('expiresAt').value = '';
        document.getElementById('ipWhitelist').value = '';
        document.getElementById('ipBlacklist').value = '';
        document.getElementById('modelMappings').value = '';
        // New fields
        document.getElementById('maxTotalTokens').value = '';
        document.getElementById('maxContextTokens').value = '';
        document.getElementById('customPrefills').value = '';
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
    const maxRpm = parseInt(document.getElementById('maxRpm').value);
    const maxRpd = parseInt(document.getElementById('maxRpd').value);
    const targetUrl = document.getElementById('targetUrl').value.trim() || null;
    const targetApiKey = document.getElementById('targetApiKey').value.trim() || null;
    const httpReferer = document.getElementById('httpReferer').value.trim() || null;
    const noAuth = document.getElementById('noAuth').checked;

    // New fields
    const useProxy = document.getElementById('useProxy').checked;
    const disableModelFetch = document.getElementById('disableModelFetch').checked;
    const expiresAt = document.getElementById('expiresAt').value || null;
    const ipWhitelist = document.getElementById('ipWhitelist').value.trim() || null;
    const ipBlacklist = document.getElementById('ipBlacklist').value.trim() || null;
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
                max_rpm: maxRpm,
                max_rpd: maxRpd,
                target_url: targetUrl,
                target_api_key: targetApiKey,
                http_referer: httpReferer,
                no_auth: noAuth,
                use_proxy: useProxy,
                disable_model_fetch: disableModelFetch,
                expires_at: expiresAt,
                ip_whitelist: ipWhitelist,
                ip_blacklist: ipBlacklist,
                model_mappings: modelMappings,
                providers: providers,
                provider_rotation_frequency: providerRotationFrequency
            })
        });

        if (!response.ok) {
            throw new Error('Failed to generate API key');
        }

        const data = await response.json();

        closeGenerateModal();

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
    currentEditKeyId = keyId;
    document.getElementById('editMaxRpm').value = currentRpm;
    document.getElementById('editMaxRpd').value = currentRpd;

    // Populate new fields if keyData is provided
    if (keyData) {
        document.getElementById('editTargetUrl').value = keyData.target_url || '';
        document.getElementById('editTargetApiKey').value = keyData.target_api_key || '';
        document.getElementById('editHttpReferer').value = keyData.http_referer || '';
        document.getElementById('editNoAuth').checked = keyData.no_auth || false;
        // Note: SQLite stores booleans as integers (0/1), so we use Boolean() for proper conversion
        document.getElementById('editUseProxy').checked = keyData.use_proxy === undefined || keyData.use_proxy === null ? true : Boolean(keyData.use_proxy);
        document.getElementById('editDisableModelFetch').checked = keyData.disable_model_fetch || false;
        document.getElementById('editExpiresAt').value = keyData.expires_at || '';
        document.getElementById('editIpWhitelist').value = keyData.ip_whitelist || '';
        document.getElementById('editIpBlacklist').value = keyData.ip_blacklist || '';
        document.getElementById('editModelMappings').value = keyData.model_mappings ?
            JSON.stringify(keyData.model_mappings, null, 2) : '';

        // New fields
        document.getElementById('editMaxTotalTokens').value = keyData.max_total_tokens || '';
        document.getElementById('editMaxContextTokens').value = keyData.max_context_tokens || '';
        document.getElementById('editCustomPrefills').value = keyData.custom_prefills ?
            (typeof keyData.custom_prefills === 'string' ? keyData.custom_prefills : JSON.stringify(keyData.custom_prefills, null, 2)) : '';

        // Load model mappings into builder
        loadModelMappingsToBuilder(keyData.model_mappings ? JSON.stringify(keyData.model_mappings, null, 2) : '', true);

        // Populate providers
        populateEditProviders(keyData.providers);

        // Set rotation frequency
        const rotationFreq = keyData.provider_rotation_frequency || 1;
        document.getElementById('editProviderRotationFrequency').value = rotationFreq;
        updateEditRotationFrequencyLabel(rotationFreq);
    } else {
        // Reset to defaults if no keyData
        document.getElementById('editTargetUrl').value = '';
        document.getElementById('editTargetApiKey').value = '';
        document.getElementById('editHttpReferer').value = '';
        document.getElementById('editNoAuth').checked = false;
        document.getElementById('editUseProxy').checked = true;
        document.getElementById('editDisableModelFetch').checked = false;
        document.getElementById('editExpiresAt').value = '';
        document.getElementById('editIpWhitelist').value = '';
        document.getElementById('editIpBlacklist').value = '';
        document.getElementById('editModelMappings').value = '';
        // New fields
        document.getElementById('editMaxTotalTokens').value = '';
        document.getElementById('editMaxContextTokens').value = '';
        document.getElementById('editCustomPrefills').value = '';
        clearEditModelMappingsBuilder();
        clearEditProviders();
        document.getElementById('editProviderRotationFrequency').value = 1;
        updateEditRotationFrequencyLabel(1);
    }

    document.getElementById('editModal').classList.add('active');
}

// Close edit modal
function closeEditModal() {
    document.getElementById('editModal').classList.remove('active');
    currentEditKeyId = null;
}

// Save edited limits and settings
async function saveEdits() {
    const maxRpm = parseInt(document.getElementById('editMaxRpm').value);
    const maxRpd = parseInt(document.getElementById('editMaxRpd').value);
    const httpReferer = document.getElementById('editHttpReferer').value.trim() || null;

    // Target URL and API key fields (CRITICAL - these were missing!)
    const targetUrl = document.getElementById('editTargetUrl').value.trim() || null;
    const targetApiKey = document.getElementById('editTargetApiKey').value.trim() || null;
    const noAuth = document.getElementById('editNoAuth').checked;

    // New fields
    const useProxy = document.getElementById('editUseProxy').checked;
    const disableModelFetch = document.getElementById('editDisableModelFetch').checked;
    const expiresAt = document.getElementById('editExpiresAt').value || null;
    const ipWhitelist = document.getElementById('editIpWhitelist').value.trim() || null;
    const ipBlacklist = document.getElementById('editIpBlacklist').value.trim() || null;
    const modelMappingsStr = document.getElementById('editModelMappings').value.trim();

    // Get providers and rotation frequency
    const providers = getEditProviders();
    const providerRotationFrequency = parseInt(document.getElementById('editProviderRotationFrequency').value) || 1;

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
        const response = await apiFetch(`/admin/keys/${currentEditKeyId}/limits`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                max_rpm: maxRpm,
                max_rpd: maxRpd,
                target_url: targetUrl,
                target_api_key: targetApiKey,
                no_auth: noAuth,
                http_referer: httpReferer,
                use_proxy: useProxy,
                disable_model_fetch: disableModelFetch,
                expires_at: expiresAt,
                ip_whitelist: ipWhitelist,
                ip_blacklist: ipBlacklist,
                model_mappings: modelMappings,
                providers: providers,
                provider_rotation_frequency: providerRotationFrequency
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

// Toggle API key proxy setting
async function toggleKeyProxy(keyId) {
    try {
        const response = await apiFetch(`/admin/keys/${keyId}/toggle-proxy`, {
            method: 'PUT'
        });

        if (!response.ok) {
            throw new Error('Failed to toggle proxy setting');
        }

        loadAPIKeys();
        addConsoleLog('SUCCESS', `Toggled proxy for API key ID ${keyId}`);
        showToast('Proxy setting updated', 'success');
    } catch (error) {
        console.error('Error toggling proxy:', error);
        addConsoleLog('ERROR', `Failed to toggle proxy: ${error.message}`);
        showToast('Failed to toggle proxy', 'error');
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
            tbody.innerHTML = '<tr><td colspan="7" class="loading">No recent requests</td></tr>';
        } else {
            tbody.innerHTML = logs.map(log => {
                const time = new Date(log.request_time.includes('Z') ? log.request_time : log.request_time + 'Z').toLocaleTimeString();
                const totalTokens = log.tokens_used || (log.input_tokens || 0) + (log.output_tokens || 0);
                const cost = log.cost !== undefined && log.cost !== null ? `$${log.cost.toFixed(4)}` : '-';
                const clientIp = log.client_ip || 'N/A';
                return `
                    <tr>
                        <td class="time-cell">${time}</td>
                        <td class="model-cell">${escapeHtml(log.model || 'Unknown')}</td>
                        <td class="token-cell input">${formatNumber(log.input_tokens || 0)}</td>
                        <td class="token-cell output">${formatNumber(log.output_tokens || 0)}</td>
                        <td class="token-cell total">${formatNumber(totalTokens)}</td>
                        <td class="cost-cell">${cost}</td>
                        <td class="ip-cell">${escapeHtml(clientIp)}</td>
                    </tr>
                `;
            }).join('');
        }

    } catch (error) {
        console.error('Error loading recent requests:', error);
        const tbody = document.getElementById('requestLogBody');
        tbody.innerHTML = '<tr><td colspan="6" class="loading" style="color: var(--danger-color);">Failed to load</td></tr>';
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

    // Refresh chart data every 10 seconds
    setInterval(() => {
        const activeBtn = document.querySelector('.chart-period-btn.active');
        const period = activeBtn ? (activeBtn.textContent.toLowerCase().includes('24h') ? '24h' : '7d') : '24h';
        loadChartData(period);
    }, 10000);
}

// Refresh usage stats for all keys without reloading the entire list
async function refreshAllKeyUsage() {
    try {
        const response = await apiFetch('/admin/keys/list');
        const data = await response.json();

        for (const key of data.keys) {
            try {
                const usageResponse = await apiFetch(`/admin/keys/${key.id}/usage`);
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

// ==================== WEBSCRAPINGAPI TOGGLE ====================

// Load WebScrapingAPI status on page load
async function loadWebScrapingAPIStatus() {
    try {
        const response = await apiFetch('/admin/settings/webscrapingapi');
        const data = await response.json();

        const toggle = document.getElementById('webscrapingToggle');
        const container = document.getElementById('webscrapingToggleContainer');

        if (toggle && container) {
            toggle.checked = data.enabled;

            // Update container styling based on status
            container.classList.remove('active', 'inactive', 'no-key');

            if (!data.has_key) {
                container.classList.add('no-key');
                toggle.disabled = true;
                container.title = 'WebScrapingAPI key not configured';
            } else if (data.enabled) {
                container.classList.add('active');
                container.title = 'WebScrapingAPI is enabled - requests are proxied';
            } else {
                container.classList.add('inactive');
                container.title = 'WebScrapingAPI is disabled - direct requests';
            }
        }
    } catch (error) {
        console.error('Error loading WebScrapingAPI status:', error);
    }
}

// Toggle WebScrapingAPI on/off
async function toggleWebScrapingAPI() {
    const toggle = document.getElementById('webscrapingToggle');
    const container = document.getElementById('webscrapingToggleContainer');

    if (!toggle) return;

    const newState = toggle.checked;

    try {
        const response = await apiFetch('/admin/settings/webscrapingapi', {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ enabled: newState })
        });

        if (!response.ok) {
            throw new Error('Failed to update WebScrapingAPI settings');
        }

        const data = await response.json();

        // Update container styling
        if (container) {
            container.classList.remove('active', 'inactive');
            if (data.enabled) {
                container.classList.add('active');
                container.title = 'WebScrapingAPI is enabled - requests are proxied';
            } else {
                container.classList.add('inactive');
                container.title = 'WebScrapingAPI is disabled - direct requests';
            }
        }

        addConsoleLog('SUCCESS', `WebScrapingAPI ${data.enabled ? 'enabled' : 'disabled'}`);
        showToast(`WebScrapingAPI ${data.enabled ? 'enabled' : 'disabled'}`, 'success');

    } catch (error) {
        console.error('Error toggling WebScrapingAPI:', error);
        // Revert toggle state on error
        toggle.checked = !newState;
        addConsoleLog('ERROR', `Failed to toggle WebScrapingAPI: ${error.message}`);
        showToast('Failed to update WebScrapingAPI settings', 'error');
    }
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