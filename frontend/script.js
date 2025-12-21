/**
 * OpenAI Proxy - Public Landing Page Script
 * Handles API key validation, model listing, and analytics
 */

// Global state
let currentModels = [];
let currentApiKey = '';
let usageLogsInterval = null;

// DOM Elements
const apiKeyInput = document.getElementById('apiKeyInput');
const validateBtn = document.getElementById('validateBtn');
const keyInfoSection = document.getElementById('keyInfo');
const statsSection = document.getElementById('statsSection');
const modelsSection = document.getElementById('modelsSection');
const errorSection = document.getElementById('errorSection');
const modelSearch = document.getElementById('modelSearch');
const modelsList = document.getElementById('modelsList');

// Initialize
// Initialize
document.addEventListener('DOMContentLoaded', () => {
    // Event listeners
    validateBtn.addEventListener('click', handleValidate);
    apiKeyInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            handleValidate();
        }
    });
    modelSearch.addEventListener('input', handleModelSearch);
    
    // Initialize analytics
    fetchGlobalAnalytics();
    
    // Refresh analytics every 30 seconds
    setInterval(fetchGlobalAnalytics, 30000);
    
    // Check for API key in URL params
    const urlParams = new URLSearchParams(window.location.search);
    const keyParam = urlParams.get('key');
    if (keyParam) {
        apiKeyInput.value = keyParam;
        handleValidate();
    }
    
});
// ==================== GLOBAL ANALYTICS ====================

/**
 * Fetch and display global analytics
 */
async function fetchGlobalAnalytics() {
    const statusIndicator = document.getElementById('analyticsStatus');
    
    try {
        // Use public stats endpoint (no auth required)
        const response = await fetch('/api/public-stats');
        if (!response.ok) {
            throw new Error('Failed to fetch analytics');
        }
        
        const data = await response.json();
        displayGlobalAnalytics(data);
        
        // Update status
        if (statusIndicator) {
            const dot = statusIndicator.querySelector('.status-dot');
            const text = statusIndicator.querySelector('.status-text');
            if (dot) dot.classList.add('active');
            if (text) text.textContent = 'Live';
        }
    } catch (error) {
        console.error('Error fetching analytics:', error);
        
        // Update status to show error
        if (statusIndicator) {
            const dot = statusIndicator.querySelector('.status-dot');
            const text = statusIndicator.querySelector('.status-text');
            if (dot) dot.classList.remove('active');
            if (text) text.textContent = 'Error';
        }
    }
}

/**
 * Display global analytics data
 */
function displayGlobalAnalytics(data) {
    // Total Requests
    const totalRequests = document.getElementById('globalTotalRequests');
    if (totalRequests) {
        totalRequests.textContent = formatNumber(data.total_requests || 0);
    }
    
    // Success Rate
    const successRate = document.getElementById('globalSuccessRate');
    if (successRate) {
        const rate = data.success_rate || 0;
        successRate.textContent = `${rate.toFixed(1)}%`;
    }
    
    // Total Tokens
    const totalTokens = document.getElementById('globalTotalTokens');
    if (totalTokens) {
        totalTokens.textContent = formatNumber(data.total_tokens || 0);
    }
    
    // Input Tokens
    const inputTokens = document.getElementById('globalInputTokens');
    if (inputTokens) {
        inputTokens.textContent = formatNumber(data.input_tokens || 0);
    }
    
    // Output Tokens
    const outputTokens = document.getElementById('globalOutputTokens');
    if (outputTokens) {
        outputTokens.textContent = formatNumber(data.output_tokens || 0);
    }
    
    // Top Model
    const topModel = document.getElementById('globalTopModel');
    if (topModel) {
        const modelName = data.top_model || 'N/A';
        // Truncate long model names
        topModel.textContent = modelName.length > 20 ? modelName.substring(0, 17) + '...' : modelName;
        topModel.title = modelName; // Full name on hover
    }
    
    // Active Keys
    const activeKeys = document.getElementById('globalActiveKeys');
    if (activeKeys) {
        activeKeys.textContent = formatNumber(data.active_keys || 0);
    }
}

/**
 * Handle the validate button click
 */
async function handleValidate() {
    const apiKey = apiKeyInput.value.trim();
    
    if (!apiKey) {
        showError('Please enter an API key');
        return;
    }
    
    currentApiKey = apiKey;
    setLoading(true);
    hideAllSections();
    
    try {
        // Validate the key
        const keyInfo = await validateKey(apiKey);
        
        if (keyInfo.valid) {
            displayKeyInfo(keyInfo);
            // Fetch and display stats
            await fetchAndDisplayStats(apiKey, keyInfo);
            // Fetch and display usage logs
            await fetchAndDisplayUsageLogs(apiKey);
            // Start auto-refresh for usage logs (every 3 seconds)
            startUsageLogsAutoRefresh(apiKey);
            // Fetch models
            await fetchAndDisplayModels(apiKey);
        } else {
            showError('This API key is not valid or has been disabled.');
        }
    } catch (error) {
        console.error('Validation error:', error);
        showError(error.message || 'Failed to validate API key. Please try again.');
    } finally {
        setLoading(false);
    }
}

/**
 * Validate an API key against the backend
 */
async function validateKey(apiKey) {
    const response = await fetch(`/api/key-info/${encodeURIComponent(apiKey)}`);
    
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(error.detail || 'Failed to validate key');
    }
    
    return await response.json();
}

/**
 * Fetch models from the target API
 */
async function fetchModels(apiKey) {
    const response = await fetch(`/api/key-models/${encodeURIComponent(apiKey)}`);
    
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(error.detail || 'Failed to fetch models');
    }
    
    return await response.json();
}

/**
 * Display key information in the UI
 */
function displayKeyInfo(info) {
    keyInfoSection.classList.remove('hidden');
    
    // Update status badge
    const statusBadge = document.getElementById('keyStatus');
    statusBadge.textContent = info.enabled ? 'Active' : 'Disabled';
    statusBadge.className = `status-badge ${info.enabled ? 'valid' : 'invalid'}`;
    
    // Update info fields
    document.getElementById('keyName').textContent = info.name || 'Unnamed Key';
    document.getElementById('keyPrefix').textContent = info.prefix || '-';
    document.getElementById('keyEnabled').textContent = info.enabled ? 'Enabled' : 'Disabled';
    document.getElementById('keyCustomTarget').textContent = info.has_custom_target ? 'Yes' : 'No (Default)';
    document.getElementById('keyRpm').textContent = info.max_rpm || 'Unlimited';
    document.getElementById('keyRpd').textContent = info.max_rpd || 'Unlimited';
}

/**
 * Fetch key statistics from the backend
 */
async function fetchKeyStats(apiKey) {
    const response = await fetch(`/api/key-stats/${encodeURIComponent(apiKey)}`);
    
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(error.detail || 'Failed to fetch stats');
    }
    
    return await response.json();
}

/**
 * Format large numbers (e.g., 1234 -> 1.2K, 1234567 -> 1.2M)
 */
function formatNumber(num) {
    if (num === null || num === undefined) return '0';
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

/**
 * Fetch and display key statistics
 */
async function fetchAndDisplayStats(apiKey, keyInfo) {
    try {
        const stats = await fetchKeyStats(apiKey);
        displayKeyStats(stats, keyInfo);
    } catch (error) {
        console.error('Error fetching stats:', error);
        // Don't show error, just hide stats section
        statsSection.classList.add('hidden');
    }
}

/**
 * Display key statistics in the UI
 */
function displayKeyStats(stats, keyInfo) {
    statsSection.classList.remove('hidden');
    
    // RPD Progress - backend uses requests_today, not today_requests
    const maxRpd = keyInfo.max_rpd || stats.max_rpd || 0;
    const todayRequests = stats.requests_today || 0;
    const rpdUsed = stats.rpd_used || todayRequests;
    const remaining = stats.rpd_remaining !== undefined ? stats.rpd_remaining : Math.max(0, maxRpd - rpdUsed);
    const percentage = maxRpd > 0 ? Math.min(100, (rpdUsed / maxRpd) * 100) : 0;
    
    document.getElementById('rpdText').textContent = maxRpd > 0
        ? `${rpdUsed} / ${maxRpd}`
        : `${rpdUsed} / ∞`;
    
    const progressBar = document.getElementById('rpdProgressBar');
    progressBar.style.width = maxRpd > 0 ? `${percentage}%` : '0%';
    
    // Update progress bar color based on usage
    progressBar.classList.remove('warning', 'danger');
    if (percentage >= 90) {
        progressBar.classList.add('danger');
    } else if (percentage >= 70) {
        progressBar.classList.add('warning');
    }
    
    document.getElementById('rpdRemaining').textContent = maxRpd > 0
        ? `${remaining} requests remaining today`
        : 'Unlimited requests';
    
    // Stats Grid - calculate success_rate from successful_requests / total_requests
    const totalRequests = stats.total_requests || 0;
    const successfulRequests = stats.successful_requests || 0;
    const successRate = totalRequests > 0 ? (successfulRequests / totalRequests) * 100 : 100;
    
    document.getElementById('totalRequests').textContent = formatNumber(totalRequests);
    document.getElementById('successRate').textContent = `${successRate.toFixed(1)}%`;
    document.getElementById('totalTokens').textContent = formatNumber(stats.total_tokens || 0);
    document.getElementById('todayRequests').textContent = formatNumber(todayRequests);
    
    // Top Model Section - backend uses request_count, not count
    const topModelSection = document.getElementById('topModelSection');
    const topModels = stats.top_models || [];
    
    if (topModels.length > 0) {
        topModelSection.classList.remove('hidden');
        const topModel = topModels[0];
        document.getElementById('topModelName').textContent = topModel.model || 'Unknown';
        document.getElementById('topModelCount').textContent = `${formatNumber(topModel.request_count || topModel.count || 0)} requests`;
    } else {
        topModelSection.classList.add('hidden');
    }
    
    // Top Models List
    const topModelsListSection = document.getElementById('topModelsListSection');
    const topModelsList = document.getElementById('topModelsList');
    
    if (topModels.length > 1) {
        topModelsListSection.classList.remove('hidden');
        
        const rankClasses = ['gold', 'silver', 'bronze', '', ''];
        const html = topModels.slice(0, 5).map((model, index) => {
            const rankClass = rankClasses[index] || '';
            return `
                <div class="top-model-item">
                    <div class="model-info">
                        <span class="rank ${rankClass}">${index + 1}</span>
                        <span class="model-name">${escapeHtml(model.model || 'Unknown')}</span>
                    </div>
                    <span class="request-count">${formatNumber(model.request_count || model.count || 0)} req</span>
                </div>
            `;
        }).join('');
        
        topModelsList.innerHTML = html;
    } else {
        topModelsListSection.classList.add('hidden');
    }
}

/**
 * Fetch and display models
 */
async function fetchAndDisplayModels(apiKey) {
    modelsSection.classList.remove('hidden');
    modelsList.innerHTML = `
        <div class="loading-models">
            <svg class="spinner" viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3" fill="none" stroke-dasharray="30 70"/>
            </svg>
            <span>Loading models...</span>
        </div>
    `;
    
    try {
        const modelsData = await fetchModels(apiKey);
        
        currentModels = modelsData.models || [];
        
        // Update count and source
        document.getElementById('modelCount').textContent = modelsData.count || 0;
        
        const sourceBadge = document.getElementById('modelSource');
        if (modelsData.source === 'custom') {
            sourceBadge.textContent = 'Custom API';
            sourceBadge.classList.add('custom');
        } else {
            sourceBadge.textContent = 'Default API';
            sourceBadge.classList.remove('custom');
        }
        
        // Display models
        renderModels(currentModels);
    } catch (error) {
        console.error('Error fetching models:', error);
        modelsList.innerHTML = `
            <div class="no-models">
                <p>Failed to load models: ${error.message}</p>
            </div>
        `;
    }
}

/**
 * Render the models list
 */
function renderModels(models) {
    if (!models || models.length === 0) {
        modelsList.innerHTML = `
            <div class="no-models">
                <p>No models available</p>
            </div>
        `;
        return;
    }
    
    const html = models.map(model => {
        const modelId = typeof model === 'string' ? model : (model.id || model.name || 'Unknown');
        const owner = typeof model === 'object' ? (model.owned_by || model.owner || '') : '';
        
        return `
            <div class="model-item" data-model-id="${escapeHtml(modelId)}">
                <span class="model-name">${escapeHtml(modelId)}</span>
                ${owner ? `<span class="model-owner">${escapeHtml(owner)}</span>` : ''}
            </div>
        `;
    }).join('');
    
    modelsList.innerHTML = html;
}

/**
 * Handle model search
 */
function handleModelSearch(e) {
    const searchTerm = e.target.value.toLowerCase().trim();
    
    if (!searchTerm) {
        renderModels(currentModels);
        return;
    }
    
    const filtered = currentModels.filter(model => {
        const modelId = typeof model === 'string' ? model : (model.id || model.name || '');
        const owner = typeof model === 'object' ? (model.owned_by || model.owner || '') : '';
        
        return modelId.toLowerCase().includes(searchTerm) || 
               owner.toLowerCase().includes(searchTerm);
    });
    
    renderModels(filtered);
    
    // Update count to show filtered results
    document.getElementById('modelCount').textContent = `${filtered.length}/${currentModels.length}`;
}

/**
 * Show error message
 */
function showError(message) {
    hideAllSections();
    errorSection.classList.remove('hidden');
    document.getElementById('errorMessage').textContent = message;
}

/**
 * Hide all result sections
 */
function hideAllSections() {
    keyInfoSection.classList.add('hidden');
    statsSection.classList.add('hidden');
    modelsSection.classList.add('hidden');
    errorSection.classList.add('hidden');
    
    // Hide usage logs section
    const usageLogsSection = document.getElementById('usageLogsSection');
    if (usageLogsSection) {
        usageLogsSection.classList.add('hidden');
    }
    
    // Stop auto-refresh when hiding sections
    stopUsageLogsAutoRefresh();
}

// ==================== USAGE LOGS ====================

/**
 * Fetch usage logs for a specific API key
 */
async function fetchUsageLogs(apiKey) {
    const response = await fetch(`/api/key-usage-logs/${encodeURIComponent(apiKey)}`);
    
    if (!response.ok) {
        const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(error.detail || 'Failed to fetch usage logs');
    }
    
    return await response.json();
}

/**
 * Fetch and display usage logs
 */
async function fetchAndDisplayUsageLogs(apiKey) {
    const usageLogsSection = document.getElementById('usageLogsSection');
    const usageLogsTable = document.getElementById('usageLogsTable');
    
    if (!usageLogsSection || !usageLogsTable) return;
    
    usageLogsSection.classList.remove('hidden');
    
    try {
        const data = await fetchUsageLogs(apiKey);
        displayUsageLogs(data.logs || []);
    } catch (error) {
        console.error('Error fetching usage logs:', error);
        usageLogsTable.innerHTML = `
            <tr>
                <td colspan="5" class="no-logs">Failed to load usage logs: ${escapeHtml(error.message)}</td>
            </tr>
        `;
    }
}

/**
 * Display usage logs in the table
 */
function displayUsageLogs(logs) {
    const usageLogsTable = document.getElementById('usageLogsTable');
    if (!usageLogsTable) return;
    
    if (!logs || logs.length === 0) {
        usageLogsTable.innerHTML = `
            <tr>
                <td colspan="5" class="no-logs">No usage logs yet</td>
            </tr>
        `;
        return;
    }
    
    const html = logs.map(log => {
        const timestamp = new Date(log.request_time).toLocaleString();
        const model = log.model || 'Unknown';
        const inputTokens = log.input_tokens || 0;
        const outputTokens = log.output_tokens || 0;
        const totalTokens = log.tokens_used || (inputTokens + outputTokens);
        const statusClass = log.success ? 'success' : 'error';
        const statusText = log.success ? '✓' : '✗';
        
        return `
            <tr class="${statusClass}">
                <td class="log-time">${escapeHtml(timestamp)}</td>
                <td class="log-model">${escapeHtml(model)}</td>
                <td class="log-tokens">${formatNumber(inputTokens)}</td>
                <td class="log-tokens">${formatNumber(outputTokens)}</td>
                <td class="log-tokens">${formatNumber(totalTokens)}</td>
            </tr>
        `;
    }).join('');
    
    usageLogsTable.innerHTML = html;
}

/**
 * Start auto-refresh for usage logs (every 3 seconds)
 */
function startUsageLogsAutoRefresh(apiKey) {
    // Clear any existing interval
    stopUsageLogsAutoRefresh();
    
    // Start new interval
    usageLogsInterval = setInterval(async () => {
        try {
            await fetchAndDisplayUsageLogs(apiKey);
        } catch (error) {
            console.error('Error refreshing usage logs:', error);
        }
    }, 3000);
}

/**
 * Stop auto-refresh for usage logs
 */
function stopUsageLogsAutoRefresh() {
    if (usageLogsInterval) {
        clearInterval(usageLogsInterval);
        usageLogsInterval = null;
    }
}

/**
 * Set loading state
 */
function setLoading(loading) {
    const btnText = validateBtn.querySelector('.btn-text');
    const btnLoader = validateBtn.querySelector('.btn-loader');
    
    if (loading) {
        btnText.classList.add('hidden');
        btnLoader.classList.remove('hidden');
        validateBtn.disabled = true;
        apiKeyInput.disabled = true;
    } else {
        btnText.classList.remove('hidden');
        btnLoader.classList.add('hidden');
        validateBtn.disabled = false;
        apiKeyInput.disabled = false;
    }
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Copy text to clipboard
 */
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        // Could show a toast notification here
        console.log('Copied to clipboard:', text);
    }).catch(err => {
        console.error('Failed to copy:', err);
    });
}
