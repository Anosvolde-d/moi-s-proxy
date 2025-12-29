/**
 * Claim Page JavaScript
 * Handles claim code submission, fetching claimed keys, and copy functionality
 * Requirements: 1.6, 6.3, 7.3, 8.2
 */

// State
let currentUser = null;
let claimedKeys = [];

/**
 * Initialize the page on load
 * Requirements: 7.5 - Authentication guard
 */
document.addEventListener('DOMContentLoaded', async () => {
    await loadUserInfo();
    await loadClaimedKeys();
    
    // Focus the claim code input
    const input = document.getElementById('claimCodeInput');
    if (input) {
        input.focus();
    }
});

/**
 * Load current user information from the API
 * Requirements: 7.5 - Redirect to login if not authenticated
 */
async function loadUserInfo() {
    try {
        const response = await fetch('/api/auth/me');
        
        if (!response.ok) {
            if (response.status === 401) {
                // Not authenticated, redirect to login
                window.location.href = '/login';
                return;
            }
            throw new Error('Failed to load user info');
        }
        
        currentUser = await response.json();
        renderUserInfo();
    } catch (error) {
        console.error('Error loading user info:', error);
        // Redirect to login on error
        window.location.href = '/login';
    }
}

/**
 * Render user information in the header
 */
function renderUserInfo() {
    if (!currentUser) return;
    
    const avatarEl = document.getElementById('userAvatar');
    const nameEl = document.getElementById('userName');
    const idEl = document.getElementById('userId');
    
    // Set avatar URL - use avatar_url from API if available, otherwise construct it
    if (currentUser.avatar_url) {
        avatarEl.src = currentUser.avatar_url;
    } else if (currentUser.avatar) {
        avatarEl.src = `https://cdn.discordapp.com/avatars/${currentUser.id}/${currentUser.avatar}.png`;
    } else {
        // Default Discord avatar
        avatarEl.src = `https://cdn.discordapp.com/embed/avatars/${parseInt(currentUser.id) % 5}.png`;
    }
    avatarEl.alt = `${currentUser.username}'s avatar`;
    
    // Add error handler for broken avatar images
    avatarEl.onerror = function() {
        this.onerror = null; // Prevent infinite loop
        this.src = `https://cdn.discordapp.com/embed/avatars/${parseInt(currentUser.id) % 5}.png`;
    };
    
    // Set username (prefer global_name if available)
    nameEl.textContent = currentUser.global_name || currentUser.username;
    
    // Set user ID
    idEl.textContent = `ID: ${currentUser.id}`;
}

/**
 * Handle claim form submission
 * Requirements: 1.6 - POST to /api/claim-code
 */
async function handleClaimSubmit(event) {
    event.preventDefault();
    
    const input = document.getElementById('claimCodeInput');
    const btn = document.getElementById('claimBtn');
    const btnText = document.getElementById('claimBtnText');
    const messageEl = document.getElementById('claimMessage');
    
    const claimCode = input.value.trim();
    
    if (!claimCode) {
        showClaimMessage('Please enter a claim code', 'error');
        return false;
    }
    
    // Show loading state
    btn.disabled = true;
    btn.classList.add('loading');
    btnText.innerHTML = '<span class="spinner"></span> Claiming...';
    messageEl.className = 'claim-message';
    messageEl.style.display = 'none';
    
    try {
        const response = await fetch('/api/claim-code', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ code: claimCode }),
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            // Extract error message from response
            const errorMessage = data.detail || data.message || 'Failed to claim key';
            throw new Error(typeof errorMessage === 'string' ? errorMessage : JSON.stringify(errorMessage));
        }
        
        // Success!
        if (data.already_claimed) {
            showClaimMessage(`You already have a key for this code: ${maskKey(data.sub_key_prefix)}`, 'info');
            showToast('You already claimed this key!', 'info');
        } else {
            showClaimMessage(`Key claimed successfully! Your key: ${maskKey(data.sub_key)}`, 'success');
            showToast('Key claimed successfully!', 'success');
        }
        
        // Clear input
        input.value = '';
        
        // Reload keys list
        await loadClaimedKeys();
        
    } catch (error) {
        console.error('Error claiming key:', error);
        // Ensure error message is a string
        const errorMessage = typeof error === 'string' ? error : (error.message || 'Failed to claim key');
        showClaimMessage(errorMessage, 'error');
        showToast(errorMessage, 'error');
    } finally {
        // Reset button state
        btn.disabled = false;
        btn.classList.remove('loading');
        btnText.textContent = 'Claim Key';
    }
    
    return false;
}

/**
 * Show claim message
 */
function showClaimMessage(message, type) {
    const messageEl = document.getElementById('claimMessage');
    messageEl.textContent = message;
    messageEl.className = `claim-message ${type}`;
}

/**
 * Load user's claimed keys from the API
 * Requirements: 6.1 - Fetch and display /api/my-keys
 */
async function loadClaimedKeys() {
    const keysGrid = document.getElementById('keysGrid');
    const keysCount = document.getElementById('keysCount');
    
    try {
        showLoadingState();
        
        const response = await fetch('/api/my-keys');
        
        if (!response.ok) {
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }
            // Try to get error message from response
            let errorMessage = 'Failed to load keys';
            try {
                const errorData = await response.json();
                errorMessage = errorData.detail || errorData.message || errorMessage;
            } catch (e) {
                // Ignore JSON parse errors
            }
            throw new Error(errorMessage);
        }
        
        const data = await response.json();
        claimedKeys = data.keys || [];
        
        // Update count
        keysCount.textContent = `(${claimedKeys.length} key${claimedKeys.length !== 1 ? 's' : ''})`;
        
        renderKeys();
    } catch (error) {
        console.error('Error loading keys:', error);
        // Ensure we always pass a string to showErrorState
        const errorMessage = typeof error === 'string' ? error : (error.message || 'Failed to load keys');
        showErrorState(errorMessage);
    }
}

/**
 * Show loading state in the grid
 */
function showLoadingState() {
    const keysGrid = document.getElementById('keysGrid');
    keysGrid.innerHTML = `
        <div class="loading-container">
            <div class="loading-spinner"></div>
            <p class="loading-text">Loading your keys...</p>
        </div>
    `;
}

/**
 * Show error state in the grid
 */
function showErrorState(message) {
    const keysGrid = document.getElementById('keysGrid');
    keysGrid.innerHTML = `
        <div class="empty-container">
            <div class="empty-icon">⚠️</div>
            <p class="empty-message">${escapeHtml(message)}</p>
        </div>
    `;
}

/**
 * Show empty state when no keys are claimed
 */
function showEmptyState() {
    const keysGrid = document.getElementById('keysGrid');
    keysGrid.innerHTML = `
        <div class="empty-container">
            <div class="empty-icon">🎫</div>
            <p class="empty-message">No keys claimed yet. Enter a claim code above to get started!</p>
        </div>
    `;
}

/**
 * Render all key cards in the grid
 * Requirements: 6.1
 */
function renderKeys() {
    const keysGrid = document.getElementById('keysGrid');
    
    if (claimedKeys.length === 0) {
        showEmptyState();
        return;
    }
    
    keysGrid.innerHTML = claimedKeys.map((key, index) => createKeyCard(key, index)).join('');
}

/**
 * Create HTML for a single key card
 * Requirements: 6.3 - Copy button with redirect to validator
 */
function createKeyCard(key, index) {
    const isActive = key.enabled !== false && key.master_enabled !== false;
    const maskedKey = maskKey(key.sub_key);
    
    return `
        <div class="key-card ${isActive ? '' : 'disabled'}" data-key-index="${index}">
            <div class="key-card-header">
                <div>
                    <div class="key-name">${escapeHtml(key.master_key_name || 'API Key')}</div>
                    <div class="key-prefix">${escapeHtml(key.sub_key_prefix || 'Moi-sub-...')}</div>
                </div>
                <div class="key-status ${isActive ? 'active' : 'inactive'}">
                    <span class="status-dot"></span>
                    ${isActive ? 'Active' : 'Inactive'}
                </div>
            </div>
            <div class="key-value">${escapeHtml(maskedKey)}</div>
            <div class="key-actions">
                <button class="key-btn primary" onclick="copyKey(${index})" data-copy-btn="${index}">
                    📋 Copy Key
                </button>
                <button class="key-btn" onclick="testKey(${index})">
                    🧪 Test
                </button>
            </div>
        </div>
    `;
}

/**
 * Mask a key for display, showing only prefix and last few characters
 */
function maskKey(key) {
    if (!key || key.length < 15) return key || '';
    
    // Show first 12 chars and last 4 chars
    const prefix = key.substring(0, 12);
    const suffix = key.substring(key.length - 4);
    return `${prefix}...${suffix}`;
}

/**
 * Copy a key to clipboard
 * Requirements: 6.3
 */
async function copyKey(index) {
    const key = claimedKeys[index];
    if (!key || !key.sub_key) {
        showToast('Key not found', 'error');
        return;
    }
    
    try {
        await navigator.clipboard.writeText(key.sub_key);
        
        // Visual feedback
        const btn = document.querySelector(`[data-copy-btn="${index}"]`);
        if (btn) {
            btn.classList.add('copied');
            btn.innerHTML = '✓ Copied!';
            
            // Reset after 2 seconds
            setTimeout(() => {
                btn.classList.remove('copied');
                btn.innerHTML = '📋 Copy Key';
            }, 2000);
        }
        
        showToast('Key copied to clipboard!', 'success');
    } catch (error) {
        console.error('Failed to copy:', error);
        showToast('Failed to copy key', 'error');
        
        // Fallback for older browsers
        fallbackCopy(key.sub_key);
    }
}

/**
 * Test a key by redirecting to validator
 * Requirements: 7.3
 */
function testKey(index) {
    const key = claimedKeys[index];
    if (!key || !key.sub_key) {
        showToast('Key not found', 'error');
        return;
    }
    
    // Copy key to clipboard first
    navigator.clipboard.writeText(key.sub_key).then(() => {
        showToast('Key copied! Redirecting to validator...', 'success');
        
        // Redirect to validator after a short delay
        setTimeout(() => {
            window.location.href = '/validator';
        }, 1000);
    }).catch(() => {
        // Just redirect without copying
        window.location.href = '/validator';
    });
}

/**
 * Fallback copy method for browsers without clipboard API
 */
function fallbackCopy(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.left = '-9999px';
    document.body.appendChild(textarea);
    textarea.select();
    
    try {
        document.execCommand('copy');
        showToast('Key copied to clipboard!', 'success');
    } catch (err) {
        showToast('Failed to copy key', 'error');
    }
    
    document.body.removeChild(textarea);
}

/**
 * Show a toast notification
 */
function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    
    // Ensure message is a string
    let displayMessage = message;
    if (typeof message === 'object') {
        displayMessage = message.message || message.detail || JSON.stringify(message);
    }
    displayMessage = String(displayMessage || 'An error occurred');
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <span>${type === 'success' ? '✓' : type === 'error' ? '✕' : 'ℹ'}</span>
        <span>${escapeHtml(displayMessage)}</span>
    `;
    
    container.appendChild(toast);
    
    // Remove after 3 seconds
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
