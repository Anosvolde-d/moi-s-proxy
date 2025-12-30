/**
 * Keys Page JavaScript
 * Handles fetching user keys, rendering key cards, and copy functionality
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6
 */

// State
let currentUser = null;
let userKeys = [];

/**
 * Initialize the page on load
 */
document.addEventListener('DOMContentLoaded', async () => {
    await loadUserInfo();
    await loadUserKeys();
});

/**
 * Load current user information from the API
 * Requirements: 4.5
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
 * Requirements: 4.5
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
 * Load user's key variations from the API
 * Requirements: 4.1, 4.6
 */
async function loadUserKeys() {
    const keysGrid = document.getElementById('keysGrid');
    
    try {
        showLoadingState();
        
        const response = await fetch('/api/user/keys');
        
        if (!response.ok) {
            if (response.status === 401) {
                window.location.href = '/login';
                return;
            }
            throw new Error('Failed to load keys');
        }
        
        const data = await response.json();
        userKeys = data.keys || [];
        
        renderKeys();
    } catch (error) {
        console.error('Error loading keys:', error);
        showErrorState(error.message);
    }
}

/**
 * Show loading state in the grid
 */
function showLoadingState() {
    const keysGrid = document.getElementById('keysGrid');
    keysGrid.innerHTML = `
        <div class="loading-container" id="loadingState">
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
        <div class="error-container">
            <div class="error-icon">⚠️</div>
            <p class="error-message">${escapeHtml(message)}</p>
            <button class="retry-btn" onclick="loadUserKeys()">Try Again</button>
        </div>
    `;
}

/**
 * Show empty state when no keys are available
 */
function showEmptyState() {
    const keysGrid = document.getElementById('keysGrid');
    keysGrid.innerHTML = `
        <div class="empty-container">
            <div class="empty-icon">🔑</div>
            <p class="empty-message">No API keys available yet. Check back later!</p>
        </div>
    `;
}

/**
 * Render all key cards in the bento grid
 * Requirements: 4.1, 4.2
 */
function renderKeys() {
    const keysGrid = document.getElementById('keysGrid');
    
    if (userKeys.length === 0) {
        showEmptyState();
        return;
    }
    
    keysGrid.innerHTML = userKeys.map((key, index) => createKeyCard(key, index)).join('');
}

/**
 * Create HTML for a single key card
 * Requirements: 4.2, 4.3, 4.4
 */
function createKeyCard(key, index) {
    const maskedVariation = maskKey(key.variation);
    const isActive = key.enabled !== false;
    
    return `
        <div class="key-card" data-key-index="${index}">
            <div class="key-card-header">
                <div>
                    <div class="key-name">${escapeHtml(key.name || 'API Key')}</div>
                    <div class="key-prefix">${escapeHtml(key.prefix || 'Moi-...')}</div>
                </div>
                <div class="key-status ${isActive ? 'active' : 'inactive'}">
                    <span class="status-dot"></span>
                    ${isActive ? 'Active' : 'Inactive'}
                </div>
            </div>
            <div class="key-variation">
                <div class="key-variation-masked">${escapeHtml(maskedVariation)}</div>
            </div>
            <button class="key-copy-btn" onclick="copyKey(${index})" data-copy-btn="${index}">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                </svg>
                Copy Key
            </button>
        </div>
    `;
}

/**
 * Mask a key for display, showing only prefix and last few characters
 * Requirements: 4.3
 */
function maskKey(key) {
    if (!key || key.length < 20) return key;
    
    // Show first 12 chars and last 4 chars
    const prefix = key.substring(0, 12);
    const suffix = key.substring(key.length - 4);
    return `${prefix}...${suffix}`;
}

/**
 * Copy a key to clipboard
 * Requirements: 4.4
 */
async function copyKey(index) {
    const key = userKeys[index];
    if (!key || !key.variation) {
        showToast('Key not found', 'error');
        return;
    }
    
    try {
        await navigator.clipboard.writeText(key.variation);
        
        // Visual feedback
        const btn = document.querySelector(`[data-copy-btn="${index}"]`);
        if (btn) {
            btn.classList.add('copied');
            btn.innerHTML = `
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <polyline points="20 6 9 17 4 12"></polyline>
                </svg>
                Copied!
            `;
            
            // Reset after 2 seconds
            setTimeout(() => {
                btn.classList.remove('copied');
                btn.innerHTML = `
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                    </svg>
                    Copy Key
                `;
            }, 2000);
        }
        
        showToast('Key copied to clipboard!', 'success');
    } catch (error) {
        console.error('Failed to copy:', error);
        showToast('Failed to copy key', 'error');
        
        // Fallback for older browsers
        fallbackCopy(key.variation);
    }
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
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <span>${type === 'success' ? '✓' : type === 'error' ? '✕' : 'ℹ'}</span>
        <span>${escapeHtml(message)}</span>
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
