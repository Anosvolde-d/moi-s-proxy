/**
 * User Dashboard JavaScript
 * 
 * Handles loading and displaying user dashboard data including:
 * - User info and authentication
 * - Quota display with progress bar
 * - Statistics (total requests)
 * - Top 5 models
 * - Recent usage logs
 * - Announcements with poll voting
 * 
 * **Validates: Requirements 2.2, 2.3, 2.4, 2.5, 2.6, 4.3, 4.4, 5.1, 5.2, 5.3**
 */

// ==================== INITIALIZATION ====================

document.addEventListener('DOMContentLoaded', async () => {
    try {
        // Check authentication first
        const authResponse = await fetch('/api/auth/me');
        if (!authResponse.ok) {
            // Not logged in - redirect to login
            window.location.href = '/login';
            return;
        }
        
        const user = await authResponse.json();
        
        // Update user info in header
        updateUserInfo(user);
        
        // Load dashboard data
        await loadDashboardData();
        
        // Load announcements
        await loadAnnouncements();
        
        // Hide loading overlay
        hideLoading();
        
        // Set up auto-refresh every 30 seconds
        setInterval(loadDashboardData, 30000);
        setInterval(loadAnnouncements, 60000);
        
    } catch (error) {
        console.error('Error initializing dashboard:', error);
        hideLoading();
        showError('Failed to load dashboard. Please try again.');
    }
});

// ==================== USER INFO ====================

function updateUserInfo(user) {
    const avatarEl = document.getElementById('userAvatar');
    const nameEl = document.getElementById('userName');
    
    if (avatarEl) {
        if (user.avatar_url) {
            avatarEl.src = user.avatar_url;
        } else if (user.avatar && user.id) {
            avatarEl.src = `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png`;
        } else {
            avatarEl.src = `https://cdn.discordapp.com/embed/avatars/${(parseInt(user.id) || 0) % 5}.png`;
        }
        avatarEl.onerror = function() {
            this.onerror = null;
            this.src = `https://cdn.discordapp.com/embed/avatars/${(parseInt(user.id) || 0) % 5}.png`;
        };
    }
    
    if (nameEl) {
        nameEl.textContent = user.global_name || user.username || 'User';
    }
}

// ==================== DASHBOARD DATA ====================

async function loadDashboardData() {
    try {
        const response = await fetch('/api/user/dashboard');
        
        if (response.status === 401) {
            window.location.href = '/login';
            return;
        }
        
        if (response.status === 403) {
            // User is banned
            showBannedMessage();
            return;
        }
        
        if (!response.ok) {
            throw new Error('Failed to load dashboard data');
        }
        
        const data = await response.json();
        
        // Check if user has a sub-key
        if (!data.has_sub_key) {
            showNoKeySection();
            return;
        }
        
        // Hide no-key section and show dashboard
        hideNoKeySection();
        
        // Update quota display
        updateQuotaDisplay(data.quota);
        
        // Update statistics
        updateStats(data.stats);
        
        // Update top models
        updateTopModels(data.stats?.top_models || []);
        
        // Update recent logs
        updateRecentLogs(data.recent_logs || []);
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    }
}

// ==================== QUOTA DISPLAY ====================
// **Validates: Requirements 2.2**

function updateQuotaDisplay(quota) {
    if (!quota) return;
    
    const usedEl = document.getElementById('quotaUsed');
    const limitEl = document.getElementById('quotaLimit');
    const progressEl = document.getElementById('quotaProgress');
    const remainingEl = document.getElementById('quotaRemaining');
    const resetEl = document.getElementById('quotaReset');
    
    const used = quota.used || 0;
    const limit = quota.limit || 0;
    const remaining = quota.remaining || 0;
    
    if (usedEl) usedEl.textContent = `$${used.toFixed(2)}`;
    if (limitEl) limitEl.textContent = `$${limit.toFixed(2)}`;
    if (remainingEl) remainingEl.textContent = `$${remaining.toFixed(2)} remaining`;
    
    // Calculate progress percentage
    const percentage = limit > 0 ? Math.min((used / limit) * 100, 100) : 0;
    
    if (progressEl) {
        progressEl.style.width = `${percentage}%`;
        
        // Add warning/danger classes based on usage
        progressEl.classList.remove('warning', 'danger');
        if (percentage >= 90) {
            progressEl.classList.add('danger');
        } else if (percentage >= 70) {
            progressEl.classList.add('warning');
        }
    }
    
    // Update reset time
    if (resetEl) {
        if (quota.reset_at) {
            const resetDate = new Date(quota.reset_at);
            resetEl.textContent = `Resets at ${resetDate.toLocaleTimeString()}`;
        } else {
            resetEl.textContent = 'Resets at midnight UTC';
        }
    }
}

// ==================== STATISTICS ====================
// **Validates: Requirements 2.4, 2.5**

function updateStats(stats) {
    if (!stats) return;
    
    const totalRequestsEl = document.getElementById('totalRequests');
    
    if (totalRequestsEl) {
        totalRequestsEl.textContent = formatNumber(stats.total_requests || 0);
    }
}

function updateTopModels(models) {
    const listEl = document.getElementById('topModelsList');
    if (!listEl) return;
    
    if (!models || models.length === 0) {
        listEl.innerHTML = '<p class="empty-state">No model usage data yet</p>';
        return;
    }
    
    listEl.innerHTML = models.slice(0, 5).map((model, index) => `
        <div class="model-item">
            <span class="model-rank">#${index + 1}</span>
            <span class="model-name">${escapeHtml(model.model || 'Unknown')}</span>
            <span class="model-count">${formatNumber(model.count || 0)} requests</span>
        </div>
    `).join('');
}

// ==================== RECENT LOGS ====================
// **Validates: Requirements 2.3**

function updateRecentLogs(logs) {
    const tableBody = document.getElementById('logsTableBody');
    if (!tableBody) return;
    
    if (!logs || logs.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="5" class="empty-state">No usage logs yet</td></tr>';
        return;
    }
    
    tableBody.innerHTML = logs.map(log => {
        const timestamp = log.timestamp ? new Date(log.timestamp).toLocaleString() : '-';
        const model = log.model || '-';
        const inputTokens = log.input_tokens || log.tokens_input || 0;
        const outputTokens = log.output_tokens || log.tokens_output || 0;
        const cost = log.cost || 0;
        
        return `
            <tr>
                <td class="timestamp">${timestamp}</td>
                <td class="model-cell">${escapeHtml(model)}</td>
                <td class="tokens-cell">${formatNumber(inputTokens)}</td>
                <td class="tokens-cell">${formatNumber(outputTokens)}</td>
                <td class="cost-cell">$${cost.toFixed(4)}</td>
            </tr>
        `;
    }).join('');
}

// ==================== ANNOUNCEMENTS ====================
// **Validates: Requirements 5.1, 5.2, 5.3**

async function loadAnnouncements() {
    try {
        const response = await fetch('/api/announcements');
        if (!response.ok) {
            throw new Error('Failed to load announcements');
        }
        
        const data = await response.json();
        const announcements = data.announcements || [];
        
        const sectionEl = document.getElementById('announcementsSection');
        const listEl = document.getElementById('announcementsList');
        
        if (!sectionEl || !listEl) return;
        
        if (announcements.length === 0) {
            sectionEl.style.display = 'none';
            return;
        }
        
        sectionEl.style.display = 'block';
        listEl.innerHTML = announcements.map(ann => renderAnnouncement(ann)).join('');
        
    } catch (error) {
        console.error('Error loading announcements:', error);
    }
}

/**
 * Simple markdown parser for announcement content
 * Supports: **bold**, *italic*, `code`, [links](url), and line breaks
 */
function parseMarkdown(text) {
    if (!text) return '';
    
    // First escape HTML to prevent XSS
    let html = escapeHtml(text);
    
    // Parse markdown patterns
    // Bold: **text** or __text__
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/__(.+?)__/g, '<strong>$1</strong>');
    
    // Italic: *text* or _text_
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
    html = html.replace(/_(.+?)_/g, '<em>$1</em>');
    
    // Strikethrough: ~~text~~
    html = html.replace(/~~(.+?)~~/g, '<del>$1</del>');
    
    // Inline code: `code`
    html = html.replace(/`([^`]+)`/g, '<code class="inline-code">$1</code>');
    
    // Links: [text](url)
    html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer" class="md-link">$1</a>');
    
    // Line breaks
    html = html.replace(/\n/g, '<br>');
    
    return html;
}

function renderAnnouncement(announcement) {
    const type = announcement.type || 'info';
    const title = escapeHtml(announcement.title || 'Announcement');
    const content = parseMarkdown(announcement.content || '');
    const createdAt = announcement.created_at 
        ? new Date(announcement.created_at).toLocaleDateString() 
        : '';
    
    let pollHtml = '';
    if (type === 'poll' && announcement.poll_options) {
        pollHtml = renderPollOptions(announcement);
    }
    
    // Render media if present
    let mediaHtml = '';
    if (announcement.media_url) {
        mediaHtml = renderMedia(announcement.media_url, announcement.media_type);
    }
    
    // Render reactions
    let reactionsHtml = renderReactions(announcement);
    
    return `
        <div class="announcement-item ${type}" data-id="${announcement.id}">
            <div class="announcement-header">
                <span class="announcement-title">${getTypeIcon(type)} ${title}</span>
                <span class="announcement-date">${createdAt}</span>
            </div>
            <div class="announcement-content">${content}</div>
            ${mediaHtml}
            ${pollHtml}
            ${reactionsHtml}
        </div>
    `;
}

function getTypeIcon(type) {
    switch (type) {
        case 'warning': return '⚠️';
        case 'error': return '🚨';
        case 'poll': return '📊';
        default: return '📢';
    }
}

function renderPollOptions(announcement) {
    const options = announcement.poll_options || [];
    const results = announcement.vote_results || {};
    const userVote = announcement.user_vote;
    
    // Calculate total votes
    const totalVotes = Object.values(results).reduce((sum, count) => sum + count, 0);
    
    const optionsHtml = options.map(option => {
        const count = results[option] || 0;
        const percentage = totalVotes > 0 ? (count / totalVotes) * 100 : 0;
        const isSelected = userVote === option;
        
        return `
            <div class="poll-option ${isSelected ? 'selected' : ''}" 
                 onclick="voteOnPoll(${announcement.id}, '${escapeHtml(option)}')"
                 data-option="${escapeHtml(option)}">
                <span class="poll-option-text">${escapeHtml(option)}</span>
                <div class="poll-option-bar">
                    <div class="poll-option-fill" style="width: ${percentage}%"></div>
                </div>
                <span class="poll-option-count">${count} (${percentage.toFixed(0)}%)</span>
            </div>
        `;
    }).join('');
    
    return `<div class="poll-options">${optionsHtml}</div>`;
}

async function voteOnPoll(announcementId, option) {
    try {
        const response = await fetch(`/api/announcements/${announcementId}/vote`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ option })
        });
        
        if (response.status === 401) {
            alert('Please log in to vote');
            return;
        }
        
        if (!response.ok) {
            const error = await response.json();
            alert(error.detail || 'Failed to vote');
            return;
        }
        
        // Reload announcements to show updated results
        await loadAnnouncements();
        
    } catch (error) {
        console.error('Error voting:', error);
        alert('Failed to submit vote');
    }
}

// Make voteOnPoll available globally
window.voteOnPoll = voteOnPoll;

// ==================== MEDIA RENDERING ====================

function renderMedia(mediaUrl, mediaType) {
    if (!mediaUrl) return '';
    
    if (mediaType === 'audio') {
        return `
            <div class="announcement-media audio-media">
                <audio controls preload="metadata">
                    <source src="${escapeHtml(mediaUrl)}" type="audio/mpeg">
                    Your browser does not support the audio element.
                </audio>
            </div>
        `;
    } else {
        // Default to image
        return `
            <div class="announcement-media image-media">
                <img src="${escapeHtml(mediaUrl)}" alt="Announcement media" 
                     onclick="openImageModal('${escapeHtml(mediaUrl)}')"
                     onerror="this.style.display='none'">
            </div>
        `;
    }
}

function openImageModal(imageUrl) {
    // Create a simple modal to view the image full size
    const modal = document.createElement('div');
    modal.className = 'image-modal';
    modal.innerHTML = `
        <div class="image-modal-backdrop" onclick="this.parentElement.remove()"></div>
        <div class="image-modal-content">
            <img src="${escapeHtml(imageUrl)}" alt="Full size image">
            <button class="image-modal-close" onclick="this.parentElement.parentElement.remove()">×</button>
        </div>
    `;
    document.body.appendChild(modal);
}

// Make openImageModal available globally
window.openImageModal = openImageModal;

// ==================== REACTIONS ====================

// Common emoji reactions
const REACTION_EMOJIS = ['👍', '❤️', '😂', '😮', '😢', '🔥', '🎉', '👏'];

function renderReactions(announcement) {
    const reactions = announcement.reactions || {};
    const userReactions = announcement.user_reactions || [];
    const announcementId = announcement.id;
    
    // Build reaction buttons HTML
    let reactionsHtml = '';
    
    // Show existing reactions with counts
    for (const [emoji, count] of Object.entries(reactions)) {
        if (count > 0) {
            const isUserReaction = userReactions.includes(emoji);
            reactionsHtml += `
                <button class="reaction-btn ${isUserReaction ? 'active' : ''}" 
                        onclick="toggleReaction(${announcementId}, '${emoji}')"
                        title="${count} reaction${count !== 1 ? 's' : ''}">
                    <span class="reaction-emoji">${emoji}</span>
                    <span class="reaction-count">${count}</span>
                </button>
            `;
        }
    }
    
    // Add emoji picker button
    reactionsHtml += `
        <button class="reaction-btn add-reaction-btn" 
                onclick="showEmojiPicker(event, ${announcementId})"
                title="Add reaction">
            <span class="reaction-emoji">➕</span>
        </button>
    `;
    
    return `<div class="reactions-container" data-announcement-id="${announcementId}">${reactionsHtml}</div>`;
}

function showEmojiPicker(event, announcementId) {
    event.stopPropagation();
    
    // Remove any existing picker
    const existingPicker = document.querySelector('.emoji-picker');
    if (existingPicker) {
        existingPicker.remove();
    }
    
    // Create emoji picker
    const picker = document.createElement('div');
    picker.className = 'emoji-picker';
    picker.innerHTML = REACTION_EMOJIS.map(emoji => 
        `<button class="emoji-option" onclick="selectEmoji(event, ${announcementId}, '${emoji}')">${emoji}</button>`
    ).join('');
    
    // Position near the button
    const btn = event.target.closest('.add-reaction-btn');
    const rect = btn.getBoundingClientRect();
    picker.style.position = 'fixed';
    picker.style.top = `${rect.bottom + 5}px`;
    picker.style.left = `${rect.left}px`;
    
    document.body.appendChild(picker);
    
    // Close picker when clicking outside
    setTimeout(() => {
        document.addEventListener('click', function closePicker(e) {
            if (!picker.contains(e.target)) {
                picker.remove();
                document.removeEventListener('click', closePicker);
            }
        });
    }, 0);
}

async function selectEmoji(event, announcementId, emoji) {
    event.stopPropagation();
    
    // Remove picker
    const picker = document.querySelector('.emoji-picker');
    if (picker) picker.remove();
    
    // Add the reaction
    await toggleReaction(announcementId, emoji, true);
}

async function toggleReaction(announcementId, emoji, forceAdd = false) {
    try {
        // Check if user already has this reaction
        const container = document.querySelector(`.reactions-container[data-announcement-id="${announcementId}"]`);
        const existingBtn = container?.querySelector(`.reaction-btn.active[onclick*="'${emoji}'"]`);
        
        const method = (existingBtn && !forceAdd) ? 'DELETE' : 'POST';
        
        const response = await fetch(`/api/announcements/${announcementId}/react`, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ emoji })
        });
        
        if (response.status === 401) {
            alert('Please log in to react');
            return;
        }
        
        if (!response.ok) {
            const error = await response.json();
            console.error('Reaction error:', error.detail);
            return;
        }
        
        // Reload announcements to show updated reactions
        await loadAnnouncements();
        
    } catch (error) {
        console.error('Error toggling reaction:', error);
    }
}

// Make functions available globally
window.showEmojiPicker = showEmojiPicker;
window.selectEmoji = selectEmoji;
window.toggleReaction = toggleReaction;

// ==================== UI HELPERS ====================

function showNoKeySection() {
    const noKeySection = document.getElementById('noKeySection');
    const dashboardGrid = document.querySelector('.dashboard-grid');
    const modelsSection = document.querySelector('.models-list')?.closest('section');
    const logsSection = document.querySelector('.logs-table')?.closest('section');
    
    if (noKeySection) noKeySection.style.display = 'block';
    if (dashboardGrid) dashboardGrid.style.display = 'none';
    if (modelsSection) modelsSection.style.display = 'none';
    if (logsSection) logsSection.style.display = 'none';
}

function hideNoKeySection() {
    const noKeySection = document.getElementById('noKeySection');
    const dashboardGrid = document.querySelector('.dashboard-grid');
    const modelsSection = document.querySelector('.models-list')?.closest('section');
    const logsSection = document.querySelector('.logs-table')?.closest('section');
    
    if (noKeySection) noKeySection.style.display = 'none';
    if (dashboardGrid) dashboardGrid.style.display = 'grid';
    if (modelsSection) modelsSection.style.display = 'block';
    if (logsSection) logsSection.style.display = 'block';
}

function showBannedMessage() {
    const container = document.querySelector('.main-content');
    if (container) {
        container.innerHTML = `
            <section class="glass-card" style="text-align: center; padding: 48px;">
                <div style="font-size: 4rem; margin-bottom: 16px;">🚫</div>
                <h2 style="margin-bottom: 8px;">Account Banned</h2>
                <p style="color: var(--text-secondary);">Your account has been banned from using this service.</p>
            </section>
        `;
    }
}

function hideLoading() {
    const overlay = document.getElementById('loadingOverlay');
    if (overlay) {
        overlay.classList.add('hidden');
        setTimeout(() => overlay.remove(), 300);
    }
}

function showError(message) {
    const container = document.querySelector('.main-content');
    if (container) {
        container.innerHTML = `
            <section class="glass-card" style="text-align: center; padding: 48px;">
                <div style="font-size: 4rem; margin-bottom: 16px;">❌</div>
                <h2 style="margin-bottom: 8px;">Error</h2>
                <p style="color: var(--text-secondary);">${escapeHtml(message)}</p>
                <button class="btn-primary" onclick="location.reload()" style="margin-top: 16px;">
                    Retry
                </button>
            </section>
        `;
    }
}

// ==================== UTILITY FUNCTIONS ====================

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}
