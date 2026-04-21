/**
 * Coder's Bible — Frontend Application
 * Handles UI interactions, API calls, and result rendering.
 */

const API_BASE = 'http://127.0.0.1:5090/api';

// ─── DOM References ──────────────────────────────────────────
const els = {
    input: document.getElementById('snippet-input'),
    btnAnalyze: document.getElementById('btn-analyze'),
    btnClear: document.getElementById('btn-clear'),
    charCount: document.getElementById('char-count'),
    statsCount: document.getElementById('stats-count'),
    loading: document.getElementById('loading-indicator'),
    results: document.getElementById('results-section'),
    hero: document.getElementById('hero-section'),
    // Result cards
    langBadge: document.getElementById('lang-badge'),
    langName: document.getElementById('lang-name'),
    langConfidence: document.getElementById('lang-confidence'),
    langReasoning: document.getElementById('lang-reasoning'),
    inputPreview: document.getElementById('input-preview'),
    alternatives: document.getElementById('alternatives'),
    breakdownCard: document.getElementById('breakdown-card'),
    breakdownBody: document.getElementById('breakdown-body'),
    safetyIcon: document.getElementById('safety-icon'),
    safetyBody: document.getElementById('safety-body'),
    keywordsBody: document.getElementById('keywords-body'),
    bibleResults: document.getElementById('bible-results'),
    resultCount: document.getElementById('result-count'),
    domainsGrid: document.getElementById('domains-grid'),
};

// ─── State ───────────────────────────────────────────────────
let isAnalyzing = false;

// ─── Init ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    setupListeners();
});

// ─── Event Listeners ─────────────────────────────────────────
function setupListeners() {
    // Input changes
    els.input.addEventListener('input', () => {
        const len = els.input.value.length;
        els.charCount.textContent = `${len.toLocaleString()} / 10,000`;
        els.btnAnalyze.disabled = len === 0;
        els.btnClear.style.display = len > 0 ? 'block' : 'none';
    });

    // Analyze button
    els.btnAnalyze.addEventListener('click', () => analyze());

    // Clear button
    els.btnClear.addEventListener('click', () => {
        els.input.value = '';
        els.input.dispatchEvent(new Event('input'));
        els.results.style.display = 'none';
        els.input.focus();
    });

    // Keyboard shortcut: Ctrl+Enter to analyze
    els.input.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            if (!els.btnAnalyze.disabled) analyze();
        }
    });

    // Quick example chips
    document.querySelectorAll('.example-chip').forEach(chip => {
        chip.addEventListener('click', () => {
            els.input.value = chip.dataset.snippet;
            els.input.dispatchEvent(new Event('input'));
            analyze();
        });
    });
}

// ─── Analyze ─────────────────────────────────────────────────
async function analyze() {
    const snippet = els.input.value.trim();
    if (!snippet || isAnalyzing) return;

    isAnalyzing = true;
    els.btnAnalyze.disabled = true;
    els.loading.style.display = 'flex';
    els.results.style.display = 'none';

    try {
        const response = await fetch(`${API_BASE}/analyze`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ snippet })
        });

        if (!response.ok) throw new Error(`Server error: ${response.status}`);

        const data = await response.json();
        renderResults(data);
    } catch (err) {
        console.error('Analysis failed:', err);
        showError(err.message);
    } finally {
        isAnalyzing = false;
        els.btnAnalyze.disabled = false;
        els.loading.style.display = 'none';
    }
}

// ─── Render Results ──────────────────────────────────────────
function renderResults(data) {
    // Language Detection
    const lang = data.language;
    els.langName.textContent = lang.language;
    els.langName.style.color = lang.color;
    
    // Use confidence_label if provided, otherwise fallback to percentage
    const confLabel = data.confidence_label ? `${data.confidence_label} (${Math.round(lang.confidence * 100)}%)` : `${Math.round(lang.confidence * 100)}% confidence`;
    els.langConfidence.textContent = confLabel;
    
    els.langBadge.style.borderColor = lang.color + '40';
    els.inputPreview.textContent = data.input;

    // Reasoning
    if (data.reasoning) {
        els.langReasoning.style.display = 'block';
        els.langReasoning.textContent = data.reasoning;
    } else {
        els.langReasoning.style.display = 'none';
    }

    // Alternatives
    if (lang.alternatives && lang.alternatives.length > 0) {
        els.alternatives.innerHTML = lang.alternatives.map(alt =>
            `<span class="alt-badge">${alt.language} <span style="opacity:0.5">${Math.round(alt.confidence * 100)}%</span></span>`
        ).join('');
    } else {
        els.alternatives.innerHTML = '<span class="alt-badge">No alternatives detected</span>';
    }

    // Safety
    renderSafety(data.safety);

    // Keywords
    if (data.keywords && data.keywords.length > 0) {
        els.keywordsBody.innerHTML = data.keywords.map(kw =>
            `<span class="keyword-tag">${escapeHtml(kw)}</span>`
        ).join('');
    } else {
        els.keywordsBody.innerHTML = '<span style="color:var(--text-muted);font-size:12px;">No keywords extracted</span>';
    }

    // Breakdown / Decomposition
    if (data.breakdown && data.breakdown.length > 0) {
        els.breakdownCard.style.display = 'block';
        els.breakdownBody.innerHTML = data.breakdown.map(item => `
            <div style="background: rgba(255,255,255,0.02); padding: 8px 12px; border-radius: 4px; border-left: 2px solid var(--accent-blue);">
                <div style="font-weight: 500; font-size: 13px; color: var(--text-main); margin-bottom: 2px;">${escapeHtml(item.concept)}</div>
                <div style="font-size: 12px; color: var(--text-muted);">${escapeHtml(item.description)}</div>
            </div>
        `).join('');
    } else {
        els.breakdownCard.style.display = 'none';
    }

    // Bible Results
    renderBibleResults(data.results, data.result_count);

    // Show results
    els.results.style.display = 'flex';
    els.results.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// ─── Render Safety ───────────────────────────────────────────
function renderSafety(safety) {
    let html = '';

    if (safety.level === 'danger') {
        els.safetyIcon.textContent = '⚠️';
        html += `<div class="safety-status danger">⚠️ Potentially dangerous operation detected</div>`;
        if (safety.warnings.length > 0) {
            html += '<div class="safety-warnings">';
            safety.warnings.forEach(w => {
                html += `<div class="warning-item">⚡ ${escapeHtml(w.message)}</div>`;
            });
            html += '</div>';
        }
    } else if (safety.level === 'safe') {
        els.safetyIcon.textContent = '✅';
        html += `<div class="safety-status safe">✅ ${escapeHtml(safety.safe_notes[0] || 'Safe operation')}</div>`;
    } else {
        els.safetyIcon.textContent = '🛡️';
        html += `<div class="safety-status neutral">No specific safety concerns identified</div>`;
    }

    els.safetyBody.innerHTML = html;
}

// ─── Render Bible Results ────────────────────────────────────
function renderBibleResults(results, total) {
    els.resultCount.textContent = `${total || results.length} matches`;

    if (!results || results.length === 0) {
        els.bibleResults.innerHTML = `
            <div style="padding: 24px; text-align: center; color: var(--text-muted); font-size: 13px;">
                No matching fragments found in the Bible.
            </div>`;
        return;
    }

    els.bibleResults.innerHTML = results.map(r => `
        <div class="bible-item">
            <div class="bible-item-header">
                <span class="bible-source" title="${escapeHtml(r.source)}">${escapeHtml(r.source)}</span>
                <span class="bible-tier tier-${r.tier}">Tier ${r.tier}</span>
                <span class="bible-relevance">${r.relevance.toFixed(2)}</span>
            </div>
            <div class="bible-content">${escapeHtml(r.content)}</div>
        </div>
    `).join('');
}

// ─── Load Stats ──────────────────────────────────────────────
async function loadStats() {
    try {
        const response = await fetch(`${API_BASE}/stats`);
        if (!response.ok) return;

        const data = await response.json();
        els.statsCount.textContent = data.total_fragments?.toLocaleString() || '—';

        // Render domain cards
        if (data.domains && data.domains.length > 0) {
            els.domainsGrid.innerHTML = data.domains.map(d => `
                <div class="domain-card" data-domain="${escapeHtml(d.name)}" onclick="searchDomain('${escapeHtml(d.name)}')">
                    <div class="domain-dot" style="background:${d.color}; box-shadow: 0 0 6px ${d.color}40;"></div>
                    <span class="domain-name">${escapeHtml(d.name)}</span>
                    <span class="domain-count">${d.count.toLocaleString()}</span>
                </div>
            `).join('');
        }
    } catch (err) {
        els.statsCount.textContent = 'offline';
        console.warn('Could not load stats:', err);
    }
}

// ─── Domain Click Search ─────────────────────────────────────
function searchDomain(domain) {
    els.input.value = `# Show me ${domain} examples`;
    els.input.dispatchEvent(new Event('input'));
    analyze();
}

// ─── Error Display ───────────────────────────────────────────
function showError(message) {
    els.results.style.display = 'flex';
    els.results.innerHTML = `
        <div class="result-card" style="border-color: var(--danger);">
            <div class="card-header">
                <div class="card-icon">❌</div>
                <h3>Error</h3>
            </div>
            <div style="padding: 20px; color: var(--danger); font-size: 13px;">
                ${escapeHtml(message)}
                <br><br>
                <span style="color: var(--text-muted);">Make sure the backend server is running on port 5090.</span>
            </div>
        </div>`;
}

// ─── Utility ─────────────────────────────────────────────────
function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
