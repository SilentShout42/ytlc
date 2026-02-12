// YouTube Live Chat Stream Analyzer - Frontend JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // DOM Elements
    const searchForm = document.getElementById('search-form');
    const clearBtn = document.getElementById('clear-btn');
    const closePlotBtn = document.getElementById('close-plot-btn');
    const resultsPanel = document.getElementById('results-panel');
    const resultsContainer = document.getElementById('results-container');
    const resultCount = document.getElementById('result-count');
    const plotPanel = document.getElementById('plot-panel');
    const plotContainer = document.getElementById('plot-container');
    const plotTitle = document.getElementById('plot-title');
    const loading = document.getElementById('loading');
    const errorMessage = document.getElementById('error-message');

    // Stats elements
    const statVideos = document.getElementById('stat-videos');
    const statMessages = document.getElementById('stat-messages');
    const statDates = document.getElementById('stat-dates');

    // Load initial statistics
    loadStats();

    // Event Listeners
    searchForm.addEventListener('submit', handleSearch);
    clearBtn.addEventListener('click', handleClear);
    closePlotBtn.addEventListener('click', closePlot);

    /**
     * Load database statistics
     */
    async function loadStats() {
        try {
            const response = await fetch('/api/stats');
            const data = await response.json();

            if (data.success) {
                statVideos.textContent = formatNumber(data.videos);
                statMessages.textContent = formatNumber(data.messages);

                if (data.earliest_stream && data.latest_stream) {
                    const earliest = new Date(data.earliest_stream).toLocaleDateString();
                    const latest = new Date(data.latest_stream).toLocaleDateString();
                    statDates.textContent = `${earliest} - ${latest}`;
                } else {
                    statDates.textContent = 'No data';
                }
            }
        } catch (error) {
            console.error('Error loading stats:', error);
        }
    }

    /**
     * Handle search form submission
     */
    async function handleSearch(e) {
        e.preventDefault();

        // Get form values
        const formData = new FormData(searchForm);
        const params = new URLSearchParams();

        for (const [key, value] of formData.entries()) {
            if (value.trim()) {
                params.append(key, value);
            }
        }

        // Show loading indicator
        showLoading();
        closePlot();

        try {
            const response = await fetch(`/api/search?${params}`);
            const data = await response.json();

            hideLoading();

            if (data.success) {
                displayResults(data.results, data.count);
            } else {
                showError(data.error || 'An error occurred while searching');
            }
        } catch (error) {
            hideLoading();
            showError('Failed to connect to the server');
            console.error('Search error:', error);
        }
    }

    /**
     * Display search results
     */
    function displayResults(results, count) {
        if (count === 0) {
            resultsContainer.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">No streams found matching your criteria.</p>';
            resultsPanel.style.display = 'block';
            resultCount.textContent = '(0 results)';
            return;
        }

        resultsContainer.innerHTML = '';
        resultCount.textContent = `(${count} result${count !== 1 ? 's' : ''})`;

        results.forEach(result => {
            const resultItem = createResultItem(result);
            resultsContainer.appendChild(resultItem);
        });

        resultsPanel.style.display = 'block';

        // Scroll to results
        resultsPanel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }

    /**
     * Create a result item element
     */
    function createResultItem(result) {
        const div = document.createElement('div');
        div.className = 'result-item';

        const title = document.createElement('div');
        title.className = 'result-title';
        title.textContent = result.title || 'Untitled Stream';

        const meta = document.createElement('div');
        meta.className = 'result-meta';

        const dateSpan = document.createElement('span');
        dateSpan.innerHTML = `📅 ${formatDate(result.release_timestamp)}`;

        const channelSpan = document.createElement('span');
        channelSpan.innerHTML = `📺 ${result.channel_name || 'Unknown Channel'}`;

        const durationSpan = document.createElement('span');
        durationSpan.innerHTML = `⏱️ ${formatDuration(result.duration)}`;

        const liveSpan = document.createElement('span');
        liveSpan.innerHTML = result.was_live ? '🔴 Was Live' : '📹 VOD';

        meta.appendChild(dateSpan);
        meta.appendChild(channelSpan);
        meta.appendChild(durationSpan);
        meta.appendChild(liveSpan);

        div.appendChild(title);
        div.appendChild(meta);

        // Add click handler to load plot
        div.addEventListener('click', () => loadPlot(result.video_id));

        return div;
    }

    /**
     * Load and display plot for a video
     */
    async function loadPlot(videoId) {
        showLoading();

        const windowSize = document.getElementById('window-size').value || 5;

        try {
            const response = await fetch(`/api/plot/${videoId}?window_size=${windowSize}`);
            const data = await response.json();

            hideLoading();

            if (data.success) {
                displayPlot(data);
            } else {
                showError(data.error || 'Failed to generate plot');
            }
        } catch (error) {
            hideLoading();
            showError('Failed to load plot');
            console.error('Plot error:', error);
        }
    }

    /**
     * Display the plot
     */
    function displayPlot(data) {
        plotTitle.textContent = data.title;
        plotContainer.innerHTML = data.div;

        // Execute the Bokeh script
        const scriptTag = document.createElement('script');
        scriptTag.textContent = data.script;
        document.body.appendChild(scriptTag);

        plotPanel.style.display = 'block';
        plotPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    /**
     * Close the plot panel
     */
    function closePlot() {
        plotPanel.style.display = 'none';
        plotContainer.innerHTML = '';
    }

    /**
     * Clear the search form
     */
    function handleClear() {
        searchForm.reset();
        resultsPanel.style.display = 'none';
        closePlot();
        resultsContainer.innerHTML = '';
    }

    /**
     * Show loading indicator
     */
    function showLoading() {
        loading.style.display = 'flex';
    }

    /**
     * Hide loading indicator
     */
    function hideLoading() {
        loading.style.display = 'none';
    }

    /**
     * Show error message
     */
    function showError(message) {
        errorMessage.textContent = message;
        errorMessage.style.display = 'block';

        setTimeout(() => {
            errorMessage.style.display = 'none';
        }, 5000);
    }

    /**
     * Format a number with commas
     */
    function formatNumber(num) {
        if (num === undefined || num === null) return '-';
        return num.toLocaleString();
    }

    /**
     * Format a date string
     */
    function formatDate(dateString) {
        if (!dateString) return 'Unknown Date';

        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    /**
     * Format duration string
     */
    function formatDuration(duration) {
        if (!duration) return 'Unknown';

        // Parse duration string like "3:45:30" or "PT3H45M30S"
        const match = duration.match(/(\d+):(\d+):(\d+)/);
        if (match) {
            const hours = parseInt(match[1]);
            const minutes = parseInt(match[2]);
            const seconds = parseInt(match[3]);

            if (hours > 0) {
                return `${hours}h ${minutes}m`;
            } else if (minutes > 0) {
                return `${minutes}m ${seconds}s`;
            } else {
                return `${seconds}s`;
            }
        }

        return duration;
    }
});
