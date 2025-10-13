let isLoading = false;

function showMessage(message, type) {
    const msgEl = document.getElementById('statusMessage');
    msgEl.textContent = message;
    msgEl.className = `status-message ${type}`;
    msgEl.style.display = 'block';
    setTimeout(() => msgEl.style.display = 'none', 3000);
}

function updateToggleState(allPaused) {
    const toggle = document.getElementById('toggleSwitch');

    if (allPaused) {
        toggle.classList.remove('on');
    } else {
        toggle.classList.add('on');
    }
}

async function loadCurrentState() {
    try {
        const response = await fetch('/dag-toggle/status');
        const data = await response.json();
        if (!data.error) {
            updateToggleState(data.all_paused);
        }
    } catch (error) {
        console.error('Error loading state:', error);
    }
}

async function toggleAllDAGs() {
    if (isLoading) return;

    isLoading = true;
    const toggle = document.getElementById('toggleSwitch');
    toggle.classList.add('loading');

    try {
        const response = await fetch('/dag-toggle/toggle', { method: 'POST' });
        const data = await response.json();
        
        if (data.status === 'success') {
            // Just update the toggle state, no success message
            const newState = data.action === 'paused';
            updateToggleState(newState);
        } else {
            showMessage(`❌ ${data.error}`, 'error');
        }
    } catch (error) {
        showMessage(`❌ Error: ${error.message}`, 'error');
    } finally {
        isLoading = false;
        toggle.classList.remove('loading');
    }
}

// Load initial state when page loads
loadCurrentState();
