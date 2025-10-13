// DAG Toggle Widget for Airflow Dashboard
console.log('Loading DAG Toggle Widget...');

function DAGToggleWidget(props) {
    console.log('DAGToggleWidget loaded with props:', props);

    const [allPaused, setAllPaused] = React.useState(false);
    const [loading, setLoading] = React.useState(false);
    const [message, setMessage] = React.useState({ text: '', type: '' });

    // Load current DAG status and move widget to stats section
    React.useEffect(() => {
        // Load DAG status
        fetch('/dag-toggle/status')
            .then(response => response.json())
            .then(data => {
                if (!data.error) {
                    setAllPaused(data.all_paused || false);
                }
            })
            .catch(error => console.error('Error loading DAG status:', error));

        // Move widget to the stats section after a brief delay
        const moveToStatsSection = () => {
            const statsContainer = document.querySelector('.chakra-stack.css-1kydda');
            const widget = document.querySelector('[data-dag-toggle-widget]');

            if (statsContainer && widget && !statsContainer.contains(widget)) {
                console.log('Moving DAG toggle widget to stats section...');
                statsContainer.appendChild(widget);
            }
        };

        // Try multiple times in case the DOM isn't ready
        setTimeout(moveToStatsSection, 100);
        setTimeout(moveToStatsSection, 500);
        setTimeout(moveToStatsSection, 1000);
    }, []);

    const showMessage = (text, type) => {
        setMessage({ text, type });
        setTimeout(() => setMessage({ text: '', type: '' }), 3000);
    };

    const toggleAllDAGs = async () => {
        if (loading) return;

        setLoading(true);
        try {
            const response = await fetch('/dag-toggle/toggle', { method: 'POST' });
            const data = await response.json();

            if (data.status === 'success') {
                // Just update the toggle state, no success message
                setAllPaused(data.action === 'paused');
            } else {
                showMessage(`❌ ${data.error}`, 'error');
            }
        } catch (error) {
            showMessage(`❌ Error: ${error.message}`, 'error');
        } finally {
            setLoading(false);
        }
    };

    return React.createElement('div', {
        'data-dag-toggle-widget': 'true', // Marker for relocation
        style: {
            background: 'linear-gradient(135deg, #EBE9F8 0%, #FFFFFF 100%)',
            border: '1px solid #8887C0',
            borderRadius: '12px',
            padding: '12px 16px', // Reduced height by using smaller padding
            margin: '10px',
            boxShadow: '0 2px 8px rgba(16, 25, 53, 0.08)',
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif',
            textAlign: 'center',
            maxWidth: '280px',
            minWidth: '220px',
            flex: '0 0 auto', // Prevent flexbox stretching
            minHeight: 'auto' // Let content determine height
        }
    }, [

        // Toggle Container
        React.createElement('div', {
            key: 'toggle-container',
            style: {
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: '12px',
                marginBottom: message.text ? '10px' : '5px'
            },
            title: 'Toggle to pause/unpause all DAGs' // Tooltip on hover
        }, [
            React.createElement('button', {
                key: 'toggle-switch',
                onClick: toggleAllDAGs,
                disabled: loading,
                style: {
                    position: 'relative',
                    width: '45px',  // 25% smaller than 60px
                    height: '22px', // 25% smaller than 30px
                    background: allPaused ? '#8887C0' : '#0BCD93',
                    borderRadius: '15px',
                    border: 'none',
                    cursor: loading ? 'not-allowed' : 'pointer',
                    transition: 'all 0.3s ease',
                    opacity: loading ? 0.6 : 1,
                    outline: 'none'
                }
            }, React.createElement('div', {
                style: {
                    position: 'absolute',
                    top: '2px',
                    left: allPaused ? '2px' : '25px', // Adjusted for smaller toggle
                    width: '18px', // 25% smaller than 24px
                    height: '18px',
                    background: '#FFFFFF',
                    borderRadius: '50%',
                    transition: 'all 0.3s ease',
                    boxShadow: '0 2px 4px rgba(0, 0, 0, 0.2)',
                    animation: loading ? 'pulse 1s infinite' : 'none'
                }
            })),

            React.createElement('span', {
                key: 'title-label',
                style: {
                    fontSize: '16px',
                    fontWeight: '600',
                    color: '#101935',
                    letterSpacing: '-0.2px'
                }
            }, 'All DAGs')
        ]),

        // Status Message
        message.text ? React.createElement('div', {
            key: 'message',
            style: {
                padding: '8px 12px',
                borderRadius: '8px',
                fontSize: '12px',
                fontWeight: '500',
                background: message.type === 'success' ? 'rgba(11, 205, 147, 0.1)' : 'rgba(220, 38, 127, 0.1)',
                color: message.type === 'success' ? '#0BCD93' : '#dc267f',
                border: `1px solid ${message.type === 'success' ? 'rgba(11, 205, 147, 0.3)' : 'rgba(220, 38, 127, 0.3)'}`,
                marginBottom: '10px'
            }
        }, message.text) : null

    ]);
}

// Add pulse animation styles
const style = document.createElement('style');
style.textContent = `
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
`;
document.head.appendChild(style);

globalThis['DAG Toggle Widget'] = DAGToggleWidget; // Matching the plugin name
globalThis.AirflowPlugin = DAGToggleWidget; // Fallback that Airflow looks for
