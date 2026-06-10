// State Management
let destinations = [];
let bridges = [];
let logs = [];
let eventSource = null;

// DOM Elements
const countQueuesEl = document.getElementById('count-queues');
const countTopicsEl = document.getElementById('count-topics');
const countBridgesEl = document.getElementById('count-bridges');
const countSentEl = document.getElementById('count-sent');
const countBridgedEl = document.getElementById('count-bridged');

const destinationsTableBody = document.getElementById('destinations-table').querySelector('tbody');
const bridgesTableBody = document.getElementById('bridges-table').querySelector('tbody');
const liveLogsContainer = document.getElementById('live-logs-container');

// Forms & Selects
const destForm = document.getElementById('destination-form');
const bridgeForm = document.getElementById('bridge-form');
const publishForm = document.getElementById('publish-form');
const bridgeSourceSelect = document.getElementById('bridge-source');
const bridgeTargetSelect = document.getElementById('bridge-target');
const pubDestTypeSelect = document.getElementById('pub-dest-type');
const pubDestNameSelect = document.getElementById('pub-dest-name');
const readerQueueNameSelect = document.getElementById('reader-queue-name');
const propertiesContainer = document.getElementById('properties-container');

// App Initialization
document.addEventListener('DOMContentLoaded', () => {
    // Load Initial Data
    refreshData();
    setupEventSource();

    // Event Listeners
    destForm.addEventListener('submit', handleCreateDestination);
    bridgeForm.addEventListener('submit', handleCreateBridge);
    publishForm.addEventListener('submit', handlePublishMessage);

    pubDestTypeSelect.addEventListener('change', populatePublishDestinationDropdown);

    // Initial properties row
    addPropertyRow();
});

// Refresh Data from APIs
async function refreshData() {
    await Promise.all([
        fetchDestinations(),
        fetchBridges(),
        fetchHistory(),
        fetchConnectionInfo()
    ]);
    updateDashboardMetrics();
    renderTopology();
}

// Fetch Client Connection Info
async function fetchConnectionInfo() {
    try {
        const res = await fetch('/api/connection-info');
        const info = await res.json();
        
        document.getElementById('conn-provider-url').textContent = info.providerUrl;
        document.getElementById('conn-factory-class').textContent = info.connectionFactoryClass;
        document.getElementById('conn-jndi-class').textContent = info.jndiContextFactory;
        document.getElementById('conn-credentials').textContent = `${info.username} / ${info.password}`;
        
        const tcpStatus = document.getElementById('client-tcp-status');
        if (info.providerUrl.startsWith('tcp')) {
            tcpStatus.textContent = 'TCP Port 61616';
            tcpStatus.style.background = 'rgba(74, 222, 128, 0.1)';
            tcpStatus.style.color = 'var(--success)';
            tcpStatus.style.borderColor = 'rgba(74, 222, 128, 0.2)';
        } else {
            tcpStatus.textContent = 'VM Isolated';
            tcpStatus.style.background = 'rgba(245, 158, 11, 0.1)';
            tcpStatus.style.color = 'var(--warning)';
            tcpStatus.style.borderColor = 'rgba(245, 158, 11, 0.2)';
        }
    } catch (err) {
        console.error('Error fetching connection properties:', err);
    }
}

// Fetch Destinations
async function fetchDestinations() {
    try {
        const res = await fetch('/api/destinations');
        destinations = await res.json();
        renderDestinationsTable();
        populateDestinationDropdowns();
    } catch (err) {
        console.error('Error fetching destinations:', err);
    }
}

// Fetch Bridges
async function fetchBridges() {
    try {
        const res = await fetch('/api/bridges');
        bridges = await res.json();
        renderBridgesTable();
    } catch (err) {
        console.error('Error fetching bridges:', err);
    }
}

// Fetch History Logs
async function fetchHistory() {
    try {
        const res = await fetch('/api/messages/history');
        logs = await res.json();
        renderLogs();
    } catch (err) {
        console.error('Error fetching message history:', err);
    }
}

// Update Dashboard Stats Counters
function updateDashboardMetrics() {
    const queueCount = destinations.filter(d => d.type === 'QUEUE').length;
    const topicCount = destinations.filter(d => d.type === 'TOPIC').length;
    const bridgeCount = bridges.length;

    countQueuesEl.textContent = queueCount;
    countTopicsEl.textContent = topicCount;
    countBridgesEl.textContent = bridgeCount;

    // Count sent and bridged messages from logs
    const publishCount = logs.filter(l => l.actionType === 'PUBLISH').length;
    const forwardCount = logs.filter(l => l.actionType === 'BRIDGE_FORWARD').length;

    countSentEl.textContent = publishCount;
    countBridgedEl.textContent = forwardCount;
}

// Setup EventSource for SSE Real-time Updates
function setupEventSource() {
    if (eventSource) {
        eventSource.close();
    }

    eventSource = new EventSource('/api/events');

    // Handle generic events and update stats
    eventSource.addEventListener('message-published', (e) => {
        const logItem = JSON.parse(e.data);
        addLogEntry(logItem);
        // Flash topic node in topology
        flashTopologyNode('topic', logItem.destination);
        // Increment UI published count
        countSentEl.textContent = parseInt(countSentEl.textContent) + 1;
        
        // Find destination in array and increment locally for immediate update
        const dest = destinations.find(d => d.name === logItem.destination && d.type === 'TOPIC');
        if (dest) {
            dest.messageCount = (dest.messageCount || 0) + 1;
            renderDestinationsTable();
        }
    });

    eventSource.addEventListener('message-bridged', (e) => {
        const logItem = JSON.parse(e.data);
        addLogEntry(logItem);
        
        // Trigger packet flow animation down the bridge
        animateBridgePacket(logItem.destination, logItem.targetDestination);
        
        // Flash target queue node
        setTimeout(() => {
            flashTopologyNode('queue', logItem.targetDestination);
        }, 800); // Trigger when packet reaches destination (animation duration)

        // Increment UI bridged count
        countBridgedEl.textContent = parseInt(countBridgedEl.textContent) + 1;
    });

    eventSource.addEventListener('message-consumed', (e) => {
        const logItem = JSON.parse(e.data);
        addLogEntry(logItem);
        // Flash queue node in topology
        flashTopologyNode('queue', logItem.destination);
        
        // Find destination in array and decrement locally
        const dest = destinations.find(d => d.name === logItem.destination && d.type === 'QUEUE');
        if (dest && dest.messageCount > 0) {
            dest.messageCount--;
            renderDestinationsTable();
        }
    });

    eventSource.addEventListener('stats-updated', (e) => {
        const stat = JSON.parse(e.data);
        const dest = destinations.find(d => d.name === stat.name && d.type === stat.type);
        if (dest) {
            dest.messageCount = stat.messageCount;
            renderDestinationsTable();
        }
    });

    // Handle structural config changes
    const configChangeHandler = () => {
        refreshData();
    };

    eventSource.addEventListener('destination-created', configChangeHandler);
    eventSource.addEventListener('destination-deleted', configChangeHandler);
    eventSource.addEventListener('bridge-created', configChangeHandler);
    eventSource.addEventListener('bridge-deleted', configChangeHandler);
    eventSource.addEventListener('bridge-updated', configChangeHandler);

    eventSource.onerror = (err) => {
        console.error('SSE connection error, reconnecting...', err);
        setTimeout(setupEventSource, 5000);
    };
}

// Render Destinations Table
function renderDestinationsTable() {
    destinationsTableBody.innerHTML = '';
    if (destinations.length === 0) {
        destinationsTableBody.innerHTML = `<tr><td colspan="5" class="text-center placeholder-text">No destinations declared yet.</td></tr>`;
        return;
    }

    destinations.forEach(dest => {
        const row = document.createElement('tr');
        const formattedDate = new Date(dest.createdAt).toLocaleTimeString();
        row.innerHTML = `
            <td><strong>${dest.name}</strong></td>
            <td><span class="badge-type ${dest.type.toLowerCase()}">${dest.type}</span></td>
            <td><code>${dest.messageCount}</code></td>
            <td class="text-secondary">${formattedDate}</td>
            <td class="text-right">
                <button class="btn-delete" onclick="deleteDestination('${dest.type}', '${dest.name}')" title="Delete Destination">🗑️</button>
            </td>
        `;
        destinationsTableBody.appendChild(row);
    });
}

// Render Bridges Table
function renderBridgesTable() {
    bridgesTableBody.innerHTML = '';
    if (bridges.length === 0) {
        bridgesTableBody.innerHTML = `<tr><td colspan="6" class="text-center placeholder-text">No pairing bridges configured.</td></tr>`;
        return;
    }

    bridges.forEach(b => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><code>#${b.id}</code></td>
            <td><span class="badge-type topic">TOPIC</span> <strong>${b.sourceTopic}</strong></td>
            <td><span class="badge-type queue">QUEUE</span> <strong>${b.targetQueue}</strong></td>
            <td><code class="text-primary">${b.selector || '-'}</code></td>
            <td>
                <label class="switch">
                    <input type="checkbox" ${b.enabled ? 'checked' : ''} onchange="toggleBridge(${b.id})">
                    <span class="slider"></span>
                </label>
            </td>
            <td class="text-right">
                <button class="btn-delete" onclick="deleteBridge(${b.id})" title="Delete Bridge">🗑️</button>
            </td>
        `;
        bridgesTableBody.appendChild(row);
    });
}

// Populate dropdown lists for bridges and publisher forms
function populateDestinationDropdowns() {
    // Clean
    bridgeSourceSelect.innerHTML = '<option value="">-- Select Topic --</option>';
    bridgeTargetSelect.innerHTML = '<option value="">-- Select Queue --</option>';
    readerQueueNameSelect.innerHTML = '';

    const topics = destinations.filter(d => d.type === 'TOPIC');
    const queues = destinations.filter(d => d.type === 'QUEUE');

    topics.forEach(t => {
        const opt = document.createElement('option');
        opt.value = t.name;
        opt.textContent = t.name;
        bridgeSourceSelect.appendChild(opt);
    });

    queues.forEach(q => {
        const opt = document.createElement('option');
        opt.value = q.name;
        opt.textContent = q.name;
        bridgeTargetSelect.appendChild(opt.cloneNode(true));
        readerQueueNameSelect.appendChild(opt);
    });

    populatePublishDestinationDropdown();
}

function populatePublishDestinationDropdown() {
    pubDestNameSelect.innerHTML = '';
    const type = pubDestTypeSelect.value;
    const filtered = destinations.filter(d => d.type === type);

    if (filtered.length === 0) {
        pubDestNameSelect.innerHTML = '<option value="">-- No destinations of this type --</option>';
        return;
    }

    filtered.forEach(d => {
        const opt = document.createElement('option');
        opt.value = d.name;
        opt.textContent = d.name;
        pubDestNameSelect.appendChild(opt);
    });
}

// REST Form Submissions
async function handleCreateDestination(e) {
    e.preventDefault();
    const name = document.getElementById('dest-name').value.trim();
    const type = document.getElementById('dest-type').value;

    try {
        const res = await fetch('/api/destinations', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, type })
        });
        const data = await res.json();
        if (res.ok) {
            destForm.reset();
        } else {
            alert(data.error || 'Failed to create destination');
        }
    } catch (err) {
        console.error(err);
    }
}

async function deleteDestination(type, name) {
    if (!confirm(`Are you sure you want to delete ${type.toLowerCase()} '${name}'?`)) return;
    try {
        const res = await fetch(`/api/destinations/${type}/${name}`, { method: 'DELETE' });
        const data = await res.json();
        if (!res.ok) alert(data.error || 'Failed to delete destination');
    } catch (err) {
        console.error(err);
    }
}

async function handleCreateBridge(e) {
    e.preventDefault();
    const sourceTopic = bridgeSourceSelect.value;
    const targetQueue = bridgeTargetSelect.value;
    const selector = document.getElementById('bridge-selector').value.trim();

    try {
        const res = await fetch('/api/bridges', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sourceTopic, targetQueue, selector })
        });
        const data = await res.json();
        if (res.ok) {
            bridgeForm.reset();
        } else {
            alert(data.error || 'Failed to create bridge pairing');
        }
    } catch (err) {
        console.error(err);
    }
}

async function deleteBridge(id) {
    if (!confirm(`Delete bridge pairing #${id}?`)) return;
    try {
        const res = await fetch(`/api/bridges/${id}`, { method: 'DELETE' });
        const data = await res.json();
        if (!res.ok) alert(data.error || 'Failed to delete bridge');
    } catch (err) {
        console.error(err);
    }
}

async function toggleBridge(id) {
    try {
        const res = await fetch(`/api/bridges/${id}/toggle`, { method: 'POST' });
        const data = await res.json();
        if (!res.ok) alert(data.error || 'Failed to toggle bridge state');
    } catch (err) {
        console.error(err);
    }
}

// Dynamic properties UI row management
function addPropertyRow() {
    const row = document.createElement('div');
    row.className = 'prop-row';
    row.innerHTML = `
        <input type="text" class="prop-key" placeholder="Key (e.g. region)" required>
        <select class="prop-type">
            <option value="string">String</option>
            <option value="number">Number</option>
            <option value="boolean">Boolean</option>
        </select>
        <input type="text" class="prop-value" placeholder="Value (e.g. US)" required>
        <button type="button" class="btn-remove-prop" onclick="this.parentElement.remove()">&times;</button>
    `;
    propertiesContainer.appendChild(row);
}

// Publish Message
async function handlePublishMessage(e) {
    e.preventDefault();
    const destination = pubDestNameSelect.value;
    const type = pubDestTypeSelect.value;
    const payload = document.getElementById('pub-payload').value;

    // Collect headers properties
    const headers = {};
    const rows = propertiesContainer.querySelectorAll('.prop-row');
    rows.forEach(row => {
        const key = row.querySelector('.prop-key').value.trim();
        const propType = row.querySelector('.prop-type').value;
        const valStr = row.querySelector('.prop-value').value.trim();

        if (key) {
            if (propType === 'number') {
                headers[key] = Number(valStr);
            } else if (propType === 'boolean') {
                headers[key] = valStr.toLowerCase() === 'true';
            } else {
                headers[key] = valStr;
            }
        }
    });

    try {
        const res = await fetch('/api/messages/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ destination, type, payload, headers })
        });
        const data = await res.json();
        if (res.ok) {
            // Reset fields but keep properties count tidy
            document.getElementById('pub-payload').value = '';
            propertiesContainer.innerHTML = '';
            addPropertyRow();
        } else {
            alert(data.error || 'Failed to publish message');
        }
    } catch (err) {
        console.error(err);
    }
}

// Lab Panel Tab switching
function switchLabTab(tab) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    if (tab === 'publish') {
        document.querySelector('.tab-btn[onclick*="publish"]').classList.add('active');
        document.getElementById('tab-publish').classList.add('active');
    } else {
        document.querySelector('.tab-btn[onclick*="consume"]').classList.add('active');
        document.getElementById('tab-consume').classList.add('active');
    }
}

// Browse Queue Messages (No consumption/acknowledgement)
async function browseQueueMessages() {
    const queueName = readerQueueNameSelect.value;
    if (!queueName) {
        alert('Please select a queue first');
        return;
    }

    const browserContainer = document.getElementById('queue-browser-results');
    const outputMeta = document.getElementById('output-meta');
    browserContainer.innerHTML = '<div class="text-center py-5 placeholder-text">Browsing messages...</div>';
    outputMeta.textContent = `Browsing: ${queueName}`;

    try {
        const res = await fetch(`/api/queues/${queueName}/browse`);
        const messages = await res.json();
        
        browserContainer.innerHTML = '';
        if (messages.length === 0) {
            browserContainer.innerHTML = '<div class="text-center py-5 placeholder-text">Queue is empty.</div>';
            return;
        }

        messages.forEach(msg => {
            const item = document.createElement('div');
            item.className = 'browse-item';
            item.onclick = () => showMessageDetail(msg, queueName);
            
            const timeStr = new Date(msg.timestamp).toLocaleTimeString();
            item.innerHTML = `
                <div class="browse-item-header">
                    <span>ID: ${msg.id.substring(0, 18)}...</span>
                    <span>${timeStr}</span>
                </div>
                <div class="browse-item-body">${escapeHtml(msg.payload)}</div>
            `;
            browserContainer.appendChild(item);
        });
    } catch (err) {
        console.error(err);
        browserContainer.innerHTML = '<div class="text-center py-5 text-danger">Failed to browse queue.</div>';
    }
}

// Consume Message from Queue (Acknowledge / deletes from queue)
async function consumeQueueMessage() {
    const queueName = readerQueueNameSelect.value;
    if (!queueName) {
        alert('Please select a queue first');
        return;
    }

    const browserContainer = document.getElementById('queue-browser-results');
    const outputMeta = document.getElementById('output-meta');
    outputMeta.textContent = `Consuming: ${queueName}`;

    try {
        const res = await fetch(`/api/queues/${queueName}/consume`, { method: 'POST' });
        
        if (res.ok) {
            const msg = await res.json();
            if (msg.message === 'Queue is empty') {
                browserContainer.innerHTML = '<div class="text-center py-5 placeholder-text">Queue is empty. No messages available to consume.</div>';
                return;
            }
            
            // Draw consumed details
            browserContainer.innerHTML = `
                <div class="browse-item" style="border-color: var(--success); cursor: default;">
                    <div class="browse-item-header" style="color: var(--success)">
                        <span>Consumed Message ID: ${msg.id}</span>
                        <span>Just now</span>
                    </div>
                    <div class="browse-item-body" style="white-space: pre-wrap; font-family: var(--font-mono); overflow: auto; max-height: 120px;">
                        ${escapeHtml(msg.payload)}
                    </div>
                    <div class="mt-2" style="font-size: 10px; color: var(--text-secondary)">
                        <strong>Headers:</strong> ${JSON.stringify(msg.headers)}
                    </div>
                </div>
            `;
        } else {
            browserContainer.innerHTML = '<div class="text-center py-5 text-danger">Failed to consume message.</div>';
        }
    } catch (err) {
        console.error(err);
        browserContainer.innerHTML = '<div class="text-center py-5 text-danger">Failed to consume message.</div>';
    }
}

// Render Logs Table
function renderLogs() {
    liveLogsContainer.innerHTML = '';
    if (logs.length === 0) {
        liveLogsContainer.innerHTML = '<div class="placeholder-text text-center py-5">No active logs.</div>';
        return;
    }
    logs.forEach(logItem => {
        appendLogToContainer(logItem, false);
    });
}

function clearLiveLogs() {
    logs = [];
    liveLogsContainer.innerHTML = '<div class="placeholder-text text-center py-5">Logs cleared. Waiting for events...</div>';
    updateDashboardMetrics();
}

function addLogEntry(logItem) {
    // Remove placeholder if present
    const placeholder = liveLogsContainer.querySelector('.placeholder-text');
    if (placeholder) {
        liveLogsContainer.innerHTML = '';
    }

    logs.unshift(logItem);
    if (logs.length > 100) {
        logs.pop();
    }

    appendLogToContainer(logItem, true);
    updateDashboardMetrics();
}

function appendLogToContainer(logItem, atTop) {
    const entry = document.createElement('div');
    const typeClass = logItem.actionType.toLowerCase();
    entry.className = `log-entry ${typeClass}`;

    const dateStr = new Date(logItem.timestamp).toLocaleTimeString();
    
    let description = '';
    if (logItem.actionType === 'PUBLISH') {
        description = `Published to <strong>${logItem.destination}</strong>: "${truncateText(logItem.payload, 45)}"`;
    } else if (logItem.actionType === 'BRIDGE_FORWARD') {
        description = `Forwarded <strong>${logItem.destination}</strong> ➔ <strong>${logItem.targetDestination}</strong>: "${truncateText(logItem.payload, 40)}"`;
    } else if (logItem.actionType === 'CONSUME') {
        description = `Consumed from <strong>${logItem.destination}</strong>: "${truncateText(logItem.payload, 45)}"`;
    }

    entry.innerHTML = `
        <span class="log-time">${dateStr}</span>
        <span class="log-action ${typeClass}">${logItem.actionType.replace('_', ' ')}</span>
        <span class="log-text">${description}</span>
        <button class="log-details-btn" onclick="showLogDetail('${logItem.id}')">Inspect</button>
    `;

    if (atTop) {
        liveLogsContainer.insertBefore(entry, liveLogsContainer.firstChild);
    } else {
        liveLogsContainer.appendChild(entry);
    }
}

// Modals Inspector operations
function showLogDetail(logId) {
    const item = logs.find(l => l.id === logId);
    if (item) {
        showMessageDetail(item, item.destination);
    }
}

function showMessageDetail(msg, destinationName) {
    document.getElementById('detail-msg-id').textContent = msg.id || 'N/A';
    document.getElementById('detail-msg-time').textContent = new Date(msg.timestamp).toLocaleString();
    document.getElementById('detail-msg-dest').textContent = destinationName;

    const headersEl = document.getElementById('detail-msg-headers');
    const payloadEl = document.getElementById('detail-msg-payload');

    headersEl.textContent = JSON.stringify(msg.headers || {}, null, 2);
    
    // Attempt pretty-print if JSON
    try {
        const parsed = JSON.parse(msg.payload);
        payloadEl.textContent = JSON.stringify(parsed, null, 2);
    } catch {
        payloadEl.textContent = msg.payload;
    }

    document.getElementById('message-detail-modal').classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// Render dynamic Topology map in SVG
function renderTopology() {
    const svg = document.getElementById('topology-graph');
    
    // Clear all except defs
    const defs = svg.querySelector('defs');
    svg.innerHTML = '';
    svg.appendChild(defs);

    const topics = destinations.filter(d => d.type === 'TOPIC');
    const queues = destinations.filter(d => d.type === 'QUEUE');

    const width = svg.clientWidth || 800;
    const height = 320;
    svg.setAttribute('height', height);

    // Coordinate math
    const xTopics = 80;
    const xQueues = width - 80;

    const topicNodes = {};
    const queueNodes = {};

    // Draw topics
    topics.forEach((t, index) => {
        const y = topics.length === 1 ? height / 2 : 40 + (index * (height - 80) / (topics.length - 1));
        topicNodes[t.name] = { x: xTopics, y: y };

        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'node-group');
        g.setAttribute('id', `node-topic-${t.name}`);

        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', xTopics);
        circle.setAttribute('cy', y);
        circle.setAttribute('r', 16);
        circle.setAttribute('class', 'node-circle node-topic');
        g.appendChild(circle);

        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', xTopics - 24);
        text.setAttribute('y', y + 4);
        text.setAttribute('class', 'node-text');
        text.setAttribute('text-anchor', 'end');
        text.textContent = t.name;
        g.appendChild(text);

        const badge = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        badge.setAttribute('x', xTopics);
        badge.setAttribute('y', y + 26);
        badge.setAttribute('class', 'node-subtext');
        badge.setAttribute('text-anchor', 'middle');
        badge.textContent = `Pubs: ${t.messageCount || 0}`;
        g.appendChild(badge);

        svg.appendChild(g);
    });

    // Draw queues
    queues.forEach((q, index) => {
        const y = queues.length === 1 ? height / 2 : 40 + (index * (height - 80) / (queues.length - 1));
        queueNodes[q.name] = { x: xQueues, y: y };

        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'node-group');
        g.setAttribute('id', `node-queue-${q.name}`);

        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', xQueues);
        circle.setAttribute('cy', y);
        circle.setAttribute('r', 16);
        circle.setAttribute('class', 'node-circle node-queue');
        g.appendChild(circle);

        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', xQueues + 24);
        text.setAttribute('y', y + 4);
        text.setAttribute('class', 'node-text');
        text.setAttribute('text-anchor', 'start');
        text.textContent = q.name;
        g.appendChild(text);

        const badge = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        badge.setAttribute('x', xQueues);
        badge.setAttribute('y', y + 26);
        badge.setAttribute('class', 'node-subtext');
        badge.setAttribute('text-anchor', 'middle');
        badge.textContent = `Depth: ${q.messageCount || 0}`;
        g.appendChild(badge);

        svg.appendChild(g);
    });

    // Draw lines for bridges
    bridges.forEach(b => {
        const tPos = topicNodes[b.sourceTopic];
        const qPos = queueNodes[b.targetQueue];

        if (tPos && qPos) {
            const bridgeId = `bridge-path-${b.sourceTopic}-${b.targetQueue}`.replace(/\./g, '_');
            
            // Draw smooth bezier curve or straight line.
            // Bezier curve using control points gives a highly organic premium interface
            const dx = (qPos.x - tPos.x) / 2;
            const pathData = `M ${tPos.x + 16} ${tPos.y} C ${tPos.x + 16 + dx} ${tPos.y}, ${qPos.x - 16 - dx} ${qPos.y}, ${qPos.x - 16} ${qPos.y}`;

            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', pathData);
            path.setAttribute('id', bridgeId);
            path.setAttribute('class', b.enabled ? 'bridge-line' : 'bridge-line-inactive');
            svg.insertBefore(path, svg.firstChild); // Draw behind nodes

            // Center descriptor badge for the selector if present
            if (b.selector) {
                const midX = (tPos.x + qPos.x) / 2;
                // Calculate cubic bezier midpoint
                const midY = (tPos.y + qPos.y) / 2;

                const textB = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                textB.setAttribute('x', midX);
                textB.setAttribute('y', midY - 6);
                textB.setAttribute('font-size', '9px');
                textB.setAttribute('text-anchor', 'middle');
                textB.setAttribute('fill', 'var(--primary)');
                textB.setAttribute('font-weight', '500');
                textB.textContent = `Filter: ${truncateText(b.selector, 15)}`;
                svg.appendChild(textB);
            }
        }
    });
}

// Animate packet travelling down a bridge path
function animateBridgePacket(sourceTopic, targetQueue) {
    const svg = document.getElementById('topology-graph');
    const pathId = `bridge-path-${sourceTopic}-${targetQueue}`.replace(/\./g, '_');
    const path = document.getElementById(pathId);

    if (!path) return;

    // Create dot packet
    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    circle.setAttribute('r', '6');
    circle.setAttribute('fill', 'var(--primary)');
    circle.setAttribute('style', 'filter: drop-shadow(0 0 6px var(--primary))');

    // Create path motion animation
    const anim = document.createElementNS('http://www.w3.org/2000/svg', 'animateMotion');
    anim.setAttribute('dur', '0.8s');
    anim.setAttribute('repeatCount', '1');
    anim.setAttribute('fill', 'freeze');

    const mpath = document.createElementNS('http://www.w3.org/2000/svg', 'mpath');
    mpath.setAttributeNS('http://www.w3.org/1999/xlink', 'href', `#${pathId}`);

    anim.appendChild(mpath);
    circle.appendChild(anim);
    svg.appendChild(circle);

    // Remove circle after animation ends
    setTimeout(() => {
        circle.remove();
    }, 850);
}

// Flash SVG Node on messaging action
function flashTopologyNode(type, name) {
    const nodeId = `node-${type}-${name}`;
    const group = document.getElementById(nodeId);
    if (!group) return;

    const circle = group.querySelector('.node-circle');
    if (circle) {
        const flashClass = type === 'topic' ? 'flash-topic' : 'flash-queue';
        circle.classList.add(flashClass);
        setTimeout(() => {
            circle.classList.remove(flashClass);
        }, 600);
    }
}

// Window resizing adjustments for SVG topology redraw
window.addEventListener('resize', () => {
    renderTopology();
});

// Helper utilities
function truncateText(text, max) {
    if (!text) return '';
    return text.length > max ? text.substring(0, max) + '...' : text;
}

function escapeHtml(string) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return string.replace(/[&<>"']/g, function(m) { return map[m]; });
}
