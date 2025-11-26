// API Base URL (Assuming Flask is running locally on port 5000)
const API_BASE_URL = "http://127.0.0.1:5000";

// --- ZOOM/PAN STATE ---
// Load saved scale from localStorage, default to 1.0 if none found
let currentScale = parseFloat(localStorage.getItem('currentScale')) || 1.0; 
const ZOOM_STEP = 0.15;
const MIN_SCALE = 0.1;
const MAX_SCALE = 3.0;

const vizWrapper = document.getElementById('tree-visualization');
const contentWrapper = document.getElementById('tree-content-wrapper');

// Global state for the zoom bar
let isZoomBarVisible = false;
const zoomBarContainer = document.getElementById('zoom-bar-container');
const zoomToggleButton = document.getElementById('zoom-toggle-button');

// Global state for the filter panel
let isFilterPanelVisible = false;
const filterPanelContainer = document.getElementById('filter-panel-container');
const filterToggleButton = document.getElementById('filter-toggle-button');
// --- END ZOOM/PAN STATE ---


// State management
let retryCount = 0;
const MAX_RETRIES = 5;
let nodeMap = {}; 
let parentMap = {};
let nodeStats = {}; // Cache for IN/OUT counts
let stableRootId = null; // Stores the permanently stable root ID

// Global state for focusing on a node after creation/update
let nodeToFocusId = null; 

// GLOBAL SET: Tracks nodes already rendered to prevent duplication/misplacement
let renderedNodes = new Set();
// Show only a single node card (used for search-by-name/ID)
let singleNodeMode = false; 

// Map status values to Tailwind classes for color coding
const STATUS_CLASSES = {
    'Completed': { bg: 'bg-green-200', border: 'border-green-500', text: 'text-green-800', badge: 'bg-green-500 text-white' },
    'Processing': { bg: 'bg-orange-200', border: 'border-orange-500', text: 'text-orange-800', badge: 'bg-orange-500 text-white' },
    'New': { bg: 'bg-gray-100', border: 'border-gray-400', text: 'text-gray-800', badge: 'bg-gray-500 text-white' },
};
const DEFAULT_STATUS = STATUS_CLASSES['New'];

function getStatusClasses(status) {
    return STATUS_CLASSES[status] || DEFAULT_STATUS;
}

// --- Utility Functions ---

function getStableColor(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    const h = hash % 360;
    const s = 65; 
    const l = 85; 
    return `hsl(${h}, ${s}%, ${l}%)`;
}
function getBreadcrumbPath(nodeId) {
    const names = [];
    let currentId = nodeId;
    let guard = 0;

    while (currentId && nodeMap[currentId] && guard < 1000) {
        names.push(nodeMap[currentId].name);
        if (!parentMap[currentId]) break;
        currentId = parentMap[currentId];
        guard++;
    }

    return names.reverse().join(' > ');
}
function buildSubtreeLines(nodeId, prefix = '') {
    const node = nodeMap[nodeId];
    if (!node || !Array.isArray(node.children)) return [];

    const lines = [];
    for (const childId of node.children) {
        const child = nodeMap[childId];
        if (!child) continue;

        // connector from parent downwards
        lines.push(prefix + '|');

        // child line
        lines.push(prefix + '|–– ' + child.name);

        // recurse into grandchildren with extra indent
        const childSub = buildSubtreeLines(childId, prefix + '      ');
        lines.push(...childSub);
    }
    return lines;
}
function formatTreePath(pathString) {
    // pathString is like "Dynamic Services > Domestic Services Pages > ... "
    const parts = pathString.split('>').map(p => p.trim()).filter(Boolean);
    if (parts.length === 0) return '';

    let lines = [];
    // Root line
    lines.push(parts[0]);

    let prefix = '';
    for (let i = 1; i < parts.length; i++) {
        const name = parts[i];
        // Between levels, add the vertical bar line
        lines.push(prefix + '|');
        // Then add the branch segment
        lines.push(prefix + '|–– ' + name);
        // Increase indent for next level
        prefix += '      ';
    }
    return lines.join('\n');
}
// --- Fetch Functions ---
async function fetchWithRetry(endpoint, options = {}) {
    try {
        const response = await fetch(`${API_BASE_URL}${endpoint}`, options);
        
        if (!response.ok) {
            let errorMsg = `HTTP error! Status: ${response.status} for ${endpoint}.`;
            try {
                const errorData = await response.json();
                errorMsg = errorData.error || errorMsg;
            } catch (e) { /* Ignore non-JSON errors */ }
            throw new Error(errorMsg);
        }
        
        retryCount = 0; 
        return await response.json();

    } catch (error) {
        if (retryCount < MAX_RETRIES) {
            retryCount++;
            const delay = Math.pow(2, retryCount) * 100;
            await new Promise(resolve => setTimeout(resolve, delay));
            return fetchWithRetry(endpoint, options);
        } else {
            showMessage(`Fatal Error: ${error.message}. Is your Flask server running? Redirecting to setup...`, 'error');
            setTimeout(() => {
                window.location.href = 'index.html';
            }, 1000);
            throw error;
        }
    }
}

function showMessage(message, type) {
    const statusDiv = document.getElementById('status-message');
    statusDiv.textContent = message;
    statusDiv.classList.remove('hidden', 'bg-green-100', 'text-green-800', 'bg-red-100', 'text-red-800', 'bg-blue-100', 'text-blue-800');

    if (type === 'success') {
        statusDiv.classList.add('bg-green-100', 'text-green-800');
    } else if (type === 'error') {
        statusDiv.classList.add('bg-red-100', 'text-red-800');
    } else {
        statusDiv.classList.add('bg-blue-100', 'text-blue-800');
    }

    setTimeout(() => statusDiv.classList.add('hidden'), 5000);
}

// --- ZOOM/PAN Functions ---
function applyZoom(scale) {
    currentScale = Math.min(Math.max(scale, MIN_SCALE), MAX_SCALE);
    contentWrapper.style.transform = `scale(${currentScale})`;
    localStorage.setItem('currentScale', currentScale.toFixed(2)); // Save the new scale
}

function zoomIn() {
    applyZoom(currentScale + ZOOM_STEP);
}

function zoomOut() {
    applyZoom(currentScale - ZOOM_STEP);
}

function resetZoom() {
    if (!contentWrapper.scrollWidth || !vizWrapper.clientWidth) {
        applyZoom(1.0);
        return;
    }
    
    // Calculate required scale to fit entire content width into the visualization window
    const contentWidth = contentWrapper.scrollWidth;
    const containerWidth = vizWrapper.clientWidth;
    
    // Add a small buffer (50px)
    const scaleFactor = Math.min(1.0, (containerWidth - 50) / contentWidth);
    
    applyZoom(scaleFactor);
    
    // Center/Scroll to the top of the content
    vizWrapper.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
}

function toggleZoomBar() {
    isZoomBarVisible = !isZoomBarVisible;
    
    // Toggle the main container visibility
    if (isZoomBarVisible) {
        zoomBarContainer.classList.remove('hidden', 'translate-x-full');
        zoomBarContainer.classList.add('translate-x-0');
        
        // Change button icon to 'Minimize' (for closing the bar)
        zoomToggleButton.innerHTML = '<svg data-lucide="minimize" width="20" height="20"></svg>';
    } else {
        zoomBarContainer.classList.remove('translate-x-0');
        zoomBarContainer.classList.add('translate-x-full');

        // Hide after transition (Tailwind transition is 300ms)
        setTimeout(() => {
            zoomBarContainer.classList.add('hidden');
        }, 300);
        
        // Change button icon back to 'Maximize' (for opening the bar)
        zoomToggleButton.innerHTML = '<svg data-lucide="maximize" width="20" height="20"></svg>';
    }
    // Re-create lucide icons after changing innerHTML
    window.lucide.createIcons();
}

// NEW FUNCTION: Focus/Center the view on a specific node
function focusNode(nodeId) {
    const targetElement = document.getElementById(`node-${nodeId}`);
    if (!targetElement) {
        console.warn(`Node element with ID node-${nodeId} not found for focusing.`);
        return;
    }
    
    // Add a slightly increased delay to guarantee DOM reflow/element size calculations are complete.
    setTimeout(() => {
        
        // 1. Get the center position of the node relative to the content wrapper
        // Use offsetWidth/Height for stable dimensions before or after render
        const nodeCenterX = (targetElement.offsetLeft + (targetElement.offsetWidth / 2));
        const nodeCenterY = (targetElement.offsetTop + (targetElement.offsetHeight / 2));
        
        // 2. Calculate the target scroll position in the visualization wrapper
        // Adjust for current scale to find the correct scroll position
        const targetScrollLeft = nodeCenterX * currentScale - (vizWrapper.clientWidth / 2);
        const targetScrollTop = nodeCenterY * currentScale - (vizWrapper.clientHeight / 2) + 100; 
        
        // 3. Apply the scroll
        vizWrapper.scrollTo({
            top: targetScrollTop,
            left: targetScrollLeft,
            behavior: 'smooth'
        });

        // Optional: Highlight the node briefly
        targetElement.classList.add('shadow-outline', 'ring-4', 'ring-blue-500', 'ring-opacity-70', 'transition-all', 'duration-500');
        setTimeout(() => {
            targetElement.classList.remove('shadow-outline', 'ring-4', 'ring-blue-500', 'ring-opacity-70', 'transition-all', 'duration-500');
        }, 1500);
    }, 100); // Increased delay
}
// --- END NEW FUNCTION ---


// --- Filter Panel Functions ---

function toggleFilterPanel() {
    isFilterPanelVisible = !isFilterPanelVisible;
    
    if (isFilterPanelVisible) {
        filterPanelContainer.classList.remove('-translate-x-full');
        filterPanelContainer.classList.add('translate-x-0');
    } else {
        filterPanelContainer.classList.remove('translate-x-0');
        filterPanelContainer.classList.add('-translate-x-full');
    }
    window.lucide.createIcons();
}

function isNodeVisible(nodeId) {
    const node = nodeMap[nodeId];
    if (!node) return false;

    // 1. Connection filter
    const connectionFilter = document.getElementById('connection-filter-select').value;
    const stats = nodeStats[nodeId];

    if (stats) {
        if (connectionFilter === 'inbound' && stats.inboundCount === 0) {
            return false;
        }
        if (connectionFilter === 'outbound' && stats.outboundCount === 0) {
            return false;
        }
    }

    // 2. Status filter
    const statusFilterEl = document.getElementById('status-filter-select');
    if (statusFilterEl) {
        const statusFilter = statusFilterEl.value; // 'all', 'New', 'Processing', 'Completed'
        if (statusFilter !== 'all' && node.status !== statusFilter) {
            return false;
        }
    }

    return true;
}
function getFirstUrl(text) {
    if (!text) return null;
    const urlRegex = /(https?:\/\/[^\s]+)/;
    const match = text.match(urlRegex);
    return match ? match[1] : null;
}
function linkifyDescription(text) {
    if (!text) return "";
    const urlRegex = /(https?:\/\/[^\s]+)/g;
    return text.replace(urlRegex, (url) => {
        const safeUrl = url.replace(/"/g, "&quot;"); // basic escaping
        return `<a href="${safeUrl}" target="_blank" rel="noopener noreferrer" class="text-blue-600 underline">${url}</a>`;
    });
}
// Main Filter Application Logic (Called by input/select change)
// Main Filter Application Logic (Called by input/select change)
// Main Filter Application Logic (Called by input/select change)
// Main Filter Application Logic (Called by input/select change)
async function applyFilters() {
    const nameInput = document.getElementById('search-filter-input');
    const idInput = document.getElementById('search-id-input');
    const connectionFilter = document.getElementById('connection-filter-select').value;
    const vizWrapper = document.getElementById('tree-content-wrapper');

    const nameQ = nameInput ? nameInput.value.trim().toLowerCase() : '';
    const idQ = idInput ? idInput.value.trim().toLowerCase() : '';

    // --- 1. Handle search by Name/Description + Friendly ID (top-left number) ---
    if (nameQ.length >= 2 || idQ.length >= 1) {
        let foundNodeId = null;

        for (const id in nodeMap) {
            const node = nodeMap[id];

            // Name/description match (if nameQ provided)
            let matchesName = true;
            if (nameQ.length >= 2) {
                matchesName =
                    (node.name || '').toLowerCase().includes(nameQ) ||
                    (node.description || '').toLowerCase().includes(nameQ);
            }

            // ID match (if idQ provided) – ONLY friendlyId like "01", "02"
            let matchesId = true;
            if (idQ.length >= 1) {
                matchesId = (node.friendlyId || '').toLowerCase().includes(idQ);
            }

            if (matchesName && matchesId) {
                foundNodeId = id;
                break;
            }
        }

        if (foundNodeId) {
            singleNodeMode = true;                    // show only this node
            loadAndRenderVisuals(foundNodeId);        // render from that node
            const n = nodeMap[foundNodeId];
            showMessage(
                `Displaying only: ${n.name} (ID ${n.friendlyId || ''})`,
                'info'
            );
            return;
        } else {
            singleNodeMode = false;
            vizWrapper.innerHTML = '<p class="text-center text-gray-500 italic p-10">No node found matching your search.</p>';
            return;
        }
    }

    // --- 2. No search text: apply connection/status filters on full tree ---

    // For IN/OUT filters, ensure stats are loaded
    if (connectionFilter === 'inbound' || connectionFilter === 'outbound') {
        await fetchAllStats();
    }

    const rootId = stableRootId || Object.keys(nodeMap)[0] || null;
    if (!rootId) {
        vizWrapper.innerHTML = '<p class="text-center text-gray-500 italic p-10">No nodes to display.</p>';
        return;
    }

    // Reset single-node mode so children show normally
    singleNodeMode = false;

    // Render full tree from root; visibility controlled only by connection/status in isNodeVisible
    loadAndRenderVisuals(rootId);
}
// NEW: Function to cache all node stats needed for connection filtering
async function fetchAllStats() {
    if (Object.keys(nodeStats).length === Object.keys(nodeMap).length && Object.keys(nodeMap).length > 0) {
        return; 
    }
    
    const statPromises = Object.keys(nodeMap).map(async nodeId => {
        try {
            const inboundData = await fetchWithRetry(`/inbound_stats/${encodeURIComponent(nodeId)}`);
            const outboundData = await fetchWithRetry(`/outbound_stats/${encodeURIComponent(nodeId)}`);
            
            nodeStats[nodeId] = {
                inboundCount: inboundData.total_inbound_count,
                outboundCount: outboundData.total_outbound_count
            };
        } catch (e) {
            nodeStats[nodeId] = { inboundCount: 0, outboundCount: 0 };
            console.warn(`Failed to fetch stats for node ${nodeId}`);
        }
    });
    await Promise.all(statPromises);
}


// --- Node Control Functions ---
async function deleteNode(contentId, name) {
    closeDeleteConfirmModal();
    try {
        await fetchWithRetry(`/node/delete/${encodeURIComponent(contentId)}`, { method: 'DELETE' }); 
        showMessage(`Node '${name}' deleted successfully.`, 'success');
        loadAndRenderTree(); // Must perform full reload after delete
    } catch (error) {
        showMessage(`Failed to delete node: ${error.message}`, 'error');
    }
}

async function handleEditSubmit(e) {
    e.preventDefault();
    const contentId = document.getElementById('edit-content-id').value;
    const newName = document.getElementById('edit-name').value.trim();
    const newDescription = document.getElementById('edit-description').value.trim();
    const newStatus = document.getElementById('edit-status').value; 
    closeEditModal(); 
    try {
        const options = {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newName, description: newDescription, status: newStatus })
        };
        await fetchWithRetry(`/node/update/${encodeURIComponent(contentId)}`, options);
        
        showMessage(`Node updated to '${newName}' (Status: ${newStatus}).`, 'success');
        
        // Update local cache immediately
        nodeMap[contentId].name = newName;
        nodeMap[contentId].description = newDescription;
        nodeMap[contentId].status = newStatus;

        nodeToFocusId = contentId;
        loadAndRenderVisuals(stableRootId); 

    } catch (error) {
        showMessage(`Failed to update node: ${error.message}`, 'error');
        loadAndRenderTree(); // Fallback to full reload on failure
    }
}

// --- Modal Control Functions ---

// Handles link deletion from the Info Modal
async function deleteRelationFromModal(parentId, childId, parentName, childName) {
    if (!confirm(`Are you sure you want to delete the link:\n\n${parentName} → ${childName}?\n\nThis will remove the static relationship and all click statistics associated with this link.`)) {
        return;
    }

    closeInfoModal(); 
    showMessage(`Deleting link: ${parentName} → ${childName}...`, 'error');

    try {
        const options = {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ parentId: parentId, childId: childId })
        };
        
        await fetchWithRetry('/relation/delete', options);
        
        showMessage(`Successfully deleted link: ${parentName} → ${childName}. Reloading tree...`, 'success');
        
        // Set focus on the node whose detail panel was open (parentId)
        nodeToFocusId = parentId; 
        // *** CRITICAL FIX: Must perform full reload after deleting a relation to re-calculate stable hierarchy. ***
        loadAndRenderTree(); 
    } catch (error) {
        showMessage(`Failed to delete link: ${error.message}`, 'error');
        loadAndRenderTree();
    }
}

async function openInfoModal(nodeId) { 
    const node = nodeMap[nodeId]; 
    if (!node) return;
    const statusInfo = getStatusClasses(node.status);
    
    // Basic details
    document.getElementById('info-node-name').textContent = node.name;
    document.getElementById('info-node-id').textContent = node.contentId;

    const infoDescriptionEl = document.getElementById('info-description');
    const rawDesc = node.description || '';

    // Show ONLY the link (if any) above, not full description
    const firstUrl = getFirstUrl(rawDesc);
    if (firstUrl) {
        const safeUrl = firstUrl.replace(/"/g, '&quot;');
        infoDescriptionEl.innerHTML =
            `<a href="${safeUrl}" target="_blank" rel="noopener noreferrer" class="text-blue-600 underline">${firstUrl}</a>`;
    } else {
        // No URL: leave this line empty
        infoDescriptionEl.innerHTML = '';
    }

    // Full description in gray box (single place)
    const fullDescEl = document.getElementById('info-node-description');
    fullDescEl.textContent = rawDesc || 'No description provided.';

    const statusSpan = document.getElementById('info-node-status');
    statusSpan.textContent = node.status;
    statusSpan.className = `px-2 py-0.5 rounded text-xs font-medium ${statusInfo.badge}`;

    // Tree-style path including children
    const rawPath = getBreadcrumbPath(nodeId);          // "root > ... > current node"
    const baseTree = formatTreePath(rawPath);           // multi-line tree for that path

    const depth = rawPath.split('>').map(p => p.trim()).filter(Boolean).length;
    const childPrefix = '      '.repeat(depth);
    const subtreeLines = buildSubtreeLines(nodeId, childPrefix);

    let finalText = baseTree;
    if (subtreeLines.length > 0) {
        finalText += '\n' + subtreeLines.join('\n');
    }

    const pathEl = document.getElementById('info-path');
    if (pathEl) {
        pathEl.textContent = finalText;
    }

    document.getElementById('info-modal').style.display = 'flex';
}
function closeInfoModal() {
    document.getElementById('info-modal').style.display = 'none';
}
async function openInboundSection(nodeId) {
    await openInfoModal(nodeId);
    const inboundSection = document.getElementById('inbound-details-display');
    if (inboundSection) {
        inboundSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}

async function openOutboundSection(nodeId) {
    await openInfoModal(nodeId);
    const outboundSection = document.getElementById('outbound-details-display');
    if (outboundSection) {
        outboundSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
}
function openEditModal(nodeId) { 
    const node = nodeMap[nodeId]; 
    if (!node) return;
    document.getElementById('edit-node-name-old-display').textContent = node.name;
    document.getElementById('edit-content-id').value = node.contentId;
    document.getElementById('edit-name').value = node.name;
    document.getElementById('edit-description').value = node.description;
    document.getElementById('edit-status').value = node.status; 
    document.getElementById('edit-modal').style.display = 'flex';
}
function closeEditModal() {
    document.getElementById('edit-modal').style.display = 'none';
    document.getElementById('edit-node-form').reset();
}
function openDeleteConfirmModal(nodeId) { 
    const node = nodeMap[nodeId]; 
    if (!node) return;
    document.getElementById('delete-node-name').textContent = node.name;
    const confirmBtn = document.getElementById('confirm-delete-button');
    confirmBtn.onclick = () => deleteNode(node.contentId, node.name);
    document.getElementById('delete-confirm-modal').style.display = 'flex';
}
function closeDeleteConfirmModal() {
    document.getElementById('delete-confirm-modal').style.display = 'none';
}
function updateTotalNodeCount() {
    const el = document.getElementById('total-node-count');
    if (!el) return;
    const count = Object.keys(nodeMap || {}).length;
    el.textContent = `Total nodes: ${count}`;
}
function openChildModal(parentId, parentName) {
    document.getElementById('parent-name-display').textContent = parentName;
    document.getElementById('modal-parent-id').value = parentId;
    document.getElementById('child-modal').style.display = 'flex';
}
function closeChildModal() {
    document.getElementById('child-modal').style.display = 'none';
    document.getElementById('create-child-form').reset();
}
function openSearchLinkModal(parentId, parentName) {
    document.getElementById('link-parent-name-display').textContent = parentName;
    document.getElementById('link-modal-parent-id').value = parentId;
    document.getElementById('search-results-list').innerHTML = '';
    document.getElementById('search-input').value = '';
    document.getElementById('search-status-message').textContent = 'Start searching to find nodes to link.';
    document.getElementById('confirm-link-button').disabled = true;
    document.getElementById('search-link-modal').style.display = 'flex';
}
async function openInboundDetails(nodeId) {
    const node = nodeMap[nodeId];
    if (!node) return;

    try {
        const inboundData = await fetchWithRetry(`/inbound_stats/${encodeURIComponent(nodeId)}`);

        const overlay = document.createElement('div');
        overlay.className = 'fixed inset-0 bg-black bg-opacity-40 flex items-center justify-center z-50';
        overlay.id = 'inbound-popup-overlay';

        const contentHtml = `
            <div class="bg-white rounded-xl shadow-2xl max-w-lg w-full max-h-[80vh] overflow-y-auto p-4 border border-gray-200">
                <div class="flex items-center justify-between mb-2">
                    <h2 class="text-lg font-semibold text-gray-800">
                        Inbound Links – ${node.name}
                    </h2>
                    <button class="text-gray-500 hover:text-gray-700 text-sm px-2 py-1 rounded"
                            onclick="document.getElementById('inbound-popup-overlay')?.remove()">
                        ✕
                    </button>
                </div>
                <p class="text-sm text-gray-700 mb-2">
                    Total inbound clicks:
                    <span class="font-bold">${inboundData.total_inbound_count}</span>
                </p>
                <h3 class="text-sm font-semibold text-gray-700 mb-2">Inbound Node List:</h3>
                ${
                    inboundData.inbound_connections.length === 0
                        ? '<p class="text-xs text-gray-500 italic mb-2">No inbound connections recorded.</p>'
                        : inboundData.inbound_connections.map(conn => {
                            const source = nodeMap[conn.sourceId] || {};
                            const desc = linkifyDescription(source.description || 'No description.');

                            return `
                                <div class="mb-2 p-2 rounded-lg border border-gray-200 bg-gray-50">
                                    <p class="text-sm font-medium text-gray-800">
                                        ${source.name || 'Unknown'} (${conn.count} clicks)
                                    </p>
                                    <p class="text-xs text-gray-600">
                                        Status: ${source.status || 'N/A'} |
                                        ID: ${(source.contentId || '').substring(0,8)}...
                                    </p>
                                    <p class="text-xs text-gray-600 mt-0.5">${desc}</p>
                                </div>
                            `;
                        }).join('')
                }
            </div>
        `;

        overlay.innerHTML = contentHtml;
        document.body.appendChild(overlay);
    } catch (e) {
        alert('Failed to load inbound details: ' + e.message);
    }
}

async function openOutboundDetails(nodeId) {
    const node = nodeMap[nodeId];
    if (!node) return;

    try {
        const outboundData = await fetchWithRetry(`/outbound_stats/${encodeURIComponent(nodeId)}`);

        const overlay = document.createElement('div');
        overlay.className = 'fixed inset-0 bg-black bg-opacity-40 flex items-center justify-center z-50';
        overlay.id = 'outbound-popup-overlay';

        const contentHtml = `
            <div class="bg-white rounded-xl shadow-2xl max-w-lg w-full max-h-[80vh] overflow-y-auto p-4 border border-gray-200">
                <div class="flex items-center justify-between mb-2">
                    <h2 class="text-lg font-semibold text-gray-800">
                        Outbound Links – ${node.name}
                    </h2>
                    <button class="text-gray-500 hover:text-gray-700 text-sm px-2 py-1 rounded"
                            onclick="document.getElementById('outbound-popup-overlay')?.remove()">
                        ✕
                    </button>
                </div>
                <p class="text-sm text-gray-700 mb-2">
                    Total outbound clicks:
                    <span class="font-bold">${outboundData.total_outbound_count}</span>
                </p>
                <h3 class="text-sm font-semibold text-gray-700 mb-2">Out-bound Node List:</h3>
                ${
                    outboundData.outbound_connections.length === 0
                        ? '<p class="text-xs text-gray-500 italic mb-2">No outbound connections recorded.</p>'
                        : outboundData.outbound_connections.map(conn => {
                            const target = nodeMap[conn.targetId] || {};
                            const desc = linkifyDescription(target.description || 'No description.');

                            return `
                                <div class="mb-2 p-2 rounded-lg border border-gray-200 bg-gray-50">
                                    <p class="text-sm font-medium text-gray-800">
                                        ${target.name || 'Unknown'} (${conn.count} clicks)
                                    </p>
                                    <p class="text-xs text-gray-600">
                                        Status: ${target.status || 'N/A'} |
                                        ID: ${(target.contentId || '').substring(0,8)}...
                                    </p>
                                    <p class="text-xs text-gray-600 mt-0.5">${desc}</p>
                                </div>
                            `;
                        }).join('')
                }
            </div>
        `;

        overlay.innerHTML = contentHtml;
        document.body.appendChild(overlay);
    } catch (e) {
        alert('Failed to load outbound details: ' + e.message);
    }
}
function closeSearchLinkModal() {
    document.getElementById('search-link-modal').style.display = 'none';
}
async function handleSearch() {
    const parentId = document.getElementById('link-modal-parent-id').value;
    const searchTerm = document.getElementById('search-input').value.trim();
    const resultsList = document.getElementById('search-results-list');
    const statusMsg = document.getElementById('search-status-message');

    resultsList.innerHTML = '';

    if (searchTerm.length < 3) {
        statusMsg.textContent = 'Please enter at least 3 characters to search.';
        document.getElementById('confirm-link-button').disabled = true;
        return;
    }

    statusMsg.textContent = 'Searching...';

    try {
        const safeSearchTerm = searchTerm.replace(/\s/g, '_');

        // Use generic search – includes existing children
        const endpoint = `/node/search/${encodeURIComponent(safeSearchTerm)}`;

        const results = await fetchWithRetry(endpoint);
        statusMsg.textContent = '';
        document.getElementById('confirm-link-button').disabled = false;

        results.forEach(node => {
            // Skip linking the node to itself
            if (node.contentId === parentId) {
                return;
            }

            nodeMap[node.contentId] = node;
            const breadcrumb = getBreadcrumbPath(node.contentId);

            const listItem = document.createElement('li');
            listItem.className = 'flex items-start p-2 bg-white rounded-lg shadow-sm border border-gray-100';
            listItem.innerHTML = `
                <input type="checkbox" id="link-node-${node.contentId}" name="link-node" value="${node.contentId}" 
                        class="mt-1 mr-3 h-4 w-4 text-green-600 border-gray-300 rounded focus:ring-green-500">
                <label for="link-node-${node.contentId}" class="flex-1 cursor-pointer">
                    <span class="font-semibold text-sm text-gray-800">${node.name}</span> 
                    <span class="text-xs text-gray-500">(${node.status})</span><br>
                    <span class="text-xs text-gray-600 truncate block">${node.description || 'No description.'}</span>
                    <span class="text-[10px] text-gray-400 truncate block mt-0.5">${breadcrumb}</span>
                </label>
            `;
            resultsList.appendChild(listItem);
        });
    } catch (error) {
        statusMsg.textContent = error.message.includes('404') 
            ? `No matching, unrelated nodes found for "${searchTerm}".` 
            : `Search failed: ${error.message}`;
        document.getElementById('confirm-link-button').disabled = true;
    }
}

async function handleLinkSelected() {
    const parentId = document.getElementById('link-modal-parent-id').value;
    const parentName = nodeMap[parentId].name;
    const checkboxes = document.querySelectorAll('#search-results-list input[name="link-node"]:checked');
    
    if (checkboxes.length === 0) {
        showMessage('No nodes selected for linking.', 'error');
        return;
    }
    
    closeSearchLinkModal(); 
    let successCount = 0;
    
    // Collect IDs of nodes whose stats need updating
    const nodesToUpdate = new Set([parentId]);
    
    for (const checkbox of checkboxes) {
    const childId = checkbox.value;
    const childName = nodeMap[childId] ? nodeMap[childId].name : 'Unknown Node';

    try {
        showMessage(`1/2: Creating static link ${parentName} → ${childName}...`, 'info');

        // Step 1: Create or confirm the Static Relationship (idempotent)
        const relationOptions = {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ parentId: parentId, childId: childId })
        };

        try {
            await fetchWithRetry('/relation/create', relationOptions);
        } catch (e) {
            // If relationship already exists, ignore and continue to record click
            if (!e.message.includes('Relationship exists')) {
                throw e;  // real error
            }
        }

        // Step 2: Record a "Click" (updates IN/OUT counters even for existing links)
        showMessage(`2/2: Recording initial click to update IN/OUT counters...`, 'info');
        const clickOptions = {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sourceId: parentId, targetId: childId })
        };
        await fetchWithRetry('/link/click', clickOptions);

        nodesToUpdate.add(childId);
        successCount++;
    } catch (error) {
        showMessage(`Failed to link ${childName}: ${error.message}`, 'error');
    }
}
    
    if (successCount > 0) {
        showMessage(`Successfully linked ${successCount} node(s) to ${parentName}. Updating view...`, 'success');
        
        // Re-render the visuals to update stat badges/data and maintain zoom/position
        nodeToFocusId = parentId;
        // *** CRITICAL FIX: Use loadAndRenderTree for full hierarchy refresh ***
        loadAndRenderTree(); 

    } else {
        showMessage('No new links were successfully created.', 'error');
    }
}

// NEW FUNCTION: Applies colors to the IN/OUT counts based on value (0 or >0)
function applyStatColors(nodeId) {
    const inboundSpan = document.getElementById(`inbound-stat-${nodeId}`);
    const outboundSpan = document.getElementById(`outbound-stat-${nodeId}`);
    
    if (inboundSpan) {
        if (parseInt(inboundSpan.textContent) > 0) {
            inboundSpan.classList.add('inbound-active');
        } else {
            inboundSpan.classList.add('inbound-inactive');
        }
    }
    
    if (outboundSpan) {
        if (parseInt(outboundSpan.textContent) > 0) {
            outboundSpan.classList.add('outbound-active');
        } else {
            outboundSpan.classList.add('outbound-inactive');
        }
    }
}


// --- NEW FUNCTION: Fetch and Update Click Stats for a single node with retries ---
async function updateNodeStats(nodeId) {
    const statsDiv = document.getElementById(`stats-${nodeId}`);
    if (!statsDiv) return;
    
    // Set loading state immediately
    statsDiv.innerHTML = '<span class="text-gray-400 text-[8px] italic">Loading stats...</span>';
    
    let inboundCount = 0;
    let outboundCount = 0;
    const MAX_STAT_RETRIES = 3; 

    // --- Fetch Stats with Retries ---
    for (let i = 0; i < MAX_STAT_RETRIES; i++) {
        try {
            // Attempt to fetch both inbound and outbound data
            const inboundData = await fetchWithRetry(`/inbound_stats/${encodeURIComponent(nodeId)}`);
            const outboundData = await fetchWithRetry(`/outbound_stats/${encodeURIComponent(nodeId)}`);

            inboundCount = inboundData.total_inbound_count;
            outboundCount = outboundData.total_outbound_count;
            
            // Cache stats for filtering logic
            nodeStats[nodeId] = { inboundCount, outboundCount }; 
            
            // If data is successfully fetched, break the loop
            break; 

        } catch (error) {
            // If it's the last attempt and it failed, log the error
            if (i === MAX_STAT_RETRIES - 1) {
                console.error(`Failed to load stats for ${nodeId} after ${MAX_STAT_RETRIES} attempts.`, error);
                statsDiv.innerHTML = '<span class="text-red-500 text-[8px]">Stats Error</span>';
                return;
            }
            // Wait briefly before retrying
            await new Promise(resolve => setTimeout(resolve, 500)); 
        }
    }
    
    // --- Final Display ---
    // MODIFIED: Use new class structure for styling.
        statsDiv.innerHTML = `
            <div class="flex justify-around text-xs font-bold pt-1 border-t border-gray-300 mt-1">
                <button type="button"
                        class="text-gray-700 focus:outline-none"
                        onclick="openInboundDetails('${nodeId}')">
                    IN:
                    <span id="inbound-stat-${nodeId}" class="inbound-stat ml-1">${inboundCount}</span>
                </button>
                <button type="button"
                        class="text-gray-700 focus:outline-none"
                        onclick="openOutboundDetails('${nodeId}')">
                    OUT:
                    <span id="outbound-stat-${nodeId}" class="outbound-stat ml-1">${outboundCount}</span>
                </button>
            </div>
        `;

    // --- Schedule CSS coloring after DOM update ---
    setTimeout(() => applyStatColors(nodeId), 50);

    // Recalculate horizontal lines after this node’s width may have changed
    setTimeout(updateHorizontalLines, 0);
}


// --- Node Rendering Logic ---

/**
 * Renders a single node card and its children recursively.
 */
// --- Node Rendering Logic ---

function renderNode(nodeId, nodeMap, level = 0) {
    const node = nodeMap[nodeId];

    if (!node) {
        return '';
    }

    // --- CRITICAL FILTER CHECK ---
    if (!isNodeVisible(nodeId)) {
        return '';
    }

    const isAlreadyRendered = renderedNodes.has(nodeId);

    // Stop recursion if already drawn (prevents cross-linked nodes from shifting position)
    if (isAlreadyRendered) {
        return '';
    }

    renderedNodes.add(nodeId);

    const nodeName = node.name;
    const nodeIdStr = node.contentId;
    const friendlyId = node.friendlyId || '';
    const statusClasses = getStatusClasses(node.status);

    // 1. Ensure stable order: Sort children IDs alphabetically for consistent sibling arrangement.
    const sortedChildren = (node.children || []).sort();
    const renderableChildrenIds = sortedChildren;
    const hasChildren = renderableChildrenIds.length > 0;

    // --- Schedule stat update after rendering ---
    setTimeout(() => updateNodeStats(nodeId), 100);

    // --- Children rendering ---
    let childrenHtml = '';
    if (hasChildren) {
        // If we are in "single node mode" and this is the root of the render,
        // do NOT render any children – just the single node card.
        if (!(singleNodeMode && level === 0)) {
            const childNodesHtml = renderableChildrenIds
                .map(childId => renderNode(childId, nodeMap, level + 1))
                .join('');

            if (childNodesHtml.trim() !== '') {
                // Add a class if there is only one child wrapper
                const containerClass =
                    renderableChildrenIds.length === 1 ? ' single-child-container' : '';
                childrenHtml = `<div class="tree-container${containerClass}">${childNodesHtml}</div>`;
            }
        }
    }

    // --- Icon Logic: All icons are black, no background circles ---
    let actionIcons = '';
    const iconStyle = `width="12" height="12" class="text-gray-800" stroke-width="2.5"`;

    actionIcons += `
        <button class="info-btn" onclick="openInfoModal('${nodeIdStr}')" title="View Description/Stats">
            <svg data-lucide="info" ${iconStyle}></svg>
        </button>
    `;

    actionIcons += `
        <button class="edit-btn" onclick="openEditModal('${nodeIdStr}')" title="Edit Node Details">
            <svg data-lucide="pencil" ${iconStyle}></svg>
        </button>
    `;

    actionIcons += `
        <button class="search-btn" onclick="openSearchLinkModal('${nodeIdStr}', '${nodeName}')" title="Search & Link Nodes">
            <svg data-lucide="search" ${iconStyle}></svg>
        </button>
    `;

    // Optional external link icon if description has a URL (BLUE)
    const firstUrl = getFirstUrl(node.description);
    if (firstUrl) {
        const safeUrl = firstUrl.replace(/"/g, '&quot;');
        actionIcons += `
            <button class="link-btn" onclick="window.open('${safeUrl}', '_blank')" title="Open link from description">
                <svg data-lucide="link" width="12" height="12" class="text-blue-600" stroke-width="2.5"></svg>
            </button>
        `;
    }

    // Conditional Delete Icon (Only on Leaf Nodes)
    if ((node.children || []).length === 0) {
        actionIcons += `
            <button class="delete-btn" onclick="openDeleteConfirmModal('${nodeIdStr}')" title="Delete Node">
                <svg data-lucide="trash-2" width="12" height="12" class="text-red-600" stroke-width="2.5"></svg>
            </button>
        `;
    }

    // Add Child Icon
    actionIcons += `
        <button class="add-child-btn" onclick="openChildModal('${nodeIdStr}', '${nodeName}')" title="Add New Child">
            <svg data-lucide="plus" ${iconStyle}></svg>
        </button>
    `;

    // --- Node HTML structure: wrapper carries level-based line color ---
    const wrapperClass = hasChildren ? 'node-wrapper has-children' : 'node-wrapper';
    const levelColor = getLevelColor(level);

    return `
        <div class="${wrapperClass}" style="--line-color: ${levelColor};">
            <div class="node-card ${statusClasses.bg} p-2 rounded-xl border ${statusClasses.border} shadow-lg node-box relative" id="node-${nodeIdStr}">
                <!-- Friendly short ID in top-left -->
                <div class="absolute top-1 left-2 text-[9px] font-semibold text-gray-500">
                    ${friendlyId}
                </div>

                <div class="node-action-bar">
                    ${actionIcons}
                </div>
                <h3 class="text-xs ${statusClasses.text} pl-4 pr-4 whitespace-normal text-center">${nodeName}</h3>
                <p class="text-[7px] text-gray-600 pl-4 pr-4">Status: ${node.status}</p>
                <p class="text-[7px] text-gray-600 pl-4 pr-4">
                    ID: <span class="font-mono">${nodeIdStr.substring(0, 8)}...</span>
                </p>
                
                <div id="stats-${nodeIdStr}" class="p-0.5">
                    <span class="text-gray-400 text-[8px] italic">Loading stats...</span>
                </div>
            </div>
            ${childrenHtml}
        </div>
    `;
}
/**
 * Fetches the entire graph structure and renders the tree starting from the root.
 * This is the function we want to minimize calling, but it's necessary for structural changes.
 */
/**
 * Fetches the entire graph structure and renders the tree starting from the root.
 * This is the function we want to minimize calling, but it's necessary for structural changes.
 */
async function loadAndRenderTree() {
    const vizWrapper = document.getElementById('tree-content-wrapper');
    vizWrapper.innerHTML = '<p class="text-center text-gray-500 italic p-10">Loading tree structure...</p>';
    
    renderedNodes.clear(); 
    
    try {
        const response = await fetchWithRetry('/tree');
        
        if (!response || response.length === 0) {
            vizWrapper.innerHTML = '<p class="text-center text-red-500 italic p-10">No Root Node found. <a href="index.html" class="text-indigo-600 font-semibold hover:underline">Click here to create the root node.</a></p>';
            return;
        }

        // 1. Map all nodes by ID
        nodeMap = {};
        parentMap = {};
        
        response.forEach(node => {
            nodeMap[node.contentId] = { ...node }; 
        });

        // Build parentMap: childId -> parentId
        response.forEach(node => {
            if (Array.isArray(node.children)) {
                node.children.forEach(childId => {
                    if (childId) {
                        parentMap[childId] = node.contentId;
                    }
                });
            }
        });

        // Assign friendly short IDs (01, 02, 03, ...)
        assignFriendlyIds(response);
        updateTotalNodeCount();

        // 2. Identify the Root Node 
        let rootNodeId = null;
        
        if (response.length > 0) {
            rootNodeId = response[0].contentId; 
            stableRootId = rootNodeId; 
        } else {
            return;
        }
        
        const rootName = nodeMap[rootNodeId].name;
        
        // 3. Render visuals (uses loadAndRenderVisuals to handle zoom/focus logic)
        loadAndRenderVisuals(rootNodeId);

        // 4. Initial Scroll/Reset (Only if no focus node was requested by DML actions)
        if (!nodeToFocusId) {
            vizWrapper.scrollTo({ top: 0, left: 0, behavior: 'smooth' });
        }

        showMessage(`Tree loaded successfully, starting from root: ${rootName}.`, 'success');

    } catch (error) {
        vizWrapper.innerHTML = '<p class="text-center text-red-500 italic p-10">Error loading graph. Check backend connection or console for details.</p>';
        console.error("Tree loading failed:", error);
    }
}
function assignFriendlyIds(orderArray = null) {
    // If we have an explicit order (e.g. /tree response), use that.
    // Otherwise, fall back to current nodeMap insertion order.
    let nodesInOrder;

    if (orderArray && Array.isArray(orderArray)) {
        // Map the response array (which is in creation order) back to nodeMap entries
        nodesInOrder = orderArray
            .map(n => nodeMap[n.contentId])
            .filter(Boolean);
    } else {
        // Object.values preserves insertion order in modern JS engines
        nodesInOrder = Object.values(nodeMap || {});
    }

    if (!nodesInOrder.length) return;

    let counter = 1;
    nodesInOrder.forEach(node => {
        node.friendlyId = String(counter).padStart(2, '0'); // 01, 02, 03...
        counter += 1;
    });
}
/**
 * Renders the visualization using current map (no fetch).
 * This is the function called by filter/zoom actions.
 */
function loadAndRenderVisuals(rootOverrideId = null) {
    const vizWrapper = document.getElementById('tree-content-wrapper');
    renderedNodes.clear(); 
    vizWrapper.innerHTML = '<p class="text-center text-gray-500 italic p-10">Rendering tree...</p>';

    let rootNodeId = rootOverrideId || stableRootId || Object.keys(nodeMap)[0];
    if (!rootNodeId || !nodeMap[rootNodeId]) {
         // Re-fetch map if map is empty but we expect a root (edge case for corruption)
         if (stableRootId) loadAndRenderTree(); 
         return;
    }
    
    const treeHtml = renderNode(rootNodeId, nodeMap,0);
    vizWrapper.innerHTML = treeHtml; 
    window.lucide.createIcons();
    
    // 1. Apply saved zoom level (retains user's zoom)
    applyZoom(currentScale); 

    // 2. Apply Focus after rendering
    if (nodeToFocusId) {
        focusNode(nodeToFocusId);
        nodeToFocusId = null; 
    }

    // 3. Explicitly trigger stat loading for all visible nodes
    for (const id in nodeMap) {
        if(document.getElementById(`node-${id}`)) { 
            updateNodeStats(id);
        }
    }
    
    // 4. Recalculate horizontal line widths
    // Use setTimeout to ensure the DOM has settled and sizes are final.
    setTimeout(updateHorizontalLines, 250); 
}

/**
 * Calculates the width for the horizontal line based on center-to-center distance.
 */
// function updateHorizontalLines() {
//     document.querySelectorAll(".tree-container").forEach(container => {
//         const children = container.children;
        
//         // Skip if less than 2 children (single child case is handled by CSS class)
//         if (children.length < 2) return; 

//         // 1. Get the center of the first child's wrapper
//         const firstChild = children[0];
//         const firstCenter = firstChild.offsetLeft + (firstChild.offsetWidth / 2);

//         // 2. Get the center of the last child's wrapper
//         const lastChild = children[children.length - 1];
//         const lastCenter = lastChild.offsetLeft + (lastChild.offsetWidth / 2);

//         // 3. Calculate the line span (center-to-center)
//         const lineSpan = lastCenter - firstCenter;

//         // FIX: Use exactly the center-to-center distance. The CSS translateX will handle the final pixel perfect alignment.
//         container.style.setProperty("--children-width", lineSpan + "px"); 
//     });
// }


// --- Event Listeners (FIXED) ---
function updateHorizontalLines() {
    document.querySelectorAll(".tree-container").forEach(container => {
        const children = container.children;

        // No horizontal line needed if fewer than 2 children
        if (children.length < 2) return;

        const firstChild = children[0];
        const lastChild = children[children.length - 1];

        // Centers within the container’s coordinate system
        const start = firstChild.offsetLeft + (firstChild.offsetWidth / 2);
        const end   = lastChild.offsetLeft  + (lastChild.offsetWidth  / 2);

        container.style.setProperty("--line-start", start + "px");
        container.style.setProperty("--line-end",   end   + "px");
    });
}
function getLevelColor(level) {
    // “Endless” cycle of pleasant HSL colors based on level
    const hue = (level * 57 + 137) % 360;  // spreads hues around the wheel
    const saturation = 55;
    const lightness = 70;
    return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
}

// Use DOMContentLoaded to ensure elements are available for listeners
document.addEventListener('DOMContentLoaded', () => {
    
    // Event listener for adding a new child node
    document.getElementById('create-child-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const parentId = document.getElementById('modal-parent-id').value;
        const parentName = nodeMap[parentId].name;

        const childName = document.getElementById('child-name').value.trim();
        const childDescription = document.getElementById('child-description').value.trim();

        closeChildModal(); 
        
        let childId = null;
        try {
            // 1. Create the Child Node
            showMessage(`1/2: Creating new node '${childName}'...`, 'info');
            const nodeOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: childName, description: childDescription, status: 'New' })
            };
            const childResult = await fetchWithRetry('/node/create', nodeOptions);
            childId = childResult.contentId;
            
            // 2. Create the Relationship
            showMessage(`2/2: Linking '${parentName}' to '${childName}'...`, 'info');
            const relationOptions = {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ parentId: parentId, childId: childId })
            };
            await fetchWithRetry('/relation/create', relationOptions);
            
            showMessage(`Child '${childName}' added and linked successfully.`, 'success');
            
            // --- CRITICAL FIX: Set focus ID and reload to reflect new structure and keep position ---
            if (childId) {
                nodeToFocusId = childId; // Set focus to the new node
            }
            loadAndRenderTree(); 
 // Necessary full refresh to update hierarchy
            
        } catch (error) {
            showMessage(`Failed to create or link child node: ${error.message}.`, 'error');
            loadAndRenderTree(); // Fallback to full refresh on failure
        }
    });

    document.getElementById('edit-node-form').addEventListener('submit', handleEditSubmit);
    document.getElementById('modal-cancel-child').addEventListener('click', closeChildModal);

    document.getElementById('start-search-button').addEventListener('click', handleSearch);
    document.getElementById('confirm-link-button').addEventListener('click', handleLinkSelected);
    
    // Handle search on Enter keypress
    document.getElementById('search-input').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault(); 
            handleSearch();
        }
    });
    
    // Initial load
    loadAndRenderTree();
});

// Expose functions globally for HTML-inline event handlers (like onclick)
window.zoomIn = zoomIn;
window.zoomOut = zoomOut;
window.resetZoom = resetZoom;
window.toggleZoomBar = toggleZoomBar;
window.toggleFilterPanel = toggleFilterPanel;
window.applyFilters = applyFilters;
window.openInfoModal = openInfoModal;
window.closeInfoModal = closeInfoModal;
window.openEditModal = openEditModal;
window.closeEditModal = closeEditModal;
window.openDeleteConfirmModal = openDeleteConfirmModal;
window.closeDeleteConfirmModal = closeDeleteConfirmModal;
window.openChildModal = openChildModal;
window.closeChildModal = closeChildModal;
window.openSearchLinkModal = openSearchLinkModal;
window.closeSearchLinkModal = closeSearchLinkModal;
window.deleteRelationFromModal = deleteRelationFromModal;
window.loadAndRenderVisuals = loadAndRenderVisuals; // Exposed for filter reset
window.stableRootId = stableRootId; // Exposed for filter reset (will be updated after load)
window.openInboundDetails = openInboundDetails;
window.openOutboundDetails = openOutboundDetails;