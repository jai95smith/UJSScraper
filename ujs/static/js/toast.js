/**
 * Shared toast notification system.
 * Usage: showToast('Settings saved') or showToast('Error', 'error')
 */
(function() {
  let container;

  function getContainer() {
    if (!container) {
      container = document.createElement('div');
      container.id = 'toast-container';
      container.style.cssText = 'position:fixed;bottom:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px;pointer-events:none';
      document.body.appendChild(container);
    }
    return container;
  }

  window.showToast = function(message, type) {
    type = type || 'success';
    const toast = document.createElement('div');
    const colors = {
      success: 'border-color:rgba(200,160,58,0.4);color:#c8a03a',
      error: 'border-color:rgba(239,68,68,0.4);color:#f87171',
      info: 'border-color:rgba(90,122,160,0.4);color:#8fa8c8'
    };
    const icons = {
      success: '<svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>',
      error: '<svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>',
      info: '<svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>'
    };
    toast.style.cssText = 'pointer-events:auto;background:#0f1e38;border:1px solid;border-radius:8px;padding:10px 16px;font-size:13px;display:flex;align-items:center;gap:8px;box-shadow:0 4px 12px rgba(0,0,0,0.3);opacity:0;transform:translateY(8px);transition:opacity 0.2s,transform 0.2s;' + (colors[type] || colors.info);
    toast.innerHTML = (icons[type] || icons.info) + '<span>' + message + '</span>';
    getContainer().appendChild(toast);
    requestAnimationFrame(() => { toast.style.opacity = '1'; toast.style.transform = 'translateY(0)'; });
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(8px)';
      setTimeout(() => toast.remove(), 200);
    }, 2000);
  };
})();
