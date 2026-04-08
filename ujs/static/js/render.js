/**
 * Shared rendering pipeline for court chat responses.
 * Used by both chat.html (browser) and tests/test_render.js (Node).
 *
 * Exports (via global or module.exports):
 *   esc2, renderTableBlock, renderMdTable, renderMd
 */

function esc2(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function renderTableBlock(json) {
  try {
    const t = JSON.parse(json);
    const th = t.headers.map(h =>
      `<th class="text-left text-court-dim font-medium px-2 py-1.5 border-b border-navy-border bg-navy/50 text-[12px] whitespace-nowrap">${esc2(h)}</th>`
    ).join('');
    const rows = t.rows.map(r => '<tr>' + r.map(c =>
      `<td class="text-left text-court-text px-2 py-1 border-b border-navy-border/50 text-[12px]">${esc2(String(c))}</td>`
    ).join('') + '</tr>').join('');
    const title = t.title ? `<div class="text-white font-semibold text-[13px] mb-2">${esc2(t.title)}</div>` : '';
    return `<div class="my-3">${title}<div class="overflow-x-auto"><table class="w-full border-collapse"><thead><tr>${th}</tr></thead><tbody>${rows}</tbody></table></div></div>`;
  } catch(e) { return `<div class="text-red-400 text-[12px]">Table error: ${esc2(e.message)}</div>`; }
}

function renderMdTable(block) {
  const rows = block.trim().split('\n').filter(r => r.includes('|'));
  if (rows.length < 2) return block;
  const hasSep = /^\|?\s*[-:\s|]+$/.test(rows[1]);
  const dataRows = hasSep ? [rows[0], ...rows.slice(2)] : rows;
  const parse = r => { let s=r.trim(); if(s[0]==='|')s=s.slice(1); if(s.slice(-1)==='|')s=s.slice(0,-1); return s.split('|').map(c=>c.trim()); };
  let out = '<div class="overflow-x-auto my-3"><table class="w-full text-[12px] border-collapse">';
  dataRows.forEach((r,i) => {
    const cells = parse(r);
    const tag = (i===0 && hasSep) ? 'th' : 'td';
    const cls = tag==='th' ? 'text-left text-court-dim font-medium px-2 py-1.5 border-b border-navy-border bg-navy/50' : 'text-left text-court-text px-2 py-1 border-b border-navy-border/50';
    out += '<tr>' + cells.map(c=>`<${tag} class="${cls}">${esc2(c)}</${tag}>`).join('') + '</tr>';
  });
  return out + '</table></div>';
}

function renderMd(text) {
  // Step 1: Extract fenced blocks into placeholders
  const blocks = [];
  let html = text.replace(/```(\w+)\s*\n([\s\S]*?)```/g, function(_, type, content) {
    const idx = blocks.length;
    blocks.push({type, content: content.trim()});
    return `\u2603BLOCK${idx}\u2603`;
  });

  // Step 2: Escape HTML entities FIRST — prevents XSS from response content
  html = html.replace(/&/g, '&amp;');
  html = html.replace(/</g, '&lt;');
  html = html.replace(/>/g, '&gt;');
  html = html.replace(/\n{3,}/g, '\n\n');
  html = html.replace(/^---$/gm, '<hr class="border-navy-border my-3">');
  html = html.replace(/((?:^\|.+$\n?){2,})/gm, m => renderMdTable(m));
  html = html.replace(/^### (.+)$/gm, '<h4 class="text-white font-semibold text-[14px] mt-4 mb-1">$1</h4>');
  html = html.replace(/^## (.+)$/gm, '<h3 class="text-white font-semibold text-[15px] mt-5 mb-1.5 pb-1 border-b border-navy-border">$1</h3>');
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/\*{1,2}Source:\s*(.+?)\*{0,2}$/gm, '<div class="text-gold text-[12px] mt-2">Source: $1</div>');
  html = html.replace(/\*Searching for news coverage\.\.\.\*/g,
    '<div class="text-court-dim text-[13px] mt-3 flex items-center gap-2 animate-pulse">' +
    '<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>' +
    'Searching for news coverage...</div>');
  html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
  html = html.replace(/^- (.+)$/gm, '<div class="pl-4 py-[1px] text-[14px] before:content-[\'·\'] before:text-gold before:mr-2">$1</div>');
  html = html.replace(/^\d+\. (.+)$/gm, '<div class="pl-4 py-[1px] text-[14px]">$1</div>');
  html = html.replace(/\n\n/g, '</p><p class="mt-2">');
  html = html.replace(/\n/g, '<br>');
  html = '<p>' + html + '</p>';
  html = html.replace(/<p class="mt-2"><\/p>/g, '');
  html = html.replace(/<p><\/p>/g, '');

  // Step 3: Replace placeholders with rendered blocks
  html = html.replace(/\u2603BLOCK(\d+)\u2603/g, function(_, idx) {
    const b = blocks[parseInt(idx)];
    if (b.type === 'table') return renderTableBlock(b.content);
    if (b.type === 'chart') return `<div data-chart='${esc2(b.content)}' class="my-3 py-2 text-[11px] text-court-dim">Loading chart...</div>`;
    return `<pre class="bg-navy/50 rounded p-2 text-[12px] my-2 overflow-x-auto"><code>${esc2(b.content)}</code></pre>`;
  });

  return html;
}

// Export for Node.js tests
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { esc2, renderTableBlock, renderMdTable, renderMd };
}
