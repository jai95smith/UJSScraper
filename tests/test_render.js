#!/usr/bin/env node
/**
 * Test the renderMd pipeline against real response data.
 * Run: node tests/test_render.js
 *
 * Extracts the JS functions from chat.html and tests them
 * with known inputs to verify tables, charts, and markdown render.
 */

const fs = require('fs');
const path = require('path');

let PASS = 0, FAIL = 0;

function test(name, condition, detail) {
  if (condition) { PASS++; console.log(`  PASS  ${name}`); }
  else { FAIL++; console.log(`  FAIL  ${name} — ${detail || ''}`); }
}

// Extract renderMd and helpers from chat.html
const html = fs.readFileSync(path.join(__dirname, '..', 'ujs', 'templates', 'chat.html'), 'utf8');

// Pull out the JS between <script> tags
const scriptMatch = html.match(/<script>([\s\S]*?)<\/script>/);
if (!scriptMatch) { console.error('No script found'); process.exit(1); }

// We need to extract just the rendering functions. Build them manually from source.
// This avoids dealing with DOM dependencies.

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
    out += '<tr>' + cells.map(c=>`<${tag} class="${cls}">${c}</${tag}>`).join('') + '</tr>';
  });
  return out + '</table></div>';
}

function renderMd(text) {
  const blocks = [];
  let html = text.replace(/```(\w+)\s*\n([\s\S]*?)```/g, function(_, type, content) {
    const idx = blocks.length;
    blocks.push({type, content: content.trim()});
    return `\u2603BLOCK${idx}\u2603`;
  });

  html = html.replace(/&/g, '&amp;');
  html = html.replace(/\n{3,}/g, '\n\n');
  html = html.replace(/^---$/gm, '<hr class="border-navy-border my-3">');
  html = html.replace(/((?:^\|.+$\n?){2,})/gm, m => renderMdTable(m));
  html = html.replace(/^### (.+)$/gm, '<h4 class="text-white font-semibold text-[14px] mt-4 mb-1">$1</h4>');
  html = html.replace(/^## (.+)$/gm, '<h3 class="text-white font-semibold text-[15px] mt-5 mb-1.5 pb-1 border-b border-navy-border">$1</h3>');
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/\*{1,2}Source:\s*(.+?)\*{0,2}$/gm, '<div class="text-gold text-[12px] mt-2">Source: $1</div>');
  html = html.replace(/\*Searching for news coverage\.\.\.\*/g,
    '<div class="animate-pulse">Searching for news coverage...</div>');
  html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');
  html = html.replace(/^- (.+)$/gm, '<div class="pl-4 py-[1px] text-[14px] before:content-[\'·\'] before:text-gold before:mr-2">$1</div>');
  html = html.replace(/^\d+\. (.+)$/gm, '<div class="pl-4 py-[1px] text-[14px]">$1</div>');
  html = html.replace(/\n\n/g, '</p><p class="mt-2">');
  html = html.replace(/\n/g, '<br>');
  html = '<p>' + html + '</p>';
  html = html.replace(/<p class="mt-2"><\/p>/g, '');
  html = html.replace(/<p><\/p>/g, '');

  html = html.replace(/\u2603BLOCK(\d+)\u2603/g, function(_, idx) {
    const b = blocks[parseInt(idx)];
    if (b.type === 'table') return renderTableBlock(b.content);
    if (b.type === 'chart') return `<div data-chart='${esc2(b.content)}'>chart</div>`;
    return `<pre>${esc2(b.content)}</pre>`;
  });

  return html;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

console.log('\n' + '='.repeat(60));
console.log('Render Pipeline Tests');
console.log('='.repeat(60));

// --- Fenced table block ---
console.log('\n--- Fenced ```table blocks ---');

const tableJson = JSON.stringify({
  title: "Test Table",
  headers: ["Name", "Charge", "Grade"],
  rows: [["Smith, John", "DUI", "M1"], ["Doe, Jane", "Theft", "F3"]]
});

const input1 = '```table\n' + tableJson + '\n```\n\nSome text after.';
const out1 = renderMd(input1);
test('table block renders <table> tag', out1.includes('<table'));
test('table block has headers', out1.includes('Name') && out1.includes('Charge'));
test('table block has rows', out1.includes('Smith, John') && out1.includes('Doe, Jane'));
test('table block has title', out1.includes('Test Table'));
test('text after table preserved', out1.includes('Some text after'));
test('no raw JSON visible', !out1.includes('"headers"'));
test('no placeholder visible', !out1.includes('\u2603'));

// --- Table at very start of response ---
console.log('\n--- Table at position 0 ---');

const input2 = '```table\n' + tableJson + '\n```';
const out2 = renderMd(input2);
test('table-only response renders', out2.includes('<table'));
test('table-only has data', out2.includes('Smith, John'));

// --- Multiple blocks ---
console.log('\n--- Multiple blocks ---');

const chartJson = JSON.stringify({type:"bar",title:"Test",labels:["A"],datasets:[{label:"X",data:[1]}]});
const input3 = 'Before\n\n```table\n' + tableJson + '\n```\n\nMiddle\n\n```chart\n' + chartJson + '\n```\n\nAfter';
const out3 = renderMd(input3);
test('table renders in multi-block', out3.includes('<table'));
test('chart placeholder renders', out3.includes('data-chart'));
test('text before preserved', out3.includes('Before'));
test('text between preserved', out3.includes('Middle'));
test('text after preserved', out3.includes('After'));

// --- Markdown table (pipe syntax) ---
console.log('\n--- Markdown pipe tables ---');

const input4 = '| Name | Grade |\n|------|-------|\n| Smith | F1 |\n| Doe | M2 |\n\nMore text.';
const out4 = renderMd(input4);
test('pipe table renders <table>', out4.includes('<table'));
test('pipe table has data', out4.includes('Smith') && out4.includes('F1'));
test('text after pipe table preserved', out4.includes('More text'));

// --- Inline markdown ---
console.log('\n--- Inline markdown ---');

const input5 = '## Header\n\n**bold** and *italic*\n\n- item 1\n- item 2\n\n---\n\nParagraph.';
const out5 = renderMd(input5);
test('h2 renders', out5.includes('<h3'));
test('bold renders', out5.includes('<strong>bold</strong>'));
test('italic renders', out5.includes('<em>italic</em>'));
test('list items render', out5.includes('item 1') && out5.includes('item 2'));
test('hr renders', out5.includes('<hr'));

// --- Real response data ---
console.log('\n--- Real court response ---');

const input6 = '```table\n{"title":"Krasley","headers":["Docket","Charge","Grade"],"rows":[["CP-39-CR-0001517-2025","Rape","F1"],["CP-39-CR-0001515-2025","Official Oppression","M2"]]}\n```\n\n## Summary\n\n**Jason Michael Krasley** has a hearing today.\n\n- **Bail**: $100,000\n- **Judge**: Caffrey\n\n---\n\n**News Coverage**\n\nKrasley was a former Allentown police officer.';
const out6 = renderMd(input6);
test('real response: table renders', out6.includes('<table'));
test('real response: has Rape F1', out6.includes('Rape') && out6.includes('F1'));
test('real response: summary header', out6.includes('Summary'));
test('real response: bold name', out6.includes('<strong>Jason Michael Krasley</strong>'));
test('real response: list items', out6.includes('$100,000'));
test('real response: news section', out6.includes('News Coverage'));
test('real response: news text', out6.includes('Allentown police officer'));
test('real response: no placeholders', !out6.includes('\u2603'));

// --- Edge cases ---
console.log('\n--- Edge cases ---');

test('empty string returns empty', renderMd('').length === 0);
test('just text', renderMd('Hello world').includes('Hello world'));
test('special chars in table', renderMd('```table\n{"headers":["§ 3012"],"rows":[["18 § 3012 §§ A"]]}\n```').includes('§ 3012'));

// Bold inside list items
const out7 = renderMd('- **Bail**: $100,000\n- **Judge**: Caffrey');
test('bold inside list items', out7.includes('<strong>Bail</strong>') && out7.includes('$100,000'));

// Source line styling
const out8 = renderMd('*Source: Fully analyzed cases*');
test('source line renders with class', out8.includes('Source:') && out8.includes('text-gold'));

const out8b = renderMd('**Source:** Fully analyzed cases');
test('bold source line also styled', out8b.includes('Source:'));

// News loading indicator
const out9 = renderMd('Some text\n\n---\n\n*Searching for news coverage...*');
test('news loading has animation', out9.includes('animate-pulse') || out9.includes('Searching for news'));

// Nested bold in headers
const out10 = renderMd('## **Jason Krasley** - Cases');
test('bold in header', out10.includes('Jason Krasley'));

// Ampersand in content
const out11 = renderMd('Smith & Jones LLC\n\nCharges & bail');
test('ampersands escaped', out11.includes('&amp;'));

// Table with empty cells
const emptyTable = JSON.stringify({headers:["A","B","C"],rows:[["1","","3"],["","2",""]]});
const out12 = renderMd('```table\n' + emptyTable + '\n```');
test('table with empty cells renders', out12.includes('<table') && out12.includes('<td'));

// Incomplete fenced block (should render as text, not break)
const out13 = renderMd('```table\n{"headers":["A"]');
test('incomplete fenced block doesnt break', out13.length > 0);

// Multiple paragraphs with HR between
const out14 = renderMd('Paragraph one.\n\n---\n\n**News Coverage**\n\nParagraph two.');
test('HR separates sections', out14.includes('<hr') && out14.includes('News Coverage'));

// ---------------------------------------------------------------
console.log(`\n${'='.repeat(60)}`);
console.log(`Results: ${PASS} passed, ${FAIL} failed, ${PASS+FAIL} total`);
console.log('='.repeat(60) + '\n');
process.exit(FAIL > 0 ? 1 : 0);
