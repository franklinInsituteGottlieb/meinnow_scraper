const fs = require('fs/promises');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const CSV_PATH = path.resolve(__dirname, '..', 'data', 'meinnow_forward_visibility.csv');
const KEYWORDS_CSV_PATH = path.resolve(__dirname, '..', 'keywords_vertical.csv');
const APP_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbxZjui3kQep0Hivd2Srr1BW3s2YOV9iQa2awE9Dp-gl2alqOgTccn9dbjszyKHzlCNQ/exec';

/** Lädt Keyword → Vertical aus keywords_vertical.csv (weight wird ignoriert). */
async function loadKeywordCategoryMap() {
  const raw = await fs.readFile(KEYWORDS_CSV_PATH, 'utf8');
  const lines = raw
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0);

  if (lines.length <= 1) {
    return new Map();
  }

  const headers = lines[0].split(',').map(h => h.trim().toLowerCase());
  const keywordIdx = headers.indexOf('keyword');
  const verticalIdx = headers.indexOf('vertical');

  if (keywordIdx === -1 || verticalIdx === -1) {
    throw new Error('keywords_vertical.csv benötigt die Spalten "keyword" und "vertical".');
  }

  const map = new Map();
  for (let i = 1; i < lines.length; i += 1) {
    const cells = lines[i].split(',').map(c => c.trim());
    const keyword = cells[keywordIdx] ?? '';
    const vertical = cells[verticalIdx] ?? 'UNKNOWN';
    if (keyword) {
      map.set(keyword.toLowerCase(), vertical);
    }
  }
  return map;
}

function getKeywordCategory(keyword, categoryMap) {
  return categoryMap.get(keyword.toLowerCase()) || 'UNKNOWN';
}

function parsePercent(value) {
  if (!value || value === '') return 0;
  const normalized = String(value).trim().replace(/%$/, '');
  const num = Number.parseFloat(normalized);
  return Number.isFinite(num) ? num : 0;
}

async function loadCsv(categoryMap) {
  const raw = await fs.readFile(CSV_PATH, 'utf8');
  const lines = raw
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0);

  if (lines.length <= 1) {
    console.warn('⚠️ CSV enthält keine Datenzeilen.');
    return [];
  }

  const headers = lines[0].split(',').map(h => h.trim());
  const dateIdx = headers.indexOf('date');
  const keywordIdx = headers.indexOf('keyword');
  const categoryIdx = headers.indexOf('category');

  if (dateIdx === -1 || keywordIdx === -1) {
    throw new Error('CSV benötigt mindestens die Spalten "date" und "keyword".');
  }

  // Finde alle Visibility-Spalten dynamisch
  const visibilityIndices = {};
  headers.forEach((header, idx) => {
    if (header.endsWith('_visibility_percent')) {
      const label = header.replace('_visibility_percent', '');
      visibilityIndices[label] = idx;
    }
  });
  const visibilityTotalIdx = headers.indexOf('visibility_total');

  return lines.slice(1).map(line => {
    const cells = line.split(',').map(cell => cell.trim());
    const keyword = cells[keywordIdx] || '';
    const category = categoryIdx !== -1
      ? (cells[categoryIdx] || getKeywordCategory(keyword, categoryMap))
      : getKeywordCategory(keyword, categoryMap);
    
    const entry = {
      date: cells[dateIdx] || '',
      keyword,
      category,
    };
    
    for (const [label, idx] of Object.entries(visibilityIndices)) {
      entry[label] = idx === -1 ? null : parsePercent(cells[idx]);
    }
    const totalRaw = visibilityTotalIdx >= 0 ? cells[visibilityTotalIdx] : '';
    entry.visibility_total = totalRaw === '' || totalRaw === null ? 0 : Number(totalRaw) || 0;
    
    return entry;
  });
}

async function postVisibility(entry) {
  const payload = {
    action: 'visibility_metrics',
    date: entry.date,
    keyword: entry.keyword,
    category: entry.category,
    visibility_total: entry.visibility_total ?? 0,
  };
  
  for (const [key, value] of Object.entries(entry)) {
    if (key !== 'date' && key !== 'keyword' && key !== 'category' && key !== 'visibility_total') {
      payload[`${key}_visibility_percent`] = value;
    }
  }

  const response = await fetch(APP_SCRIPT_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status} ${response.statusText}: ${text}`);
  }

  const resultText = await response.text();
  let result;
  try {
    result = JSON.parse(resultText);
  } catch (error) {
    result = { raw: resultText };
  }

  return result;
}

async function main() {
  console.log('📂 Lade Keyword-Verticals aus keywords_vertical.csv...');
  const categoryMap = await loadKeywordCategoryMap();
  console.log('📂 Lade CSV-Daten...');
  const entries = await loadCsv(categoryMap);

  if (entries.length === 0) {
    console.log('Keine Einträge zum Senden.');
    return;
  }

  console.log(`🚀 Sende ${entries.length} Einträge an Google Apps Script...\n`);

  for (const entry of entries) {
    try {
      const result = await postVisibility(entry);
      console.log(`✅ ${entry.keyword} (${entry.category}, ${entry.date}):`, result.message || result.raw || 'OK');
    } catch (error) {
      console.error(`❌ Fehler bei ${entry.keyword} (${entry.date}):`, error.message);
    }
  }

  console.log(`\n✅ Fertig! ${entries.length} Einträge verarbeitet.`);
}

if (require.main === module) {
  main().catch(error => {
    console.error('❌ Unbehandelter Fehler:', error);
    process.exitCode = 1;
  });
}

