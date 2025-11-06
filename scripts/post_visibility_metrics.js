const fs = require('fs/promises');
const path = require('path');

const DEFAULT_INPUT_PATH = path.resolve(process.cwd(), 'data', 'meinnow_forward_visibility.csv');
const APP_SCRIPT_URL = process.env.GOOGLE_SHEET_APP_SCRIPT_URL;

if (!APP_SCRIPT_URL) {
  console.error('❌ Environment variable GOOGLE_SHEET_APP_SCRIPT_URL ist nicht gesetzt.');
  process.exit(1);
}

function parsePercent(value) {
  if (value == null || value === '') return 0;
  const normalized = String(value).trim().replace(/%$/, '');
  const num = Number.parseFloat(normalized);
  return Number.isFinite(num) ? num : 0;
}

async function loadCsv(filePath = DEFAULT_INPUT_PATH) {
  const raw = await fs.readFile(filePath, 'utf8');
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
  const forwardIdx = headers.indexOf('forward_visibility_percent');
  const franklinIdx = headers.indexOf('franklin_visibility_percent');

  if (dateIdx === -1 || keywordIdx === -1) {
    throw new Error('CSV benötigt mindestens die Spalten "date" und "keyword".');
  }

  return lines.slice(1).map(line => {
    const cells = line.split(',').map(cell => cell.trim());
    return {
      date: cells[dateIdx] ?? '',
      keyword: cells[keywordIdx] ?? '',
      forward: forwardIdx === -1 ? null : parsePercent(cells[forwardIdx]),
      franklin: franklinIdx === -1 ? null : parsePercent(cells[franklinIdx]),
    };
  });
}

async function postVisibility(entry, options = {}) {
  const payload = {
    date: entry.date,
    brand: entry.keyword,
    forward_visibility_percent: entry.forward,
    franklin_visibility_percent: entry.franklin,
    source: 'visibility_csv',
  };

  if (options.dryRun) {
    console.log('📝 Dry-Run Payload:', JSON.stringify(payload));
    return { ok: true, dryRun: true };
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
  let json;
  try {
    json = JSON.parse(resultText);
  } catch (error) {
    json = { raw: resultText };
  }

  return json;
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');
  const entries = await loadCsv();

  if (entries.length === 0) {
    console.log('Keine Einträge zu senden.');
    return;
  }

  console.log(`🚀 Sende ${entries.length} Einträge an Google Apps Script${dryRun ? ' (Dry-Run)' : ''}...`);

  for (const entry of entries) {
    try {
      const result = await postVisibility(entry, { dryRun });
      console.log(`✅ ${entry.keyword} (${entry.date}):`, result);
    } catch (error) {
      console.error(`❌ Fehler bei ${entry.keyword} (${entry.date}):`, error.message);
    }
  }
}

if (require.main === module) {
  main().catch(error => {
    console.error('❌ Unbehandelter Fehler:', error);
    process.exitCode = 1;
  });
}

