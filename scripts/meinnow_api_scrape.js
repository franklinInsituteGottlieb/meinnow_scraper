const fs = require('fs/promises');
const path = require('path');

const API_ENDPOINT = 'https://rest.mein-now.de/now-prod/suche/pc/v1/bildungsangebot';
const KEYWORDS_CSV_PATH = path.resolve(__dirname, '..', 'keywords_vertical.csv');
const PAGE_RANGE = { start: 1, end: 4 };

/** Lädt Keywords aus keywords_vertical.csv (nur keyword-Spalte, weight ignoriert). */
async function loadKeywordsFromCsv() {
  const raw = await fs.readFile(KEYWORDS_CSV_PATH, 'utf8');
  const lines = raw
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0);

  if (lines.length <= 1) {
    throw new Error('keywords_vertical.csv enthält keine Datenzeilen.');
  }

  const headers = lines[0].split(',').map(h => h.trim().toLowerCase());
  const keywordIdx = headers.indexOf('keyword');
  if (keywordIdx === -1) {
    throw new Error('keywords_vertical.csv benötigt die Spalte "keyword".');
  }

  const keywords = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cells = lines[i].split(',').map(c => c.trim());
    const keyword = cells[keywordIdx] ?? '';
    if (keyword) {
      keywords.push(keyword);
    }
  }
  return keywords;
}

function buildRequestUrl(keyword, page) {
  const url = new URL(API_ENDPOINT);
  url.searchParams.set('ortsunabhaengig', 'false');
  url.searchParams.set('page', String(page));
  url.searchParams.set('sort', 'std');
  url.searchParams.set('sw', keyword);
  url.searchParams.set('ute', 'false');
  url.searchParams.set('dac', 'false');
  return url.toString();
}

async function fetchPage(keyword, page) {
  const requestUrl = buildRequestUrl(keyword, page);
  const response = await fetch(requestUrl, {
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`Fehler beim Abrufen: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  return { keyword, page, data };
}

async function scrapeApi() {
  const searchKeywords = await loadKeywordsFromCsv();
  console.log(`📋 ${searchKeywords.length} Keywords aus ${KEYWORDS_CSV_PATH} geladen.`);

  const allResults = [];

  for (const keyword of searchKeywords) {
    console.log(`\n🔎 Keyword "${keyword}"`);

    for (let page = PAGE_RANGE.start; page <= PAGE_RANGE.end; page += 1) {
      try {
        console.log(`  → Lade Seite ${page}`);
        const result = await fetchPage(keyword, page);
        allResults.push(result);
      } catch (error) {
        console.error(`  ⚠️ Fehler Keyword "${keyword}" Seite ${page}:`, error.message);
      }
    }
  }

  if (allResults.length === 0) {
    console.warn('Keine Ergebnisse gespeichert.');
    return;
  }

  const outputPath = path.resolve(process.cwd(), 'data', 'meinnow_api_results.json');
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, JSON.stringify(allResults, null, 2), 'utf8');
  console.log(`\n📦 API-Ergebnisse gespeichert unter ${outputPath}`);
}

if (require.main === module) {
  scrapeApi().catch(error => {
    console.error('❌ Unbehandelter Fehler:', error);
    process.exitCode = 1;
  });
}

