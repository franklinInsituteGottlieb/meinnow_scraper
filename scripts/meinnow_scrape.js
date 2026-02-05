const fs = require('fs/promises');
const path = require('path');
const dotenv = require('dotenv');
const puppeteer = require('puppeteer');

dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const BASE_SEARCH_URL = 'https://mein-now.de/weiterbildungssuche/';
const LOAD_MORE_BUTTON_SELECTOR = '#load_more_angebote';
const LOAD_MORE_ITERATIONS = 4;

const KEYWORDS_CSV_PATH = path.resolve(__dirname, '..', 'keywords_vertical.csv');

/** Lädt Keywords und Vertical aus keywords_vertical.csv (Spalte weight wird ignoriert). */
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
  const verticalIdx = headers.indexOf('vertical');

  if (keywordIdx === -1 || verticalIdx === -1) {
    throw new Error('keywords_vertical.csv benötigt die Spalten "keyword" und "vertical".');
  }

  const searchKeywords = [];
  const categoryMap = new Map();

  for (let i = 1; i < lines.length; i += 1) {
    const cells = lines[i].split(',').map(c => c.trim());
    const keyword = cells[keywordIdx] ?? '';
    const vertical = cells[verticalIdx] ?? 'UNKNOWN';
    if (keyword) {
      searchKeywords.push(keyword);
      categoryMap.set(keyword.toLowerCase(), vertical);
    }
  }

  return { searchKeywords, categoryMap };
}

function getKeywordCategory(keyword, categoryMap) {
  return categoryMap.get(keyword.toLowerCase()) || 'UNKNOWN';
}

const VISIBILITY_PATTERNS = {
  forward: /forward/i,
  franklin: /franklin/i,
  impaqt: /impaqt/i,
};
const APP_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbxZjui3kQep0Hivd2Srr1BW3s2YOV9iQa2awE9Dp-gl2alqOgTccn9dbjszyKHzlCNQ/exec';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function postVisibilityMetrics(entries) {
  if (!APP_SCRIPT_URL) {
    console.warn('⚠️ GOOGLE_SHEET_APP_SCRIPT_URL nicht gesetzt – überspringe API-POST.');
    return;
  }

  const POST_DELAY_MS = 500; // Delay zwischen POSTs, um Rate-Limiting zu vermeiden
  const MAX_RETRIES = 3;
  const RETRY_DELAY_MS = 2000; // Delay bei Retry nach Rate-Limit

  let successCount = 0;
  let failCount = 0;

  console.log(`\n📤 Starte POST von ${entries.length} Einträgen an Google Sheets...`);

  for (let i = 0; i < entries.length; i += 1) {
    const entry = entries[i];
    const payload = {
      action: 'visibility_metrics',
      date: entry.date,
      keyword: entry.keyword,
      category: entry.category,
      visibility_total: entry.visibility_total ?? 0,
    };
    // Dynamisch alle Metriken hinzufügen
    for (const [label, value] of Object.entries(entry.metrics)) {
      payload[`${label}_visibility_percent`] = value;
    }

    let retries = 0;
    let success = false;

    while (retries <= MAX_RETRIES && !success) {
      try {
        const response = await fetch(APP_SCRIPT_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          const text = await response.text();
          
          // Rate-Limiting (429) → Retry mit Delay
          if (response.status === 429 && retries < MAX_RETRIES) {
            retries += 1;
            console.warn(`⚠️ Rate-Limit bei ${entry.keyword}, Retry ${retries}/${MAX_RETRIES} in ${RETRY_DELAY_MS}ms...`);
            await sleep(RETRY_DELAY_MS);
            continue;
          }
          
          throw new Error(`HTTP ${response.status} ${response.statusText}: ${text.substring(0, 100)}`);
        }

        const resultText = await response.text();
        console.log(`✅ [${i + 1}/${entries.length}] ${entry.keyword} (${entry.category}): gespeichert`);
        success = true;
        successCount += 1;
      } catch (error) {
        if (retries < MAX_RETRIES && error.message.includes('429')) {
          retries += 1;
          console.warn(`⚠️ Rate-Limit bei ${entry.keyword}, Retry ${retries}/${MAX_RETRIES} in ${RETRY_DELAY_MS}ms...`);
          await sleep(RETRY_DELAY_MS);
          continue;
        }
        
        console.error(`❌ [${i + 1}/${entries.length}] Fehler bei ${entry.keyword} (${entry.date}):`, error.message);
        failCount += 1;
        success = false;
        break;
      }
    }

    // Delay zwischen POSTs (außer beim letzten)
    if (i < entries.length - 1) {
      await sleep(POST_DELAY_MS);
    }
  }

  console.log(`\n📊 POST-Zusammenfassung: ${successCount} erfolgreich, ${failCount} fehlgeschlagen von ${entries.length} Einträgen.`);
}

async function waitForNetworkIdle(page, timeout = 500, maxInflight = 2) {
  let inflight = 0;
  let fulfill;

  const promise = new Promise(resolve => {
    fulfill = resolve;
  });

  let timeoutId = setTimeout(() => fulfill(), timeout);

  const onRequest = () => {
    inflight += 1;
    clearTimeout(timeoutId);
  };

  const onRequestFinished = () => {
    if (inflight > 0) inflight -= 1;
    if (inflight <= maxInflight) {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => fulfill(), timeout);
    }
  };

  page.on('request', onRequest);
  page.on('requestfinished', onRequestFinished);
  page.on('requestfailed', onRequestFinished);

  await promise;

  page.off('request', onRequest);
  page.off('requestfinished', onRequestFinished);
  page.off('requestfailed', onRequestFinished);
}

function buildSearchUrl(keyword) {
  const url = new URL(BASE_SEARCH_URL);
  url.searchParams.set('sw', keyword);
  return url.toString();
}

async function clickLoadMore(page) {
  const buttonExists = await page.$(LOAD_MORE_BUTTON_SELECTOR);
  if (!buttonExists) {
    console.warn('Button "Weitere Angebote anzeigen" nicht gefunden.');
    return false;
  }

  const clicked = await page.evaluate(selector => {
    const btn = document.querySelector(selector);
    if (!btn) return false;

    ['pointerdown', 'mousedown', 'mouseup', 'click'].forEach(type => {
      btn.dispatchEvent(
        new MouseEvent(type, {
          bubbles: true,
          cancelable: true,
          view: window,
        }),
      );
    });

    return true;
  }, LOAD_MORE_BUTTON_SELECTOR);

  if (!clicked) {
    console.warn('Button konnte nicht geklickt werden.');
    return false;
  }

  await waitForNetworkIdle(page, 1000);
  return true;
}

async function scrapeOffers(page, keyword) {
  return page.evaluate(activeKeyword => {
    const items = [
      ...document.querySelectorAll(
        'ul.now-card-stack li.now-card.now-link-card.now-with-tag',
      ),
    ];

    return items.map(li => {
      const termineText =
        li.querySelector('.now-card-tag-text')?.textContent.trim() || '';
      const termineMatch = termineText.match(/(\d+)/);
      const termine = termineMatch ? parseInt(termineMatch[1], 10) : null;

      const titleSpan = li.querySelector(
        'h2.now-heading.heading span:not(.sr-only)',
      );
      const title = (titleSpan || li.querySelector('h2.now-heading.heading'))?.textContent.trim() || '';

      let provider = '';
      const providerSpan = li.querySelector('span[id$="_anbieter"]');
      if (providerSpan) {
        provider = providerSpan.textContent.trim();
      } else {
        const srOnly = li.querySelector('h2.now-heading.heading .sr-only');
        if (srOnly) {
          provider = srOnly.textContent.replace(/^Bildungsanbieter\s*/i, '').trim();
        }
      }

      return { termine, title, provider, keyword: activeKeyword };
    });
  }, keyword);
}

async function runScrapeOnce() {
  const headlessInput = (process.env.PUPPETEER_HEADLESS ?? 'true').toString().toLowerCase();
  const headless = ['1', 'true', 'yes', 'on'].includes(headlessInput);
  const slowMoEnv = Number.parseInt(process.env.PUPPETEER_SLOW_MO ?? '0', 10);
  const launchOptions = {
    headless,
    defaultViewport: headless ? { width: 1280, height: 720 } : null,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      ...(headless ? [] : ['--start-maximized']),
    ],
    slowMo: Number.isNaN(slowMoEnv) ? 0 : slowMoEnv,
  };

  if (process.env.PUPPETEER_EXECUTABLE_PATH) {
    launchOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
  }

  const { searchKeywords, categoryMap } = await loadKeywordsFromCsv();
  console.log(`📋 ${searchKeywords.length} Keywords aus ${KEYWORDS_CSV_PATH} geladen.`);

  console.log(`🚀 Starte Puppeteer (headless=${headless})`);
  const browser = await puppeteer.launch(launchOptions);
  const page = await browser.newPage();

  const allOffers = [];
  const successfulKeywords = new Set();
  const failedKeywords = [];

  // Jedes Keyword einzeln scrapen, damit Fehler bei einem Keyword nicht alles stoppen
  for (const keyword of searchKeywords) {
    try {
      const searchUrl = buildSearchUrl(keyword);
      console.log(`\n🔎 Starte Keyword "${keyword}" → ${searchUrl}`);

      // Timeout erhöht auf 60 Sekunden für langsame Seiten
      await page.goto(searchUrl, { 
        waitUntil: 'networkidle2',
        timeout: 60000 
      });

      for (let i = 0; i < LOAD_MORE_ITERATIONS; i += 1) {
        const success = await clickLoadMore(page);
        if (!success) {
          console.warn(`Abbruch nach ${i} zusätzlichen Klicks (Keyword "${keyword}").`);
          break;
        }
        console.log(`"Weitere Angebote" Klick ${i + 1} abgeschlossen (Keyword "${keyword}").`);
      }

      const offers = await scrapeOffers(page, keyword);
      console.log(`Insgesamt ${offers.length} Angebote für Keyword "${keyword}".`);

      allOffers.push(...offers);
      successfulKeywords.add(keyword);
    } catch (error) {
      console.error(`❌ Fehler beim Scrapen von Keyword "${keyword}":`, error.message);
      failedKeywords.push({ keyword, error: error.message });
      // Weiter mit dem nächsten Keyword
    }
  }

  // Zusammenfassung der erfolgreichen/fehlgeschlagenen Keywords
  console.log(`\n📊 Scraping-Zusammenfassung: ${successfulKeywords.size} erfolgreich, ${failedKeywords.length} fehlgeschlagen von ${searchKeywords.length} Keywords.`);
  if (failedKeywords.length > 0) {
    console.log('⚠️ Fehlgeschlagene Keywords:');
    failedKeywords.forEach(({ keyword, error }) => {
      console.log(`   - ${keyword}: ${error}`);
    });
  }

  // Visibility-Metriken für ALLE Keywords berechnen (auch die, die fehlgeschlagen sind)
  const today = new Date().toISOString().split('T')[0];
  const summaryHeader = ['date', 'keyword', 'category'];
  for (const label of Object.keys(VISIBILITY_PATTERNS)) {
    summaryHeader.push(`${label}_visibility_percent`);
  }
  summaryHeader.push('visibility_total');
  const summaryLines = [summaryHeader.join(',')];
  const summaryEntries = [];

  for (const keyword of searchKeywords) {
    const offersForKeyword = allOffers.filter(offer => offer.keyword === keyword);
    const totalCount = offersForKeyword.length;
    const visibilityValues = [];
    const metrics = {};

    for (const [label, pattern] of Object.entries(VISIBILITY_PATTERNS)) {
      const matchCount = offersForKeyword.filter(offer =>
        offer.provider && pattern.test(offer.provider),
      ).length;
      const visibility = totalCount === 0 ? 0 : (matchCount / totalCount) * 100;
      visibilityValues.push(`${visibility.toFixed(2)}%`);
      metrics[label] = Number(visibility.toFixed(2));
    }

    const category = getKeywordCategory(keyword, categoryMap);
    summaryLines.push(`${today},${keyword},${category},${visibilityValues.join(',')},${totalCount}`);
    summaryEntries.push({
      date: today,
      keyword,
      category,
      metrics,
      visibility_total: totalCount,
    });
  }

  // Dateien speichern
  if (allOffers.length > 0) {
    const aggregatePath = path.resolve(process.cwd(), 'data', 'meinnow_offers_all.json');
    await fs.mkdir(path.dirname(aggregatePath), { recursive: true });
    await fs.writeFile(aggregatePath, JSON.stringify(allOffers, null, 2), 'utf8');
    console.log(`\n📦 Ergebnis gespeichert unter ${aggregatePath}`);
  }

  const summaryPath = path.resolve(process.cwd(), 'data', 'meinnow_forward_visibility.csv');
  await fs.mkdir(path.dirname(summaryPath), { recursive: true });
  await fs.writeFile(summaryPath, summaryLines.join('\n'), 'utf8');
  console.log(`📈 Sichtbarkeitsübersicht gespeichert unter ${summaryPath}`);

  // WICHTIG: POSTs IMMER ausführen, auch wenn einige Keywords fehlgeschlagen sind
  console.log(`\n📤 Starte POST von ${summaryEntries.length} Visibility-Metriken an Google Sheets...`);
  await postVisibilityMetrics(summaryEntries);

  await browser.close();
}

async function main() {
  console.log('▶️ Starte einmaligen Scrape-Lauf.');
  const start = Date.now();
  try {
    await runScrapeOnce();
    const duration = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`✅ Scrape abgeschlossen (${duration}s).`);
  } catch (error) {
    console.error('❌ Fehler im Scrape-Durchlauf:', error);
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main();
}

