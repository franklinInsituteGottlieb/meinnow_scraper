const fs = require('fs/promises');
const path = require('path');
const dotenv = require('dotenv');
const puppeteer = require('puppeteer');

dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const BASE_SEARCH_URL = 'https://mein-now.de/weiterbildungssuche/';
const LOAD_MORE_BUTTON_SELECTOR = '#load_more_angebote';
const LOAD_MORE_ITERATIONS = 4;
const SEARCH_KEYWORDS = [
  'produktmanager',
  'projektmanagement',
  'sales',
  'vertrieb',
];
const VISIBILITY_PATTERNS = {
  forward: /forward/i,
  franklin: /franklin/i,
};
const APP_SCRIPT_URL = process.env.GOOGLE_SHEET_APP_SCRIPT_URL || 'https://script.google.com/macros/s/AKfycbyuopZkF_1x3QSFmb_Dn2-ql6lQrSogdCjHMFMwBwOPZWZkKvSCy6qY_zG08a2oSOTM/exec';

async function postVisibilityMetrics(entries) {
  if (!APP_SCRIPT_URL) {
    console.warn('⚠️ GOOGLE_SHEET_APP_SCRIPT_URL nicht gesetzt – überspringe API-POST.');
    return;
  }

  for (const entry of entries) {
    const payload = {
      action: 'visibility_metrics',
      date: entry.date,
      keyword: entry.keyword,
      forward_visibility_percent: entry.metrics.forward,
      franklin_visibility_percent: entry.metrics.franklin,
    };

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
        throw new Error(`HTTP ${response.status} ${response.statusText}: ${text}`);
      }

      const resultText = await response.text();
      console.log(`📤 Sichtbarkeit gesendet (${entry.keyword}, ${entry.date}):`, resultText);
    } catch (error) {
      console.error(`❌ Fehler beim Senden (${entry.keyword}, ${entry.date}):`, error.message);
    }
  }
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

  console.log(`🚀 Starte Puppeteer (headless=${headless})`);
  const browser = await puppeteer.launch(launchOptions);
  const page = await browser.newPage();

  try {
    const allOffers = [];

    for (const keyword of SEARCH_KEYWORDS) {
      const searchUrl = buildSearchUrl(keyword);
      console.log(`\n🔎 Starte Keyword "${keyword}" → ${searchUrl}`);

      await page.goto(searchUrl, { waitUntil: 'networkidle2' });

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
    }

    if (allOffers.length > 0) {
      const aggregatePath = path.resolve(process.cwd(), 'data', 'meinnow_offers_all.json');
      await fs.mkdir(path.dirname(aggregatePath), { recursive: true });
      await fs.writeFile(aggregatePath, JSON.stringify(allOffers, null, 2), 'utf8');
      console.log(`\n📦 Ergebnis gespeichert unter ${aggregatePath}`);

      const today = new Date().toISOString().split('T')[0];
      const summaryHeader = ['date', 'keyword'];
      for (const label of Object.keys(VISIBILITY_PATTERNS)) {
        summaryHeader.push(`${label}_visibility_percent`);
      }
      const summaryLines = [summaryHeader.join(',')];
      const summaryEntries = [];

      for (const keyword of SEARCH_KEYWORDS) {
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

        summaryLines.push(`${today},${keyword},${visibilityValues.join(',')}`);
        summaryEntries.push({
          date: today,
          keyword,
          metrics,
        });
      }

      const summaryPath = path.resolve(process.cwd(), 'data', 'meinnow_forward_visibility.csv');
      await fs.mkdir(path.dirname(summaryPath), { recursive: true });
      await fs.writeFile(summaryPath, summaryLines.join('\n'), 'utf8');
      console.log(`📈 Sichtbarkeitsübersicht gespeichert unter ${summaryPath}`);

      await postVisibilityMetrics(summaryEntries);
    }
  } catch (error) {
    console.error('Fehler beim Scrapen:', error);
  } finally {
    await browser.close();
  }
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

