const fs = require('fs/promises');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

const CSV_PATH = path.resolve(__dirname, '..', 'data', 'meinnow_forward_visibility.csv');
const APP_SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbxZjui3kQep0Hivd2Srr1BW3s2YOV9iQa2awE9Dp-gl2alqOgTccn9dbjszyKHzlCNQ/exec';

// Keyword-Kategorien (aus meinnow_scrape.js)
const ITS_KEYWORDS = [
  'JavaScript', 'Python', 'SQL', 'Java', 'Künstliche Intelligenz',
  'programmierung', 'App Programmieren Lernen', 'Excel Kurs', 'HTML',
  'Maschinelles Lernen', 'Phyton Kurs', 'C++ Lernen', 'Apps programmieren',
  'Java Script Lernen', 'Python3 Kurs', 'Javascripts Lernen',
  'Informationstechnologie Weiterbildung', 'Programmierung Lernen', 'excel',
  'Phyton Lernen', 'Quereinstieg It', 'informationstechnologie',
  'programmiersprache', 'Programmiersprache Lernen', 'Hmtl Lernen',
  'Javascript Lernen', 'C++', 'Excel Grundlagen', 'java script', 'Sql Lernen',
  'Verkauf Kurs', 'Verkäufer werden', 'Verkauf lernen', 'Verkauf Training',
  'verkaufen lernen', 'Vertrieb lernen', 'Vertrieb Einstieg',
  'Verkäufer Weiterbildung', 'Verkauf ohne Erfahrung', 'Sales',
  'Quereinstieg Verkauf', 'Verkauf Schulung', 'Vertriebs Kurs',
  'Vertrieb Training', 'Telefonverkauf Kurs', 'Kaltakquise lernen',
  'Verkäufer Job', 'IT Sales',
];

const PM_KEYWORDS = [
  'Controller Weiterbildung', 'Kooperation', 'Weiterbildung Qualitätsmanager',
  'pflegedienstleitung', 'Controlling', 'Qualitätsmanagment Weiterbildung',
  'Qualitätsmanagment', 'Controlling Weiterbildung', 'Projektmanagement',
  'Projektmanagement Weiterbildung', 'Qualitätsmanagement',
  'Weiterbildung Projektmanager', 'Projektmanager Weiterbildung',
  'projektmanagement', 'Soziales Lernen', 'soziale Arbeit', 'controlling',
  'qualitätsmanagement', 'Qualitätsmanagement Weiterbildung',
  'Marketing Weiterbildung', 'Vertrieb', 'Quereinstieg Vertrieb',
  'Coaching Weiterbildung', 'Handelsfachwirt Weiterbildung',
  'Ihk Weiterbildung', 'Industriekauffrau Weiterbildung',
  'Betriebswirt Weiterbildung', 'Fachwirt Weiterbildung',
  'Projektmanagement', 'Projektmanager', 'Projektmanagement lernen',
  'Projektplanung lernen', 'PM Weiterbildung', 'Projektleiten lernen',
  'Projektmanagement Basics', 'Projektmanager Einstieg', 'PM Einsteiger',
  'Projektmanagement Schulung', 'Projektmanagement Fortbildung',
  'Quereinstieg Projektmanagement', 'Projektmanagement Job', 'PM Grundlagen',
  'PM Training', 'Projektarbeit lernen', 'PM Kurs', 'Teamarbeit lernen',
];

const AI_AUTOMATION_KEYWORDS = [
  'KI Consultant', 'KI Berater', 'KI & Automation Consultant', 'KI Consultant Weiterbildung',
  'Automation Specialist', 'Automation Manager', 'Automatisierung Manager',
  'Process Manager Digitalisierung','Marketing Automation', 'Marketing Automation Manager', 'Marketing Automation Weiterbildung',
  'Sales Automation','Vertrieb Automation', 'CRM Automation',
  'Low-Code Developer', 'No-Code Developer', 'Zapier', 'Make', 'n8n', 'Airtable',
  'AI Product Manager', 'KI Produktmanager', 'Digital Transformation Manager',
  'Chatbot', 'KI Chatbot', 'AI Chatbot', 'KI Support', 'AI Support',
  'Prompt Engineering', 'KI Tools', 'Machine Learning', 'Maschinelles Lernen',
  'KI Strategie', 'AI Strategie', 'KI Einführung', 'AI Einführung',
  'KI Marketing', 'AI Marketing', 'Marketing 4.0', 'Vertrieb 4.0',
  'KI Vertrieb', 'AI Sales', 'Sales 4.0',
  'KI lernen', 'AI lernen', 'KI Kurs', 'AI Kurs', 'Quereinstieg KI',
  'KI Job', 'KI Karriere',
];

const KEYWORD_CATEGORY_MAP = new Map();
ITS_KEYWORDS.forEach(kw => KEYWORD_CATEGORY_MAP.set(kw.toLowerCase(), 'ITS'));
PM_KEYWORDS.forEach(kw => KEYWORD_CATEGORY_MAP.set(kw.toLowerCase(), 'PM'));
AI_AUTOMATION_KEYWORDS.forEach(kw => KEYWORD_CATEGORY_MAP.set(kw.toLowerCase(), 'AI_AUTOMATION'));

function getKeywordCategory(keyword) {
  return KEYWORD_CATEGORY_MAP.get(keyword.toLowerCase()) || 'UNKNOWN';
}

function parsePercent(value) {
  if (!value || value === '') return 0;
  const normalized = String(value).trim().replace(/%$/, '');
  const num = Number.parseFloat(normalized);
  return Number.isFinite(num) ? num : 0;
}

async function loadCsv() {
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
  const forwardIdx = headers.indexOf('forward_visibility_percent');
  const franklinIdx = headers.indexOf('franklin_visibility_percent');

  if (dateIdx === -1 || keywordIdx === -1) {
    throw new Error('CSV benötigt mindestens die Spalten "date" und "keyword".');
  }

  return lines.slice(1).map(line => {
    const cells = line.split(',').map(cell => cell.trim());
    const keyword = cells[keywordIdx] || '';
    const category = categoryIdx !== -1 
      ? (cells[categoryIdx] || getKeywordCategory(keyword))
      : getKeywordCategory(keyword);
    return {
      date: cells[dateIdx] || '',
      keyword,
      category,
      forward: forwardIdx === -1 ? null : parsePercent(cells[forwardIdx]),
      franklin: franklinIdx === -1 ? null : parsePercent(cells[franklinIdx]),
    };
  });
}

async function postVisibility(entry) {
  const payload = {
    action: 'visibility_metrics',
    date: entry.date,
    keyword: entry.keyword,
    category: entry.category,
    forward_visibility_percent: entry.forward,
    franklin_visibility_percent: entry.franklin,
  };

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
  console.log('📂 Lade CSV-Daten...');
  const entries = await loadCsv();

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

