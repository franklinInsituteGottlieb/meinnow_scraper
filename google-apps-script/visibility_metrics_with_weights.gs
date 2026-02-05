// ============================================================
// GOOGLE APPS SCRIPT – Spreadsheet ID & Einstieg
// ============================================================
// SHEET-ANLEITUNG (in dieser Tabelle):
// -------------------------------------
// VORHANDEN LASSEN: raw_data, page_logs, navigation_logs, leads, Pivot Table 9
// visibility_metrics2: Wird vom Script beschrieben (Scraper-POSTs).
//   → Spalten anpassen: Alte Spalte "category" LÖSCHEN. Stattdessen müssen stehen:
//     date | keyword | forward_visibility_percent | franklin_visibility_percent |
//     impaqt_visibility_percent | visibility_total | source | received_at
//     (Falls du die alte Struktur hattest: Header-Zeile auf diese 8 Spalten setzen;
//      alte Datenzeilen können bleiben, neue haben dann die neuen Spalten.)
// HINZUFÜGEN (neue Tabellen):
//   - keyword_weights: Einmal anlegen (Script legt sie bei Bedarf leer an).
//     Inhalt: Inhalt von keywords_vertical.csv einfügen – Spalten: keyword | vertical | weight
//   - visibility_weighted: Wird nur vom Script geschrieben (computeWeightedVisibility_).
//   - visibility_by_vertical: Wird nur vom Script geschrieben (computeWeightedVisibility_).
// NICHTS LÖSCHEN: raw_data, page_logs, navigation_logs, leads.
// ============================================================

const SS_ID = '1P9XHDYFStRo8B2cCN4aojkAdzhjzqON-i-tVVboxoG0';

function getSS_() {
  return SpreadsheetApp.openById(SS_ID);
}

// ============================================================
// doPost – Webhook-Eingang
// ============================================================

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);

    if (data && data.action === 'navigation') {
      const result = logNavigation_(data);
      return createResponse(result.status, result.message);
    }

    if (data && (data.action === 'track' || data.route === 'track')) {
      addTrackEvent_(data);
      return createResponse('success', 'Track-Event gespeichert');
    }

    if (data && data.action === 'visibility_metrics') {
      addVisibilityMetrics_(data);
      return createResponse('success', 'Visibility-Daten gespeichert');
    }

    if (data && (data.type === 'lead' || data.action === 'lead' || data.route === 'lead')) {
      addLead_(data);
      return createResponse('success', 'Lead gespeichert');
    }

    if (!data.date || !data.brand) {
      return createResponse('error', 'Fehlende Pflichtfelder: date und brand');
    }
    addDataToSheet(data);
    return createResponse('success', 'Daten erfolgreich hinzugefügt');
  } catch (error) {
    Logger.log('Fehler in doPost: ' + error.toString());
    return createResponse('error', error.toString());
  }
}

// ============================================================
// Navigation Tracking
// ============================================================

function logNavigation_(data) {
  const ss = getSS_();
  let sh = ss.getSheetByName('navigation_logs');

  if (!sh) {
    sh = ss.insertSheet('navigation_logs');
    sh.appendRow(['Brand', 'Timestamp', 'Hashed ID', 'Type', 'URL', 'Referrer']);
    sh.setColumnWidth(1, 100);
    sh.setColumnWidth(2, 150);
    sh.setColumnWidth(3, 200);
    sh.setColumnWidth(5, 400);
    sh.setColumnWidth(6, 300);
  }

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) return { status: 'error', message: 'Server Busy' };

  try {
    const timestamp = new Date();
    const brand = data.brand || 'forward';
    const hashedId = data.uid || 'anon';
    const type = data.type || 'unknown';
    const url = data.url || '';
    const referrer = data.referrer || '';

    const lastRow = sh.getLastRow();
    if (lastRow > 1) {
      const startRow = Math.max(2, lastRow - 20);
      const numRows = lastRow - startRow + 1;
      const recentData = sh.getRange(startRow, 1, lastRow, 6).getValues();

      const isDuplicate = recentData.some(function (row) {
        const rowTime = new Date(row[1]);
        const rowId = row[2];
        const rowType = row[3];
        const rowUrl = row[4];
        const secondsDiff = (timestamp - rowTime) / 1000;
        return (rowId === hashedId && rowUrl === url && rowType === type && secondsDiff < 60);
      });

      if (isDuplicate) return { status: 'skipped', message: 'Duplicate filtered' };
    }

    sh.appendRow([brand, timestamp, hashedId, type, url, referrer]);
    return { status: 'success', message: 'Logged' };
  } catch (e) {
    return { status: 'error', message: e.toString() };
  } finally {
    lock.releaseLock();
  }
}

// ============================================================
// Raw Data (Fallback)
// ============================================================

function addDataToSheet(data) {
  const spreadsheet = getSS_();
  const sheet = spreadsheet.getSheetByName('raw_data');
  if (!sheet) throw new Error('Sheet "raw_data" nicht gefunden');

  const newRow = [
    data.date, data.brand,
    data.titel_published || 0, data.titel_publishing || 0,
    data.titel_not_published || 0, data.titel_blocked || 0,
    data.termine_published || 0, data.termine_publishing || 0,
    data.termine_not_published || 0, data.termine_blocked || 0
  ];

  const existingRowIndex = findExistingRow(sheet, data.date, data.brand);
  if (existingRowIndex !== -1) {
    sheet.getRange(existingRowIndex, 1, 1, newRow.length).setValues([newRow]);
  } else {
    sheet.appendRow(newRow);
  }
}

function findExistingRow(sheet, date, brand) {
  const values = sheet.getDataRange().getValues();
  const searchDate = String(date).trim();
  const searchBrand = String(brand).trim();

  for (var i = 1; i < values.length; i++) {
    var rowDateStr;
    if (Object.prototype.toString.call(values[i][0]) === '[object Date]') {
      rowDateStr = Utilities.formatDate(values[i][0], Session.getScriptTimeZone(), 'yyyy-MM-dd');
    } else {
      rowDateStr = String(values[i][0]).trim();
    }
    if (rowDateStr === searchDate && String(values[i][1]).trim() === searchBrand) {
      return i + 1;
    }
  }
  return -1;
}

// ============================================================
// Response & Track Events & Leads
// ============================================================

function createResponse(status, message) {
  return ContentService.createTextOutput(JSON.stringify({
    status: status,
    message: message,
    timestamp: new Date().toISOString()
  })).setMimeType(ContentService.MimeType.JSON);
}

function addTrackEvent_(data) {
  const ss = getSS_();
  var sh = ss.getSheetByName('page_logs');
  if (!sh) sh = ss.insertSheet('page_logs');
  ensureTrackHeaders_(sh);
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const brand = (data && data.brand) ? String(data.brand) : 'forward';
    sh.appendRow([
      brand, new Date(), (data && data.ts) ? data.ts : new Date().toISOString(),
      (data && data.session_id) || '', (data && data.course_id) || '',
      (data && data.meinnow_course_type) || '', (data && data.meinnow_course_duration) || ''
    ]);
  } finally {
    lock.releaseLock();
  }
}

function ensureTrackHeaders_(sh) {
  const desiredHeaders = ['brand', 'received_at', 'ts', 'session_id', 'course_id', 'meinnow_course_type', 'meinnow_course_duration'];
  if (sh.getLastRow() === 0) {
    sh.appendRow(desiredHeaders);
    return;
  }
  var headerValues = sh.getRange(1, 1, 1, Math.max(sh.getLastColumn(), desiredHeaders.length)).getValues()[0].map(function (v) { return String(v || '').trim(); });
  if (headerValues[0].toLowerCase() !== 'brand') sh.insertColumnBefore(1);
  sh.getRange(1, 1, 1, desiredHeaders.length).setValues([desiredHeaders]);
}

// ============================================================
// Visibility Metrics (Rohdaten) – Struktur: date, keyword, forward_%, franklin_%, impaqt_%, visibility_total, source, received_at
// ============================================================

function addVisibilityMetrics_(data) {
  const ss = getSS_();
  var sh = ss.getSheetByName('visibility_metrics2');
  if (!sh) sh = ss.insertSheet('visibility_metrics2');
  ensureVisibilityHeaders_(sh);
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    sh.appendRow([
      data.date || '',
      data.keyword || '',
      Number(data.forward_visibility_percent) || 0,
      Number(data.franklin_visibility_percent) || 0,
      Number(data.impaqt_visibility_percent) || 0,
      Number(data.visibility_total) || 0,
      data.source || 'scraper',
      new Date()
    ]);
  } finally {
    lock.releaseLock();
  }
}

function ensureVisibilityHeaders_(sh) {
  const headers = [
    'date',
    'keyword',
    'forward_visibility_percent',
    'franklin_visibility_percent',
    'impaqt_visibility_percent',
    'visibility_total',
    'source',
    'received_at'
  ];
  if (sh.getLastRow() === 0) sh.appendRow(headers);
  else sh.getRange(1, 1, 1, headers.length).setValues([headers]);
}

// ============================================================
// Leads
// ============================================================

function addLead_(data) {
  const ss = getSS_();
  var sh = ss.getSheetByName('leads');
  if (!sh) sh = ss.insertSheet('leads');
  ensureLeadHeaders_(sh);
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const tag = (data && data.tag) ? String(data.tag) : (data && data.route) || (data && data.action) || (data && data.type) || '';
    sh.appendRow([
      new Date(), tag, data.closeLeadID || '', data.course_id || '',
      data.brand || '', data.vertical || '', data.title || ''
    ]);
  } finally {
    lock.releaseLock();
  }
}

function ensureLeadHeaders_(sh) {
  const headers = ['received_at', 'tag', 'closeLeadID', 'course_id', 'brand', 'vertical', 'title'];
  if (sh.getLastRow() === 0) sh.appendRow(headers);
  else sh.getRange(1, 1, 1, headers.length).setValues([headers]);
}

// ============================================================
// Keyword-Gewichte (keywords_vertical.csv) – Sheet "keyword_weights"
// ============================================================

function ensureKeywordWeightsSheet_() {
  const ss = getSS_();
  var sh = ss.getSheetByName('keyword_weights');
  if (!sh) {
    sh = ss.insertSheet('keyword_weights');
    sh.appendRow(['keyword', 'vertical', 'weight']);
    sh.getRange(1, 1, 1, 3).setFontWeight('bold');
  }
  return sh;
}

// ============================================================
// Gewichtete Visibility – aus visibility_metrics2 × keyword_weights
// Führe computeWeightedVisibility_() manuell aus oder per Zeit-Trigger.
// ============================================================

function computeWeightedVisibility_() {
  const ss = getSS_();

  var metricsSh = ss.getSheetByName('visibility_metrics2');
  if (!metricsSh || metricsSh.getLastRow() < 2) {
    Logger.log('visibility_metrics2 leer oder fehlt.');
    return;
  }

  var weightsSh = ss.getSheetByName('keyword_weights');
  if (!weightsSh || weightsSh.getLastRow() < 2) {
    ensureKeywordWeightsSheet_();
    Logger.log('Bitte keyword_weights mit Inhalt von keywords_vertical.csv füllen (keyword, vertical, weight).');
    return;
  }

  var metricsData = metricsSh.getDataRange().getValues();
  var metricsHeaders = metricsData[0].map(function (h) { return String(h || '').trim().toLowerCase(); });
  var weightData = weightsSh.getDataRange().getValues();
  var weightHeaders = weightData[0].map(function (h) { return String(h || '').trim().toLowerCase(); });

  var dateIdx = metricsHeaders.indexOf('date');
  var keywordIdx = metricsHeaders.indexOf('keyword');
  var forwardIdx = metricsHeaders.indexOf('forward_visibility_percent');
  var franklinIdx = metricsHeaders.indexOf('franklin_visibility_percent');
  var impaqtIdx = metricsHeaders.indexOf('impaqt_visibility_percent');
  var totalIdx = metricsHeaders.indexOf('visibility_total');

  var kwKeywordIdx = weightHeaders.indexOf('keyword');
  var kwVerticalIdx = weightHeaders.indexOf('vertical');
  var kwWeightIdx = weightHeaders.indexOf('weight');
  if (dateIdx === -1 || keywordIdx === -1 || kwKeywordIdx === -1 || kwWeightIdx === -1) {
    Logger.log('Erforderliche Spalten in visibility_metrics2 oder keyword_weights fehlen.');
    return;
  }

  var weightByKeyword = {};
  var verticalByKeyword = {};
  for (var w = 1; w < weightData.length; w++) {
    var kw = String(weightData[w][kwKeywordIdx] || '').trim().toLowerCase();
    var wgt = Number(weightData[w][kwWeightIdx]);
    if (kw && !isNaN(wgt)) weightByKeyword[kw] = wgt;
    if (kw && kwVerticalIdx >= 0) verticalByKeyword[kw] = String(weightData[w][kwVerticalIdx] || '').trim();
  }

  var weightedRows = [];
  var weightedHeader = [
    'date', 'keyword', 'vertical',
    'forward_visibility_percent', 'franklin_visibility_percent', 'impaqt_visibility_percent',
    'visibility_total', 'weight',
    'weighted_forward', 'weighted_franklin', 'weighted_impaqt'
  ];
  weightedRows.push(weightedHeader);

  for (var i = 1; i < metricsData.length; i++) {
    var row = metricsData[i];
    var keyword = String(row[keywordIdx] || '').trim();
    var keyLower = keyword.toLowerCase();
    var weight = weightByKeyword[keyLower] != null ? weightByKeyword[keyLower] : 1;
    var vertical = (verticalByKeyword[keyLower] != null && verticalByKeyword[keyLower] !== '') ? verticalByKeyword[keyLower] : '';

    var fwd = Number(row[forwardIdx]) || 0;
    var frk = Number(row[franklinIdx]) || 0;
    var imp = (impaqtIdx >= 0) ? (Number(row[impaqtIdx]) || 0) : 0;
    var tot = (totalIdx >= 0) ? (Number(row[totalIdx]) || 0) : 0;

    weightedRows.push([
      row[dateIdx] || '',
      keyword,
      vertical,
      fwd,
      frk,
      imp,
      tot,
      weight,
      fwd * weight,
      frk * weight,
      imp * weight
    ]);
  }

  var outSh = ss.getSheetByName('visibility_weighted');
  if (!outSh) outSh = ss.insertSheet('visibility_weighted');
  outSh.clear();
  if (weightedRows.length > 0) {
    outSh.getRange(1, 1, weightedRows.length, weightedRows[0].length).setValues(weightedRows);
    outSh.getRange(1, 1, 1, weightedRows[0].length).setFontWeight('bold');
  }

  var sumByKey = {};
  for (var j = 1; j < weightedRows.length; j++) {
    var r = weightedRows[j];
    var d = String(r[0] || '').trim();
    var cat = String(r[2] || '').trim();
    var w = Number(r[7]) || 0;
    if (!w) continue;
    var key = d + '|' + cat;
    if (!sumByKey[key]) {
      sumByKey[key] = { date: d, vertical: cat, sumFwd: 0, sumFrk: 0, sumImp: 0, sumWeight: 0 };
    }
    sumByKey[key].sumFwd += Number(r[8]) || 0;
    sumByKey[key].sumFrk += Number(r[9]) || 0;
    sumByKey[key].sumImp += Number(r[10]) || 0;
    sumByKey[key].sumWeight += w;
  }

  var aggRows = [['date', 'vertical', 'weighted_avg_forward', 'weighted_avg_franklin', 'weighted_avg_impaqt', 'sum_weight']];
  var keys = Object.keys(sumByKey);
  for (var k = 0; k < keys.length; k++) {
    var o = sumByKey[keys[k]];
    var sw = o.sumWeight;
    aggRows.push([
      o.date,
      o.vertical,
      sw ? (o.sumFwd / sw) : 0,
      sw ? (o.sumFrk / sw) : 0,
      sw ? (o.sumImp / sw) : 0,
      sw
    ]);
  }

  var aggSh = ss.getSheetByName('visibility_by_vertical');
  if (!aggSh) aggSh = ss.insertSheet('visibility_by_vertical');
  aggSh.clear();
  if (aggRows.length > 0) {
    aggSh.getRange(1, 1, aggRows.length, aggRows[0].length).setValues(aggRows);
    aggSh.getRange(1, 1, 1, aggRows[0].length).setFontWeight('bold');
  }

  Logger.log('computeWeightedVisibility_: visibility_weighted und visibility_by_vertical aktualisiert.');
}

// ============================================================
// Pivot / Chart
// ============================================================

const PIVOT_SHEET_NAME = 'Pivot Table 9';

function refreshPivotChart() {
  const ss = getSS_();
  var sh = ss.getSheetByName(PIVOT_SHEET_NAME);
  if (!sh) throw new Error('Sheet nicht gefunden: ' + PIVOT_SHEET_NAME);
  sh.getCharts().forEach(function (c) { sh.removeChart(c); });
  var chart = sh.newChart()
    .setChartType(Charts.ChartType.COLUMN)
    .addRange(sh.getDataRange())
    .setOption('useFirstRowAsHeaders', true)
    .setOption('useFirstColumnAsDomain', true)
    .setOption('isStacked', true)
    .setPosition(2, 1, 0, 0)
    .build();
  sh.insertChart(chart);
}
