# Was du im Google Sheet machen musst

## Tabellen unverändert lassen
- **raw_data** – bleibt
- **page_logs** – bleibt  
- **navigation_logs** – bleibt
- **leads** – bleibt
- **Pivot Table 9** – bleibt (für refreshPivotChart)

---

## visibility_metrics2 anpassen

Diese Tabelle wird vom Scraper (POST `action: 'visibility_metrics'`) beschrieben.

**Soll-Struktur (8 Spalten):**

| date | keyword | forward_visibility_percent | franklin_visibility_percent | impaqt_visibility_percent | visibility_total | source | received_at |
|------|---------|----------------------------|-----------------------------|---------------------------|------------------|--------|-------------|

**Was du tun musst:**
1. Alte Spalte **category** löschen (falls vorhanden).
2. Zwei neue Spalten einfügen: **impaqt_visibility_percent** und **visibility_total** (z. B. nach `franklin_visibility_percent`).
3. Zwei Spalten sicherstellen: **source** und **received_at** (hattest du schon).

Wenn die Tabelle bisher nur 6 Spalten hatte: Header-Zeile in Zeile 1 auf genau die 8 Spalten oben setzen. Alte Datenzeilen können bleiben; neue Einträge haben dann alle 8 Spalten.

---

## Neue Tabellen hinzufügen

### 1. keyword_weights (du füllst sie einmal)

- **Anlegen:** Entweder manuell einen neuen Tab „keyword_weights“ anlegen oder einmal die Funktion **ensureKeywordWeightsSheet_** im Script ausführen – dann wird das leere Sheet erstellt.
- **Inhalt:** Inhalt von **keywords_vertical.csv** einfügen (z. B. CSV in Excel/Sheets öffnen und hier reinkopieren).
- **Spalten:**  
  **keyword** | **vertical** | **weight**

Ohne gefülltes **keyword_weights** funktioniert die Gewichtung nicht (computeWeightedVisibility_ bricht ab oder meldet es).

### 2. visibility_weighted (nur vom Script)

- Wird **nur vom Script** geschrieben, wenn du **computeWeightedVisibility_** ausführst.
- Du musst **nichts** anlegen – das Script legt das Sheet bei Bedarf an und überschreibt den Inhalt.

### 3. visibility_by_vertical (nur vom Script)

- Wird **nur vom Script** geschrieben, wenn du **computeWeightedVisibility_** ausführst.
- Du musst **nichts** anlegen – das Script legt das Sheet bei Bedarf an und überschreibt den Inhalt.

---

## Nichts löschen

- **raw_data**, **page_logs**, **navigation_logs**, **leads**, **Pivot Table 9** nicht löschen.

---

## Kurz-Checkliste

- [ ] **visibility_metrics2:** 8 Spalten (category entfernt, impaqt_visibility_percent + visibility_total + source + received_at dabei)
- [ ] **keyword_weights:** Tab angelegt, Inhalt von keywords_vertical.csv eingefügt (keyword, vertical, weight)
- [ ] **visibility_weighted** und **visibility_by_vertical:** können leer bleiben, werden vom Script erstellt/überschrieben
- [ ] Script **computeWeightedVisibility_** einmal (oder per Trigger) ausführen, damit die gewichteten Sheets befüllt werden
