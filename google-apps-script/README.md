# Google Apps Script: Visibility + Gewichtung

## Ablauf

1. **visibility_metrics2** – Rohdaten (kommen per POST vom Scraper)  
   Spalten: `date`, `keyword`, `forward_visibility_percent`, `franklin_visibility_percent`, `impaqt_visibility_percent`, `visibility_total`, `source`, `received_at`  
   (Kein `category` im Sheet – Vertical kommt bei der Gewichtung aus **keyword_weights**.)

2. **keyword_weights** – Gewichte aus `keywords_vertical.csv`  
   Inhalt des Sheets: Spalten `keyword`, `vertical`, `weight` (z. B. CSV einfügen oder aus `keywords_vertical.csv` importieren).

3. **computeWeightedVisibility_()** ausführen → erzeugt/aktualisiert:
   - **visibility_weighted**: pro Zeile aus visibility_metrics2 + `weight` aus keyword_weights + `weighted_forward`, `weighted_franklin`, `weighted_impaqt` (= Visibility × weight).
   - **visibility_by_vertical**: pro Datum + Vertical ein gewichteter Durchschnitt (z. B. `sum(forward_% × weight) / sum(weight)`).

## Was du im bestehenden Script ändern musst

1. **addVisibilityMetrics_** und **ensureVisibilityHeaders_** durch die Versionen aus `visibility_metrics_with_weights.gs` ersetzen (inkl. impaqt und visibility_total).

2. **ensureKeywordWeightsSheet_** und **computeWeightedVisibility_** aus `visibility_metrics_with_weights.gs` in dein Script kopieren.

3. **keyword_weights** befüllen:  
   In Google Sheets Tab „keyword_weights“ anlegen (falls noch nicht da), Kopfzeile: `keyword`, `vertical`, `weight`.  
   Inhalt von `keywords_vertical.csv` einfügen (z. B. CSV in Sheets importieren und in dieses Sheet kopieren).

4. **Gewichtung ausführen** (nach neuen Visibility-POSTs oder bei Bedarf):  
   Im Apps-Script-Editor Funktion **computeWeightedVisibility_** auswählen und „Ausführen“ klicken. Optional: Zeit-Trigger einrichten, der `computeWeightedVisibility_` z. B. täglich ausführt.

## Optional: Gewichtung nach jedem POST

In **addVisibilityMetrics_** am Ende (nach `lock.releaseLock();`) einfügen:

```javascript
computeWeightedVisibility_();
```

Dann wird nach jedem eingehenden Visibility-POST automatisch gewichtet (kann bei vielen Keywords etwas dauern).
