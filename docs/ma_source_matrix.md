# Massachusetts Property Data Source Matrix (Free-First)

This document inventories public sources we can target before paying for licensed feeds. It will grow as we enumerate every municipality; Version 0 focuses on registries plus the major assessor platforms that cover most Massachusetts residents.

## 1. Registry of Deeds Sources

| District | Counties / Coverage | Portal URL | Platform | Free Access? | Scrape Notes |
| --- | --- | --- | --- | --- | --- |
| Barnstable | Barnstable County (Cape Cod) | https://www.masslandrecords.com/Barnstable | ACS MassLandRecords | Yes | ASP.NET form; supports book/page & party search. TIFF images; need OCR for older scans. |
| Berkshire Middle | City of Pittsfield & central Berkshire | https://www.masslandrecords.com/BerkshireMiddle | ACS MassLandRecords | Yes | Similar to Barnstable; sometimes rate-limits after ~50 docs/hour. |
| Berkshire Northern | Williamstown/North Adams area | https://www.masslandrecords.com/BerkshireNorth | ACS MassLandRecords | Yes | Occasionally redirects to index.aspx?CountyName=Berkshire; persist cookies. |
| Berkshire Southern | Great Barrington region | https://www.masslandrecords.com/BerkshireSouth | ACS MassLandRecords | Yes | Uses same viewstate tokens; minimal traffic, so keep polite delays. |
| Bristol North | Taunton-based properties | https://www.masslandrecords.com/BristolNorth | ACS MassLandRecords | Yes | Large volume; enable paging support for >50 results. |
| Bristol South | Fall River/New Bedford | https://www.masslandrecords.com/BristolSouth | ACS MassLandRecords | Yes | Requires party search for best coverage; use instrument type filters (Mortgage, Lien). |
| Dukes | Martha’s Vineyard | https://www.masslandrecords.com/Dukes | ACS MassLandRecords | Yes | Low volume; documents often PDF already (no TIFF). |
| Essex North | Lawrence/Haverhill | https://www.masslandrecords.com/EssexNorth | ACS MassLandRecords | Yes | Many LIS pendens filings; ensure parser handles foreclosure docs. |
| Essex South | Salem/Beverly | https://www.masslandrecords.com/EssexSouth | ACS MassLandRecords | Yes | Frequent session expirations; re-login for long jobs. |
| Franklin | Entire Franklin County | https://www.masslandrecords.com/Franklin | ACS MassLandRecords | Yes | Offers CSV index download per query; harvest metadata before document fetch. |
| Hampden | Springfield region | https://www.masslandrecords.com/Hampden | ACS MassLandRecords | Yes | Heavy traffic; throttle to <1 req/s. |
| Hampshire | Northampton/Amherst | https://www.masslandrecords.com/Hampshire | ACS MassLandRecords | Yes | Document viewer returns PDF by default—no TIFF conversion needed. |
| Middlesex North | Cambridge/Somerville | https://www.masslandrecords.com/MiddlesexNorth | ACS MassLandRecords | Yes | Many scanned images; add OCR queue. |
| Middlesex South | Framingham, Newton, etc. | https://www.masslandrecords.com/MiddlesexSouth | ACS MassLandRecords | Yes | Highest volume registry; consider caching index pages to avoid repeated searches. |
| Nantucket | Nantucket County | https://www.masslandrecords.com/Nantucket | ACS MassLandRecords | Yes | Alternate site https://www.nantucket-ma.gov/DocumentCenter; but defaults to MassLandRecords. |
| Norfolk | Dedham + South Shore | https://www.masslandrecords.com/Norfolk | ACS MassLandRecords | Yes | Provides JSON index endpoint behind the UI; inspect network calls for easier parsing. |
| Plymouth | Whole Plymouth County | https://www.masslandrecords.com/Plymouth | ACS MassLandRecords | Yes | Offers nightly “Recorded Today” export; still free. |
| Suffolk | Boston, Chelsea, Revere, Winthrop | https://www.masslandrecords.com/Suffolk | ACS MassLandRecords | Yes | Enables instrument type filtering via querystring; required for performance. |
| Worcester North | Fitchburg/Leominster | https://www.masslandrecords.com/WorcesterNorth | ACS MassLandRecords | Yes | Rarely updates overnight; schedule scrapes mid-morning. |
| Worcester South | Worcester Metro | https://www.masslandrecords.com/WorcesterSouth | ACS MassLandRecords | Yes | Slightly different field names in index table; adjust parser. |

*All registries other than Suffolk use the same ACS e-Recording stack; Nantucket also mirrors it. No paid logins required as long as we respect their usage policy.*

## 2. Municipal Assessor / Tax Data Platforms

| Platform | Est. MA Municipalities | Example Towns | Access Pattern | Download Format | Free Path Notes |
| --- | --- | --- | --- | --- | --- |
| Vision Government Solutions | ~175 | Boston, Cambridge, Somerville, Newton, Barnstable, Springfield | Search via https://gis.vgsi.com/{TownName} | HTML property card + optional CSV/Excel export link | Many towns expose “Download” button (CSV). Others require scraping HTML tables; no CAPTCHA. |
| Patriot Properties | ~60 | Brookline, Quincy, Waltham, Danvers, Medford | http://www.patriotproperties.com/vision/Applications/ParcelSearch/?Town={Town} style | HTML-only property cards | Need to submit parcel ID via POST; HTML structured tables make parsing straightforward. |
| Tyler/iRespond/iASWorld | ~40 | Worcester, Lowell, Lawrence, Springfield commercial division | Mix of https://data.tylertech.com/ or custom iasWorld domains | CSV export for entire roll or per-query Excel | Some require selecting “Download Full Tax Roll” but still public; watch for 20–30 MB files. |
| CAI Technologies / AxisGIS | ~30 | Amherst, Northampton, Provincetown, Concord | https://maps.axisgis.com/{Town}/?parcel=... (REST JSON) | JSON features via `/arcgis/rest/services/.../query` | Provide API-style query with format=json; free though throttle to 200 requests/min. |
| Munis / MUNET (Tyler) | ~15 | Beverly, Peabody, Gloucester | https://munisweb.townname.gov/ | HTML tables + CSV export per tax year | Some towns require selecting “Real Estate Tax Bill” to view assessed values; no login. |
| ROK Technologies / PeopleGIS | ~10 | Newton GIS (legacy), Brookline GIS | ArcGIS REST endpoints | JSON | Already used for GIS layers; include assessed values sometimes, but completeness varies. |
| Static PDF Rolls | ~20 | Small towns (e.g., Mount Washington, Monroe) | PDF downloads from town clerk sites | PDF | Download once per fiscal year; run tabular OCR + heuristics. |
| Open Data Portals | 5 major cities | Boston (Analyze Boston), Cambridge Open Data, Worcester, Springfield, Somerville | CKAN/Socrata portals | CSV/GeoJSON API | Ideal for bulk pulls; include historical assessments and tax bills. |

## 3. Municipal Tracking Template

Use the following columns when expanding this matrix to all 351 municipalities:
1. Municipality name + Muni code.  
2. Primary assessor portal URL.  
3. Platform/vendor.  
4. Data fields confirmed (assessed total, land, building, tax, exemptions).  
5. Download method (CSV, Excel, HTML scrape, PDF).  
6. Captcha or rate-limit considerations.  
7. Last confirmed refresh frequency.  
8. Optional paid alternative (if any).

We should track this in a shared spreadsheet (or Django model) so the scraper scheduler can mark coverage status per town.

## 4. Potential Blockers & Notes
- **Session-based portals**: ACS sites rely on `__VIEWSTATE` and session cookies; scraper SDK must manage stateful POST chains and auto-refresh when tokens expire.
- **TIFF/PDF OCR load**: Mortgage PDFs pre-2004 are often scans. Encourage batching to an OCR queue so ingestion jobs stay fast.
- **AxisGIS throttling**: 429 limits apply when spamming table layers; set concurrency to <3 per domain.
- **Static PDFs**: Some tiny towns only upload annual PDF “Commitment Books.” These remain free but require manual parsing per year; consider volunteer backlog or low-priority queue.
- **Robots.txt compliance**: MassLandRecords allows automated access if not abusive; log our UA and slow down to avoid bans.

This matrix is the starting point for engineering tasks: implement the registry scraper (section 1), then tackle assessor platforms in order of coverage (Vision → Patriot → Tyler → AxisGIS → custom/PDF). Update this file as each municipality gets classified.
