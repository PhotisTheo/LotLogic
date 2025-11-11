# Free-First Massachusetts Property Data Pipeline

## 1. Goals
- Eliminate the Attom API dependency by replacing each field we currently surface (mortgage, tax, foreclosure, and risk data) with information gathered from public Massachusetts sources.
- Keep ongoing costs at \$0 by favoring public records portals and downloadable municipal rolls; only flag paid options when a free path is impossible or too brittle.
- Provide enough structure that engineers can begin building scrapers/ETL jobs immediately while product keeps Attom as a fallback.

## 2. What Attom Currently Provides
The Attom data model in `leadcrm/leads/models.py:293` stores the exact attributes we must reproduce:

| Category | Fields we use | How UI uses them |
| --- | --- | --- |
| Mortgage | `mortgage_loan_amount`, `mortgage_loan_type`, `mortgage_lender_name`, `mortgage_interest_rate`, `mortgage_term_years`, `mortgage_recording_date`, `mortgage_due_date` | Shown directly on deal sheets; also feed `_calculate_mortgage_balance_from_attom` in `leadcrm/leads/views.py:102` for mortgage balance/equity/monthly payment estimates. |
| Foreclosure | `pre_foreclosure`, `foreclosure_stage`, `foreclosure_recording_date`, `foreclosure_auction_date`, `foreclosure_estimated_value`, `foreclosure_judgment_amount`, `foreclosure_default_amount`, `foreclosure_document_type` | Drive alert badges and foreclosure sections in property modals (`leadcrm/leads/views.py:3284`). |
| Tax | `tax_assessment_year`, `tax_assessed_value`, `tax_amount_annual`, `tax_delinquent_year` | Displayed in the property panel (screenshot) and used for ROI/equity calculations. |
| Risk | `propensity_to_default_score`, `propensity_to_default_decile` | Used for prioritization badges (queried near `leadcrm/leads/views.py:3268`). |
| Raw payload | `raw_response` | Enables troubleshooting; we should continue storing the original source documents/JSON. |

These requirements define the minimum viable data products for the new pipeline.

## 3. Massachusetts Data Sources (Free-First)

### 3.1 Registry of Deeds / Mortgage & Foreclosure Data
- **MassLandRecords.com** covers 28 Registry of Deeds districts (Barnstable, Berkshire Middle, Bristol North/South, Dukes, Essex North/South, Franklin, Hampden, Hampshire, Middlesex North/South, Norfolk, Plymouth, Suffolk, Worcester North/South, etc.).
- Provides free search by address/owner; returns index data (book/page, instrument type, recording date) plus downloadable TIFF/PDF images of deeds, mortgages, discharges, LIS pendens, foreclosure notices.
- Strategy: scripted form submissions (POST w/ __VIEWSTATE for ASP.NET), download PDF/TIFF, parse index table for lender name, amount, document type, dates; run OCR (Tesseract) when the amount is only embedded in images.
- Coverage gaps: Nantucket registry has its own portal but still free; add custom adapter.

### 3.2 Municipal Assessors / Tax Rolls
- **Vision Government Solutions** (~175 MA municipalities) — often provide a CSV/Excel export link plus detailed HTML property cards. Fields include assessed value (land/building/total), tax year, exemptions, and recent sales.
- **Patriot Properties** (~60 towns) — typically provide printable property cards; some have CSV downloads. When only HTML is available, we can scrape tabular data.
- **Tyler iasWorld / Municipal Systems** (~40 towns) — mix of CSV downloads and paginated HTML.
- **CAI Technologies / AxisGIS** (~30 towns) — GIS portal with parcel attributes accessible through JSON endpoints (free with no auth).
- **Custom portals** (~40 towns) — small towns that use PDFs or basic tables. We can scrape/parse or, if the PDF is static, download once per revaluation cycle.
- **Boston, Cambridge, Somerville, Worcester, etc.** publish full assessor/tax rolls on Open Data portals (CSV/GeoJSON).

### 3.3 Supplemental Free Sources
- **Massachusetts Department of Revenue (DOR)**: certifies values and sometimes posts assessment summaries; good for QA.
- **County/town foreclosure auction calendars**: some sheriffs (e.g., Worcester, Plymouth) post notices that can confirm foreclosure stages.
- **US Postal Service APIs / libpostal**: free tools for address normalization (already open-source).

## 4. Free-First Scraper & ETL Architecture

### 4.1 Acquisition Layer
1. **Registry worker**  
   - Input: parcel address/APN from our parcels table.  
   - Action: query MassLandRecords via scripted HTTP session, collect index rows matching parcel owner or address, download documents tagged as Mortgage, Assignment, Discharge, Foreclosure, etc.  
   - Output: normalized mortgage/foreclosure JSON + PDF path stored in S3 (or local bucket).  
   - Tools: `requests`, `beautifulsoup4`, `pdfminer.six`, `pytesseract`, `opencv-python` (all free/open-source).
2. **Assessor worker**  
   - For each platform, build a module (`sources/vision.py`, `sources/patriot.py`, …) that handles login-less downloads or HTML scraping.  
   - Output: structured record `{parcel_id, muni_code, tax_year, assessed_land, assessed_building, assessed_total, tax_levy}`.
3. **Scheduling**  
   - Use Celery or RQ with Redis (already free) to queue jobs per parcel or municipality.  
   - Add throttling/backoff so we respect site limits (e.g., 1 req/s per municipality domain).

### 4.2 Parsing & Normalization
- **Document parsing**:  
  - Extract lender, amount, dates using regex templates keyed by document type (Mortgage, Assignment, Notice of Sale, etc.).  
  - Fallback to OCR for scanned images; keep extracted text for audit.
- **Tax roll parsing**:  
  - Use pandas to load CSV/Excel; for HTML, use `pandas.read_html` or BeautifulSoup.  
  - Normalize parcel IDs to a canonical format (Map-Block-Lot padded) and map municipal codes to MassGIS LOC_IDs.
- **Feature derivation**:  
  - Monthly payment = amortization calculation using scraped principal, interest rate (if not present, estimate from Freddie Mac weekly data per recording year).  
  - Propensity proxy = rule-based score from mortgage age, LTV (using our parcel value), tax delinquency signals.
- **Quality checks**:  
  - Reject records where `mortgage_loan_amount <= 0` or `tax_assessed_value` is missing.  
  - Cross-check addresses via libpostal + USPS to ensure we hit the correct parcel.

### 4.3 Storage & Serving
- **Raw artifacts**: store PDFs/CSVs in object storage (S3-compatible) with metadata referencing parcel + source URL/time.
- **Processed tables**: new Django models (or existing `AttomData` replacement) to hold mortgage/tax/foreclosure fields plus provenance (`source_name`, `scraped_at`).
- **Caching**: mark data fresh for 90 days for taxes, 30 days for mortgage/foreclosure; background jobs refresh as needed.
- **Observability**: log scrape success rate per municipality; Prometheus/Grafana (open-source) for alerts when success drops.

## 5. Implementation Roadmap

| Phase | Duration | Deliverables |
| --- | --- | --- |
| 0. Prep (Week 1) | 1 week | Source matrix spreadsheet (28 registries + 351 municipalities) with portal URLs, platform vendor, data format, free/paid flag, scrape difficulty notes. |
| 1. Infrastructure (Week 2) | 1 week | Celery/RQ workers, storage bucket, shared schema definition, base scraper SDK (session handling, rate limiting, logging). |
| 2. Registry Pipeline (Weeks 3-6) | 4 weeks | MassLandRecords adapter covering top 10 high-volume districts; OCR + parser templates for mortgage/foreclosure docs; nightly job to refresh active parcels. |
| 3. Assessor Pipeline (Weeks 4-10) | 6 weeks (overlapping) | Vision + Patriot + Tyler module support; ingest top 50 municipalities by lead volume; normalization rules for assessed values/tax years. |
| 4. Derived Features & QA (Weeks 8-12) | 4 weeks | Implement mortgage balance + payment estimates using scraped data; add QC dashboards; define fallback logic when data missing. |
| 5. Long Tail & Attom Sunset (Weeks 12-16) | 4 weeks | Cover remaining municipalities (custom scrapers or manual CSV uploads); add propensity proxy; feature flag to disable Attom once coverage >95%. |

## 6. Immediate Next Steps
1. Build the detailed source matrix (include URL, platform, captcha notes, download link).  
2. Stand up the scraper SDK repo structure (`sources/{registries,assessors}`, `parsers`, `normalizers`).  
3. Prototype the MassLandRecords scraper against a single parcel to validate form submission + PDF parsing.  
4. Document legal/compliance guidelines (respect robots.txt, throttle, identify ourselves in User-Agent).

This plan keeps all data acquisition free by default, while still noting where optional paid feeds could save time if we later choose to invest.
