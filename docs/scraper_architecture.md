# Free-First Scraper Architecture

This document outlines the code structure and runbook for the Massachusetts data pipeline. The focus is on free sources, so everything here uses open-source dependencies and avoids paid APIs.

## 1. Repository Structure

```
leadcrm/
├─ data_pipeline/
│  ├─ __init__.py
│  ├─ settings.py            # global config: storage paths, throttle defaults, OCR toggle
│  ├─ jobs/
│  │  ├─ registry_job.py     # orchestrates MassLandRecords fetch + parsing
│  │  ├─ assessor_job.py     # runs per-town assessor ingestion
│  │  └─ task_queue.py       # Celery/RQ task definitions
│  ├─ sources/
│  │  ├─ registries/
│  │  │  ├─ base.py          # BaseRegistrySource (session mgmt, search helpers)
+│  │  │  ├─ masslandrecords.py
│  │  │  └─ nantucket.py     # for non-ACS edge cases
│  │  ├─ assessors/
│  │  │  ├─ base.py          # BaseAssessorSource (parcel lookup, CSV/HTML loader)
│  │  │  ├─ vision.py
│  │  │  ├─ patriot.py
│  │  │  ├─ tyler.py
│  │  │  ├─ axisgis.py
│  │  │  └─ pdf_static.py
│  ├─ parsers/
│  │  ├─ mortgage_parser.py  # regex + ML extraction for lenders/amounts
│  │  ├─ foreclosure_parser.py
│  │  └─ assessor_parser.py
│  ├─ normalizers/
│  │  ├─ addresses.py        # libpostal/USPS helpers
│  │  ├─ parcels.py          # Map-Block-Lot normalization, loc_id mapping
│  │  └─ financials.py       # monthly payment, propensity proxy
│  ├─ storage/
│  │  ├─ files.py            # S3/local storage abstraction for PDFs/CSVs
│  │  └─ database.py         # upsert logic into Django models
│  ├─ logging.py             # structured logging + metrics
│  └─ cli.py                 # developer CLI to run jobs manually
└─ docs/
   ├─ free_first_pipeline.md
   └─ ma_source_matrix.md
```

### Base classes
- `BaseRegistrySource`: handles session bootstrapping (`__VIEWSTATE` extraction), throttling, result pagination, and document download. Subclasses implement `search_parcel`, `fetch_document`, and `parse_index_row`.
- `BaseAssessorSource`: provides HTTP helpers, caching of exports, and `fetch_parcel_data(parking_id)` returning normalized dicts.
- Both base classes emit `ScrapeResult` dataclasses containing metadata (source name, request time, raw artifacts path) to keep provenance consistent.

## 2. Job Configuration

| Config Item | Location | Notes |
| --- | --- | --- |
| `DATA_PIPELINE_STORAGE_ROOT` | `.env` + `data_pipeline/settings.py` | Points to S3 bucket or local path for raw files. |
| `SCRAPER_USER_AGENT` | settings | Identify the app to registries/assessors. |
| `REGISTRY_THROTTLE_RPS` | settings | Default 0.5 req/s per registry; override in source matrix JSON. |
| `ASSESSOR_REFRESH_DAYS` | settings | 365-day default; some cities refresh quarterly. |
| `SOURCE_MATRIX` | `data_pipeline/config/sources.json` | Generated from `docs/ma_source_matrix.md`; contains municipality metadata, platform, URLs, and throttling flags. |

`sources.json` example:
```json
{
  "registries": [
    {
      "id": "suffolk",
      "name": "Suffolk County",
      "adapter": "masslandrecords",
      "base_url": "https://www.masslandrecords.com/Suffolk",
      "throttle_rps": 0.4,
      "notes": "Instrument filter required for performance.",
      "instrument_types": ["MORTGAGE", "LIS PENDENS"],
      "results_table_id": "ctl00_cphMain_gvDocuments",
      "document_link_selector": "a",
      "search_modes": {
        "owner": {
          "form_path": "DocumentSearch.aspx",
          "fields": {
            "owner": "ctl00$cphMain$txtName"
          },
          "submit_field": "ctl00$cphMain$btnSearchName"
        },
        "address": {
          "form_path": "DocumentSearch.aspx",
          "fields": {
            "street_number": "ctl00$cphMain$txtStreetNo",
            "street_name": "ctl00$cphMain$txtStreetName"
          },
          "submit_field": "ctl00$cphMain$btnSearchStreet"
        }
      }
    }
  ],
  "municipalities": [
    {
      "muni_code": "2507000",
      "name": "Boston",
      "platform": "vision",
      "url": "https://gis.vgsi.com/boston/",
      "download": "https://data.boston.gov/dataset/property-assessment",
      "refresh_days": 90,
      "captcha": false
    }
  ]
}
```

The job queue consumes this JSON to decide which adapter to instantiate.

**MassLandRecords config tips**
- `search_modes` describes how to populate the ASP.NET form. For each mode, set `form_path` (relative URL), `fields` mapping from logical keys (`owner`, `owner_first`, `owner_last`, `street_number`, `street_name`, `address`, etc.) to real input names, and either `submit_field` (preferred) or `event_target`. When a dropdown must switch between owner vs. address searches (e.g., `SearchCriteriaName1$DDL_SearchName`), add it under `static_fields` with the value captured from the option list (e.g., `"Street Name"`).
- `instrument_types` lets you limit parsed results to mortgages/foreclosure docs only; omit to ingest everything.
- `results_table_id`/`document_link_selector` should match the HTML table and anchor used by the registry. Use browser dev tools once to capture the real IDs per county and drop them into `sources.json`.
- Some counties (Essex, Nantucket) still use the legacy ALIS interface instead of ASP.NET. The same adapter works—just point `form_path` at the `ALIS/WW400R.HTM?...` URL and map the `W9***` field names you captured from dev tools.
- When registries require separate first/last/middle name inputs (Middlesex), use the logical keys `owner_first`, `owner_middle`, and `owner_last`. The scraper splits the owner string accordingly before submitting the form.

**Vision assessor config tips**
- For CKAN-backed cities (Boston), set `resource_id`, `tax_year`, and `source_url`. The adapter will call `/api/3/action/datastore_search` and handle pagination automatically.
- For CSV-only Vision towns, set `download_url`; the adapter will eventually stream and parse the file (placeholder today).
- Run `python -m leadcrm.data_pipeline.cli assessor-run --municipality <code>` to execute a one-off ingestion once the config entry exists.

## 3. Task Queue & Scheduling
- Use Celery (with Redis) so we can schedule daily registry refreshes and weekly/monthly assessor loads. Each task includes:
  - `source_id` (registry or municipality)
  - `parcel_identifiers` (list; registry jobs might include owner + address)
  - `force_refresh` flag (ignore cache)
- Celery beat schedules:
  - `registry.daily`: refresh parcels touched in past 30 days.
  - `assessor.monthly`: refresh by municipality according to `refresh_days`.
  - `backfill.queue`: handle long-tail tasks manually uploaded via CLI.

## 4. Prototype Registry Scraper Workflow
1. **Select parcel**: CLI command `python -m leadcrm.data_pipeline.cli registry-run --registry suffolk --address "123 Main St, Boston"` loads parcel metadata (owner, loc_id).
2. **Instantiate adapter**: `MassLandRecordsSource` inherits `BaseRegistrySource` and logs in to Suffolk portal, maintaining session cookies.
3. **Search & collect results**: Adapter submits search form using owner/addr, paginates results, and filters to instrument types `[Mortgage, MORTGAGE, LIS PENDENS]`.
4. **Download documents**: For each relevant entry, adapter downloads PDF/TIFF to storage, returns metadata (book/page, doc_date, lender text snippet).
5. **Parse content**: `mortgage_parser` extracts loan amount, lender, term, interest rate (if textual). OCR executed asynchronously via `pytesseract` when needed.
6. **Normalize & save**: `normalizers.parcels` maps registry APN to our `loc_id`. `storage.database` upserts into `AttomData` replacement table, setting provenance to `masslandrecords`.
7. **Emit metrics**: Task logs success/failure counts, OCR duration, and attaches raw artifacts path for audit.

## 5. Testing Strategy
- Unit tests for each adapter using recorded HTML/PDF fixtures (store in `tests/fixtures/registries/...`).
- Integration test (CI optional) that runs the CLI in “snapshot” mode against cached HTML to verify parser outputs.
- Continuous QA job verifying that daily scrapes produce non-empty records; alert when fields are missing (e.g., mortgage amount null rate >20%).

This architecture lets us onboard free sources incrementally: start with `masslandrecords` registry adapter, then add `vision` assessor module and expand coverage per the source matrix.
