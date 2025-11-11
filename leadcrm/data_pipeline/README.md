# Free-First Data Pipeline

A comprehensive data scraping pipeline to replace the expensive Attom API by collecting property data directly from free Massachusetts public sources.

## Overview

This pipeline scrapes and processes:
- **Registry of Deeds data**: Mortgages, foreclosures, liens from MassLandRecords
- **Assessor data**: Tax assessments, property values from municipal sources
- **Document parsing**: Extracts loan amounts, interest rates, terms from PDFs/TIFFs
- **Corporate filings**: LLC owners, business contacts from MA Secretary of Commonwealth

All data is stored in Django models (`AttomData` for property data, `CorporateEntity` for LLC owners) with full provenance tracking.

## Features

✅ **20 Registry Districts Configured** - All Massachusetts Registry of Deeds
✅ **Smart Form Handling** - Handles both ASP.NET and legacy ALIS forms
✅ **Document Download** - Automatically downloads PDF/TIFF documents
✅ **Intelligent Parsing** - Extracts mortgage data using regex + ML patterns
✅ **OCR Support** - Processes scanned documents (requires Tesseract)
✅ **Database Integration** - Saves directly to Django models (AttomData, CorporateEntity)
✅ **90-Day Caching** - Automatically skips scraping if data is fresh
✅ **180-Day Corporate Caching** - LLC owner data cached for 6 months
✅ **Throttling & Rate Limiting** - Respects site limits automatically
✅ **Provenance Tracking** - Full audit trail of data sources
✅ **LLC Owner Lookup** - Automatically resolves corporate ownership to actual people

## Architecture

```
data_pipeline/
├── sources/           # Scraper adapters
│   ├── registries/   # Registry of Deeds scrapers
│   ├── assessors/    # Municipal assessor scrapers
│   └── corporate/    # Corporate filing scrapers (LLC owners)
├── parsers/          # Document parsing (PDF/TIFF)
├── normalizers/      # Data normalization
├── storage/          # Database persistence
├── jobs/             # Orchestration jobs
└── config/           # Registry configurations
```

## Quick Start

### Installation

```bash
# Activate virtual environment
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM
source venv/bin/activate

# Install dependencies
cd leadcrm
pip install -r requirements.txt

# For OCR support, install Tesseract (macOS)
brew install tesseract
```

### Usage

```bash
# Show all configured registries
python -m data_pipeline.cli show-config

# Test scraping Suffolk registry by owner (dry-run)
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --owner "Smith" \
  --dry-run

# Scrape by address and save to database (respects 90-day cache)
python -m data_pipeline.cli registry-run \
  --registry plymouth \
  --address "123 Main St"

# Force refresh even if cached data exists
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --loc-id "2507000_0501234000" \
  --force-refresh

# Custom cache age (30 days instead of default 90)
python -m data_pipeline.cli registry-run \
  --registry norfolk \
  --owner "Johnson" \
  --max-cache-age 30

# Scrape assessor data
python -m data_pipeline.cli assessor-run \
  --municipality 2507000 \
  --parcel-id 0501234000

# Look up LLC owner (when GIS shows corporate ownership)
python -m data_pipeline.cli corporate-run \
  --entity-name "ABC REALTY LLC"

# Force refresh LLC data even if cached
python -m data_pipeline.cli corporate-run \
  --entity-name "XYZ PROPERTIES LLC" \
  --force-refresh

# Dry-run to see what would be scraped
python -m data_pipeline.cli corporate-run \
  --entity-name "MAIN STREET LLC" \
  --dry-run
```

## Configured Registries

All 20 Massachusetts Registry of Deeds districts are configured:

| Registry | Coverage | Throttle | Notes |
|----------|----------|----------|-------|
| Suffolk | Boston, Chelsea, Revere, Winthrop | 0.4 RPS | Instrument filters required |
| Essex North/South | Lawrence, Salem, Beverly | 0.4 RPS | Uses ALIS WW400 form |
| Middlesex North/South | Cambridge, Newton, Framingham | 0.4 RPS | Highest volume |
| Worcester North/South | Worcester, Fitchburg | 0.4 RPS | Address search available |
| Norfolk | Dedham, South Shore | 0.4 RPS | JSON endpoint available |
| Plymouth | Plymouth County | 0.4 RPS | Nightly exports available |
| Barnstable | Cape Cod | 0.4 RPS | TIFF images common |
| Hampden | Springfield | 0.3 RPS | Heavy traffic, slower |
| Hampshire | Northampton, Amherst | 0.4 RPS | PDFs by default |
| Franklin | Franklin County | 0.4 RPS | CSV index downloads |
| Berkshire (N/M/S) | Berkshires | 0.3-0.4 RPS | Rate limits on Middle |
| Bristol North/South | Taunton, Fall River, New Bedford | 0.4 RPS | Large volume |
| Dukes | Martha's Vineyard | 0.4 RPS | Low volume |
| Nantucket | Nantucket | 0.4 RPS | Mirrors MassLandRecords |

## Data Flow

```
1. CLI Command
   ↓
2. Registry Job (jobs/registry_job.py)
   ↓
3. MassLandRecords Adapter (sources/registries/masslandrecords.py)
   ├─ Submit search form
   ├─ Parse results table
   └─ Download documents
   ↓
4. Mortgage Parser (parsers/mortgage_parser.py)
   ├─ Extract PDF text (pdfplumber)
   ├─ Or run OCR (pytesseract)
   └─ Parse loan data (regex)
   ↓
5. Database Storage (storage/database.py)
   └─ Save to AttomData model
```

## Configuration

Registry configurations are in `config/sources.json`:

```json
{
  "id": "suffolk",
  "name": "Suffolk County Registry of Deeds",
  "adapter": "masslandrecords",
  "base_url": "https://www.masslandrecords.com/Suffolk",
  "throttle_rps": 0.4,
  "instrument_types": ["MORTGAGE", "LIS PENDENS"],
  "search_modes": {
    "owner": {
      "form_path": "DocumentSearch.aspx",
      "fields": {
        "owner": "ctl00$cphMain$txtName"
      },
      "submit_field": "ctl00$cphMain$btnSearchName"
    }
  }
}
```

## Data Mapping to Django Models

### AttomData Model (Property Data)

#### Mortgage Records
| Scraped Field | AttomData Field |
|---------------|----------------|
| loan amount | `mortgage_loan_amount` |
| lender name | `mortgage_lender_name` |
| interest rate | `mortgage_interest_rate` |
| term (years) | `mortgage_term_years` |
| recording date | `mortgage_recording_date` |

#### Foreclosure Records
| Scraped Field | AttomData Field |
|---------------|----------------|
| LIS PENDENS instrument | `pre_foreclosure = True` |
| filing date | `foreclosure_recording_date` |
| stage | `foreclosure_stage` |

#### Tax Assessment Records
| Scraped Field | AttomData Field |
|---------------|----------------|
| assessed value | `tax_assessed_value` |
| tax amount | `tax_amount_annual` |
| tax year | `tax_assessment_year` |

### CorporateEntity Model (LLC Owner Data)

When GIS parcels show LLC ownership (e.g., "ABC REALTY LLC"), this scraper automatically resolves the actual owner:

| Scraped Field | CorporateEntity Field | Use Case |
|---------------|----------------------|----------|
| entity name | `entity_name` | Legal LLC name |
| entity ID | `entity_id` | State filing number |
| entity type | `entity_type` | LLC, Corp, LLP, etc. |
| status | `status` | Active, Dissolved, etc. |
| principal name | `principal_name` | **Actual owner/manager name** |
| principal title | `principal_title` | Managing Member, President, etc. |
| business phone | `business_phone` | **Contact phone number** |
| business address | `business_address` | Principal office address |
| registered agent | `registered_agent` | Legal service agent |
| formation date | `formation_date` | When LLC was formed |
| last annual report | `last_annual_report` | Most recent filing date |

**Cache**: 180 days (6 months) - LLC officers rarely change

## Mortgage Parser

The parser extracts data from PDF/TIFF documents using multiple strategies:

### Text Extraction
- **PDFs**: Uses `pdfplumber` for text-based PDFs
- **TIFFs**: Uses `pytesseract` OCR for scanned images

### Data Extraction Patterns

**Loan Amount**:
- "Principal Amount: $450,000"
- "Loan Amount: $450000.00"
- "Sum of Four Hundred Fifty Thousand Dollars"

**Interest Rate**:
- "Interest Rate: 5.25%"
- "At a rate of 5.25% per annum"
- "Bearing interest at 5.25%"

**Term**:
- "Term: 30 years"
- "360 months term"

**Lender**:
- "Lender: Wells Fargo Bank"
- "From: Bank of America"

## Database Storage

All records are saved to the `AttomData` model with:

- **Automatic upserts** - Updates existing records by `loc_id`
- **Provenance tracking** - All sources stored in `raw_response` JSON field
- **Timestamps** - `created_at` and `last_updated` for cache management

Example provenance:
```json
{
  "scrape_sources": [
    {
      "source": "registry",
      "registry_id": "suffolk",
      "instrument_type": "MORTGAGE",
      "document_date": "2024-01-15",
      "document_path": "/path/to/doc.pdf",
      "metadata": {...}
    }
  ]
}
```

## Intelligent Caching System

The pipeline includes a **90-day caching system** to avoid unnecessary scraping:

### How It Works

1. **Before scraping**, the system checks if data already exists for the `loc_id`
2. **If cached data is found**, it checks the `last_updated` timestamp
3. **If data is < 90 days old**, scraping is **skipped automatically**
4. **If data is > 90 days old**, scraping proceeds and updates the record

### Cache Behavior

```bash
# First scrape - data doesn't exist
python -m data_pipeline.cli registry-run --registry suffolk --loc-id "2507000_123"
# ✓ Scrapes data, saves to database

# Second scrape (same day) - cache is fresh
python -m data_pipeline.cli registry-run --registry suffolk --loc-id "2507000_123"
# ✓ SKIPPED - "Cache HIT: Data is 0 days old (fresh)"

# Third scrape (100 days later) - cache is stale
python -m data_pipeline.cli registry-run --registry suffolk --loc-id "2507000_123"
# ✓ Scrapes data, updates existing record
```

### Override Caching

```bash
# Force refresh even if cache is fresh
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --loc-id "2507000_123" \
  --force-refresh

# Use custom cache age (e.g., 30 days for high-priority properties)
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --loc-id "2507000_123" \
  --max-cache-age 30
```

### Benefits

✅ **Reduces scraping load** - No unnecessary requests to registries
✅ **Faster performance** - Instant return if cache is fresh
✅ **Respectful** - Less burden on public registry websites
✅ **Cost-effective** - Minimal server resources used
✅ **Configurable** - Adjust cache age per use case

### Cache Management

The cache is automatically managed via Django's `last_updated` field:

- **Automatic timestamps** - Updated on every save
- **Query optimization** - Indexed by `loc_id` and `town_id`
- **No manual cleanup** - Old data naturally refreshes when scraped

### Recommended Cache Ages

| Data Type | Recommended Age | Rationale |
|-----------|----------------|-----------|
| Mortgage data | 90 days | Mortgages rarely change |
| Tax assessments | 365 days | Annual assessment cycle |
| Foreclosures | 30 days | Fast-moving situations |
| Owner info | 180 days | Moderate change rate |

## Development

### Adding a New Registry

1. Get the form field names from browser dev tools
2. Add entry to `config/sources.json`:
```json
{
  "id": "new_registry",
  "name": "New County Registry",
  "adapter": "masslandrecords",
  "base_url": "https://www.masslandrecords.com/NewCounty",
  "throttle_rps": 0.4,
  "instrument_types": ["MORTGAGE", "LIS PENDENS"],
  "search_modes": {
    "owner": {
      "form_path": "DocumentSearch.aspx",
      "fields": {
        "owner_last": "SearchFormEx1$ACSTextBox_LastName1",
        "owner_first": "SearchFormEx1$ACSTextBox_FirstName1"
      },
      "submit_field": "SearchFormEx1$btnSearch"
    }
  }
}
```

### Adding a New Assessor Platform

1. Create adapter in `sources/assessors/`
2. Inherit from `BaseAssessorSource`
3. Implement `fetch()` method
4. Add to `jobs/assessor_job.py` adapter registry

## Testing

```bash
# Test with dry-run (no database writes)
python -m data_pipeline.cli registry-run \
  --registry suffolk \
  --owner "Smith" \
  --dry-run

# Test parser directly
python -c "
from data_pipeline.parsers import mortgage_parser
result = mortgage_parser.parse_mortgage_document('path/to/mortgage.pdf')
print(result)
"
```

## Performance

- **Throttling**: 0.3-0.4 requests/second per registry (configurable)
- **Document download**: ~5-10 seconds per document
- **PDF parsing**: <1 second for text PDFs
- **OCR**: 10-30 seconds for scanned TIFFs

## Cost Savings

**Before (Attom API)**: ~$500-2000+/month
**After (Free Scraping)**: ~$65-130/month (server + storage)

**ROI**: 80-95% cost reduction!

## Troubleshooting

### "Module 'bs4' not found"
```bash
pip install beautifulsoup4
```

### "Django not available"
Make sure you're running from the `leadcrm` directory:
```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM/leadcrm
python -m data_pipeline.cli ...
```

### "Failed to locate ASP.NET hidden fields"
This is normal for first runs - the form structure may need adjustment. Check the registry's actual HTML with browser dev tools and update field names in `sources.json`.

### OCR not working
Install Tesseract:
```bash
# macOS
brew install tesseract

# Ubuntu/Debian
sudo apt-get install tesseract-ocr

# Then reinstall Python packages
pip install pytesseract pdf2image
```

## LLC Owner Integration with GIS

When you get a parcel from GIS with an LLC owner, here's the workflow:

```python
from data_pipeline.jobs.corporate_job import CorporateJob

# GIS returns owner name like "ABC REALTY LLC"
gis_owner_name = "ABC REALTY LLC"

# Look up the LLC to get actual owner
corporate_config = {
    "id": "ma_secretary",
    "name": "Massachusetts Secretary of Commonwealth",
    "adapter": "ma_secretary",
}
job = CorporateJob(corporate_config)
result = job.run(entity_name=gis_owner_name)

if result and result.get('principal_name'):
    # Now you have the actual owner!
    actual_owner = result['principal_name']
    owner_title = result.get('principal_title', '')
    business_phone = result.get('business_phone', '')

    print(f"LLC: {gis_owner_name}")
    print(f"Actual Owner: {actual_owner} ({owner_title})")
    print(f"Phone: {business_phone}")
```

This automatically:
- ✅ Searches MA Secretary of Commonwealth
- ✅ Extracts the managing member or president name
- ✅ Gets business phone number
- ✅ Caches for 180 days to avoid redundant lookups
- ✅ Stores in CorporateEntity model for reuse

## Next Steps

1. ✅ All 20 registries configured
2. ✅ Document download implemented
3. ✅ Mortgage parser built
4. ✅ Database integration complete
5. ✅ LLC owner scraping implemented
6. 🔄 Test with real properties and LLCs
7. 🔄 Integrate corporate lookup into GIS parcel ingestion
8. ⏳ Build Vision assessor adapter (175 municipalities)
9. ⏳ Add Celery for scheduled jobs
10. ⏳ Create propensity score calculator
11. ⏳ Build monitoring dashboard

## Support

For issues or questions, see:
- Architecture docs: `docs/scraper_architecture.md`
- Source matrix: `docs/ma_source_matrix.md`
- Free-first pipeline: `docs/free_first_pipeline.md`
