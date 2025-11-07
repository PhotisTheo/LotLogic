# Lead CRM Parcel Workflow Analysis

## Executive Summary

The parcel workflow in this Django CRM is a comprehensive real estate lead management system deployed on Railway. It integrates multiple external APIs, local GIS data processing, background task processing, and file storage. The workflow is currently broken on Railway due to **missing GIS data and environment configuration issues**.

---

## 1. PARCEL WORKFLOW FEATURES

### Core Workflow Components

#### 1.1 Parcel Search (Main Entry Point)
- **View Function**: `parcel_search_home` (views.py:2114)
- **URL**: `/` (parcel_search)
- **Purpose**: Full-text/filter search across MassGIS parcel database
- **Key Features**:
  - Town selection + advanced filtering by property category, address, style, equity, price ranges
  - Proximity radius search (geocoding + distance calculation)
  - Absentee owner detection
  - Pagination/result limiting
  - CSV export capability
  - Results displayed on interactive Leaflet map

#### 1.2 Parcel Detail View
- **View Function**: `parcel_search_detail` (views.py:2297)
- **URL**: `/search/parcel/<town_id>/<loc_id>/`
- **Purpose**: Display comprehensive property information with multiple data sources

**Data Displayed**:
- Property overview (ID, category, bedrooms, bathrooms, style, units)
- Location (address, city, ZIP code)
- Valuation (building/land value, estimated equity, ROI)
- Sale history (date, price, registry information)
- Owner information (names, addresses, mailing info)
- Multiple tenant units (if applicable)
- Liens and legal actions (from CourtListener integration)
- ATTOM mortgage/foreclosure data

#### 1.3 Saved Parcel Lists
- **Models**: `SavedParcelList`, `SkipTraceRecord`
- **Purpose**: Create curated lists of parcels for bulk operations
- **Features**:
  - Save search criteria + LOC_IDs for later use
  - Archive/restore functionality
  - Bulk skip trace operations
  - Export to CSV
  - Mailer generation for entire lists

---

## 2. EXTERNAL API INTEGRATIONS

### 2.1 ATTOM API (Mortgage/Foreclosure Data)
**File**: `attom_service.py` (675 lines)

**Endpoints Used**:
- `/property/expandedprofile` - Comprehensive property data
- Alternative fallbacks: `/property/detail`, `/assessment/detail`

**Data Retrieved**:
- Mortgage information (loan amount, interest rate, lender, term years)
- Foreclosure data (recording date, auction date, stage, judgment amounts)
- Tax information (assessed value, annual amount, delinquent year)
- Propensity-to-default scores (0-100 scale)
- Property category and use codes

**Caching Strategy**:
- Cross-user cache (all users share ATTOM data for same parcel)
- Default TTL: 60 days (configured via `ATTOM_CACHE_MAX_AGE_DAYS`)
- Stored in `AttomData` model (31 fields per record)
- Handles unit-level properties with special cache keys

**Configuration**:
```env
ATTOM_API_KEY=your-api-key
ATTOM_CACHE_MAX_AGE_DAYS=60
ATTOM_DEFAULT_STATE=MA
```

**Potential Issue**: If API_KEY missing, silent failure - ATTOM data simply won't load

### 2.2 CourtListener API (Liens & Legal Actions)
**File**: `background_lien_search.py` (437 lines)

**Background Processing**:
- Runs in background ThreadPoolExecutor (10 workers max)
- Triggered when viewing parcel details
- Searches for: foreclosures, evictions, civil judgments, bankruptcies
- Rate limit: 5,000 queries/hour (authenticated)

**Data Stored**:
- `LegalAction` model: case numbers, filing dates, judgments, court info
- `LienRecord` model: lien types, amounts, recording dates, releases
- `LienSearchAttempt` model: tracks search history to avoid duplicate API calls

**Cache Strategy**:
- 90-day cache (per user, per parcel)
- Prevents re-searching same parcel within 90 days
- Shared LegalActions from CourtListener have cross-user visibility

**Configuration**:
```env
COURTLISTENER_API_KEY=your-api-key
LIEN_SEARCH_AUTO_THRESHOLD=1000
```

### 2.3 MassGIS Shapefile Data (Critical Infrastructure)
**File**: `services.py` (2,700+ lines)

**Data Source**:
- State of Massachusetts GIS parcel boundaries
- Hosted at: `https://download.massgis.digital.mass.gov/shapefiles/l3parcels/`
- Files: ZIP archives containing shapefiles per town
- Coverage: All Massachusetts towns + Boston special case

**Caching**:
- `MassGISParcelCache` model: parsed parcel data (cross-user)
- TTL: 90 days
- Includes S3 backup option via `USE_S3_FOR_GIS=True`

**Critical Configuration**:
```env
USE_S3_FOR_GIS=True
AWS_STORAGE_BUCKET_NAME=your-bucket
AWS_S3_REGION_NAME=us-east-1
MASSGIS_LIVE_FETCH_ENABLED=True
MASSGIS_DIRECTORY_TIMEOUT=10
```

---

## 3. BACKGROUND TASK PROCESSING

### ThreadPoolExecutor for Lien Searches
**File**: `background_lien_search.py`

**Architecture**:
- Global `ThreadPoolExecutor` with 10 max workers
- Lazy initialization on first use
- Auto-restart on Django reload

**Potential Issue on Railway**: 
- Background threads may be terminated during deployment
- Searches in-progress will be lost
- No persistent task queue (Celery not configured)

### Management Commands
1. **cleanup_parcel_cache**: Remove cache entries older than 90 days
   ```bash
   python manage.py cleanup_parcel_cache [--dry-run] [--days 90]
   ```

2. **refresh_massgis**: Download fresh shapefile datasets
   ```bash
   python manage.py refresh_massgis --town 35 --force
   python manage.py refresh_massgis --all  # All towns
   ```

3. **backfill_skiptrace_records**: Populate missing skip trace data
4. **test_attom**: Verify ATTOM API connectivity
5. **test_propensity**: Test default scoring

---

## 4. CRITICAL MODELS

| Model | Purpose | Key Fields | Cache |
|-------|---------|-----------|-------|
| `SavedParcelList` | Curated parcel sets | town_id, loc_ids (JSON), criteria (JSON) | None |
| `AttomData` | Mortgage/foreclosure | 31 fields (mortgage, tax, foreclosure) | Cross-user, 60 days |
| `MassGISParcelCache` | Parcel data cache | parcel_data (JSON) | Cross-user, 90 days |
| `LienRecord` | Lien information | type, amount, recording_date, source | Per-user |
| `LegalAction` | Court cases | action_type, case_number, court, filing_date | Shared if CourtListener |
| `LienSearchAttempt` | Search tracking | searched_at, found_liens, found_legal_actions | Per-parcel |

---

## 5. STATIC FILES & MEDIA HANDLING

### S3 Storage Configuration
**Files**: `storage_backends.py`, `settings.py` (lines 344-383)

**Architecture**:
```
S3 Bucket Structure:
├── static/              # CSS, JavaScript, images
├── media/               # User uploads, generated mailers
├── agent_photos/        # Agent profile images
└── mailers/             # Generated PDF mailers
```

**Configuration**:
```env
USE_S3_FOR_GIS=True
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_STORAGE_BUCKET_NAME=your-bucket
AWS_S3_REGION_NAME=us-east-1
AWS_S3_CUSTOM_DOMAIN=your-bucket.s3.amazonaws.com
```

---

## 6. ENVIRONMENT VARIABLES (CRITICAL FOR RAILWAY)

### Required
```env
DJANGO_SECRET_KEY=...           # Django secret key (must be set)
DATABASE_URL=postgresql://...   # PostgreSQL connection string
ATTOM_API_KEY=...              # Property data API
COURTLISTENER_API_KEY=...      # Court records API
```

### AWS S3 (Critical for GIS Data)
```env
USE_S3_FOR_GIS=True
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_STORAGE_BUCKET_NAME=leadcrm-data
AWS_S3_REGION_NAME=us-east-1
AWS_S3_CUSTOM_DOMAIN=leadcrm-data.s3.amazonaws.com
```

### GIS Data
```env
MASSGIS_LIVE_FETCH_ENABLED=True
MASSGIS_DIRECTORY_TIMEOUT=10
ATTOM_CACHE_MAX_AGE_DAYS=60
```

### Mailer Generation
```env
MAILER_CONTACT_PHONE=555-5555
MAILER_TEXT_KEYWORD=HOME
MAILER_AGENT_NAME=Your Name
MAILER_AI_ENABLED=1
MAILER_OPENAI_MODEL=gpt-4o-mini
OPENAI_API_KEY=...
```

### Stripe (Optional)
```env
STRIPE_PUBLISHABLE_KEY=pk_...
STRIPE_SECRET_KEY=sk_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRICE_INDIVIDUAL_STANDARD=price_...
STRIPE_PRICE_TEAM_STANDARD=price_...
STRIPE_PRICE_TEAM_PLUS=price_...
```

---

## 7. RAILWAY DEPLOYMENT ISSUES (CRITICAL)

### Problem 1: GIS Data Not Persisting
**Issue**: MassGIS shapefiles downloaded to `gisdata/` are lost on Railway redeployment
**Root Cause**: Railway uses ephemeral filesystems; `/gisdata` is not backed by S3
**Solution**: 
- Enable `USE_S3_FOR_GIS=True` in Railway environment variables
- Configure AWS S3 bucket with sufficient space (100MB+ recommended)
- Shapefiles cached in S3 with 90-day TTL

**Current Status**: Not configured on Railway (likely causing 404s on parcel searches)

### Problem 2: Background Lien Searches Interrupted
**Issue**: ThreadPoolExecutor threads killed during deployment
**Root Cause**: Railway restarts dyno during new deployments
**Solution**: 
- Switch to persistent task queue (Celery + Redis), or
- Accept loss of in-progress searches (low impact since cached 90 days)

**Workaround**: Schedule `refresh_massgis` + `cleanup_parcel_cache` via Railway cron job

### Problem 3: GIS Directory Timeout
**Issue**: MassGIS directory listing times out on slow Railway networks
**Solution**: Increase timeout or disable live fetch
```env
MASSGIS_DIRECTORY_TIMEOUT=30
MASSGIS_LIVE_FETCH_ENABLED=False
```

---

## 8. POTENTIAL FAILURE POINTS

### High Risk (Breaks Core Functionality)
1. **Missing DATABASE_URL** → Cannot start application
2. **Missing DJANGO_SECRET_KEY** → Cannot start application
3. **Missing GIS data on S3** → Parcel searches return 404
4. **Missing ATTOM_API_KEY** → ATTOM data silent fail
5. **Missing COURTLISTENER_API_KEY** → Lien searches fail silently

### Medium Risk
6. **S3 permissions insufficient** → Cannot write GIS cache or static files
7. **Database connection timeout** → Slow Railway network
8. **Thread pool shutdown during deployment** → In-flight lien searches lost
9. **Shapefile corruption in S3** → MassGIS queries fail

### Low Risk
10. **OPENAI_API_KEY missing** → Mailer AI generation skipped
11. **STRIPE keys missing** → Payment flows blocked
12. **Zillow timeout** → Property photos don't load

---

## 9. KEY SOURCE FILES

| File | Lines | Purpose |
|------|-------|---------|
| `/leads/models.py` | 597 | All data models including caches |
| `/leads/views.py` | 5,600+ | View functions for parcel search/detail |
| `/leads/services.py` | 2,700+ | MassGIS integration + parcel search logic |
| `/leads/attom_service.py` | 675 | ATTOM API integration + caching |
| `/leads/background_lien_search.py` | 437 | CourtListener background searches |
| `/leads/urls.py` | 165 | URL routing for all endpoints |
| `/leadcrm/settings.py` | 384+ | Django configuration |
| `/leadcrm/storage_backends.py` | 17 | S3 storage configuration |

---

## 10. TESTING COMMANDS

```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM/leadcrm

# Test ATTOM API connectivity
python manage.py test_attom

# Test MassGIS connectivity
python manage.py refresh_massgis --town 35 --skip-remote-check

# Clean cache (dry run)
python manage.py cleanup_parcel_cache --dry-run

# Verify skip trace
python manage.py backfill_skiptrace_records

# Test production settings
python manage.py check --deploy
```

---

## 11. DEPLOYMENT CHECKLIST FOR RAILWAY

- [ ] Set `DJANGO_SECRET_KEY` (use 50-char random string)
- [ ] Set `DATABASE_URL` (Railway provides PostgreSQL)
- [ ] Set `ATTOM_API_KEY` (from ATTOM account)
- [ ] Set `COURTLISTENER_API_KEY` (from CourtListener)
- [ ] Create AWS S3 bucket (leadcrm-data recommended)
- [ ] Set AWS credentials + `AWS_STORAGE_BUCKET_NAME`
- [ ] Enable `USE_S3_FOR_GIS=True`
- [ ] Pre-populate S3 with initial shapefiles via `refresh_massgis`
- [ ] Set `MAILER_*` environment variables
- [ ] Set `STRIPE_*` keys if payments needed
- [ ] Run migrations: `python manage.py migrate`
- [ ] Create superuser: `python manage.py createsuperuser`
- [ ] Test parcel search on production

---

## 12. QUICK REFERENCE TABLE

| Component | Status | Config Needed | Risk Level | Files |
|-----------|--------|---------------|-----------|-------|
| Parcel Search | Core | town_id, MassGIS data | High | views.py:2114, services.py:1945 |
| Parcel Detail | Core | ATTOM_API_KEY | Medium | views.py:2297, attom_service.py |
| ATTOM API | Enhanced | ATTOM_API_KEY, S3 cache | Medium | attom_service.py |
| CourtListener | Enhanced | COURTLISTENER_API_KEY | Medium | background_lien_search.py |
| MassGIS Data | Critical | S3 bucket, `USE_S3_FOR_GIS=True` | High | services.py:2700+ |
| Static Files | Required | AWS S3 | Medium | storage_backends.py |
| Mailer Generation | Feature | OPENAI_API_KEY | Low | mailers.py |
| Skip Trace | Feature | BATCHDATA_API_KEY | Low | services.py |
| Database | Critical | DATABASE_URL | High | settings.py |
| Background Tasks | Enhancement | None (basic ThreadPoolExecutor) | Low | background_lien_search.py |
