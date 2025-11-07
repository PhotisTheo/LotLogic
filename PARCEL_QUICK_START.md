# Parcel Workflow - Quick Start Guide

## What Is The Parcel Workflow?

A comprehensive real estate property search and management system that:
1. Searches Massachusetts properties using MassGIS shapefile data
2. Aggregates mortgage/foreclosure data from ATTOM API
3. Searches court records (liens, evictions, bankruptcies) from CourtListener
4. Displays comprehensive property analysis on interactive maps
5. Generates personalized direct mail marketing letters (PDF)
6. Tracks property leads and contact information

**Key Components**:
- MassGIS Parcel Database (critical) - Local GIS shapefiles (1000s per town)
- ATTOM API (enhanced) - Mortgage, foreclosure, tax data
- CourtListener API (enhanced) - Court records, liens, legal actions
- Mailer System - Generate PDFs with property-specific content
- Background Tasks - Search liens without blocking user requests

---

## Main Workflow Path

```
User Enters:
  1. Town ID
  2. Filters (price, category, equity, address, style, etc.)
         |
         v
  Search in MassGIS Shapefiles
         |
         v
  Display Results on Map (Leaflet)
         |
         v
  User Clicks Property
         |
         v
  View Details:
    - Property info from MassGIS
    - Mortgage/foreclosure from ATTOM (cached)
    - Liens/legal actions from CourtListener (async background)
    - Owner contact from Skip Trace
         |
         v
  Optional: Generate Mailer, Schedule Call, Save List
```

---

## Files To Know

**Core Models** (Data Structures)
- `/leadcrm/leads/models.py` - 10 models including SavedParcelList, AttomData, LienRecord

**Views** (What User Sees)
- `/leadcrm/leads/views.py` - 150+ functions, ~5600 lines total
  - `parcel_search_home()` - Main search page
  - `parcel_search_detail()` - Parcel detail page
  - `parcel_geometry()` - GeoJSON for map polygons
  - `parcel_generate_mailer()` - PDF generation

**Services** (Business Logic)
- `/leadcrm/leads/services.py` - 2,700+ lines
  - `search_massgis_parcels()` - Shapefile search
  - `get_massgis_parcel_detail()` - Load one property
  - `geocode_address()` - Address to lat/lng
  
**API Integrations**
- `/leadcrm/leads/attom_service.py` - ATTOM API + caching (675 lines)
- `/leadcrm/leads/background_lien_search.py` - CourtListener + background tasks (437 lines)
- `/leadcrm/leads/lien_legal_service.py` - Court record searches

**URLs** (Endpoints)
- `/leadcrm/leads/urls.py` - 50+ routes for parcel, mailer, lien endpoints

**Configuration**
- `/leadcrm/leadcrm/settings.py` - Django settings (384+ lines)
- `/leadcrm/leadcrm/storage_backends.py` - S3 configuration
- `.env.example` - Environment variables reference

---

## Critical Dependencies

### Data
1. **MassGIS Shapefiles** - REQUIRED FOR CORE FUNCTIONALITY
   - Downloaded automatically when needed
   - Stored in `gisdata/` (local) or S3 (production)
   - Cache TTL: 90 days

2. **ATTOM API** - REQUIRED FOR MORTGAGE DATA
   - API Key: `ATTOM_API_KEY`
   - Cache TTL: 60 days
   - Cost: Per-query charge (usually <$1 per lookup)

3. **CourtListener API** - OPTIONAL FOR LIENS/COURT DATA
   - API Key: `COURTLISTENER_API_KEY`
   - Rate limit: 5,000 queries/hour
   - Searches run in background (async)

4. **OpenAI API** - OPTIONAL FOR AI MAILER GENERATION
   - API Key: `OPENAI_API_KEY`
   - Model: `gpt-4o-mini` (default)
   - Used to write personalized mailer text

### Infrastructure
1. **PostgreSQL Database** - REQUIRED (Railway provides)
   - Stores: cache, searches, liens, mailers, user data
   - `DATABASE_URL` environment variable

2. **AWS S3** - REQUIRED FOR PRODUCTION
   - Stores: static files, media, GIS shapefiles
   - Alternative: Local filesystem (development only)
   - Configuration: `AWS_*` environment variables

3. **Thread Pool** - Bundled with Django
   - Background lien searches (10 max workers)
   - No external service needed

---

## Railway Deployment - CRITICAL SETUP

Your current Railway deployment is broken due to **missing GIS data**. Fix it:

### Step 1: Set Railway Environment Variables
Go to Railway dashboard > Your Project > Environment Variables

**REQUIRED**:
```
DJANGO_SECRET_KEY=<50-char-random-string>
DATABASE_URL=<Railway-provides-this>
ATTOM_API_KEY=<from-ATTOM-account>
COURTLISTENER_API_KEY=<from-CourtListener-account>
```

**FOR PRODUCTION (GIS DATA)**:
```
USE_S3_FOR_GIS=True
AWS_ACCESS_KEY_ID=<your-AWS-key>
AWS_SECRET_ACCESS_KEY=<your-AWS-secret>
AWS_STORAGE_BUCKET_NAME=leadcrm-data
AWS_S3_REGION_NAME=us-east-1
AWS_S3_CUSTOM_DOMAIN=leadcrm-data.s3.amazonaws.com
```

**OPTIONAL**:
```
MASSGIS_DIRECTORY_TIMEOUT=30
MAILER_AGENT_NAME=Your Name
OPENAI_API_KEY=<for-AI-mailers>
```

### Step 2: Deploy with GIS Data
After setting environment variables, push to Railway:

```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM
git add .
git commit -m "Configure Railway environment for parcel workflow"
git push  # Railway auto-deploys on push
```

### Step 3: Populate GIS Shapefiles on S3

Once deployed, SSH into Railway and run:
```bash
railway shell
cd leadcrm
python manage.py refresh_massgis --all  # Downloads all towns (~30 min)
```

Or pre-populate common towns:
```bash
python manage.py refresh_massgis --town 35  # Boston
python manage.py refresh_massgis --town 27  # Essex County
python manage.py refresh_massgis --town 21  # Middlesex
```

---

## Typical Issues on Railway

### Problem: Parcel searches return "No results" or 404

**Root Cause**: GIS data not on S3 (Railway filesystem is ephemeral)

**Fix**:
1. Set `USE_S3_FOR_GIS=True` in Railway environment
2. Set all `AWS_*` variables
3. Run `python manage.py refresh_massgis --all` once

### Problem: ATTOM data fields blank (mortgages, foreclosures, etc.)

**Root Cause**: `ATTOM_API_KEY` not set or API disabled

**Fix**: Set `ATTOM_API_KEY` in Railway environment

### Problem: Static files (CSS, images) not loading

**Root Cause**: `collectstatic` not run or S3 not configured

**Fix**: Already handled by Railway deploy script, but verify:
- AWS S3 credentials are set
- `AWS_STORAGE_BUCKET_NAME` is set

### Problem: Database errors "relation does not exist"

**Root Cause**: Migrations not run

**Fix**:
```bash
railway shell
cd leadcrm
python manage.py migrate
```

---

## Testing Locally Before Production

```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM

# 1. Create virtual environment
python -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r leadcrm/requirements.txt

# 3. Create .env file
cat > leadcrm/.env << 'ENVEOF'
DJANGO_SECRET_KEY=dev-secret-key-12345678901234567890
DEBUG=True
DATABASE_URL=sqlite:///db.sqlite3
ATTOM_API_KEY=your-test-key
COURTLISTENER_API_KEY=your-test-key
MASSGIS_LIVE_FETCH_ENABLED=True
ENVEOF

# 4. Setup database
cd leadcrm
python manage.py migrate

# 5. Create superuser
python manage.py createsuperuser

# 6. Download sample GIS data
python manage.py refresh_massgis --town 35 --force  # Boston

# 7. Run development server
python manage.py runserver

# 8. Navigate to http://localhost:8000
#    - Test parcel search for town 35 (Boston)
#    - View parcel details
#    - Check ATTOM data loads
#    - Check if lien search runs in background
```

---

## Performance Baseline

**Expected Response Times** (from logs):
- Parcel search (10-100 results): 100-500ms
- Parcel detail page: 50-100ms + ATTOM (~2s) + background lien search (1-3s)
- Map load (50-500 parcels): 500-2000ms
- PDF mailer generation: 2-5 seconds

**If Slow**:
1. Enable S3 for GIS data (faster than local disk)
2. Verify database connection speed (Railway DB may be slow)
3. Increase Railway RAM if needed (more cost)
4. Pre-warm ATTOM cache (run searches in bulk first)

---

## Key Metrics To Monitor

In Railway logs:
```bash
railway logs

# Look for:
# - ATTOM API calls (slow if >5s)
# - MassGIS shapefile loads (should be <1s from S3)
# - Database queries (should be <100ms)
# - Background lien searches (happens async)
```

In Database:
```bash
railway shell
cd leadcrm && python manage.py shell
>>> from leads.models import AttomData, MassGISParcelCache, LienSearchAttempt
>>> AttomData.objects.count()  # ATTOM cache size
>>> MassGISParcelCache.objects.count()  # GIS cache size
>>> LienSearchAttempt.objects.filter(found_legal_actions=True).count()  # Searches with results
```

---

## Maintenance Tasks

### Weekly
- Monitor Railway logs for errors
- Check ATTOM API quota usage

### Monthly
- Run cache cleanup: `python manage.py cleanup_parcel_cache`
- Refresh stale town datasets: `python manage.py refresh_massgis --stale-days 30`

### Quarterly
- Update pip packages: `pip install -r requirements.txt --upgrade`
- Review API costs (ATTOM, CourtListener, OpenAI)
- Back up Railway PostgreSQL

---

## Architecture Diagram

```
User Browser
    |
    v
Django (Railway) - 4 workers
    |
    +---> MassGIS Shapefiles (S3 Cache)
    |         |
    |         v
    |     ZIPpy Library
    |         |
    |         v
    |     Shapefile Parser
    |
    +---> ATTOM API (HTTP)
    |         |
    |         v
    |     AttomData Cache (PostgreSQL)
    |
    +---> CourtListener API (HTTP)
    |         |
    |         v
    |     Background ThreadPool (10 workers)
    |         |
    |         v
    |     LegalAction + LienRecord (PostgreSQL)
    |
    v
PostgreSQL Database (Railway)
    |
    +---> User Sessions
    +---> Saved Lists
    +---> ATTOM Cache (60 days)
    +---> MassGIS Cache (90 days)
    +---> Liens/Legal Actions
    +---> Generated Mailers

S3 Storage (AWS)
    |
    +---> Static Files (CSS/JS)
    +---> Media (PDFs, photos)
    +---> GIS Shapefiles (ZIPs)
```

---

## Support Resources

- **MassGIS**: https://www.mass.gov/info-details/massgis-data-catalog
- **ATTOM API Docs**: https://dev.attomdataapi.com/docs
- **CourtListener API**: https://courtlistener.com/api/
- **Django Documentation**: https://docs.djangoproject.com/
- **Railway Docs**: https://docs.railway.app/

---

## Next Steps

1. **Immediate**: Set all environment variables in Railway dashboard
2. **Within 1 hour**: Deploy with `git push` and test parcel search
3. **Within 24 hours**: Run `refresh_massgis --all` to populate S3
4. **Ongoing**: Monitor logs and adjust settings as needed

