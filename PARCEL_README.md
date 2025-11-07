# Parcel Workflow Documentation

This directory contains comprehensive documentation about the parcel search and property management workflow in the Lead CRM application.

## Documentation Files

### 1. **PARCEL_QUICK_START.md** (387 lines)
**Start here if you're new to the system or need to get it working on Railway.**

Covers:
- What the parcel workflow does
- Main workflow path (diagram)
- Critical dependencies
- Railway deployment steps
- Common issues and fixes
- Local testing guide
- Performance baseline

**Read this if**: You need to deploy to production, fix Railway issues, or understand the system quickly.

---

### 2. **PARCEL_WORKFLOW.md** (376 lines)
**Comprehensive technical deep-dive into the parcel workflow architecture.**

Covers:
- All parcel workflow features (search, detail, saved lists)
- External API integrations (ATTOM, CourtListener, MassGIS)
- Background task processing (ThreadPoolExecutor)
- Data models and caching strategy
- Static files and S3 storage
- Geographic data API endpoints
- Complete environment variable reference
- Railway deployment issues with solutions
- Potential failure points and risk assessment

**Read this if**: You need detailed technical understanding, debugging, or planning improvements.

---

### 3. **PARCEL_TROUBLESHOOTING.md** (415 lines)
**Diagnostic guide for common problems and solutions.**

Covers:
- 7 major issue categories with diagnosis steps:
  1. Parcel search returns 404
  2. ATTOM data not showing
  3. Liens/legal actions missing
  4. PDF mailer generation failures
  5. Static files not loading
  6. Database errors on Railway
  7. Railway deployment failures
- Environment variable checklist (copy-paste ready)
- Performance optimization tips
- Useful command reference

**Read this if**: Something is broken and you need to fix it quickly.

---

## Quick Navigation

### I want to...

**Deploy to Railway**
→ Start with PARCEL_QUICK_START.md → Section "Railway Deployment - CRITICAL SETUP"

**Debug parcel search not working**
→ Go to PARCEL_TROUBLESHOOTING.md → Section "1. Parcel Search Returns 404"

**Understand the system architecture**
→ Read PARCEL_WORKFLOW.md → Sections 1-2 (Features & APIs)

**Fix ATTOM data not showing**
→ Go to PARCEL_TROUBLESHOOTING.md → Section "2. ATTOM Data Not Showing"

**Configure environment variables**
→ PARCEL_TROUBLESHOOTING.md → "Environment Variable Checklist"
OR PARCEL_WORKFLOW.md → Section 7 (Full reference)

**Optimize performance**
→ PARCEL_QUICK_START.md → "Performance Baseline"
OR PARCEL_TROUBLESHOOTING.md → "Performance Optimization Checklist"

**Test locally before production**
→ PARCEL_QUICK_START.md → "Testing Locally Before Production"

**Set up Railway from scratch**
→ PARCEL_QUICK_START.md → Entire document (comprehensive walkthrough)

---

## Key Facts About The Parcel Workflow

**Core Functionality**:
- Searches Massachusetts property parcels from MassGIS shapefiles
- Shows comprehensive property data including mortgages, foreclosures, liens
- Generates personalized direct mail mailers (PDFs)
- Tracks leads and contacts
- Displays properties on interactive maps

**Critical Components** (must work):
1. **MassGIS Data** - Local GIS shapefiles (stored on S3 for production)
2. **PostgreSQL Database** - Stores cache, searches, liens, mailers
3. **ATTOM API** - Mortgage and foreclosure data

**Enhanced Components** (nice to have):
- CourtListener API - Court records and liens
- OpenAI API - AI-generated mailers
- Skip Trace - Contact information
- Stripe - Payment processing

**Infrastructure**:
- Django 5.2 web framework
- Railway (PaaS hosting)
- PostgreSQL (Railway provides)
- AWS S3 (static files + GIS storage)
- ThreadPoolExecutor (background tasks)

---

## File Structure Reference

```
leadcrm/
├── leads/
│   ├── models.py                 # Data models (10 models)
│   ├── views.py                  # View functions (5,600+ lines)
│   ├── services.py               # Business logic (2,700+ lines)
│   ├── attom_service.py          # ATTOM API integration (675 lines)
│   ├── background_lien_search.py # CourtListener + background tasks (437 lines)
│   ├── urls.py                   # URL routing (50+ endpoints)
│   ├── forms.py                  # Django forms
│   ├── management/commands/
│   │   ├── cleanup_parcel_cache.py
│   │   ├── refresh_massgis.py
│   │   └── ... (other commands)
│   └── templates/leads/
│       ├── parcel_search.html
│       ├── parcel_detail.html
│       ├── saved_parcel_lists.html
│       └── ... (other templates)
├── leadcrm/
│   ├── settings.py               # Django configuration (384+ lines)
│   ├── storage_backends.py       # S3 storage setup
│   ├── urls.py                   # Project URL routing
│   └── wsgi.py                   # WSGI application
├── accounts/
│   ├── models.py                 # User profiles
│   └── views.py                  # Auth views
└── manage.py                      # Django management script
```

---

## Environment Variables Summary

### Required (Application Won't Start)
```env
DJANGO_SECRET_KEY=your-50-char-key
DATABASE_URL=postgresql://user:pass@host/db
```

### Required for Parcel Search
```env
MASSGIS_LIVE_FETCH_ENABLED=True  # Download shapefiles
USE_S3_FOR_GIS=True              # Store on S3 (production)
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_STORAGE_BUCKET_NAME=leadcrm-data
```

### Required for ATTOM Data
```env
ATTOM_API_KEY=your-api-key
ATTOM_CACHE_MAX_AGE_DAYS=60
```

### Optional but Recommended
```env
COURTLISTENER_API_KEY=xxx        # Court records
OPENAI_API_KEY=xxx               # AI mailers
STRIPE_SECRET_KEY=sk_xxx         # Payments
```

---

## Common Commands

```bash
cd /Volumes/OWC\ Envoy\ Ultra/Websites/lead_CRM/leadcrm

# Database
python manage.py migrate           # Run migrations
python manage.py makemigrations    # Create migrations
python manage.py dbshell           # Open psql

# GIS Data
python manage.py refresh_massgis --town 35           # Download one town
python manage.py refresh_massgis --all               # Download all towns
python manage.py cleanup_parcel_cache --dry-run      # Show expired cache

# Testing
python manage.py test_attom        # Verify ATTOM API
python manage.py check --deploy    # Production checks
python manage.py shell             # Python REPL

# Static Files
python manage.py collectstatic --noinput  # Upload to S3

# Users
python manage.py createsuperuser   # Create admin
python manage.py changepassword user  # Reset password
```

---

## Quick Checklist For New Deployment

- [ ] Read PARCEL_QUICK_START.md completely
- [ ] Set all required environment variables in Railway
- [ ] Deploy (`git push`)
- [ ] SSH into Railway: `railway shell`
- [ ] Run migrations: `cd leadcrm && python manage.py migrate`
- [ ] Create superuser: `python manage.py createsuperuser`
- [ ] Populate GIS data: `python manage.py refresh_massgis --all`
- [ ] Test parcel search on production
- [ ] Review Railway logs for errors
- [ ] Monitor ATTOM API quota
- [ ] Keep this documentation updated as you make changes

---

## Support

If something is broken:
1. Check PARCEL_TROUBLESHOOTING.md first
2. Review environment variables (copy-paste checklist in that file)
3. Check Railway logs: `railway logs --limit 100`
4. Test locally before making changes to production
5. Review Django/ATTOM/CourtListener docs for API-specific issues

---

## Last Updated

Generated: November 7, 2025
Coverage: Parcel workflow as implemented in current codebase
Tested On: Django 5.2, Python 3.13, Railway deployment

